import json
import logging
import random
import signal
import time
from os import environ

import docker
import requests

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s.%(msecs)03d;%(levelname)s;%(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
LOG = logging.getLogger(__file__)

ID_FIELD = "id"
NAME_FIELD = "name"
INSTANCE_NAME_LABEL = environ.get("INSTANCE_NAME_LABEL", "")

MACHINE_NAME = environ.get("MACHINE_NAME", "virtuocrat")

CONSOLE_LOGS_TARGET = "CONSOLE_LOGS_TARGET"
LOGS_TARGET_URL = environ.get('LOGS_TARGET_URL', CONSOLE_LOGS_TARGET)
LOGS_TARGET_HEADERS = environ.get("LOGS_TARGET_HEADERS", {})
if LOGS_TARGET_HEADERS:
    LOGS_TARGET_HEADERS = {h.split("=")[0].strip(): h.split("=")[1].strip() for h in LOGS_TARGET_HEADERS.split(",")}

COLLECTION_INTERVAL = int(environ.get("COLLECTION_INTERVAL", 10))
LAST_DATA_READ_AT_FILE_PATH = environ.get("LAST_DATA_READ_AT_FILE",
                                          "/tmp/docker-logs-collector-last-data-read-at.txt")

MAX_LOGS_NOT_SEND_AGO = int(environ.get("MAX_LOGS_NOT_SEND_AGO", 3600))


class DockerContainers:
    """
    Wrapper class for containers states.
    We need previous_containers field to make sure that we collect logs from containers that died after,
    but before the next logs collection.
    """

    def __init__(self, docker_client):
        self.previous_containers = []
        self.client = docker_client

    def get(self):
        def instance_name_from_label(container):
            labels = container['Labels']
            i_name = labels.get(INSTANCE_NAME_LABEL, None)
            if not i_name:
                i_name = container["Names"][0].replace("/", "")
                LOG.info(f"INSTANCE_NAME_LABEL is not set, using first name {i_name} as name")
            return i_name

        fetched = [{ID_FIELD: c['Id'], NAME_FIELD: instance_name_from_label(c)}
                   for c in self.client.containers()]

        all_containers = []
        all_containers.extend(fetched)

        for pc in self.previous_containers:
            if pc not in all_containers:
                all_containers.append(pc)

        self.previous_containers = fetched

        return all_containers


class GracefulShutdown:
    stop = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    # Args are needed due to signal handler specification
    def exit_gracefully(self, *args):
        self.stop = True


SHUTDOWN = GracefulShutdown()


def connected_docker_client_retrying():
    def new_client():
        return docker.APIClient(base_url='unix://var/run/docker.sock')

    LOG.info(f"Starting monitoring of machine {MACHINE_NAME}")
    while True:
        try:
            LOG.info("Trying to get client...")
            client = new_client()
            ver = data_object_formatted(client.version())
            LOG.info(f"Client connected, docker ver: {ver}")
            return client
        except Exception:
            retry_interval = random_retry_interval()
            log_exception(f"Problem while connecting to docker client, retrying in {retry_interval}s...")
            time.sleep(retry_interval)


def data_object_formatted(data_object):
    return json.dumps(data_object, indent=2)


def random_retry_interval():
    return round(random.uniform(1, 5), 3)


def log_exception(message):
    LOG.exception(f"{message}")
    print()


def current_timestamp():
    return int(time.time())


def current_timestamp_millis():
    return int(time.time() * 1000)


DOCKER_CONTAINERS = DockerContainers(connected_docker_client_retrying())


# for c in docker_containers.get():
#     print(c)
#     logs = docker_containers.client.logs(c[NAME_FIELD], since=current_timestamp() - 300, until=current_timestamp(),
#                               stream=False).decode(
#         "utf-8")
#     print(logs)


def keep_collecting_and_sending():
    try:
        do_keep_collecting_and_sending()
    except Exception:
        log_exception("Problem while collecting, retrying...")
        keep_collecting_and_sending()


def do_keep_collecting_and_sending():
    default_last_log_check = initial_last_logs_check()
    containers_last_log_checks = {}

    while True:
        if SHUTDOWN.stop:
            LOG.info("Shutdown requested, exiting gracefully")
            break

        LOG.info("Checking containers...")
        default_last_log_check = gather_and_send_logs(containers_last_log_checks=containers_last_log_checks,
                                                      default_last_log_check=default_last_log_check)
        print("...")

        if SHUTDOWN.stop:
            LOG.info("Shutdown requested, exiting gracefully")
            break

        print(f"Sleeping for {COLLECTION_INTERVAL}s")
        print()

        time.sleep(COLLECTION_INTERVAL)


def initial_last_logs_check():
    """
    Restarting script and gathering metadata on its start takes a while, so we go back in time by arbitrary 10s.
    """
    return current_timestamp() - 10


def gather_and_send_logs(containers_last_log_checks, default_last_log_check):
    default_last_log_check = limited_last_logs_check(default_last_log_check)

    now_before_containers_call = current_timestamp()

    running_containers = DOCKER_CONTAINERS.get()

    last_data_read_at = current_timestamp()

    containers_last_log_checks = last_logs_checks_synced_with_running_containers(running_containers,
                                                                                 containers_last_log_checks,
                                                                                 default_last_log_check)

    LOG.info(f"Have {len(running_containers)} running containers, checking their logs...")

    c_logs = containers_logs(running_containers, containers_last_log_checks)

    default_last_log_check = now_before_containers_call

    print()
    LOG.info("Logs checked.")
    print()
    send_logs_if_present(c_logs)

    print()

    update_last_data_read_at_file(last_data_read_at)

    return default_last_log_check


def limited_last_logs_check(last_logs_check):
    max_last_logs_ago = current_timestamp() - MAX_LOGS_NOT_SEND_AGO
    return max(max_last_logs_ago, last_logs_check)


def last_logs_checks_synced_with_running_containers(running_containers, containers_last_log_checks,
                                                    default_last_log_check):
    synced_checks = {}

    for c in running_containers:
        c_id = c[ID_FIELD]
        last_check = containers_last_log_checks.get(c_id)
        if last_check:
            synced_checks[c_id] = last_check
        else:
            synced_checks[c_id] = default_last_log_check

    return synced_checks


def send_logs_if_present(c_logs):
    if c_logs:
        try:
            LOG.info(f"Sending logs of {len(c_logs)} containers...")

            logs_object = {
                'machine': MACHINE_NAME,
                'logs': c_logs
            }

            if LOGS_TARGET_URL == CONSOLE_LOGS_TARGET:
                LOG.info("Console logs target...")
                print(data_object_formatted(logs_object))
                print()
            else:
                send_logs(logs_object)

            LOG.info("Logs sent")
        except Exception:
            log_exception("Failed to send logs..")
    else:
        LOG.info("No logs to send")


def send_logs(containers_logs, retries=3):
    for i in range(1 + retries):
        try:
            r = requests.post(LOGS_TARGET_URL, json=containers_logs)
            r.raise_for_status()
            return
        except Exception:
            if i < retries:
                retry_interval = random_retry_interval()
                LOG.info(f"Fail to send logs, will retry in {retry_interval}s")
                time.sleep(retry_interval)
            else:
                raise


def containers_logs(containers, containers_last_log_checks):
    c_logs = []

    for c in containers:
        c_id = c[ID_FIELD]
        c_name = c[NAME_FIELD]

        print()
        LOG.info(f"Checking {c_name}:{c_id} container logs...")

        last_logs_check = containers_last_log_checks[c_id]
        c_log = fetched_container_logs(containers_last_log_checks, last_logs_check, c_id)

        if c_log is not None:
            c_logs.append({
                'container_id': c_id,
                'container_name': c_name,
                'from': last_logs_check,
                'to': containers_last_log_checks[c_id],
                'log': c_log
            })
            print()
            LOG.info(f"LOG...{c_log}")
            print()

        LOG.info(f"{c_name}:{c_id} container logs checked")

    return c_logs


def fetched_container_logs(containers_last_log_checks, last_logs_check, container_id):
    try:
        LOG.info("Gathering logs...")

        now = current_timestamp()

        c_logs = container_logs_in_range(container_id, last_logs_check, now)

        containers_last_log_checks[container_id] = now

        LOG.info("Logs gathered")

        return c_logs if c_logs else None
    except Exception:
        log_exception("Failed to gather logs")
        return None


def update_last_data_read_at_file(read_at):
    try:
        LOG.info(f"Updating last-data-read-at file: {LAST_DATA_READ_AT_FILE_PATH}")

        with open(LAST_DATA_READ_AT_FILE_PATH, "w") as f:
            f.write(str(read_at))

        LOG.info("File updated")
        print()
    except Exception:
        log_exception("Problem while updating last data read at file...")


def container_logs_in_range(container_id, last_check, now):
    return DOCKER_CONTAINERS.client.logs(container_id, since=last_check, until=now, stream=False).decode("utf-8")


keep_collecting_and_sending()
