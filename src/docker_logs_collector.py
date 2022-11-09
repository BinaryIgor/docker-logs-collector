import json
import logging
import random
import time
from os import environ

import docker

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s.%(msecs)03d;%(levelname)s;%(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
LOG = logging.getLogger(__file__)

ID_FIELD = "id"
NAME_FIELD = "name"
INSTANCE_NAME_LABEL = environ.get("INSTANCE_NAME_LABEL", "")

machine_name = environ.get("MACHINE_NAME", "virtuocrat")

logs_target_url = environ.get('LOGS_TARGET_URL', "http://localhost:5555")
logs_target_headers = environ.get("LOGS_TARGET_HEADERS", {})
if logs_target_headers:
    logs_target_headers = {h.split("=")[0].strip(): h.split("=")[1].strip() for h in logs_target_headers.split(",")}

collection_interval = int(environ.get("COLLECTION_INTERVAL", 10))
LAST_DATA_READ_AT_FILE_PATH = environ.get("LAST_DATA_READ_AT_FILE",
                                          "/tmp/docker-logs-collector-last-data-read-at.txt")


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
                   for c in docker_client.containers()]

        all_containers = []
        all_containers.extend(fetched)

        for pc in self.previous_containers:
            if pc not in all_containers:
                all_containers.append(pc)

        self.previous_containers = fetched

        return all_containers


def connected_docker_client_retrying():
    def new_client():
        return docker.APIClient(base_url='unix://var/run/docker.sock')

    LOG.info(f"Starting monitoring of machine {machine_name}")
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


docker_client = connected_docker_client_retrying()
docker_containers = DockerContainers(docker_client)

for c in docker_containers.get():
    print(c)
    logs = docker_client.logs(c[NAME_FIELD], since=current_timestamp() - 300, until=current_timestamp(), stream=False).decode(
        "utf-8")
    print(logs)
