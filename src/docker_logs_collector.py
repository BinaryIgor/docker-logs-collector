import json
import logging
import random
import time

import docker

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s.%(msecs)03d;%(levelname)s;%(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
LOG = logging.getLogger(__file__)

machine_name = "virtuocrat"


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


docker_client = connected_docker_client_retrying()

running_containers = docker_client.containers()

for c in running_containers:
    print(c)
