import copy
import os
import yaml

CONFIG_FILE = "client_config.yaml"
CLIENT_NAME_PREFIX = "client"

# Build section
DOCKER_BUILD_SECTION_NAME = "build"
DOCKER_BUILD_CONTEXT_SUBSECTION_NAME = "context"

CONTEXT_FOLDER = "./src/client"

# Container name
CONTAINER_NAME_TAG = "container_name"

# Environment variable names
DOCKER_ENV_VARS_NAME = "environment"

## I/O
ACCOUNTS_INPUT_FILE = "ACCOUNTS_INPUT_FILE"
TRANSACTIONS_INPUT_FILE = "TRANSACTIONS_INPUT_FILE"
OUTPUT_FILE_TAG = "OUTPUT_FILE"


def get_clients_docker_services(accounts_file_path, transactions_file_path, output_file_path, total_clients):
    # Open config file
    base_path = os.path.dirname(__file__)
    config_file_path = os.path.join(base_path, CONFIG_FILE)
    with open(config_file_path, "r") as config_file:
        base_client_service_config = yaml.safe_load(config_file)

    # Create empty services list
    new_clients_services = {}
    for i in range(total_clients):
        # Copy base configuration
        new_client_service_config = copy.deepcopy(base_client_service_config)

        # Add container name
        client_name = f"{CLIENT_NAME_PREFIX}_{i}"
        new_client_service_config[CONTAINER_NAME_TAG] = client_name

        # Add context folder
        new_client_service_config[DOCKER_BUILD_SECTION_NAME][DOCKER_BUILD_CONTEXT_SUBSECTION_NAME] = CONTEXT_FOLDER

        # Add environment variables
        new_client_service_config[DOCKER_ENV_VARS_NAME].append(f"{ACCOUNTS_INPUT_FILE}={accounts_file_path}")
        new_client_service_config[DOCKER_ENV_VARS_NAME].append(f"{TRANSACTIONS_INPUT_FILE}={transactions_file_path}")
        new_client_service_config[DOCKER_ENV_VARS_NAME].append(f"{OUTPUT_FILE_TAG}={output_file_path}")

        # Add service
        new_clients_services[client_name] = new_client_service_config

    return new_clients_services