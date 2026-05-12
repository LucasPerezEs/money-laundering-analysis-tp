import yaml
import copy

# Config file
CONFIG_FILE = "data_cleaner_config.yaml"

# Build section
DOCKER_BUILD_SECTION_NAME = "build"
DOCKER_BUILD_CONTEXT_SUBSECTION_NAME = "context"

CONTEXT_FOLDER = "./src/data_cleaner"

# Environment variable names
DOCKER_ENV_VARS_NAME = "environment"

OUTPUT_EXCHANGE = "OUTPUT_EXCHANGE=cleaned_data_exchange"
INPUT_QUEUE = "INPUT_QUEUE=raw_data_queue"

def get_data_cleaner_docker_services(service_prefix, total_instances):
    with open(CONFIG_FILE, "r") as config_file:
        base_data_cleaner_service = yaml.safe_load(config_file)

    # Create all services
    data_cleaner_services = {}

    for i in range(total_instances):
        # Copy service base configuration
        new_service_config = copy.deepcopy(base_data_cleaner_service)

        # Add context folder
        new_service_config[DOCKER_BUILD_SECTION_NAME][DOCKER_BUILD_CONTEXT_SUBSECTION_NAME] = CONTEXT_FOLDER

        # Add environment variables
        new_service_config[DOCKER_ENV_VARS_NAME].append(OUTPUT_EXCHANGE)
        new_service_config[DOCKER_ENV_VARS_NAME].append(INPUT_QUEUE)

        # Add service in services dictionary
        new_service_name = f"{service_prefix}_{i}"
        data_cleaner_services[new_service_name] = new_service_config

    return data_cleaner_services