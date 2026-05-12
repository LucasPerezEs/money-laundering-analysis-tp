import yaml
import copy

# Config file
CONFIG_FILE = "gateway_config.yaml"

# Service configuration
SERVICE_NAME="gateway"

# Build section
DOCKER_BUILD_SECTION_NAME = "build"
DOCKER_BUILD_CONTEXT_SUBSECTION_NAME = "context"

CONTEXT_FOLDER = "./src/gateway"

# Environment variable names
DOCKER_ENV_VARS_NAME = "environment"

OUTPUT_QUEUE = "OUTPUT_QUEUE"
INPUT_QUEUE = "INPUT_QUEUE"

def get_gateway_docker_services(input_query_queue_prefix, total_queries, output_queue):
    # Open config file
    with open(CONFIG_FILE, "r") as config_file:
        gateway_service_config = yaml.safe_load(config_file)

    # Add context folder
    gateway_service_config[DOCKER_BUILD_SECTION_NAME][DOCKER_BUILD_CONTEXT_SUBSECTION_NAME] = CONTEXT_FOLDER

    # Add environment variables
    ## I/O
    gateway_service_config[DOCKER_ENV_VARS_NAME].append(f"{OUTPUT_QUEUE}={output_queue}")
    for i in range(total_queries):
        gateway_service_config[DOCKER_ENV_VARS_NAME].append(f"{INPUT_QUEUE}_{i}={input_query_queue_prefix}_{i}")

    # Add service name
    new_service_config = { SERVICE_NAME : gateway_service_config}

    return new_service_config