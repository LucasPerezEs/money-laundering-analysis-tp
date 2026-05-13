import argparse
import yaml

if __name__ == "__main__":
    # Create arguments parser
    argsparser = argparse.ArgumentParser()

    # Add number of clients
    argsparser.add_argument("--total_clients", default=0)

    # Get arguments
    args = argsparser.parse_args()
    total_clients = args["total_clients"]

    raise Exception("TODO: Compose generator")