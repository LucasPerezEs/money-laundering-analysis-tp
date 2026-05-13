from data_reducer.data_reducer_docker_service import get_data_cleaner_docker_services
from filter.filter_docker_service import get_filters_docker_services
from gateway.gateway_docker_service import get_gateway_docker_services

def generate_system_docker_compose():
    system = {}

    # Create gateway
    gateway = get_gateway_docker_services(
                input_query_queue_prefix="results",
                total_queries=5, output_queue="raw_data_queue"
                )
    system = system | gateway

    # Create data cleaners
    raise Exception("TODO: Faltan los data cleaners")

    # Create data
    usd_filters = get_filters_docker_services("usd_filter", 1,
                                             "Payment Currency", "US Dollar", "eq",
                                             input_queue="cleanded_data_queue",
                                             output_exchange="usd_transactions_exc",
                                             )
    system = system | usd_filters

    # Query 1
    ## Reduce data
    data_reducers_q1 = get_data_cleaner_docker_services("q1_data_reducer", 1,
                                                        ["From Bank", "Account", "To Bank", "Account.1", "Amount Paid"],
                                                        input_exchange="usd_transactions_exc",
                                                        output_queue="q1_reduced_data",
                                                        )
    system = system | data_reducers_q1

    ## Filter by amount
    q1_50_usd_filters = get_filters_docker_services("filter_50_usd", 1,
                                                    "Amount Paid", "lt", "50",
                                                    input_queue="q1_reduced_data",
                                                    output_queue="results_1",
                                                    )

    ## Return complete YAML system
    return system