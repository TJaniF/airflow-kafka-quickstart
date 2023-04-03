from airflow.decorators import dag, task 
from pendulum import datetime
from include.operators.produce import ProduceToTopicOperator
from include.operators.consume import ConsumeFromTopicOperator
import json
import logging

task_logger = logging.getLogger("airflow")

def prod_function(num, greeting):
    for i in range(num):
        yield (json.dumps(i), json.dumps(greeting))

def consume_function(message, name):
    value = json.loads(message.value())
    task_logger.info(f"{value} {name}!")

@dag(
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False
)
def produce_consume_dag():

    produce_hello = ProduceToTopicOperator(
        task_id="produce_hello",
        kafka_config_id="kafka_default",
        topic="my_topic",
        producer_function=prod_function,
        producer_function_args=[3],
        producer_function_kwargs={"greeting": "hello"},
        poll_timeout=10
    )

    consume_hello = ConsumeFromTopicOperator(
        task_id="consume_hello",
        kafka_config_id="kafka_default",
        topics=["my_topic"],
        apply_function="produce_consume_dag.consume_function",
        apply_function_kwargs={"name": "friend"},
        poll_timeout=10
    )

    produce_hello >> consume_hello


produce_consume_dag()