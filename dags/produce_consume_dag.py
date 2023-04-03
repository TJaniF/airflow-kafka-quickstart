from airflow.decorators import dag, task 
from pendulum import datetime
from include.operators.produce import ProduceToTopicOperator
from include.operators.consume import ConsumeFromTopicOperator
import json

def prod_function(num, greeting):
    for i in range(num):
        print(i)
        yield (json.dumps(i), json.dumps(greeting))


def consume_function(message):
    print(message.value())

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
        poll_timeout=10
    )

    produce_hello >> consume_hello


produce_consume_dag()