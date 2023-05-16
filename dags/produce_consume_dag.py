from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
import json

YOUR_FAVORITE_GREETING = "Hey"
YOUR_FRIENDS_NAME = "Sarah"


def prod_function(num, greeting):
    for i in range(num):
        yield (json.dumps(i), json.dumps(greeting))


def consume_function(message, name):
    key = message.key()
    greeting = json.loads(message.value())
    print(f"Message #{key}: {greeting} {name}!")


@dag(start_date=datetime(2023, 4, 1), schedule=None, catchup=False)
def produce_consume_dag():
    @task
    def get_favorite_greeting(fav_greeting=None):
        return fav_greeting

    @task
    def get_your_friends_name(your_name=None):
        return your_name

    produce_hello = ProduceToTopicOperator(
        task_id="produce_hello",
        kafka_config_id="kafka_default",
        topic="my_topic",
        producer_function=prod_function,
        producer_function_args=[3],
        producer_function_kwargs={
            "greeting": "{{ ti.xcom_pull(task_ids='get_favorite_greeting')}}"
        },
        poll_timeout=10,
    )

    consume_hello = ConsumeFromTopicOperator(
        task_id="consume_hello",
        kafka_config_id="kafka_default",
        topics=["my_topic"],
        apply_function="produce_consume_dag.consume_function",
        apply_function_kwargs={
            "name": "{{ ti.xcom_pull(task_ids='get_your_friends_name')}}"
        },
        poll_timeout=20,
        max_messages=20,
        max_batch_size=20,
    )

    get_favorite_greeting(YOUR_FAVORITE_GREETING) >> produce_hello
    get_your_friends_name(YOUR_FRIENDS_NAME) >> consume_hello

    produce_hello >> consume_hello


produce_consume_dag()
