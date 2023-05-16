"""
### DAG which produces to and consumes from a Kafka cluster

This DAG will produce messages of several elements to a Kafka cluster and consume
them.
"""

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
import json
import random

YOUR_NAME = "Jani"
YOUR_PET_NAME = "Nevermore"
NUMBER_OF_TREATS = 5
KAFKA_TOPIC = "my_topic"


def prod_function(num_treats, pet_name):
    for i in range(num_treats):
        final_treat = False
        pet_moode_post_treat = random.choices(
            ["content", "happy", "zoomy", "bouncy"], weights=[2, 2, 1, 1], k=1
        )[0]
        if i + 1 == num_treats:
            final_treat = True
        yield (
            json.dumps(i),
            json.dumps(
                {
                    "pet_name": pet_name,
                    "pet_moode_post_treat": pet_moode_post_treat,
                    "final_treat": final_treat,
                }
            ),
        )


def consume_function(message, name):
    key = json.loads(message.key())
    message_content = json.loads(message.value())
    pet_name = message_content["pet_name"]
    pet_moode_post_treat = message_content["pet_moode_post_treat"]
    print(
        f"Message #{key}: Hello {name}, your pet {pet_name} has consumed another treat and is now {pet_moode_post_treat}!"
    )


@dag(
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def produce_consume_treats():
    @task
    def get_your_pet_name(pet_name=None):
        return pet_name

    @task
    def get_number_of_treats(num_treats=None):
        return num_treats

    @task
    def get_pet_owner_name(your_name=None):
        return your_name

    produce_hello = ProduceToTopicOperator(
        task_id="produce_hello",
        kafka_config_id="kafka_default",
        topic=KAFKA_TOPIC,
        producer_function=prod_function,
        producer_function_args=["{{ ti.xcom_pull(task_ids='get_number_of_treats')}}"],
        producer_function_kwargs={
            "pet_name": "{{ ti.xcom_pull(task_ids='get_your_pet_name')}}"
        },
        poll_timeout=10,
    )

    consume_hello = ConsumeFromTopicOperator(
        task_id="consume_hello",
        kafka_config_id="kafka_default",
        topics=[KAFKA_TOPIC],
        apply_function="produce_consume_treats.consume_function",
        apply_function_kwargs={
            "name": "{{ ti.xcom_pull(task_ids='get_pet_owner_name')}}"
        },
        poll_timeout=20,
        max_messages=20,
        max_batch_size=20,
    )

    [
        get_your_pet_name(YOUR_PET_NAME),
        get_number_of_treats(NUMBER_OF_TREATS),
    ] >> produce_hello
    get_pet_owner_name(YOUR_NAME) >> consume_hello

    produce_hello >> consume_hello


produce_consume_treats()
