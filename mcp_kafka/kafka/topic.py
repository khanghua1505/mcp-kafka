import itertools
from typing import List

from loguru import logger
from kafka.admin import NewTopic, ConfigResourceType, ConfigResource

from mcp_kafka.core import Core


class KafkaTopic:
    """KafkaTopic is a class that provides methods to manage Kafka topics.
    It allows you to create, list, and delete topics in a Kafka cluster.

    Arguments:
        core (Core): An instance of the Core class.
    """

    def __init__(self, core: Core):
        self._core = core

    def create_topic(
        self,
        name: str,
        num_partitions: int = 3,
        replication_factor: int = 3,
        if_not_exists: bool = False,
        configs: dict = None,
    ):
        """Create a new topic."""
        if if_not_exists:
            existing_topics = self._core.kafka_admin_client.list_topics()
            if name in existing_topics:
                logger.info(f"Topic {name} already exists. Skipping creation.")
                return {}

        topic = NewTopic(
            name=name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            topic_configs=configs,
        )

        response = self._core.kafka_admin_client.create_topics([topic])
        logger.debug('Create topic response:')
        logger.debug(response)

        return response

    def list_topics(self):
        """List all topics in the Kafka cluster."""
        return self._core.kafka_admin_client.list_topics()

    def describe_topics(self,  topic_names: List[str],  include_topic_configs: bool = False):
        """Fetch metadata for the specified topics or all topics if None."""
        response = []
        for topic_names_chunk in itertools.batched(topic_names, 10):
            topics = self._core.kafka_admin_client.describe_topics(topic_names_chunk)
            if include_topic_configs:
                for topic in topics:
                    response = self._core.kafka_admin_client.describe_configs(
                        [ConfigResource(resource_type=ConfigResourceType.TOPIC, name=topic['topic'])])
                    topic['configs'] = response[0]
            response += topics

        return response

    def delete_topics(self, topics: List[str]):
        """Delete the specified topics."""
        response = self._core.kafka_admin_client.delete_topics(topics)
        logger.debug('Delete topics response:')
        logger.debug(response)

        return response

    def alter_topic(self, name: str, configs: dict):
        """Alter the specified topic."""
        config = ConfigResource(
            resource_type=ConfigResourceType.TOPIC,
            name=name,
            configs=configs,
        )

        response = self._core.kafka_admin_client.alter_configs([config])
        logger.debug('Alter topic response:')
        logger.debug(response)

        return response
