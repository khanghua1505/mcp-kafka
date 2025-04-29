from typing import List

from loguru import logger
from kafka.admin import NewTopic, ConfigResourceType, ConfigResource
from kafka.protocol.admin import DeleteTopicsResponse_v3, AlterConfigsResponse_v1

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
        dry_run: bool = False,
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

        response = self._core.kafka_admin_client.create_topics(
            [topic], validate_only=dry_run)
        logger.debug('Create topic response:')
        logger.debug(response)

        return response

    def list_topics(self):
        """List all topics in the Kafka cluster."""
        return self._core.kafka_admin_client.list_topics()

    def describe_topics(self, topics: List[str]):
        """Fetch metadata for the specified topics or all topics if None."""
        return self._core.kafka_admin_client.describe_topics(topics)

    def describe_topic_config(self, topic: str):
        """Fetch metadata for the specified topic."""

        response = self._core.kafka_admin_client.describe_configs([
            ConfigResource(
                resource_type=ConfigResourceType.TOPIC,
                name=topic,
            ),
        ])

        return response[0]

    def delete_topics(self, topics: List[str], dry_run: bool = False):
        """Delete the specified topics."""
        if dry_run:
            response = DeleteTopicsResponse_v3(
                throttle_time_ms=0,
                topic_error_codes=[(topic, 0) for topic in topics],
            )
            return response

        response = self._core.kafka_admin_client.delete_topics(topics)
        logger.debug('Delete topics response:')
        logger.debug(response)

        return response

    def alter_topic(self, name: str, configs: dict, dry_run: bool = False):
        """Alter the specified topic."""
        if dry_run:
            existing_topics = self._core.kafka_admin_client.describe_topics([
                                                                            name])
            if name not in existing_topics:
                return AlterConfigsResponse_v1(
                    throttle_time_ms=0,
                    resources=[
                        (3, f"The topic '{name}' does not exist.", ConfigResourceType.TOPIC, name)]
                )

            return AlterConfigsResponse_v1(
                throttle_time_ms=0,
                resources=[(0, None, ConfigResourceType.TOPIC, name)]
            )

        config = ConfigResource(
            resource_type=ConfigResourceType.TOPIC,
            name=name,
            configs=configs,
        )

        response = self._core.kafka_admin_client.alter_configs([config])
        logger.debug('Alter topic response:')
        logger.debug(response)

        return response
