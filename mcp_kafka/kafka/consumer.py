from typing import List
from mcp_kafka.core import Core


class KafkaConsumer:
    """KafkaConsumer is a class that represents a Kafka consumer."""

    def __init__(self, core: Core):
        self._core = core

    def list_consumer_groups(self, broker_ids=None):
        """List all consumer groups known to the cluster.

        Arguments:
            broker_ids ([int], optional) A list of broker node_ids to query for consumer groups.
                If set to None, will query all brokers in the cluster.
        """

        response = self._core.kafka_admin_client.list_consumer_groups(broker_ids=broker_ids)
        consumer_groups = [group[0] for group in response]
        return consumer_groups

    def list_consumer_group_offsets(self, group_id: str):
        """List all consumer group offsets for a given consumer group.

        Arguments:
            group_id (str): The ID of the consumer group to list offsets for.
        """

        return self._core.kafka_admin_client.list_consumer_group_offsets(group_id)

    def describe_consumer_groups(self, group_ids: List[str]):
        """Describe a consumer group.

        Arguments:
            group_ids: A list of consumer group IDs.
        """

        return self._core.kafka_admin_client.describe_consumer_groups(group_ids)

    def delete_consumer_group(self, group_id: str):
        """Delete consumer group.

        Arguments:
            group_id (str): The ID of the consumer group to delete.
        """

        return self._core.kafka_admin_client.delete_consumer_groups([group_id])
