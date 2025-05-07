from kafka.admin import ConfigResourceType, ConfigResource

from mcp_kafka.core import Core


class KafkaCluster:
    """KafkaCluster is a class that provides methods to manage Kafka clusters.

    Arguments:
        core (Core): An instance of the Core class.
    """

    def __init__(self, core: Core, cluster_name: str):
        self._core = core
        self._kafka_admin_client = core.kafka_admin_client(cluster_name)

    def describe_cluster(self):
        """Fetch cluster-wide metadata such as the list of brokers, the controller ID, and the cluster ID."""

        return self._kafka_admin_client.describe_cluster()

    def describe_broker(self, broker_id: str):
        """Fetch metadata for the specified broker"""

        response = self._kafka_admin_client.describe_configs([
            ConfigResource(
                resource_type=ConfigResourceType.BROKER,
                name=broker_id,
            ),
        ])

        return response[0]
