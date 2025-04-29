
"""MCP Kafka tool handlers."""
from typing import List

from loguru import logger
from fastmcp import Context

from mcp_kafka.core import CoreManager
from mcp_kafka.kafka import KafkaCluster, KafkaTopic, KafkaConsumer


async def describe_cluster(ctx: Context):
    """Fetch cluster-wide metadata such as the list of brokers, the controller ID, and the cluster ID.

    ## Usage

    Fetch cluster-wide metadata such as the list of brokers, the controller ID, and the cluster ID.

    Arguments:
        ctx: MCP context

    Returns:
        A dictionary containing the cluster metadata.
    """
    kafka_cluster = KafkaCluster(CoreManager.get_core())

    try:
        response = kafka_cluster.describe_cluster()

        return {
            'data': response,
        }
    except Exception as e:
        error_msg = f'Error describing cluster: {str(e)}'
        logger.error(error_msg)
        await ctx.error(error_msg)

        return {
            'error': error_msg,
        }


async def describe_broker(ctx: Context, broker_id: str):
    """Fetch metadata for the specified broker.

    ## Usage

    Fetch metadata for the specified broker.

    Arguments:
        ctx: MCP context
        broker_id (str): The ID of the broker to describe.

    Returns:
        A dictionary containing the broker metadata.
    """
    kafka_cluster = KafkaCluster(CoreManager.get_core())

    try:
        response = kafka_cluster.describe_broker(broker_id)

        return {
            'data': response,
        }
    except Exception as e:
        error_msg = f'Error describing broker: {str(e)}'
        logger.error(error_msg)
        await ctx.error(error_msg)

        return {
            'error': error_msg,
        }


async def create_topic(
    ctx: Context,
    name: str,
    num_partitions: int,
    replication_factor: int,
    if_not_exists: bool,
    configs: dict,
):
    """Create a new topic.

    ## Usage

    Create a new Kafka topic with customizable parameters like the number of partitions, replication factor,
    and topic-specific configurations.

    ## Recommended Usage

    - Ensure that the topic name follows Kafka's naming rules.
    - Use `if_not_exists=True` in production to avoid errors if the topic already exists.
    - When creating important topics, always provide key configurations such as:
        - `retention.ms`
        - `cleanup.policy`
        - `min.insync.replicas`
    - Ensure that the replication factor is greater than or equal to `min.insync.replicas`.
    - For fault tolerance, always set `replication.factor >= 3` and `min.insync.replicas >= 2`.

    Arguments:
        ctx: MCP context
        name (str): The name of the topic to create.
        num_partitions (int): The number of partitions for the topic. Defaults to 3.
        replication_factor (int): The replication factor for the topic. Defaults to 3.
        if_not_exists (bool): If set when creating topics, the action will only execute
            if the topic does not already exist.
        configs (dict):  A topic configuration override for the topic being created.
            The following is a list of valid configurations:
                cleanup.policy
                compression.gzip.level
                compression.lz4.level
                compression.type
                compression.zstd.level
                delete.retention.ms
                file.delete.delay.ms
                flush.messages
                flush.ms
                follower.replication.throttled.
            replicas
                index.interval.bytes
                leader.replication.throttled.replicas
                local.retention.bytes
                local.retention.ms
                max.compaction.lag.ms
                max.message.bytes
                message.timestamp.after.max.ms
                message.timestamp.before.max.ms
                message.timestamp.type
                min.cleanable.dirty.ratio
                min.compaction.lag.ms
                min.insync.replicas
                preallocate
                remote.log.copy.disable
                remote.log.delete.on.disable
                remote.storage.enable
                retention.bytes
                retention.ms
                segment.bytes
                segment.index.bytes
                segment.jitter.ms
                segment.ms
                unclean.leader.election.enable
    """
    kafka_topic = KafkaTopic(CoreManager.get_core())

    try:
        response = kafka_topic.create_topic(
            name,
            num_partitions,
            replication_factor,
            if_not_exists,
            configs,
        )

        return {
            'data': response,
        }
    except Exception as e:
        error_msg = f'Error creating topic: {str(e)}'
        logger.error(error_msg)
        await ctx.error(error_msg)

        return {
            'error': error_msg,
        }


async def list_topics(ctx: Context):
    """List all topics in the Kafka cluster.

    ## Usage

    List all topics in the Kafka cluster.

    Arguments:
        ctx: MCP context

    Returns:
        A list of topic names.
    """
    kafka_topic = KafkaTopic(CoreManager.get_core())

    try:
        response = kafka_topic.list_topics()
        return {
            'data': response,
        }
    except Exception as e:
        error_msg = f'Error listing topics: {str(e)}'
        logger.error(error_msg)
        await ctx.error(error_msg)

        return {
            'error': error_msg,
        }


async def describe_topics(ctx: Context, topics: List[str], include_topic_configs: bool = False):
    """Fetch metadata for the specified topic.

    ## Usage

    Fetch metadata for the specified topic.

    Arguments:
        ctx: MCP context
        topics: List of topics to describe. If None, all topics will be described.
        include_topic_configs (bool): If True, includes topic configurations in the response.

    Returns:
        A dictionary containing the topic descriptions.
    """
    kafka_topic = KafkaTopic(CoreManager.get_core())

    try:
        response = kafka_topic.describe_topics(topics, include_topic_configs)

        return {
            'data': response,
        }
    except Exception as e:
        error_msg = f'Error describing topic: {str(e)}'
        logger.error(error_msg)
        await ctx.error(error_msg)

        return {
            'error': error_msg,
        }


async def update_topic(ctx: Context, topic: str, configs: dict):
    """Update the specified topic.

    ## Usage

    Update the specified topic.

    Arguments:
        ctx: MCP context
        topic (str): The name of the topic to update.
        configs (dict): A dictionary of configurations to update for the topic.
    """
    kafka_topic = KafkaTopic(CoreManager.get_core())

    try:
        response = kafka_topic.alter_topic(topic, configs)

        return {
            'data': response,
        }
    except Exception as e:
        error_msg = f'Error updating topic: {str(e)}'
        logger.error(error_msg)
        await ctx.error(error_msg)

        return {
            'error': error_msg,
        }


async def delete_topic(ctx: Context, topic: str):
    """Delete the specified topic.

    ## Usage

    Delete the specified topic.

    ## Recommended Usage

    - use `dry_run=True` to validate the topic deletion without actually deleting the topic.

    Arguments:
        ctx: MCP context
        topic (str): The name of the topic to delete.
    """
    kafka_topic = KafkaTopic(CoreManager.get_core())

    try:
        response = kafka_topic.delete_topics([topic])

        return {
            'data': response,
        }
    except Exception as e:
        error_msg = f'Error deleting topic: {str(e)}'
        logger.error(error_msg)
        await ctx.error(error_msg)

        return {
            'error': error_msg,
        }


async def list_consumer_groups(ctx: Context, broker_ids=None):
    """List all consumer groups known to the cluster.

    ## Usage

    List all consumer groups known to the cluster.

    Arguments:
        ctx: MCP context
        broker_ids ([int], optional): A list of broker node_ids to query for consumer groups.
            If set to None, will query all brokers in the cluster.

    Returns:
        A list of consumer groups.
    """
    kafka_cluster = KafkaConsumer(CoreManager.get_core())

    try:
        response = kafka_cluster.list_consumer_groups(broker_ids=broker_ids)

        return {
            'data': response,
        }
    except Exception as e:
        error_msg = f'Error listing consumer groups: {str(e)}'
        logger.error(error_msg)
        await ctx.error(error_msg)

        return {
            'error': error_msg,
        }


async def list_consumer_group_offsets(ctx: Context, group_id: str):
    """List all consumer group offsets for a given consumer group.

    ## Usage

    List all consumer group offsets for a given consumer group.

    Arguments:
        ctx: MCP context
        group_id (str): The ID of the consumer group to list offsets for.

    Returns:
        A dictionary containing the consumer group offsets.
    """
    kafka_cluster = KafkaConsumer(CoreManager.get_core())

    try:
        response = kafka_cluster.list_consumer_group_offsets(group_id)

        return {
            'data': response,
        }
    except Exception as e:
        error_msg = f'Error listing consumer group offsets: {str(e)}'
        logger.error(error_msg)
        await ctx.error(error_msg)

        return {
            'error': error_msg,
        }


async def describe_consumer_groups(ctx: Context, group_ids: List[str]):
    """Describe a list of consumer groups.

    ## Usage

    Describe a list of consumer groups.

    Arguments:
        ctx: MCP context
        group_ids: The list of consumer group IDs to describe.

    Returns:
        A dictionary containing the consumer group description.
    """
    kafka_cluster = KafkaConsumer(CoreManager.get_core())

    try:
        response = kafka_cluster.describe_consumer_groups(group_ids)

        return {
            'data': response,
        }
    except Exception as e:
        error_msg = f'Error describing consumer group: {str(e)}'
        logger.error(error_msg)
        await ctx.error(error_msg)

        return {
            'error': error_msg,
        }


async def delete_consumer_group(ctx: Context, group_id: str):
    """Delete a consumer group.

    ## Usage

    Delete a consumer group.

    Arguments:
        ctx: MCP context
        group_id (str): The ID of the consumer group to delete.

    Returns:
        A dictionary containing the result of the deletion.
    """
    kafka_cluster = KafkaConsumer(CoreManager.get_core())

    try:
        response = kafka_cluster.delete_consumer_group(group_id)

        return {
            'data': response,
        }
    except Exception as e:
        error_msg = f'Error deleting consumer groups: {str(e)}'
        logger.error(error_msg)
        await ctx.error(error_msg)

        return {
            'error': error_msg,
        }
