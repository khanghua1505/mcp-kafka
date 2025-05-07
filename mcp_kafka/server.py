"""mcp-kafka server implementation."""

import os
import sys

from contextlib import asynccontextmanager

from loguru import logger
from fastmcp import FastMCP

from mcp_kafka.core import Core, CoreManager
from mcp_kafka.tools import (
    list_clusters,
    describe_cluster,
    describe_broker,
    create_topic,
    update_topic,
    list_topics,
    describe_topics,
    delete_topic,
    list_consumer_groups,
    list_consumer_group_offsets,
    describe_consumer_groups,
    delete_consumer_group,
)


# Set up logging
logger.remove()
logger.add(sys.stderr, level=os.getenv('FASTMCP_LOG_LEVEL', 'WARNING'))


@asynccontextmanager
async def server_lifespan(mcp: FastMCP):
    """Lifespan context manager for the server."""
    try:
        yield
    finally:
        logger.info('Server shutting down...')
        core = CoreManager.get_core()
        core.close()
        logger.info('Server shut down.')


mcp = FastMCP(name='mcp-kafka', lifespan=server_lifespan)

# Register tools
mcp.tool(name='list_clusters')(list_clusters)
mcp.tool(name='describe_cluster')(describe_cluster)
mcp.tool(name='describe_broker')(describe_broker)
mcp.tool(name='create_topic')(create_topic)
mcp.tool(name='update_topic')(update_topic)
mcp.tool(name='list_topics')(list_topics)
mcp.tool(name='describe_topics')(describe_topics)
mcp.tool(name='delete_topic')(delete_topic)
mcp.tool(name='list_consumer_groups')(list_consumer_groups)
mcp.tool(name='list_consumer_group_offsets')(list_consumer_group_offsets)
mcp.tool(name='describe_consumer_groups')(describe_consumer_groups)
mcp.tool(name='delete_consumer_group')(delete_consumer_group)


def main():
    """Run the MCP server with CLI argument support."""
    core = Core.from_flags()
    CoreManager.set_core(core)

    # Log startup information
    logger.info('Starting MCP Kafka Server')

    # Run server with appropriate transport
    if core.use_sse:
        logger.info(f'Using SSE transport on port {core.port}')
        mcp.settings.port = core.port
        mcp.run(transport='sse')
    else:
        logger.info('Using standard stdio transport')
        mcp.run(transport='stdio')


if __name__ == '__main__':
    main()
