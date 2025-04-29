"""mcp-kafka server core."""

import argparse
import copy

from loguru import logger
from kafka import KafkaAdminClient


class Core:
    """Core class for the mcp-kafka server.

    Keyword Arguments:
        use_sse (bool): Whether to use SSE transport. Defaults to False.
        port (int): Port to run the server on. Defaults to 8888.
        bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
            strings) that the consumer should contact to bootstrap initial
            cluster metadata. This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default port is 9092. If no servers are
            specified, will default to localhost:9092.
    """
    DEFAULT_CONFIG = {
        'use_sse': False,
        'port': 8888,
        'bootstrap_servers': 'localhost:9092',
    }

    def __init__(self, **configs):
        logger.debug(f"Starting MCP Core with configuration: {configs}")
        extra_configs = set(configs).difference(self.DEFAULT_CONFIG)
        if extra_configs:
            raise KeyError(f"Unrecognized configs: {extra_configs}")
        self.config = copy.copy(self.DEFAULT_CONFIG)
        self.config.update(configs)

        self._kafka_admin_client = None

    @staticmethod
    def from_flags():
        """Create a Core instance from command line flags.

        This method is used to parse command line arguments and create a Core
        instance with the specified configurations.

        Returns:
            An instance of the Core class.
        """
        parser = argparse.ArgumentParser(
            description='A Model Context Protocol (MCP) server for Kafka',
        )
        parser.add_argument('--sse', type=bool, default=False,
                            help='Use SSE transport.')
        parser.add_argument('--port', type=int, default=8888,
                            help='Port to run the server on.')
        parser.add_argument('--kafka-bootstrap-servers', type=str,
                            default='localhost:9092',  help='The Kafka to connect to.')
        args = parser.parse_args()

        core = Core(
            use_sse=args.sse,
            port=args.port,
            bootstrap_servers=args.kafka_bootstrap_servers,
        )

        return core

    @property
    def use_sse(self) -> bool:
        """Check if the server is using SSE transport.

        Returns:
            bool: True if using SSE transport, False otherwise.
        """
        return self.config['use_sse']

    @property
    def port(self) -> int:
        """Get the port the server is running on.

        Returns:
            int: The port number.
        """
        return self.config['port']

    @property
    def kafka_admin_client(self) -> KafkaAdminClient:
        """Get the Kafka admin client.

        Returns:
            The Kafka admin client.
        """
        if self._kafka_admin_client is None:
            self._kafka_admin_client = KafkaAdminClient(
                bootstrap_servers=self.config['bootstrap_servers'],
                client_id='mcp-kafka-admin',
            )

        return self._kafka_admin_client

    def close(self):
        """Graceful shutdown for MCP Core server."""
        if self._kafka_admin_client:
            self._kafka_admin_client.close()
            self._kafka_admin_client = None


class CoreManager:
    """Manager for the Core instance."""
    _core: Core = None

    @classmethod
    def set_core(cls, core: Core):
        """Set the core instance.

        Args:
            core (Core): The Core instance to set.
        """
        cls._core = core

    @classmethod
    def get_core(cls) -> Core:
        """Get the core instance.

        Returns:
            Core: The Core instance.
        """
        return cls._core
