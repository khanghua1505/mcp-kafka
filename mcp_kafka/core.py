"""mcp-kafka server core."""


import argparse
import copy
from typing import List, Union

import yaml
from pydantic import BaseModel, Field
from loguru import logger
from kafka import KafkaAdminClient


class SSLConfig(BaseModel):
    """SSL configuration.
    Attributes:
        ca_file (str): Path to the CA file.
        certfile (str): Path to the certificate file.
        keyfile (str): Path to the key file.
    """

    cafile: str = Field(default=None)
    certfile: str = Field(default=None)
    keyfile: str = Field(default=None)


class SALSConfig(BaseModel):
    """SASL configuration.

    Attributes:
        mechanism (str): SASL mechanism.
        username (str): SASL username.
        password (str): SASL password.
    """

    mechanism: str = Field(default=None)
    username: str = Field(default=None)
    password: str = Field(default=None)


class KafkaClusterConfig(BaseModel):
    """Cluster configuration.

    Attributes:
        description (str): Description of the cluster.
        bootstrap_servers (Union[str, List[str]]): Bootstrap servers for the cluster.
        ssl (SSLConfig): SSL configuration.
        security_protocol (str): Security protocol.
        sasl (SALSConfig): SASL configuration.
    """

    description: str = Field(default='')
    bootstrap_servers: Union[str, List[str]] = Field()
    ssl: SSLConfig = Field(default={})
    security_protocol: str = Field(default='PLAINTEXT')
    sasl: SALSConfig = Field(default={})


class KafkaClustersConfig(BaseModel):
    """Kafka clusters configuration.

    Attributes:
        clusters (dict[str, KafkaClusterConfig]): Dictionary of cluster configurations.
    """
    clusters: dict[str, KafkaClusterConfig] = Field(default={})


class Core:
    """Core class for the mcp-kafka server.

    Keyword Arguments:
        use_sse (bool): Whether to use SSE transport. Defaults to False.
        port (int): Port to run the server on. Defaults to 8888.
        clusters_config_file: Path to the configuration file for Kafka clusters.
            File format:
                ```yaml
                clusters:
                    cluster_name:
                        bootstrap_servers: [host1:port1, host2:port2]
                        ssl:
                            ca_file: path/to/ca.pem
                            certfile: path/to/cert.pem
                            keyfile: path/to/key.pem
                        security_protocol: SASL_SSL
                        sasl:
                            mechanism: SCRAM-SHA-256
                            username: user
                            password: password
                ```
    """
    DEFAULT_CONFIG = {
        'use_sse': False,
        'port': 8888,
        'clusters_config_file': '',
    }

    def __init__(self, **configs):
        logger.debug(f"Starting MCP Core with configuration: {configs}")
        extra_configs = set(configs).difference(self.DEFAULT_CONFIG)
        if extra_configs:
            raise KeyError(f"Unrecognized configs: {extra_configs}")
        self.config = copy.copy(self.DEFAULT_CONFIG)
        self.config.update(configs)
        if self.config['clusters_config_file']:
            clusters_config = self._parse_config_file(self.config['clusters_config_file'])
            self.config['clusters'] = clusters_config.clusters

        self._kafka_admin_clients = {}

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
        parser.add_argument('--clusters-config', type=str, help='Configuration file path.')
        args = parser.parse_args()

        core = Core(
            use_sse=args.sse,
            port=args.port,
            clusters_config_file=args.clusters_config,
        )

        return core

    def _parse_config_file(self, file_path):
        """Parse the configuration file.

        Args:
            file_path (str): Path to the configuration file.

        Returns:
            dict: Parsed configuration.
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            config = yaml.safe_load(content)

        return KafkaClustersConfig(**config)

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
    def clusters(self) -> List[str]:
        """Get the cluster names.

        Returns:
            List[str]: List of cluster names.
        """
        return self.config['clusters'].keys() if 'clusters' in self.config else []

    def get_cluster_config(self, cluster_name: str) -> KafkaClusterConfig:
        """Get the configuration for a specific cluster.

        Args:
            cluster_name (str): The name of the cluster.

        Returns:
            ClusterConfig: The configuration for the specified cluster.
        """
        if 'clusters' not in self.config:
            raise KeyError('No clusters found in the configuration.')

        if cluster_name not in self.config['clusters']:
            raise KeyError(f"Cluster '{cluster_name}' not found in the configuration.")

        return self.config['clusters'][cluster_name]

    def kafka_admin_client(self, cluster_name: str) -> KafkaAdminClient:
        """Get the Kafka admin client.

        Returns:
            The Kafka admin client.
        """
        if cluster_name not in self._kafka_admin_clients:
            config = self.get_cluster_config(cluster_name)
            kafka_config = dict(
                bootstrap_servers=config.bootstrap_servers,
                security_protocol=config.security_protocol,
                ssl_cafile=config.ssl.cafile,
                ssl_certfile=config.ssl.certfile,
                ssl_keyfile=config.ssl.keyfile,
                sasl_mechanism=config.sasl.mechanism,
                sasl_plain_username=config.sasl.username,
                sasl_plain_password=config.sasl.password,
            )
            self._kafka_admin_clients[cluster_name] = KafkaAdminClient(**kafka_config)

        return self._kafka_admin_clients[cluster_name]

    def close(self):
        """Graceful shutdown for MCP Core server."""
        for cluster_name, admin_client in self._kafka_admin_clients.items():
            logger.debug(f"Closing Kafka admin client for cluster: {cluster_name}")
            admin_client.close()

        self._kafka_admin_clients.clear()


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
