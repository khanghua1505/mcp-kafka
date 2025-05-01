"""mcp-kafka server core."""

import os
import argparse
import copy
import configparser
import subprocess
import shutil

from loguru import logger
from kafka import KafkaAdminClient

from mcp_kafka.utils.string import parse_value


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
        client_properties_file: The client properties file config.
            File format:
                security.protocol=""            # PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
                sasl.mechanism=""               # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
                sasl.jaas.config=""             # Login config with username/password
                ssl.truststore.location=""      # JKS file with trusted CA certs (Java only)
                ssl.truststore.password=""      # Password for truststore
    """
    DEFAULT_CONFIG = {
        'use_sse': False,
        'port': 8888,
        'bootstrap_servers': 'localhost:9092',
        'client_properties_file': '',
    }

    def __init__(self, **configs):
        logger.debug(f"Starting MCP Core with configuration: {configs}")
        extra_configs = set(configs).difference(self.DEFAULT_CONFIG)
        if extra_configs:
            raise KeyError(f"Unrecognized configs: {extra_configs}")
        self.config = copy.copy(self.DEFAULT_CONFIG)
        self.config.update(configs)
        if self.config['client_properties_file']:
            client_config = self._parse_client_properties(self.config['client_properties_file'])
            self.config['client_config'] = client_config

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
        parser.add_argument('--kafka-client-properties', type=str,
                            default='', help='Include client properties configuration')
        args = parser.parse_args()

        core = Core(
            use_sse=args.sse,
            port=args.port,
            bootstrap_servers=args.kafka_bootstrap_servers,
            client_properties_file=args.kafka_client_properties
        )

        return core

    def _parse_client_properties(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            content = '[client]\n' + f.read()

        config = configparser.ConfigParser()
        config.read_string(content)
        config = config['client']

        client_config = {}

        if 'security.protocol' in config:
            client_config['security_protocol'] = config['security.protocol']
        if 'sasl.mechanism' in config:
            client_config['sasl_mechanism'] = config['sasl.mechanism']
        if 'sasl.jaas.config' in config:
            jaas = config['sasl.jaas.config']
            username = parse_value('username', jaas)
            if username:
                client_config['sasl_plain_username'] = username
            password = parse_value('password', jaas)
            if password:
                client_config['sasl_plain_password'] = password
        if 'ssl.truststore.location' in config:
            ca_dir = '.ca'
            if os.path.isdir(ca_dir):
                shutil.rmtree(ca_dir)

            jks_path = config['ssl.truststore.location']
            jks_password = config.get('ssl.truststore.password', None)

            pem_file_path = self._convert_jks_to_pem(jks_path, jks_password, ca_dir)
            client_config['ssl_cafile'] = pem_file_path
        if 'ssl.cafile' in config:
            client_config['ssl_cafile'] = config['ssl.cafile']
        if 'ssl.certfile' in config:
            client_config['ssl_certfile'] = config['ssl.certfile']
        if 'ssl.keyfile' in config:
            client_config['ssl_keyfile'] = config['ssl.keyfile']

        return client_config

    def _convert_jks_to_pem(self, jks_path: str, jks_password: str, output_dir: dir):
        if not os.path.isfile(jks_path):
            raise FileNotFoundError(f"JKS file is not a file: {jks_path}")

        os.makedirs(output_dir, exist_ok=True)
        base_name = os.path.splitext(os.path.basename(jks_path))[0]
        p12_path = os.path.join(output_dir, f"{base_name}.p12")
        pem_path = os.path.join(output_dir, f"{base_name}_ca.pem")

        subprocess.run([
            'keytool', '-importkeystore',
            '-srckeystore', jks_path,
            '-destkeystore', p12_path,
            '-srcstoretype', 'JKS',
            '-deststoretype', 'PKCS12',
            *([
                '-srcstorepass', jks_password,
                '-deststorepass', jks_password,
            ] if jks_password else [])
        ], check=True)

        subprocess.run([
            'openssl', 'pkcs12',
            '-in', p12_path,
            '-nokeys',
            '-out', pem_path,
            *([
                '-passin', f"pass:{jks_password}"
            ] if jks_password else [])
        ], check=True)

        logger.debug(f"PEM file created: {pem_path}")
        return pem_path

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
            configs = dict(
                bootstrap_servers=self.config['bootstrap_servers'],
                client_id='mcp-kafka-admin',
                **self.config.get('client_config', {}),
            )

            self._kafka_admin_client = KafkaAdminClient(**configs)

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
