# MCP Kafka

A Model Context Protocol (MCP) server for performing Kafka client operations from AI assistants.

## Features

The mcp-kafka server provides the following Kafka operations:

- **Create Topic**: Create a new Kafka topic with configs
- **List Topics**: Get a list of all available Kafka topics in the cluster
- **Delete Topic**: Remove an existing Kafka topic.
- **Describe Topic**: Get detailed information about a specific topic, including partition details, and topic configuration details.
- **Update Topic**: Update topic configuration.
- **List Consumer Groups**: List all consumer groups known to the cluster
- **List Consumer Group Offsets**: List all consumer group offsets for a given consumer group
- **Describe Consumer Groups**: Describe a list of consumer groups
- **Delete Consumer Group**: Delete a consumer group

# Installation and Setup

1. Install uv from [Astral](https://docs.astral.sh/uv/getting-started/installation/)
2. Install Python using `uv python install 3.12`

## Configuration

1. Clone the repository:

  ```sh
  git clone https://github.com/khanghua1505/mcp-kafka.git
  ```

2. Setup mcp server configuration

  ```json
  {
    "servers": {
      "kafka": {
        "command": "uv",
        "args": [
          "--directory",
          "/path/to/mcp-kafka",
          "run",
          "server.py",
          // optional
          "--clusters-config",
          "/path/to/clusters.yaml"
        ]
      }
    }
  }
  ```

  ```yaml
  clusters:
    playground:
      description: "Playground cluster"
      bootstrap_servers: localhost:29092
      ssl:
        cafile: /Users/khanghua/Workspace/projects/mcp-kafka/.ca/kafka.conduktor.truststore_ca.pem
      security_protocol: SASL_SSL
      sasl:
        mechanism: PLAIN
        username: "client"
        password: "client-secret"

  ```
