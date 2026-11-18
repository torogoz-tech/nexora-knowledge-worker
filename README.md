# Nexora Knowledge Worker

Version 0.4.0 worker for knowledge synthesis, knowledge processing, daily developer analysis, and developer rating refresh. Its PostgreSQL operations live in the separately maintained knowledge store; it does not use the API's platform store or compile the code indexer, lexer, parser, or MCP server.

PostgreSQL remains the source of truth. RabbitMQ wakes workers; this service recovers pending work on startup and broker reconnect, then claims work using PostgreSQL locking so multiple replicas can run concurrently. Pending jobs are drained before consuming new wake-ups. A five-minute scheduler tick only creates due daily analysis jobs and is not a pending-work poll.

## Configuration

- NEXORA_DATABASE_CONNECTIONS: path to Nexora database connection settings.
- NEXORA_DATABASE_SCHEMA: PostgreSQL schema, defaults to public.
- NEXORA_REASONING_SETTINGS: path to reasoning settings JSON with key_file, model, and effort when no OpenAI runtime configuration is installed.
- NEXORA_RABBITMQ_URL: required in continuous mode.

The worker uses the API-managed OpenAI runtime configuration and encrypted secret when present. The local settings file is a fallback for installations without that runtime configuration. Keep both the settings file and database connection file outside the image and supply them through the deployment's secret mechanism.

Run nexora-knowledge-worker --help for command options. The --once option performs a single PostgreSQL recovery and drain for operator use.
