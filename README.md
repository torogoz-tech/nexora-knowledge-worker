# Nexora Knowledge Worker

Version 0.4.0 worker for knowledge synthesis, knowledge processing, daily developer analysis, developer rating refresh, and asynchronous task code reviews and contributor ratings. Knowledge persistence lives in the separately maintained knowledge store. Task review jobs use the dedicated `nexora-task-workflow` API from the immutable `nexora-job-store-v0.4.0-final` release. The bounded OpenAI transport and inference prompts are worker-owned. It does not depend on API source, `nexora-reasoning`, `nexora-platform-store`, or `analysis-engine`, and does not compile the code indexer, lexer, parser, or MCP server.

PostgreSQL remains the source of truth. RabbitMQ wakes workers; this service recovers pending work on startup and broker reconnect, then claims work using PostgreSQL locking so multiple replicas can run concurrently. Pending jobs are drained before consuming new wake-ups. A five-minute scheduler tick only creates due daily analysis jobs and is not a pending-work poll.

## Configuration

- NEXORA_DATABASE_CONNECTIONS: path to Nexora database connection settings.
- NEXORA_DATABASE_SCHEMA: Nexora platform PostgreSQL schema, defaults to `nexora_platform`.
- NEXORA_REASONING_SETTINGS: path to reasoning settings JSON with key_file, model, and effort when no OpenAI runtime configuration is installed.
- NEXORA_RABBITMQ_URL: required in continuous mode.

The worker uses the API-managed OpenAI runtime configuration and encrypted secret when present. The local settings file is a fallback for installations without that runtime configuration. Keep both the settings file and database connection file outside the image and supply them through the deployment's secret mechanism.

Run nexora-knowledge-worker --help for command options. The --once option performs a single PostgreSQL recovery and drain for operator use.
