# Nexora Knowledge Worker

Version 0.4.0 standalone worker for knowledge synthesis, knowledge processing, daily developer analysis, and developer rating refresh.

PostgreSQL remains the source of truth. RabbitMQ wakes workers; this service recovers pending work on startup and broker reconnect, then claims work using PostgreSQL locking so multiple replicas can run concurrently. A five-minute scheduler tick only creates due daily analysis jobs and is not a pending-work poll.

## Configuration

- NEXORA_DATABASE_CONNECTIONS: path to Nexora database connection settings.
- NEXORA_DATABASE_SCHEMA: PostgreSQL schema, defaults to public.
- NEXORA_REASONING_SETTINGS: path to reasoning settings JSON with key_file, model, and effort when no OpenAI runtime configuration is installed.
- NEXORA_RABBITMQ_URL: required in continuous mode.

Run nexora-knowledge-worker --help for command options. The --once option performs a single PostgreSQL recovery and drain for operator use.
