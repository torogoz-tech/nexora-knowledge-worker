# Changelog

## 0.4.0

- Extract knowledge synthesis, RAG processing, daily developer analysis, and rating refresh into an independently scalable worker.
- Use RabbitMQ as a wake-up signal and PostgreSQL claims as the source of truth, with startup and reconnect recovery.
