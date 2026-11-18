use clap::Parser;
use nexora_knowledge_worker::KnowledgeWorkerHost;
use nexora_runtime::DatabaseConnections;
use std::process::ExitCode;

mod arguments;
use arguments::Arguments;

#[tokio::main]
async fn main() -> ExitCode {
    match run(Arguments::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("nexora-knowledge-worker: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(arguments: Arguments) -> Result<(), Box<dyn std::error::Error>> {
    let connections = DatabaseConnections::load(&arguments.connections)?;
    KnowledgeWorkerHost::run(
        connections,
        arguments.schema,
        arguments.settings,
        arguments.once,
    )
    .await?;
    Ok(())
}
