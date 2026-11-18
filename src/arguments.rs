use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(version, about = "Nexora knowledge processing worker")]
pub struct Arguments {
    #[arg(long, env = "NEXORA_DATABASE_CONNECTIONS")]
    pub connections: PathBuf,
    #[arg(
        long,
        env = "NEXORA_DATABASE_SCHEMA",
        default_value = "nexora_platform"
    )]
    pub schema: String,
    #[arg(long, env = "NEXORA_REASONING_SETTINGS")]
    pub settings: PathBuf,
    #[arg(long, env = "NEXORA_WORKER_ONCE", default_value_t = false)]
    pub once: bool,
}
