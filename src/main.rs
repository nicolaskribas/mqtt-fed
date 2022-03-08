use clap::Parser;
use std::process;
use tracing::error;
use tracing_subscriber::EnvFilter;

mod announcer;
mod conf;
mod federator;
mod message;
mod worker;

fn main() -> Result<(), ()> {
    tracing_subscriber::fmt()
        .without_time()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let Args { config_file } = Args::parse();

    let config = conf::load(&config_file).unwrap_or_else(|err| {
        error!(%config_file, "problem reading the configuration file: {err}");
        process::exit(1);
    });

    federator::run(config)
}

#[derive(Parser)]
struct Args {
    #[clap(short, default_value = "mqtt-fed.toml")]
    config_file: String,
}
