use clap::Parser;
use std::process;
use tracing::error;

mod announcer;
mod conf;
mod federator;
mod handler;
mod message;
mod parents;
mod topic;

fn main() {
    tracing_subscriber::fmt::init();

    let Args { config_file } = Args::parse();

    let config = conf::load(&config_file).unwrap_or_else(|err| {
        error!("Problem reading the configuration file: {err}");
        process::exit(1);
    });

    federator::run(config);
}

#[derive(Parser)]
struct Args {
    #[clap(short, default_value = "mqtt-fed.toml")]
    config_file: String,
}
