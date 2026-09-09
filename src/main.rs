mod client;
mod common;
mod server;

use clap::Parser;

use crate::{client::run_client, server::run_server};

#[derive(clap::clap_derive::Parser)]
#[command(name = "ssm")]
#[command(about = "Simple Service Manager")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}
#[derive(clap::clap_derive::Subcommand)]
enum Commands {
    Server,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Some(Commands::Server) => {
            run_server();
        }
        None => {
            run_client();
        }
    }
}
