use clap::Parser;

// Parser tells clap to generate parsing logic for this struct
// Debug lets you print the struct with {:?} or dbg!()
#[derive(Parser, Debug)]
pub struct Args {
    pub filename: String,
    #[arg(long)] // means it must be passed like --mode ...
    pub mode: String,
    #[arg(long)]
    pub column: Option<String>,
    #[arg(long)]
    pub filter: Option<String>
}

pub fn parse_args () -> Result<Args, clap::Error> {
    // this function tries to parse the command-line arguments and returns:
    // Ok(Args) if parsing succeeds
    // Err(clap::Error) if parsing fails
    Args::try_parse()
}