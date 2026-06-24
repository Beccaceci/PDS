use clap::Parser;


// struct that allows to define the input arguments
// derive allows automatically generating the logic behind the parsing of arguments
#[derive(Parser, Debug)]
pub struct Args {
    #[arg(help = "Percorso del file .csv")]
    pub filename: String
}


// parse the input arguments
pub fn parse_args() -> Result<Args, clap::Error> {
    Args::try_parse()
}