use georuggine::client::runtime::{ClientConfig, run};
use std::io;
use std::time::Duration;

const ADDRESS_OPTION: &str = "--addr";
const DEFAULT_SERVER_ADDRESS: &str = "127.0.0.1:8080";

fn server_address<I>(mut args: I) -> io::Result<String>
where
    I: Iterator<Item = String>,
{
    let Some(option) = args.next() else {
        return Ok(DEFAULT_SERVER_ADDRESS.to_string());
    };

    if option != ADDRESS_OPTION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown option '{option}'; expected {ADDRESS_OPTION} <HOST:PORT>"),
        ));
    }

    let address = args
        .next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{ADDRESS_OPTION} requires a HOST:PORT value"),
            )
        })?;

    if args.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unexpected extra command-line arguments",
        ));
    }

    Ok(address)
}

/// Entry point for the Georuggine Vehicle Terminal (Client).
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig {
        addr: server_address(std::env::args().skip(1))?,
        route: "torino_asti.csv".to_string(),
        interval: Duration::from_secs(30),
    };

    run(config).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_the_existing_default_without_an_override() {
        assert_eq!(
            server_address(std::iter::empty()).unwrap(),
            DEFAULT_SERVER_ADDRESS
        );
    }

    #[test]
    fn configured_address_overrides_the_default() {
        let address =
            server_address(["--addr".to_string(), "192.0.2.10:9000".to_string()].into_iter())
                .unwrap();

        assert_eq!(address, "192.0.2.10:9000");
    }

    #[test]
    fn address_option_requires_a_value() {
        let error = server_address(["--addr".to_string()].into_iter()).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
