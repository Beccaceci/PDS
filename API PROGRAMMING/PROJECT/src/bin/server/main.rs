use georuggine::server::runtime::{CpuLoggerConfig, Server, ServerConfig};
use std::io;
use std::net::{IpAddr, SocketAddr};

const ADDRESS_OPTION: &str = "--addr";

fn get_local_ip() -> std::io::Result<std::net::IpAddr> {
    use std::net::UdpSocket;

    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("8.8.8.8:80")?;

    Ok(socket.local_addr()?.ip())
}

fn server_address<I>(mut args: I) -> io::Result<String>
where
    I: Iterator<Item = String>,
{
    let Some(option) = args.next() else {
        return Ok("0.0.0.0:8080".to_string());
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

fn advertised_address(bound_address: SocketAddr, local_ip: Option<IpAddr>) -> SocketAddr {
    let advertised_ip = if bound_address.ip().is_unspecified() {
        local_ip.unwrap_or(bound_address.ip())
    } else {
        bound_address.ip()
    };

    SocketAddr::new(advertised_ip, bound_address.port())
}

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    let config = ServerConfig {
        addr: server_address(std::env::args().skip(1))?,
        db_path: "georuggine.db".into(),
        cpu_logger: CpuLoggerConfig::default(),
    };

    let server = Server::bind(config).await?;
    let address = advertised_address(server.addr()?, get_local_ip().ok());
    println!("[SERVER] Client connection address: {address}");

    server.run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_address_overrides_the_default() {
        let address =
            server_address(["--addr".to_string(), "127.0.0.1:0".to_string()].into_iter()).unwrap();

        assert_eq!(address, "127.0.0.1:0");
    }

    #[test]
    fn address_option_requires_a_value() {
        let error = server_address(["--addr".to_string()].into_iter()).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn unknown_options_are_rejected() {
        let error = server_address(["--unknown".to_string()].into_iter()).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn wildcard_bind_advertises_the_local_network_address() {
        let bound_address = "0.0.0.0:43210".parse().unwrap();
        let local_ip = "192.168.1.25".parse().unwrap();

        assert_eq!(
            advertised_address(bound_address, Some(local_ip)),
            "192.168.1.25:43210".parse().unwrap()
        );
    }

    #[test]
    fn explicit_bind_advertises_the_bound_address() {
        let bound_address = "192.0.2.10:9000".parse().unwrap();
        let other_local_ip = "192.168.1.25".parse().unwrap();

        assert_eq!(
            advertised_address(bound_address, Some(other_local_ip)),
            bound_address
        );
    }
}
