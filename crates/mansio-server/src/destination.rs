//! Where a webhook may be delivered.
//!
//! A webhook is a request the server makes to an address the admin gave, so an
//! address that names a service inside the network the server runs in (the
//! loopback, a link-local address such as a cloud metadata endpoint, or a
//! private range) is refused unless `MANSIO_WEBHOOK_ALLOW_PRIVATE_DESTINATIONS`
//! says otherwise.
//!
//! The check is made on the address a delivery connects to, at delivery time,
//! and not on the address as written: a host name that resolved to a public
//! address when the webhook was made can resolve to a private one later. The
//! delivery client resolves host names through [`Guard`], which refuses a name
//! with any private address, so the address that was checked is the address
//! that is connected to. A host written as an IP address is not resolved, and
//! [`literal_is_internal`] checks it before the request is made.
//!
//! The client makes no use of a proxy named in the server's environment
//! (`HTTPS_PROXY` and the like) while the check is on: a proxy resolves the
//! host itself, and the address that was checked would not be the one used.

use std::error::Error;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// What a delivery that was refused says, in the listing: no address in it.
pub(crate) const REFUSED: &str =
    "the destination is a loopback, link-local or private address, which is refused";

/// Whether a delivery to this address stays inside the network: the loopback
/// (`127.0.0.0/8`, `::1`), the unspecified address (which reaches the local
/// host), a link-local address (`169.254.0.0/16`, `fe80::/10`), or a private
/// one (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `fc00::/7`, and the
/// deprecated site-local `fec0::/10`). An IPv6 address that carries an IPv4
/// one is judged as the IPv4 address.
pub(crate) fn is_internal(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_internal_v4(ip),
        IpAddr::V6(ip) => is_internal_v6(ip),
    }
}

fn is_internal_v4(ip: Ipv4Addr) -> bool {
    ip.is_loopback() || ip.is_unspecified() || ip.is_link_local() || ip.is_private()
}

fn is_internal_v6(ip: Ipv6Addr) -> bool {
    if ip.is_loopback() || ip.is_unspecified() {
        return true;
    }
    if let Some(mapped) = ip.to_ipv4() {
        return is_internal_v4(mapped);
    }
    let first = ip.segments()[0];
    // fe80::/10 link-local, fec0::/10 site-local, fc00::/7 unique local.
    (first & 0xffc0) == 0xfe80 || (first & 0xffc0) == 0xfec0 || (first & 0xfe00) == 0xfc00
}

/// Whether the address is written with an IP address that is internal. A host
/// name is not judged here: it is judged when it is resolved.
pub(crate) fn literal_is_internal(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    let Some(host) = parsed.host_str() else {
        return false;
    };
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    bare.parse::<IpAddr>().is_ok_and(is_internal)
}

/// The refusal, as the error a resolver gives, so that [`was_refused`] finds it
/// in the chain of a failed request.
#[derive(Debug)]
struct Refused;

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REFUSED)
    }
}

impl Error for Refused {}

/// Whether a request failed because [`Guard`] refused the host it resolved to.
pub(crate) fn was_refused(error: &(dyn Error + 'static)) -> bool {
    let mut at = Some(error);
    while let Some(error) = at {
        if error.is::<Refused>() {
            return true;
        }
        at = error.source();
    }
    false
}

/// A resolver that refuses a host name with an internal address among those
/// it resolves to, and otherwise gives them all.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Guard;

impl Resolve for Guard {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let found: Vec<SocketAddr> =
                tokio::net::lookup_host((name.as_str(), 0)).await?.collect();
            if found.iter().any(|found| is_internal(found.ip())) {
                return Err(Refused.into());
            }
            let addrs: Addrs = Box::new(found.into_iter());
            Ok(addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn internal(text: &str) -> bool {
        is_internal(text.parse().unwrap())
    }

    #[test]
    fn loopback_link_local_and_private_addresses_are_internal() {
        for text in [
            "127.0.0.1",
            "127.255.255.254",
            "0.0.0.0",
            "169.254.169.254",
            "10.0.0.1",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "::1",
            "::",
            "fe80::1",
            "fc00::1",
            "fd12:3456::1",
            "fec0::1",
            "::ffff:127.0.0.1",
            "::ffff:10.1.2.3",
            "::ffff:169.254.169.254",
        ] {
            assert!(internal(text), "{text}");
        }
    }

    #[test]
    fn public_addresses_are_not() {
        for text in [
            "93.184.216.34",
            "8.8.8.8",
            "172.15.0.1",
            "172.32.0.1",
            "192.169.0.1",
            "2606:4700::1111",
            "::ffff:8.8.8.8",
        ] {
            assert!(!internal(text), "{text}");
        }
    }

    #[test]
    fn an_address_written_as_an_ip_is_judged_and_a_name_is_left_to_the_resolver() {
        for url in [
            "http://127.0.0.1:9000/hook",
            "https://[::1]/hook",
            "http://169.254.169.254/latest/meta-data",
            "http://10.0.0.5/",
            // Written in other ways, the URL parser makes it the same address.
            "http://2130706433/",
            "http://0x7f.1/",
            "http://[::ffff:127.0.0.1]/",
        ] {
            assert!(
                literal_is_internal(url),
                "a literal destination case was not internal"
            );
        }
        for url in [
            "https://example.com/hook",
            "http://localhost:9000/",
            "https://93.184.216.34/",
            "not a url",
        ] {
            assert!(
                !literal_is_internal(url),
                "a literal destination case was internal"
            );
        }
    }

    #[tokio::test]
    async fn the_guard_refuses_a_name_that_resolves_to_the_loopback() {
        let name: Name = "localhost".parse().unwrap();
        let error = Guard.resolve(name).await.err().unwrap();
        assert!(was_refused(error.as_ref()), "{error}");
    }
}
