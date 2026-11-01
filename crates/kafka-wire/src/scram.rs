use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256, Sha512};
use std::collections::BTreeMap;

pub fn first(username: &str, nonce: &str) -> Result<String> {
    let username =
        stringprep::saslprep(username).map_err(|_| anyhow::anyhow!("invalid SCRAM username"))?;
    Ok(format!(
        "n={},r={nonce}",
        username.replace('=', "=3D").replace(',', "=2C")
    ))
}
fn attributes(message: &str) -> Result<BTreeMap<&str, &str>> {
    let mut result = BTreeMap::new();
    for part in message.split(',') {
        let (key, value) = part.split_once('=').context("invalid SCRAM attribute")?;
        if key.len() != 1 || result.insert(key, value).is_some() {
            bail!("invalid SCRAM attributes")
        }
    }
    if result.contains_key("m") {
        bail!("unsupported mandatory SCRAM extension")
    }
    Ok(result)
}

pub fn respond(
    mechanism: &str,
    password: &str,
    nonce: &str,
    first: &str,
    server: &str,
) -> Result<(String, Vec<u8>)> {
    let attrs = attributes(server)?;
    let server_nonce = attrs.get("r").context("missing SCRAM nonce")?;
    if !server_nonce.starts_with(nonce) || server_nonce.len() <= nonce.len() {
        bail!("invalid SCRAM server nonce")
    }
    let salt = STANDARD.decode(attrs.get("s").context("missing SCRAM salt")?)?;
    let iterations: u32 = attrs
        .get("i")
        .context("missing SCRAM iterations")?
        .parse()?;
    // A bounded work factor prevents a malicious broker monopolizing the runtime.
    if !(4096..=1_000_000).contains(&iterations) {
        bail!("unsafe SCRAM iteration count")
    }
    let password =
        stringprep::saslprep(password).map_err(|_| anyhow::anyhow!("invalid SCRAM password"))?;
    let final_without_proof = format!("c=biws,r={server_nonce}");
    let auth = format!("{first},{server},{final_without_proof}");
    macro_rules! calculate {
        ($hash:ty, $length:expr) => {{
            let mut salted = [0u8; $length];
            pbkdf2::pbkdf2_hmac::<$hash>(password.as_bytes(), &salt, iterations, &mut salted);
            let mac = |key: &[u8], data: &[u8]| -> Vec<u8> {
                let mut h =
                    Hmac::<$hash>::new_from_slice(key).expect("HMAC accepts any key length");
                h.update(data);
                h.finalize().into_bytes().to_vec()
            };
            let client_key = mac(&salted, b"Client Key");
            let stored_key = <$hash>::digest(&client_key);
            let signature = mac(&stored_key, auth.as_bytes());
            let proof: Vec<_> = client_key
                .iter()
                .zip(signature)
                .map(|(a, b)| a ^ b)
                .collect();
            let server_key = mac(&salted, b"Server Key");
            (proof, mac(&server_key, auth.as_bytes()))
        }};
    }
    let (proof, signature) = match mechanism {
        "SCRAM-SHA-256" => calculate!(Sha256, 32),
        "SCRAM-SHA-512" => calculate!(Sha512, 64),
        _ => bail!("unsupported SCRAM mechanism"),
    };
    Ok((
        format!("{final_without_proof},p={}", STANDARD.encode(proof)),
        signature,
    ))
}

pub fn verify(message: &[u8], expected: &[u8]) -> Result<()> {
    let attrs = attributes(std::str::from_utf8(message).context("invalid SCRAM final message")?)?;
    if attrs.contains_key("e") {
        bail!("SCRAM authentication rejected")
    }
    let actual = STANDARD.decode(attrs.get("v").context("missing SCRAM verifier")?)?;
    if actual.len() != expected.len() || !openssl::memcmp::eq(&actual, expected) {
        bail!("SCRAM server signature mismatch")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rfc7677_vector() {
        let nonce = "rOprNGfwEbeRWgbNEkqO";
        let server = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let (reply, signature) = respond(
            "SCRAM-SHA-256",
            "pencil",
            nonce,
            &first("user", nonce).unwrap(),
            server,
        )
        .unwrap();
        assert!(reply.ends_with("p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="));
        verify(
            b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=",
            &signature,
        )
        .unwrap();
        assert!(verify(b"v=AAAA", &signature).is_err());
    }
    #[test]
    fn rejects_bad_challenges() {
        for server in [
            "r=other,s=YQ==,i=4096",
            "r=nonceX,s=YQ==,i=1",
            "r=nonceX,s=YQ==,i=1000001",
            "r=nonceX,r=nonceY,s=YQ==,i=4096",
            "m=required,r=nonceX,s=YQ==,i=4096",
        ] {
            assert!(respond("SCRAM-SHA-256", "secret", "nonce", "n=u,r=nonce", server).is_err());
        }
    }
}
