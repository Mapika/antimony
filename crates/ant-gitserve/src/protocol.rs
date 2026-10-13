//! Strict pkt-line framing shared by requests and streamed responses.
use anyhow::{Result, bail, ensure};

pub fn packet(data: &[u8]) -> Vec<u8> {
    let mut out = format!("{:04x}", data.len() + 4).into_bytes();
    out.extend_from_slice(data);
    out
}

pub struct Request<'a> {
    pub command: &'a str,
    pub args: Vec<&'a str>,
}

pub fn parse(mut input: &[u8]) -> Result<Request<'_>> {
    let mut command = None;
    let mut args = Vec::new();
    let mut arguments = false;
    loop {
        ensure!(input.len() >= 4, "truncated pkt-line");
        let len = usize::from_str_radix(std::str::from_utf8(&input[..4])?, 16)?;
        input = &input[4..];
        match len {
            0 => {
                ensure!(input.is_empty() && arguments, "invalid request termination");
                break;
            }
            1 => {
                ensure!(!arguments && command.is_some(), "unexpected delimiter");
                arguments = true;
            }
            4..=65520 => {
                ensure!(input.len() >= len - 4, "truncated payload");
                let line = std::str::from_utf8(&input[..len - 4])?.trim_end_matches('\n');
                input = &input[len - 4..];
                ensure!(!line.contains(['\0', '\n', '\r']), "invalid line");
                if arguments {
                    args.push(line);
                } else if let Some(value) = line.strip_prefix("command=") {
                    ensure!(command.replace(value).is_none(), "duplicate command");
                } else if line == "object-format=sha256"
                    || line.starts_with("agent=")
                    || line.starts_with("server-option=")
                {
                } else {
                    bail!("unsupported capability");
                }
            }
            _ => bail!("invalid packet length"),
        }
    }
    Ok(Request { command: command.ok_or_else(|| anyhow::anyhow!("missing command"))?, args })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_malformed_framing() {
        for input in ["", "0003", "ffff", "0008x", "zzzz", "0000", "0001", "0002"] {
            assert!(parse(input.as_bytes()).is_err(), "{input}");
        }
        let mut request = packet(b"command=ls-refs\n");
        request.extend_from_slice(b"00010000");
        assert_eq!(parse(&request).unwrap().command, "ls-refs");
        request.extend_from_slice(b"trailing");
        assert!(parse(&request).is_err());
    }
}
