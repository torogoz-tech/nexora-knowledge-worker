use serde_json::Value;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    time::Duration,
};
pub(crate) fn receive(listener: &TcpListener) -> (TcpStream, String, Value) {
    let (mut socket, _) = listener.accept().unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let n = socket.read(&mut buffer).unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]).to_string();
            let size: usize = headers
                .lines()
                .find_map(|l| {
                    l.to_lowercase()
                        .strip_prefix("content-length:")
                        .map(|v| v.trim().parse().unwrap())
                })
                .unwrap();
            if bytes.len() >= end + 4 + size {
                let body = serde_json::from_slice(&bytes[end + 4..end + 4 + size]).unwrap();
                return (socket, headers, body);
            }
        }
    }
}
pub(crate) fn reply(mut socket: TcpStream, status: u16, value: Value) {
    let payload = value.to_string();
    write!(socket,"HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",payload.len(),payload).unwrap();
}
