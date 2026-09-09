use std::os::unix::net::UnixStream;

use crate::common::socket_path;

pub fn run_client() {
    let stream = UnixStream::connect(socket_path()).unwrap();
}
