use crate::lib::SOCKET_PATH;

fn run_client() {
    let stream = UnixStream::connect(SOCKET_PATH).unwrap();
}
