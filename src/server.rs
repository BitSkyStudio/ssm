use std::{
    collections::HashMap,
    io::{self, BufRead, BufReader, Write},
    os::unix::net::{UnixListener, UnixStream},
    process::{ChildStdin, Command, ExitStatus, Stdio},
    sync::{
        Arc,
        mpsc::{Sender, channel},
    },
};

use bincode::config::standard;
use shared_child::{SharedChild, unix::SharedChildExt};
use uuid::Uuid;

use crate::common::{
    LogEntry, NetMessageC2S, NetMessageS2C, ServiceConfig, ServiceStatus, socket_path,
};

type ServerTx = Sender<ServerMessage>;

pub fn run_server() {
    let (tx, rx) = channel();
    {
        let tx = tx.clone();
        std::thread::spawn(move || socket_server(tx));
    }
    let mut clients: HashMap<Uuid, ClientConnection> = HashMap::new();
    let mut services: HashMap<Uuid, Service> = HashMap::new();
    for service in services.values_mut() {
        if service.config.autostart {
            service.try_start(tx.clone());
        }
    }
    macro_rules! broadcast {
        ($message: expr) => {
            let msg = $message;
            clients.values_mut().for_each(|client| {
                client.send(msg.clone());
            });
        };
    }
    loop {
        match rx.recv().unwrap() {
            ServerMessage::Process { id, message } => {
                let Some(service) = services.get_mut(&id) else {
                    continue;
                };
                match message {
                    ProcessMessage::Log(log) => {
                        clients.values_mut().for_each(|client| {
                            if client.observing_log == Some(id) {
                                client.send(NetMessageS2C::AddLog(log.clone()));
                            }
                        });
                        service.logs.push(log);
                    }
                    ProcessMessage::Exit(code) => {
                        service.process = None;
                        service.status = if code.success() {
                            ServiceStatus::Down
                        } else {
                            ServiceStatus::Dead
                        };
                        broadcast!(NetMessageS2C::UpdateServiceStatus {
                            id,
                            status: service.status
                        });
                    }
                }
            }
            ServerMessage::ClientConnect(mut client) => {
                for (id, service) in &services {
                    let id = *id;
                    client.send(NetMessageS2C::UpdateServiceConfig {
                        id,
                        config: service.config.clone(),
                    });
                    client.send(NetMessageS2C::UpdateServiceStatus {
                        id,
                        status: service.status,
                    });
                }
                clients.insert(client.id, client);
            }
            ServerMessage::ClientMessage { client, message } => match message {
                NetMessageC2S::StartService(id) => {
                    let Some(service) = services.get_mut(&id) else {
                        continue;
                    };
                    service.try_start(tx.clone());
                    broadcast!(NetMessageS2C::UpdateServiceStatus {
                        id,
                        status: service.status
                    });
                }
                NetMessageC2S::StopService(id) => {
                    let Some(service) = services.get_mut(&id) else {
                        continue;
                    };
                    match service.status {
                        ServiceStatus::Running => {}
                        _ => continue,
                    }
                    let _ = service.process.as_ref().unwrap().child.send_signal(15); //SIGTERM
                    service.status = ServiceStatus::Stopping;
                    broadcast!(NetMessageS2C::UpdateServiceStatus {
                        id,
                        status: service.status
                    });
                }
                NetMessageC2S::KillService(id) => {
                    let Some(service) = services.get_mut(&id) else {
                        continue;
                    };
                    match service.status {
                        ServiceStatus::Running | ServiceStatus::Stopping => {}
                        _ => continue,
                    }
                    let _ = service.process.as_ref().unwrap().child.kill();
                }
                NetMessageC2S::MonitorLog(id) => {
                    let Some(service) = services.get_mut(&id) else {
                        continue;
                    };
                    let Some(client) = clients.get_mut(&client) else {
                        continue;
                    };
                    client.observing_log = Some(id);
                    client.send(NetMessageS2C::ClearLogs);
                    for log in &service.logs {
                        client.send(NetMessageS2C::AddLog(log.clone()));
                    }
                }
                NetMessageC2S::CancelMonitorLog => {
                    let Some(client) = clients.get_mut(&client) else {
                        continue;
                    };
                    client.observing_log = None;
                }
                NetMessageC2S::UpdateServiceConfig { id, config } => {
                    let Some(service) = services.get_mut(&id) else {
                        continue;
                    };
                    service.config = config;
                    broadcast!(NetMessageS2C::UpdateServiceConfig {
                        id,
                        config: service.config.clone()
                    });
                }
                NetMessageC2S::RemoveService(id) => {
                    let Some(service) = services.get_mut(&id) else {
                        continue;
                    };
                    match service.status {
                        ServiceStatus::Running | ServiceStatus::Stopping => {
                            continue;
                        }
                        _ => {}
                    }
                    services.remove(&id);
                    broadcast!(NetMessageS2C::RemoveService(id));
                }
                NetMessageC2S::SendIn(message) => {
                    let Some(client) = clients.get_mut(&client) else {
                        continue;
                    };
                    let Some(observing) = &client.observing_log else {
                        continue;
                    };
                    let Some(service) = services.get_mut(observing) else {
                        continue;
                    };
                    if let Some(process) = &mut service.process {
                        let log = LogEntry::In(message.clone());
                        clients.values_mut().for_each(|client| {
                            if client.observing_log == Some(service.id) {
                                client.send(NetMessageS2C::AddLog(log.clone()));
                            }
                        });
                        service.logs.push(log);
                        let _ = process.stdin.write_all(message.as_bytes());
                    }
                }
                NetMessageC2S::PauseService(id) => {
                    let Some(service) = services.get_mut(&id) else {
                        continue;
                    };
                    match service.status {
                        ServiceStatus::Running => {}
                        _ => continue,
                    }
                    let _ = service.process.as_ref().unwrap().child.send_signal(19); //SIGSTOP
                    service.status = ServiceStatus::Paused;
                    broadcast!(NetMessageS2C::UpdateServiceStatus {
                        id,
                        status: service.status
                    });
                }
                NetMessageC2S::UnpauseService(id) => {
                    let Some(service) = services.get_mut(&id) else {
                        continue;
                    };
                    match service.status {
                        ServiceStatus::Paused => {}
                        _ => continue,
                    }
                    let _ = service.process.as_ref().unwrap().child.send_signal(18); //SIGCONT
                    service.status = ServiceStatus::Running;
                    broadcast!(NetMessageS2C::UpdateServiceStatus {
                        id,
                        status: service.status
                    });
                }
            },
            ServerMessage::ClientDisconnect(id) => {
                clients.remove(&id);
            }
        }
    }
}
struct Service {
    id: Uuid,
    process: Option<ServiceProcess>,
    status: ServiceStatus,
    logs: Vec<LogEntry>,
    config: ServiceConfig,
}
impl Service {
    pub fn try_start(&mut self, tx: ServerTx) {
        match self.status {
            ServiceStatus::Running | ServiceStatus::Stopping => return,
            _ => {}
        }
        self.logs.clear();
        match ServiceProcess::start(self.id, &self.config, tx) {
            Ok(process) => {
                self.process = Some(process);
                self.status = ServiceStatus::Running;
            }
            Err(error) => {
                let mut message = String::new();
                use std::fmt::Write;
                let _ = write!(&mut message, "{:?}", error);
                self.logs.push(LogEntry::Err(message));
                self.status = ServiceStatus::Miscarried;
            }
        }
    }
}

struct ServiceProcess {
    stdin: ChildStdin,
    child: Arc<SharedChild>,
}
impl ServiceProcess {
    fn start(id: Uuid, config: &ServiceConfig, tx: ServerTx) -> io::Result<ServiceProcess> {
        /*if !std::fs::exists(&config.executable) || !std::fs::exists(&config.working_directory) {
            println!("invalid config");
        }*/
        let mut command = Command::new(&config.executable);
        command
            .current_dir(&config.working_directory)
            .args(&config.arguments[..])
            .envs(&config.environment)
            .stdout(Stdio::piped())
            .stdin(Stdio::piped())
            .stderr(Stdio::piped());
        let child = Arc::new(SharedChild::spawn(&mut command)?);
        let stdout = child.take_stdout().unwrap();
        let tx2 = tx.clone();
        std::thread::spawn(move || {
            let stdout = BufReader::new(stdout);
            for line in stdout.lines() {
                let Ok(line) = line else { break };
                let _ = tx2.send(ServerMessage::Process {
                    id,
                    message: ProcessMessage::Log(LogEntry::Out(line)),
                });
            }
        });
        let stderr = child.take_stderr().unwrap();
        let tx2 = tx.clone();
        std::thread::spawn(move || {
            let stdout = BufReader::new(stderr);
            for line in stdout.lines() {
                let Ok(line) = line else { break };
                let _ = tx2.send(ServerMessage::Process {
                    id,
                    message: ProcessMessage::Log(LogEntry::Err(line)),
                });
            }
        });
        let child2 = child.clone();
        let tx2 = tx.clone();
        std::thread::spawn(move || {
            if let Ok(status) = child2.wait() {
                let _ = tx2.send(ServerMessage::Process {
                    id,
                    message: ProcessMessage::Exit(status),
                });
            }
        });
        Ok(ServiceProcess {
            stdin: child.take_stdin().unwrap(),
            child,
        })
    }
}
enum ServerMessage {
    Process {
        id: Uuid,
        message: ProcessMessage,
    },
    ClientConnect(ClientConnection),
    ClientMessage {
        client: Uuid,
        message: NetMessageC2S,
    },
    ClientDisconnect(Uuid),
}
enum ProcessMessage {
    Log(LogEntry),
    Exit(ExitStatus),
}
fn socket_server(tx: ServerTx) {
    if std::fs::exists(socket_path()).unwrap_or(true) {
        let _ = std::fs::remove_file(socket_path());
    }
    let listener = UnixListener::bind(socket_path()).unwrap();
    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        let id = Uuid::new_v4();
        let _ = tx.send(ServerMessage::ClientConnect(ClientConnection {
            id,
            stream: stream.try_clone().unwrap(),
            observing_log: None,
        }));
        let tx = tx.clone();
        std::thread::spawn(move || {
            loop {
                match bincode::serde::decode_from_std_read::<NetMessageC2S, _, _>(
                    &mut stream,
                    standard(),
                ) {
                    Ok(message) => {
                        let _ = tx.send(ServerMessage::ClientMessage {
                            client: id,
                            message,
                        });
                    }
                    Err(_) => {
                        let _ = tx.send(ServerMessage::ClientDisconnect(id));
                        break;
                    }
                }
            }
        });
    }
}
struct ClientConnection {
    id: Uuid,
    stream: UnixStream,
    observing_log: Option<Uuid>,
}
impl ClientConnection {
    pub fn send(&mut self, message: NetMessageS2C) {
        let _ = bincode::serde::encode_into_std_write(message, &mut self.stream, standard());
    }
}
