use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::SystemTime,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

static SOCKET_PATH: OnceLock<PathBuf> = OnceLock::new();
pub fn socket_path() -> &'static Path {
    SOCKET_PATH
        .get_or_init(|| {
            let mut path = home::home_dir().unwrap();
            path.push(".ssm.sock");
            path
        })
        .as_path()
}

#[derive(Clone, Serialize, Deserialize)]
pub enum NetMessageS2C {
    UpdateServiceConfig { id: Uuid, config: ServiceConfig },
    UpdateServiceStatus { id: Uuid, status: ServiceStatus },
    RemoveService(Uuid),
    ClearLogs,
    AddLog(LogEntry),
}
#[derive(Serialize, Deserialize)]
pub enum NetMessageC2S {
    StartService(Uuid),
    StopService(Uuid),
    KillService(Uuid),
    PauseService(Uuid),
    UnpauseService(Uuid),
    MonitorLog(Uuid),
    CancelMonitorLog,
    UpdateServiceConfig { id: Uuid, config: ServiceConfig },
    RemoveService(Uuid),
    SendIn(String),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub name: String,
    pub executable: PathBuf,
    pub working_directory: PathBuf,
    pub arguments: Vec<String>,
    pub environment: HashMap<String, String>,
    pub auto_start: bool,
    pub show_timestamp: bool,
}
#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum ServiceStatus {
    Down,
    Running,
    Stopping,
    Dead,
    Miscarried,
    Paused,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub text: String,
    pub kind: LogKind,
    pub time: SystemTime,
}
#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum LogKind {
    Out,
    Err,
    In,
}
