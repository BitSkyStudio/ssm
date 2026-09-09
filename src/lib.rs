use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const SOCKET_PATH: &Path = Path::new("~/.ssm.sock");

pub enum NetMessageS2C {
    UpdateServiceConfig(ServiceConfig),
    UpdateServiceStatus { id: Uuid, status: ServiceStatus },
    RemoveService(Uuid),
    ClearLogs,
    AddLog(LogEntry),
}
pub enum NetMessageC2S {
    StartService(Uuid),
    StopService(Uuid),
    KillService(Uuid),
    PauseService(Uuid),
    UnpauseService(Uuid),
    MonitorLog(Uuid),
    CancelMonitorLog,
    UpdateServiceConfig(ServiceConfig),
    RemoveService(Uuid),
    SendIn(String),
}

#[derive(Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ServiceConfig {
    pub id: Uuid,
    pub name: String,
    pub executable: PathBuf,
    pub working_directory: PathBuf,
    pub arguments: Vec<String>,
    pub environment: HashMap<String, String>,
    pub autostart: bool,
}
#[derive(Copy, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum ServiceStatus {
    Down,
    Running,
    Stopping,
    Dead,
    Miscarried,
    Paused,
}
#[derive(Clone)]
pub enum LogEntry {
    Out(String),
    Err(String),
    In(String),
}
