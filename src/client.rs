use std::{
    collections::HashMap,
    io, iter,
    os::unix::net::UnixStream,
    process::exit,
    sync::mpsc::{Receiver, channel},
    thread,
};

use bincode::config::standard;
use crossterm::event::{self, Event, KeyCode, MouseEventKind};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, HorizontalAlignment, Layout},
    style::{Color, Modifier},
    text::{Line, Text},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
};

use ratatui::style::Stylize;
use ratatui_textarea::TextArea;
use time::{OffsetDateTime, format_description};
use uuid::Uuid;

use crate::common::{
    LogEntry, LogKind, NetMessageC2S, NetMessageS2C, ServiceConfig, ServiceStatus, socket_path,
};

pub fn run_client() {
    let Ok(stream) = UnixStream::connect(socket_path()) else {
        println!("server is not started!");
        exit(0)
    };
    ratatui::run(move |terminal| App::new(stream).run(terminal)).unwrap();
}
struct ServerConnection(UnixStream);
impl ServerConnection {
    fn send(&mut self, message: NetMessageC2S) {
        let _ = bincode::serde::encode_into_std_write(message, &mut self.0, standard());
    }
}
enum AppMessage {
    Net(NetMessageS2C),
    TermEvent(Event),
}
struct App {
    connection: ServerConnection,
    rx: Receiver<AppMessage>,
    exit: bool,
    services: HashMap<Uuid, ServiceConfig>,
    statuses: HashMap<Uuid, ServiceStatus>,
    state: AppStateKind,
}
impl App {
    fn new(stream: UnixStream) -> App {
        let (tx, rx) = channel();
        let mut stream2 = stream.try_clone().unwrap();
        let tx2 = tx.clone();
        thread::spawn(move || {
            loop {
                match bincode::serde::decode_from_std_read::<NetMessageS2C, _, _>(
                    &mut stream2,
                    standard(),
                ) {
                    Ok(message) => {
                        tx2.send(AppMessage::Net(message)).unwrap();
                    }
                    Err(_) => {
                        panic!("connection closed");
                    }
                }
            }
        });
        thread::spawn(move || {
            loop {
                let event = event::read().unwrap();
                tx.send(AppMessage::TermEvent(event)).unwrap();
            }
        });
        App {
            connection: ServerConnection(stream),
            rx,
            exit: false,
            services: HashMap::new(),
            statuses: HashMap::new(),
            state: AppStateKind::ServiceList(AppStateServiceList::new()),
        }
    }
    fn with_app_ref(&mut self, callback: impl FnOnce(&mut dyn AppState, &mut AppRef)) {
        let mut app_ref = AppRef {
            connection: &mut self.connection,
            services: &mut self.services,
            statuses: &mut self.statuses,
            next_state: None,
            exit: &mut self.exit,
        };
        callback(self.state.app_state(), &mut app_ref);
        if let Some(next_state) = app_ref.next_state {
            self.state = next_state;
        }
    }
    fn run(&mut self, terminal: &mut DefaultTerminal) -> io::Result<()> {
        while !self.exit {
            while {
                let mut repeat = false;
                terminal.draw(|frame| {
                    self.with_app_ref(|state, app| repeat |= state.render(app, frame));
                })?;
                repeat
            } {}

            match self.rx.recv().unwrap() {
                AppMessage::Net(message) => {
                    self.with_app_ref(|state, app| state.handle_message(app, &message));
                    match message {
                        NetMessageS2C::UpdateServiceConfig { id, config } => {
                            self.services.insert(id, config);
                        }
                        NetMessageS2C::UpdateServiceStatus { id, status } => {
                            self.statuses.insert(id, status);
                        }
                        NetMessageS2C::RemoveService(id) => {
                            self.services.remove(&id);
                            self.statuses.remove(&id);
                        }
                        _ => {}
                    }
                }
                AppMessage::TermEvent(event) => {
                    self.with_app_ref(|state, app| state.handle_event(app, &event));
                }
            }
        }
        Ok(())
    }
}
struct AppRef<'a> {
    connection: &'a mut ServerConnection,
    services: &'a mut HashMap<Uuid, ServiceConfig>,
    statuses: &'a mut HashMap<Uuid, ServiceStatus>,
    next_state: Option<AppStateKind>,
    exit: &'a mut bool,
}
trait AppState {
    fn render(&mut self, app: &mut AppRef, frame: &mut Frame) -> bool;
    fn handle_event(&mut self, app: &mut AppRef, event: &Event);
    fn handle_message(&mut self, app: &mut AppRef, message: &NetMessageS2C);
}
enum AppStateKind {
    ServiceList(AppStateServiceList),
    LogMonitor(AppStateLogMonitor),
}
impl AppStateKind {
    fn app_state(&mut self) -> &mut dyn AppState {
        match self {
            AppStateKind::ServiceList(state) => state,
            AppStateKind::LogMonitor(state) => state,
        }
    }
}
struct AppStateServiceList {
    list_state: ListState,
    last_selected_service: Option<Uuid>,
    next_select_service: Option<Uuid>,
}
impl AppStateServiceList {
    fn new() -> AppStateServiceList {
        AppStateServiceList {
            list_state: ListState::default().with_selected(Some(0)),
            last_selected_service: None,
            next_select_service: None,
        }
    }
}
impl AppState for AppStateServiceList {
    fn render(&mut self, app: &mut AppRef, frame: &mut Frame) -> bool {
        let mut service_list = app.services.keys().cloned().collect::<Vec<_>>();
        service_list.sort_by_key(|id| &app.services.get(id).unwrap().name);
        if let Some(to_select) = self.next_select_service.take() {
            if let Some(index) = service_list.iter().position(|s| *s == to_select) {
                self.list_state.select(Some(index));
            }
        }
        let mut list = Vec::new();
        for id in &service_list {
            let service = app.services.get(id).unwrap();
            let status = app.statuses.get(id).unwrap_or(&ServiceStatus::Down);
            let mut text = Text::default();
            text.push_line(Line::from(format!("{} - {:?}", service.name, status)));
            list.push(ListItem::new(text));
        }

        let list = List::new(list)
            .style(Color::White)
            .highlight_style(Modifier::BOLD)
            .highlight_symbol("> ");

        let chunks =
            Layout::vertical([Constraint::Min(0), Constraint::Length(2)]).split(frame.area());

        frame.render_stateful_widget(list, chunks[0], &mut self.list_state);

        frame.render_widget(
            Paragraph::new(Text::raw(
                "[S]Start [E]Stop [K]Kill [O]Pause [P]Continue [Enter]Monitor",
            ))
            .block(Block::default().borders(Borders::ALL.difference(Borders::BOTTOM))),
            chunks[1],
        );

        self.last_selected_service = match self.list_state.selected() {
            Some(selected) => service_list.get(selected).cloned(),
            None => None,
        };
        false
    }
    fn handle_event(&mut self, app: &mut AppRef, event: &Event) {
        match event {
            Event::Key(key_event) => {
                if key_event.is_press() {
                    match key_event.code {
                        KeyCode::Up => {
                            self.list_state.select_previous();
                        }
                        KeyCode::Down => {
                            self.list_state.select_next();
                        }
                        KeyCode::Enter => {
                            if let Some(id) = self.last_selected_service {
                                app.connection.send(NetMessageC2S::MonitorLog(id));
                                app.next_state =
                                    Some(AppStateKind::LogMonitor(AppStateLogMonitor::new(id)));
                            }
                        }
                        KeyCode::Char('q') => {
                            *app.exit = true;
                        }
                        KeyCode::Char('s') => {
                            let Some(id) = self.last_selected_service else {
                                return;
                            };
                            app.connection.send(NetMessageC2S::StartService(id));
                        }
                        KeyCode::Char('e') => {
                            let Some(id) = self.last_selected_service else {
                                return;
                            };
                            app.connection.send(NetMessageC2S::StopService(id));
                        }
                        KeyCode::Char('k') => {
                            let Some(id) = self.last_selected_service else {
                                return;
                            };
                            app.connection.send(NetMessageC2S::KillService(id));
                        }
                        KeyCode::Char('o') => {
                            let Some(id) = self.last_selected_service else {
                                return;
                            };
                            app.connection.send(NetMessageC2S::PauseService(id));
                        }
                        KeyCode::Char('p') => {
                            let Some(id) = self.last_selected_service else {
                                return;
                            };
                            app.connection.send(NetMessageC2S::UnpauseService(id));
                        }
                        _ => {}
                    }
                }
            }
            Event::Mouse(mouse_event) => match mouse_event.kind {
                MouseEventKind::ScrollUp => {
                    self.list_state.select_previous();
                }
                MouseEventKind::ScrollDown => {
                    self.list_state.select_next();
                }
                _ => {}
            },
            _ => {}
        }
    }
    fn handle_message(&mut self, _app: &mut AppRef, message: &NetMessageS2C) {
        match message {
            NetMessageS2C::UpdateServiceConfig { .. } => {
                if self.list_state.selected().is_none() {
                    self.list_state.select_first();
                }
            }
            _ => {}
        }
    }
}
struct AppStateLogMonitor {
    logs: Vec<LogEntry>,
    input_box: TextArea<'static>,
    service: Uuid,
}
impl AppStateLogMonitor {
    pub fn new(service: Uuid) -> AppStateLogMonitor {
        AppStateLogMonitor {
            logs: Vec::new(),
            input_box: TextArea::default(),
            service,
        }
    }
}
impl AppState for AppStateLogMonitor {
    fn render(&mut self, app: &mut AppRef, frame: &mut Frame) -> bool {
        let Some(service) = app.services.get(&self.service) else {
            app.next_state = Some(AppStateKind::ServiceList(AppStateServiceList::new()));
            return true;
        };

        let mut text = Text::default();
        let format = format_description::parse_borrowed::<1>(
            "[year]-[month]-[day] [hour]:[minute]:[second]",
        )
        .unwrap();
        for log in &self.logs {
            let datetime_utc: OffsetDateTime = log.time.into();
            let datetime_local = datetime_utc
                .to_offset(time::UtcOffset::current_local_offset().unwrap_or(time::UtcOffset::UTC));
            let time_formatted = if service.show_timestamp {
                format!("[{}]", datetime_local.format(&format).unwrap())
            } else {
                String::new()
            };
            let empty_padding: String = iter::repeat_n(' ', time_formatted.len()).collect();
            for (i, line) in log.text.lines().enumerate() {
                let line = Line::from(format!(
                    "{}{}",
                    if i == 0 {
                        &time_formatted
                    } else {
                        &empty_padding
                    },
                    line
                ));
                text.push_line(match log.kind {
                    LogKind::Out => line.white(),
                    LogKind::Err => line.red(),
                    LogKind::In => line.blue(),
                });
            }
        }
        let status = app
            .statuses
            .get(&self.service)
            .cloned()
            .unwrap_or(ServiceStatus::Down);
        let paragraph = Paragraph::new(text.clone()).block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!("{} - {:?}", service.name, status))
                .title_alignment(HorizontalAlignment::Center),
        );
        //.scroll((*current_scroll as u16, 0));
        let chunks =
            Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).split(frame.area());
        frame.render_widget(paragraph, chunks[0]);
        frame.render_widget(&self.input_box, chunks[1]);
        false
    }

    fn handle_event(&mut self, app: &mut AppRef, event: &Event) {
        match event {
            Event::Key(key_event) => {
                if key_event.is_press() {
                    match key_event.code {
                        KeyCode::Esc => {
                            app.connection.send(NetMessageC2S::CancelMonitorLog);
                            let mut service_list = AppStateServiceList::new();
                            service_list.next_select_service = Some(self.service);
                            app.next_state = Some(AppStateKind::ServiceList(service_list));
                        }
                        KeyCode::Enter => {
                            let mut text = self.input_box.lines().join("\n");
                            text.push('\n');
                            self.input_box.clear();
                            app.connection.send(NetMessageC2S::SendIn(text));
                        }
                        _ => {
                            self.input_box.input(event.clone());
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn handle_message(&mut self, _app: &mut AppRef, message: &NetMessageS2C) {
        match message {
            NetMessageS2C::ClearLogs => {
                self.logs.clear();
            }
            NetMessageS2C::AddLog(log) => {
                self.logs.push(log.clone());
            }
            _ => {}
        }
    }
}
