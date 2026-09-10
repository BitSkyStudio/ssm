use std::{
    collections::HashMap,
    io,
    os::unix::net::UnixStream,
    process::exit,
    sync::mpsc::{Receiver, channel},
    thread,
    time::Duration,
};

use bincode::config::standard;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, MouseEventKind};
use ratatui::{
    DefaultTerminal, Frame,
    buffer::Buffer,
    layout::{HorizontalAlignment, Rect},
    style::{Color, Modifier},
    symbols::border,
    text::{Line, Text},
    widgets::{Block, List, ListItem, ListState, Paragraph, StatefulWidget, Widget},
};

use ratatui::style::Stylize;
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
        };
        callback(self.state.app_state(), &mut app_ref);
        if let Some(next_state) = app_ref.next_state {
            self.state = next_state;
        }
    }
    fn run(&mut self, terminal: &mut DefaultTerminal) -> io::Result<()> {
        while !self.exit {
            terminal.draw(|frame| {
                self.with_app_ref(|state, app| state.render(app, frame));
            })?;

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
                    match event {
                        Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                            match key_event.code {
                                KeyCode::Char('q') => {
                                    self.exit = true;
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    };
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
}
trait AppState {
    fn render(&mut self, app: &mut AppRef, frame: &mut Frame);
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
}
impl AppStateServiceList {
    fn new() -> AppStateServiceList {
        AppStateServiceList {
            list_state: ListState::default().with_selected(Some(0)),
            last_selected_service: None,
        }
    }
}
impl AppState for AppStateServiceList {
    fn render(&mut self, app: &mut AppRef, frame: &mut Frame) {
        let title = Line::from(" Counter App Tutorial ".bold());
        let instructions = Line::from(vec![
            " Decrement ".into(),
            "<Left>".blue().bold(),
            " Increment ".into(),
            "<Right>".blue().bold(),
            " Quit ".into(),
            "<Q> ".blue().bold(),
        ]);
        let block = Block::bordered()
            .title(title.centered())
            .title_bottom(instructions.centered())
            .border_set(border::THICK);

        /*let counter_text = Text::from(vec![Line::from(vec![
            "Value: ".into(),
            0.to_string().yellow(),
        ])]);*/
        let mut service_list = app.services.keys().cloned().collect::<Vec<_>>();
        service_list.sort_by_key(|id| &app.services.get(id).unwrap().name);
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

        frame.render_stateful_widget(list, frame.area(), &mut self.list_state);

        self.last_selected_service = match self.list_state.selected() {
            Some(selected) => service_list.get(selected).cloned(),
            None => None,
        };

        /*Paragraph::new(counter_text)
        .centered()
        .block(block)
        .render(area, buf);*/
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
                                    Some(AppStateKind::LogMonitor(AppStateLogMonitor::new()));
                            }
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
}
impl AppStateLogMonitor {
    pub fn new() -> AppStateLogMonitor {
        AppStateLogMonitor { logs: Vec::new() }
    }
}
impl AppState for AppStateLogMonitor {
    fn render(&mut self, app: &mut AppRef, frame: &mut Frame) {
        let mut text = Text::default();
        let format =
            format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]").unwrap();
        for log in &self.logs {
            let datetime_utc: OffsetDateTime = log.time.into();
            let datetime_local = datetime_utc
                .to_offset(time::UtcOffset::current_local_offset().unwrap_or(time::UtcOffset::UTC));
            let time_formatted = datetime_local.format(&format).unwrap();
            let line = Line::from(format!("[{}]{}", time_formatted, log.text));
            text.push_line(match log.kind {
                LogKind::Out => line,
                LogKind::Err => line.red(),
                LogKind::In => line.gray(),
            });
        }
        frame.render_widget(text, frame.area());
    }

    fn handle_event(&mut self, app: &mut AppRef, event: &Event) {
        match event {
            Event::Key(key_event) => {
                if key_event.is_press() {
                    match key_event.code {
                        KeyCode::Esc => {
                            app.connection.send(NetMessageC2S::CancelMonitorLog);
                            app.next_state =
                                Some(AppStateKind::ServiceList(AppStateServiceList::new()));
                        }
                        _ => {}
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
