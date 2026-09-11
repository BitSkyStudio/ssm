use std::{
    collections::{HashMap, hash_map::Entry},
    io, iter,
    os::unix::net::UnixStream,
    process::exit,
    sync::mpsc::{Receiver, channel},
    thread,
};

use bincode::config::standard;
use crossterm::event::{self, Event, KeyCode, KeyModifiers, MouseEventKind};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, HorizontalAlignment, Layout, Margin},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{
        Block, Borders, List, ListItem, ListState, Paragraph, Scrollbar, ScrollbarOrientation,
        ScrollbarState,
    },
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
            let mut app_messages = Vec::new();
            app_messages.push(self.rx.recv().unwrap());
            while let Ok(message) = self.rx.try_recv() {
                app_messages.push(message);
            }
            for app_message in app_messages {
                match app_message {
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
                        if let Event::Key(key_event) = event {
                            if key_event.code == KeyCode::Char('c')
                                && key_event.modifiers.contains(KeyModifiers::CONTROL)
                            {
                                self.exit = true;
                            }
                        }
                    }
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
    UpdateConfig(AppStateUpdateConfig),
}
impl AppStateKind {
    fn app_state(&mut self) -> &mut dyn AppState {
        match self {
            AppStateKind::ServiceList(state) => state,
            AppStateKind::LogMonitor(state) => state,
            AppStateKind::UpdateConfig(state) => state,
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
    fn new_at(selected: Uuid) -> AppStateServiceList {
        AppStateServiceList {
            list_state: ListState::default().with_selected(Some(0)),
            last_selected_service: None,
            next_select_service: Some(selected),
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
            let status = app.statuses.get(id).cloned().unwrap_or(ServiceStatus::Down);
            let mut text = Text::default();
            let status = match status {
                ServiceStatus::Down => Span::raw("DWN").gray(),
                ServiceStatus::Running => Span::raw("RUN").green(),
                ServiceStatus::Stopping => Span::raw("STP").yellow(),
                ServiceStatus::Dead => Span::raw("DED").red(),
                ServiceStatus::Miscarried => Span::raw("MSC").light_red(),
                ServiceStatus::Paused => Span::raw("PSD").blue(),
            };
            text.push_line(Line::from(vec![
                status,
                Span::raw(format!(" {}", service.name)),
            ]));
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
                "[S]Start [D]Stop [K]Kill [O]Pause [P]Continue [E]Edit [C]Create [Del]Remove [Enter]Monitor",
            ))
            .block(Block::default().borders(Borders::TOP)),
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
                        KeyCode::Char('d') => {
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
                        KeyCode::Char('e') => {
                            let Some(id) = self.last_selected_service else {
                                return;
                            };
                            let Some(config) = app.services.get(&id) else {
                                return;
                            };
                            app.next_state = Some(AppStateKind::UpdateConfig(
                                AppStateUpdateConfig::edit(id, config),
                            ));
                        }
                        KeyCode::Char('c') => {
                            app.next_state = Some(AppStateKind::UpdateConfig(
                                AppStateUpdateConfig::empty(Uuid::new_v4()),
                            ));
                        }
                        KeyCode::Delete => {
                            let Some(id) = self.last_selected_service else {
                                return;
                            };
                            //todo: confirm dialog
                            app.connection.send(NetMessageC2S::RemoveService(id));
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
    scroll: usize,
    tracking: bool,
    last_page_size: usize,
}
impl AppStateLogMonitor {
    pub fn new(service: Uuid) -> AppStateLogMonitor {
        AppStateLogMonitor {
            logs: Vec::new(),
            input_box: TextArea::default(),
            service,
            scroll: 0,
            tracking: true,
            last_page_size: 1,
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
                .borders(Borders::BOTTOM)
                .title(format!("{} - {:?}", service.name, status))
                .title_alignment(HorizontalAlignment::Center),
        );
        let chunks =
            Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).split(frame.area());
        self.last_page_size = chunks[0].height as usize;
        let total_lines = paragraph.line_count(chunks[0].width);
        let max_scroll = total_lines.saturating_sub(self.last_page_size);
        self.scroll = self.scroll.min(max_scroll);
        if self.tracking {
            self.scroll = max_scroll;
        }
        if self.scroll == max_scroll {
            self.tracking = true;
        }

        let mut scrollbar_state = ScrollbarState::new(max_scroll)
            .viewport_content_length(self.last_page_size)
            .position(self.scroll);
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        {
            let chunks =
                Layout::horizontal([Constraint::Min(0), Constraint::Length(1)]).split(chunks[0]);
            frame.render_widget(paragraph.scroll((self.scroll as u16, 0)), chunks[0]);
            frame.render_stateful_widget(scrollbar, chunks[1], &mut scrollbar_state);
        }
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
                            app.next_state = Some(AppStateKind::ServiceList(
                                AppStateServiceList::new_at(self.service),
                            ));
                            return;
                        }
                        KeyCode::Enter => {
                            let mut text = self.input_box.lines().join("\n");
                            text.push('\n');
                            self.input_box.clear();
                            app.connection.send(NetMessageC2S::SendIn(text));
                            self.tracking = true;
                            return;
                        }
                        KeyCode::Up => {
                            self.scroll = self.scroll.saturating_sub(1);
                            self.tracking = false;
                        }
                        KeyCode::PageUp => {
                            self.scroll = self.scroll.saturating_sub(self.last_page_size);
                            self.tracking = false;
                        }
                        KeyCode::Down => {
                            self.scroll += 1;
                        }
                        KeyCode::PageDown => {
                            self.scroll += self.last_page_size;
                        }
                        KeyCode::Home => {
                            self.scroll = 0;
                            self.tracking = false;
                        }
                        KeyCode::End => {
                            self.tracking = true;
                        }
                        _ => {}
                    }
                }
            }
            Event::Mouse(mouse_event) => match mouse_event.kind {
                MouseEventKind::ScrollUp => {
                    self.scroll = self.scroll.saturating_sub(1);
                    self.tracking = false;
                }
                MouseEventKind::ScrollDown => {
                    self.scroll += 1;
                }
                _ => {}
            },
            _ => (),
        }

        self.input_box.input(event.clone());
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
struct AppStateUpdateConfig {
    id: Uuid,
    name_field: TextArea<'static>,
    executable_field: TextArea<'static>,
    working_directory_field: TextArea<'static>,
    arguments_field: TextArea<'static>,
    environment: HashMap<String, String>,
    auto_start: bool,
    show_timestamp: bool,
    cursor: CurrentlyEditing,
    environment_popup: Option<EnvironmentUpdatePopup>,
}
#[derive(Default)]
struct EnvironmentUpdatePopup {
    key: TextArea<'static>,
    value: TextArea<'static>,
    selected: bool,
}
#[derive(Copy, Clone, PartialEq)]
enum CurrentlyEditing {
    Name,
    Executable,
    WorkingDirectory,
    Arguments,
    Environment,
    AutoStart,
    ShowTimestamp,
    Save,
}
static EDIT_ORDER: [CurrentlyEditing; 8] = [
    CurrentlyEditing::Name,
    CurrentlyEditing::Executable,
    CurrentlyEditing::WorkingDirectory,
    CurrentlyEditing::Arguments,
    CurrentlyEditing::Environment,
    CurrentlyEditing::AutoStart,
    CurrentlyEditing::ShowTimestamp,
    CurrentlyEditing::Save,
];
impl AppStateUpdateConfig {
    pub fn edit(id: Uuid, config: &ServiceConfig) -> AppStateUpdateConfig {
        let mut state = Self::empty(id);
        state.name_field.insert_str(&config.name);
        state
            .executable_field
            .insert_str(config.executable.to_str().unwrap());
        state
            .working_directory_field
            .insert_str(config.working_directory.to_str().unwrap());
        state.arguments_field.insert_str(config.arguments.join(" "));
        state.environment = config.environment.clone();
        state.auto_start = config.auto_start;
        state.show_timestamp = config.show_timestamp;
        state
    }
    pub fn empty(id: Uuid) -> AppStateUpdateConfig {
        AppStateUpdateConfig {
            id,
            name_field: TextArea::default(),
            executable_field: TextArea::default(),
            working_directory_field: TextArea::default(),
            arguments_field: TextArea::default(),
            auto_start: false,
            show_timestamp: true,
            cursor: CurrentlyEditing::Name,
            environment: HashMap::new(),
            environment_popup: None,
        }
    }
}
impl AppState for AppStateUpdateConfig {
    fn render(&mut self, _app: &mut AppRef, frame: &mut Frame) -> bool {
        fn modifier_reverse_if<'a, T>(element: T, should: bool) -> T
        where
            T: Stylize<'a, T>,
        {
            if should {
                element.add_modifier(Modifier::REVERSED)
            } else {
                element
            }
        }
        fn set_field_active(field: &mut TextArea, name: &'static str, active: bool) {
            field.set_block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(modifier_reverse_if(Line::from(name), active)),
            );
            field.set_cursor_style(if active {
                Style::default().bg(Color::White)
            } else {
                Style::default()
            });
        }
        set_field_active(
            &mut self.name_field,
            "Name",
            self.cursor == CurrentlyEditing::Name,
        );
        set_field_active(
            &mut self.executable_field,
            "Executable",
            self.cursor == CurrentlyEditing::Executable,
        );
        set_field_active(
            &mut self.working_directory_field,
            "Working Directory",
            self.cursor == CurrentlyEditing::WorkingDirectory,
        );
        set_field_active(
            &mut self.arguments_field,
            "Arguments",
            self.cursor == CurrentlyEditing::Arguments,
        );
        fn create_checkbox<'a>(name: &'a str, active: bool, state: bool) -> Text<'a> {
            let mut text = Text::default();
            text.push_span(modifier_reverse_if(Span::raw(name), active));
            text.push_span(Span::raw(if state { " [X]" } else { " [ ]" }));
            text
        }
        let chunks = Layout::vertical([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(2 + self.environment.len() as u16),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(3),
        ])
        .split(frame.area());
        frame.render_widget(&self.name_field, chunks[0]);
        frame.render_widget(&self.executable_field, chunks[1]);
        frame.render_widget(&self.working_directory_field, chunks[2]);
        frame.render_widget(&self.arguments_field, chunks[3]);
        frame.render_widget(
            Paragraph::new({
                let mut text = Text::default();
                for (key, value) in &self.environment {
                    text.push_line(format!("{}={}", key, value));
                }
                text
            })
            .block(Block::bordered().title(modifier_reverse_if(
                Line::raw("Environment"),
                self.cursor == CurrentlyEditing::Environment,
            ))),
            chunks[4],
        );
        frame.render_widget(
            create_checkbox(
                "Auto Start",
                self.cursor == CurrentlyEditing::AutoStart,
                self.auto_start,
            ),
            chunks[5],
        );
        frame.render_widget(
            create_checkbox(
                "Show Timestamps",
                self.cursor == CurrentlyEditing::ShowTimestamp,
                self.show_timestamp,
            ),
            chunks[6],
        );
        frame.render_widget(
            Paragraph::new(modifier_reverse_if(
                Text::from("Commit"),
                self.cursor == CurrentlyEditing::Save,
            ))
            .block(Block::bordered()),
            {
                let mut area = chunks[7];
                area.width = 8;
                area
            },
        );

        if let Some(popup) = &mut self.environment_popup {
            let popup_area = frame
                .area()
                .centered(Constraint::Percentage(60), Constraint::Length(9));
            /*frame.render_widget(ratatui::widgets::Clear, popup_area);
            frame.render_widget(
                Text::raw("").alignment(HorizontalAlignment::Center),
                popup_area,
            );*/
            frame.render_widget(ratatui::widgets::Clear, popup_area);
            frame.render_widget(Paragraph::new("").block(Block::bordered()), popup_area);
            let chunks = Layout::vertical([
                Constraint::Length(1),
                Constraint::Length(3),
                Constraint::Length(3),
            ])
            .split(popup_area.inner(Margin::new(1, 1)));
            frame.render_widget(Text::raw("Set Environment Variable").centered(), chunks[0]);
            set_field_active(&mut popup.key, "Key", !popup.selected);
            frame.render_widget(&popup.key, chunks[1]);
            set_field_active(&mut popup.value, "Value", popup.selected);
            frame.render_widget(&popup.value, chunks[2]);
        }

        false
    }
    fn handle_event(&mut self, app: &mut AppRef, event: &Event) {
        if let Some(popup) = &mut self.environment_popup {
            if let Event::Key(key_event) = event {
                if key_event.is_press() {
                    match key_event.code {
                        KeyCode::Up | KeyCode::Down | KeyCode::Tab | KeyCode::BackTab => {
                            popup.selected ^= true;
                            return;
                        }
                        KeyCode::Esc => {
                            self.environment_popup = None;
                            return;
                        }
                        KeyCode::Enter => {
                            let key = read_field(&popup.key);
                            let value = read_field(&popup.value);
                            if !key.is_empty() {
                                match self.environment.entry(key) {
                                    Entry::Occupied(mut entry) => {
                                        if value.is_empty() {
                                            entry.remove();
                                        } else {
                                            entry.insert(value);
                                        }
                                    }
                                    Entry::Vacant(entry) => {
                                        if !value.is_empty() {
                                            entry.insert(value);
                                        }
                                    }
                                }
                                self.environment_popup = None;
                            }
                            return;
                        }
                        _ => {}
                    }
                }
            }
            if !popup.selected {
                popup.key.input(event.clone());
            } else {
                popup.value.input(event.clone());
            }
            return;
        }

        let mut scroll_cursor = |scroll: isize| {
            let mut current_index =
                EDIT_ORDER.iter().position(|e| *e == self.cursor).unwrap() as isize;
            current_index += scroll;
            current_index += EDIT_ORDER.len() as isize;
            current_index %= EDIT_ORDER.len() as isize;
            self.cursor = EDIT_ORDER[current_index as usize];
        };
        match event {
            Event::Key(key_event) => {
                if key_event.is_press() {
                    match key_event.code {
                        KeyCode::Down | KeyCode::Tab => {
                            scroll_cursor(1);
                            return;
                        }
                        KeyCode::Up | KeyCode::BackTab => {
                            scroll_cursor(-1);
                            return;
                        }
                        KeyCode::Esc => {
                            app.next_state = Some(AppStateKind::ServiceList(
                                AppStateServiceList::new_at(self.id),
                            ));
                            return;
                        }
                        KeyCode::Enter => {
                            match self.cursor {
                                CurrentlyEditing::Environment => {
                                    self.environment_popup =
                                        Some(EnvironmentUpdatePopup::default());
                                }
                                CurrentlyEditing::AutoStart => {
                                    self.auto_start ^= true;
                                }
                                CurrentlyEditing::ShowTimestamp => {
                                    self.show_timestamp ^= true;
                                }
                                CurrentlyEditing::Save => {
                                    let arguments = read_field(&self.arguments_field);
                                    app.connection.send(NetMessageC2S::UpdateServiceConfig {
                                        id: self.id,
                                        config: ServiceConfig {
                                            name: read_field(&self.name_field),
                                            executable: read_field(&self.executable_field).into(),
                                            working_directory: read_field(
                                                &self.working_directory_field,
                                            )
                                            .into(),
                                            arguments: if arguments.is_empty() {
                                                Vec::new()
                                            } else {
                                                arguments
                                                    .split(" ")
                                                    .map(|str| str.to_string())
                                                    .collect()
                                            },
                                            environment: self.environment.clone(),
                                            auto_start: self.auto_start,
                                            show_timestamp: self.show_timestamp,
                                        },
                                    });
                                    app.next_state = Some(AppStateKind::ServiceList(
                                        AppStateServiceList::new_at(self.id),
                                    ));
                                }
                                _ => {}
                            }
                            return;
                        }
                        _ => {}
                    }
                }
            }
            Event::Mouse(mouse_event) => match mouse_event.kind {
                MouseEventKind::ScrollUp => {
                    scroll_cursor(-1);
                }
                MouseEventKind::ScrollDown => {
                    scroll_cursor(1);
                }
                _ => {}
            },
            _ => (),
        }
        match self.cursor {
            CurrentlyEditing::Name => {
                self.name_field.input(event.clone());
            }
            CurrentlyEditing::Executable => {
                self.executable_field.input(event.clone());
            }
            CurrentlyEditing::WorkingDirectory => {
                self.working_directory_field.input(event.clone());
            }
            CurrentlyEditing::Arguments => {
                self.arguments_field.input(event.clone());
            }
            _ => {}
        }
    }
    fn handle_message(&mut self, _app: &mut AppRef, _message: &NetMessageS2C) {}
}
fn read_field(field: &TextArea) -> String {
    field.lines().join("\n")
}
