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
    layout::Rect,
    style::{Color, Modifier},
    symbols::border,
    text::{Line, Text},
    widgets::{Block, List, ListState, Paragraph, StatefulWidget, Widget},
};

use ratatui::style::Stylize;
use uuid::Uuid;

use crate::common::{NetMessageC2S, NetMessageS2C, ServiceConfig, ServiceStatus, socket_path};

pub fn run_client() {
    let Ok(stream) = UnixStream::connect(socket_path()) else {
        println!("server is not started!");
        exit(0)
    };
    ratatui::run(move |terminal| App::new(stream).run(terminal)).unwrap();
}
struct ServerConnection(UnixStream);
impl ServerConnection {
    pub fn send(&mut self, message: NetMessageC2S) {
        let _ = bincode::serde::encode_into_std_write(message, &mut self.0, standard());
    }
}
pub struct App {
    connection: ServerConnection,
    rx: Receiver<NetMessageS2C>,
    exit: bool,
    services: HashMap<Uuid, ServiceConfig>,
    statuses: HashMap<Uuid, ServiceStatus>,
    state: AppStateKind,
}
impl App {
    pub fn new(stream: UnixStream) -> App {
        let (tx, rx) = channel();
        let mut stream2 = stream.try_clone().unwrap();
        thread::spawn(move || {
            loop {
                match bincode::serde::decode_from_std_read::<NetMessageS2C, _, _>(
                    &mut stream2,
                    standard(),
                ) {
                    Ok(message) => {
                        tx.send(message);
                    }
                    Err(_) => {
                        panic!("connection closed");
                    }
                }
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
    pub fn with_state_ref(&mut self, callback: impl FnOnce(&mut dyn AppState, &mut AppRef)) {
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
    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> io::Result<()> {
        let mut updated = true;
        while !self.exit {
            if updated {
                terminal.draw(|frame| {
                    self.with_state_ref(|state, app| state.render(app, frame));
                })?;
            }
            updated = self.handle_events()?;
            updated |= self.handle_network();
        }
        Ok(())
    }
    fn handle_network(&mut self) -> bool {
        let mut updated = false;
        while let Ok(message) = self.rx.try_recv() {
            updated = true;
            self.with_state_ref(|state, app| state.handle_message(app, &message));
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
        updated
    }
    fn handle_events(&mut self) -> io::Result<bool> {
        Ok(match event::poll(Duration::from_millis(500))? {
            true => {
                let event = event::read()?;
                self.with_state_ref(|state, app| state.handle_event(app, &event));
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
                true
            }
            false => false,
        })
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
pub enum AppStateKind {
    ServiceList(AppStateServiceList),
}
impl AppStateKind {
    pub fn app_state(&mut self) -> &mut dyn AppState {
        match self {
            AppStateKind::ServiceList(state) => state,
        }
    }
}
pub struct AppStateServiceList {
    list_state: ListState,
}
impl AppStateServiceList {
    pub fn new() -> AppStateServiceList {
        AppStateServiceList {
            list_state: ListState::default().with_selected(Some(0)),
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

        let list = List::new(vec!["aaa", "bbb", "ccc"])
            .style(Color::White)
            .highlight_style(Modifier::REVERSED)
            .highlight_symbol("> ");

        frame.render_stateful_widget(list, frame.area(), &mut self.list_state);

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
    fn handle_message(&mut self, app: &mut AppRef, message: &NetMessageS2C) {}
}
