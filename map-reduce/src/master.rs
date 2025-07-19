// Listens for connections from workers and assign tasks
// Keeps track of tasks in progress
// Re-assign tasks when no response from worker in 10 seconds
// Broadcasts message once all tasks are completed
// Asigns both Map and Reduce tasks

use std::{collections::HashMap, fs, net::SocketAddr, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::Instant,
};
use tracing::info;

use crate::types::{Message, MessageType};

pub const MASTER_NODE_PORT: u16 = 12224;
pub const MAP_INPUT_BASE_PATH: &str = "./map/input";
pub const MAP_OUT_BASE_PATH: &str = "./map/out/";
pub const REDUCE_INPUT_BASE_PATH: &str = "./map/out/";
pub const FINAL_OUT: &str = "./reduce/out";

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
enum WorkerStatus {
    Busy,
    Waiting,
    InActive,
}

#[derive(Debug, Clone)]
pub struct Worker {
    task: Option<String>,
    heartbeat: Instant,
    status: WorkerStatus,
}

impl Worker {
    pub fn set_as_inactive(&mut self) -> Option<String> {
        let task = match self.task.clone() {
            Some(task) => task,
            None => return None,
        };

        self.status = WorkerStatus::InActive;
        self.task = None;
        Some(task)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum MasterStatus {
    Map,
    Reduce,
    Done,
}

#[derive(Debug, Clone)]
pub struct MasterNode {
    tasks: Vec<String>,
    workers: HashMap<SocketAddr, Worker>,
    streams: HashMap<SocketAddr, Arc<Mutex<TcpStream>>>,
    status: MasterStatus,
}

impl MasterNode {
    pub fn new() -> MasterNode {
        MasterNode {
            tasks: Vec::new(),
            workers: HashMap::new(),
            streams: HashMap::new(),
            status: MasterStatus::Map,
        }
    }
    pub fn setup(&mut self) {
        let map_tasks = read_task_dir(MAP_INPUT_BASE_PATH);
        info!("Read {} tasks to master node", map_tasks.len());
        self.tasks = map_tasks;
        self.status = MasterStatus::Map;
    }

    pub async fn run(master: Arc<Mutex<MasterNode>>) {
        let addr = format!("127.0.0.1:{}", MASTER_NODE_PORT);
        let listener = TcpListener::bind(addr.clone()).await.expect("msg");

        info!(
            "Master node listening on {} with listener {:?}",
            addr.clone(),
            listener
        );

        loop {
            let (socket, worker_addr) = listener.accept().await.expect("msg");
            let master_node = Arc::clone(&master);
            let socket_stream = Arc::new(Mutex::new(socket));

            tokio::spawn(async move {
                let mut buf = [0; 1024];

                loop {
                    let mut stream = socket_stream.lock().await;
                    let n = match stream.read(&mut buf).await {
                        Ok(0) => return,
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("failed to read from socket; err = {:?}", e);
                            return;
                        }
                    };

                    drop(stream);

                    let msg: Message =
                        serde_json::from_slice(&buf[..n]).expect("Failed to parse message");

                    match msg.message_type {
                        MessageType::ConnectionMessage => {
                            println!("Connection message from {:?}", worker_addr);

                            Self::handle_worker_connection(
                                Arc::clone(&master_node),
                                worker_addr,
                                Arc::clone(&socket_stream),
                            )
                            .await;
                        }
                        MessageType::ResponseMessage => {
                            Self::handle_worker_response(
                                Arc::clone(&master_node),
                                worker_addr,
                                Arc::clone(&socket_stream),
                                msg.payload,
                            )
                            .await
                        }
                        _ => {
                            Self::handle_worker_connection(
                                Arc::clone(&master_node),
                                worker_addr,
                                Arc::clone(&socket_stream),
                            )
                            .await
                        }
                    }

                    if Self::task_in_progress(&Arc::clone(&master_node)).await == false {
                        info!("Phase completed moving to next...");
                        Self::move_to_next(Arc::clone(&master_node)).await;
                    }
                }
            });
        }
    }

    async fn handle_worker_connection(
        master: Arc<Mutex<MasterNode>>,
        worker_addr: SocketAddr,
        stream: Arc<Mutex<TcpStream>>,
    ) {
        let task_assigned =
            Self::try_assign_task(Arc::clone(&master), worker_addr, Arc::clone(&stream)).await;

        if !task_assigned {
            Self::check_and_transition_phase(Arc::clone(&master)).await;
        }
    }

    async fn handle_worker_response(
        master: Arc<Mutex<MasterNode>>,
        worker_addr: SocketAddr,
        stream: Arc<Mutex<TcpStream>>,
        payload: Option<String>,
    ) {
        let response_valid = match payload {
            Some(response) => {
                let m_node = master.lock().await;
                let status = &m_node.status;
                let path = match status {
                    MasterStatus::Map => MAP_OUT_BASE_PATH,
                    MasterStatus::Reduce => FINAL_OUT,
                    _ => return,
                };
                let files = read_task_dir(path);
                drop(m_node);

                if files.contains(&response) {
                    info!("Task {} completed by {}", response, worker_addr);
                    true
                } else {
                    false
                }
            }
            None => false,
        };

        if response_valid {
            {
                let mut m_node = master.lock().await;
                if let Some(worker) = m_node.workers.get_mut(&worker_addr) {
                    worker.heartbeat = Instant::now();
                    worker.status = WorkerStatus::Waiting;
                    worker.task = None;
                }
            }

            let task_assigned =
                Self::try_assign_task(Arc::clone(&master), worker_addr, Arc::clone(&stream)).await;

            if !task_assigned {
                Self::check_and_transition_phase(Arc::clone(&master)).await;
            }
        } else {
            let task_assigned =
                Self::try_assign_task(Arc::clone(&master), worker_addr, Arc::clone(&stream)).await;

            if !task_assigned {
                Self::check_and_transition_phase(Arc::clone(&master)).await;
            }
        }
    }

    async fn try_assign_task(
        master: Arc<Mutex<MasterNode>>,
        worker_addr: SocketAddr,
        stream: Arc<Mutex<TcpStream>>,
    ) -> bool {
        let (task_to_assign, status) = {
            let mut m_node = master.lock().await;

            if !m_node.streams.contains_key(&worker_addr) {
                m_node.streams.insert(worker_addr, stream.clone());
            }

            info!("Tasks {} left to assign", m_node.tasks.len());

            if let Some(task) = m_node.tasks.pop() {
                let worker = Worker {
                    task: Some(task.clone()),
                    heartbeat: Instant::now(),
                    status: WorkerStatus::Busy,
                };

                println!(
                    "Attempting to assign task {} to worker {}",
                    task, worker_addr
                );
                m_node.workers.insert(worker_addr, worker);
                let status = m_node.status.clone();

                (Some(task), status)
            } else {
                let worker = Worker {
                    task: None,
                    heartbeat: Instant::now(),
                    status: WorkerStatus::Waiting,
                };
                m_node.workers.insert(worker_addr, worker);
                info!("No tasks for {}, worker marked as waiting", worker_addr);

                (None, m_node.status.clone())
            }
        };

        match task_to_assign {
            Some(task) => {
                Self::send_message(Arc::clone(&stream), task, MessageType::TaskMessage, &status)
                    .await;
                info!("Task sent to worker. Waiting for response");
                true
            }
            None => {
                let mut m_node = master.lock().await;
                if let Some(worker) = m_node.workers.get_mut(&worker_addr) {
                    worker.status = WorkerStatus::Waiting;
                    worker.heartbeat = Instant::now();
                    worker.task = None;
                }
                info!("No tasks for {}, worker marked as waiting", worker_addr);
                false
            }
        }
    }

    async fn check_and_transition_phase(master: Arc<Mutex<MasterNode>>) {
        let should_transition = {
            let m_node = master.lock().await;

            m_node.tasks.is_empty()
                && !m_node
                    .workers
                    .iter()
                    .any(|(_, worker)| worker.task.is_none())
        };

        if should_transition {
            info!("Phase completed, moving to next...");
            Self::move_to_next(Arc::clone(&master)).await;
        }
    }

    pub async fn send_message(
        socket_stream: Arc<Mutex<TcpStream>>,
        payload: String,
        message_type: MessageType,
        task_status: &MasterStatus,
    ) {
        let message = Message {
            message_type,
            payload: Some(payload.clone()),
            task_status: Some(task_status.clone()),
        };

        let mut stream = socket_stream.lock().await;
        let bytes = serde_json::to_vec(&message).unwrap();

        if let Err(e) = stream.write_all(&bytes).await {
            eprintln!("Failed to send message: {:?}", e);
            return;
        }

        if let Err(e) = stream.flush().await {
            eprintln!("Failed to flush stream: {:?}", e);
            return;
        }

        if let Ok(peer_addr) = stream.peer_addr() {
            println!("Sent {} bytes '{}' to {}", bytes.len(), payload, peer_addr);
        }
    }

    pub async fn task_in_progress(master: &Arc<Mutex<MasterNode>>) -> bool {
        let m_node = master.lock().await;
        if !m_node.tasks.is_empty() {
            return true;
        }

        m_node
            .workers
            .iter()
            .any(|(_, worker)| worker.task.is_some())
    }

    pub async fn move_to_next(master: Arc<Mutex<MasterNode>>) {
        let mut m_node = master.lock().await;
        info!("{:?} phase completed. Moving to next phase", m_node.status);

        match m_node.status {
            MasterStatus::Map => {
                m_node.status = MasterStatus::Reduce;
                let reduce_tasks = read_task_dir(REDUCE_INPUT_BASE_PATH);
                m_node.tasks = reduce_tasks.clone();

                info!(
                    "{} tasks attemped, {} tasks loaded",
                    reduce_tasks.len(),
                    m_node.tasks.len()
                );

                let waiting_workers: Vec<(SocketAddr, Arc<Mutex<TcpStream>>)> = {
                    m_node
                        .workers
                        .iter()
                        .filter(|(_, worker)| worker.status == WorkerStatus::Waiting)
                        .filter_map(|(addr, _)| {
                            m_node
                                .streams
                                .get(addr)
                                .map(|stream| (*addr, Arc::clone(stream)))
                        })
                        .collect()
                };

                drop(m_node);

                for (worker_addr, stream) in waiting_workers {
                    Self::try_assign_task(Arc::clone(&master), worker_addr, stream).await;
                }
            }
            MasterStatus::Reduce => {
                m_node.status = MasterStatus::Done;
                info!("MapReduce completed. Shutting down..");
                let streams = m_node.streams.clone();

                drop(m_node);

                let mut handles = Vec::new();

                for (addr, stream) in streams {
                    let stream_clone = Arc::clone(&stream);
                    info!("Sending shutdown broadcast to worker: {}", addr);

                    let handle = tokio::spawn(async move {
                        let result = tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            Self::send_message(
                                stream_clone,
                                "Done".to_string(),
                                MessageType::CompletionMessage,
                                &MasterStatus::Done,
                            ),
                        )
                        .await;

                        match result {
                            Ok(()) => {
                                info!("Successfully sent completion message to {}", addr);
                            }
                            Err(_) => {
                                eprintln!("Timeout sending completion message to {}", addr);
                            }
                        }
                    });

                    handles.push(handle);
                }

                for handle in handles {
                    let _ = handle.await;
                }

                std::process::exit(0);
            }
            _ => {
                info!("MapReduce completed. Shutting down..");
                let streams = m_node.streams.clone();

                drop(m_node);

                let mut handles = Vec::new();

                for (addr, stream) in streams {
                    let stream_clone = Arc::clone(&stream);
                    info!("Sending shutdown broadcast to worker: {}", addr);

                    let handle = tokio::spawn(async move {
                        let result = tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            Self::send_message(
                                stream_clone,
                                "Done".to_string(),
                                MessageType::CompletionMessage,
                                &MasterStatus::Done,
                            ),
                        )
                        .await;

                        match result {
                            Ok(()) => {
                                info!("Successfully sent completion message to {}", addr);
                            }
                            Err(_) => {
                                eprintln!("Timeout sending completion message to {}", addr);
                            }
                        }
                    });

                    handles.push(handle);
                }

                for handle in handles {
                    let _ = handle.await;
                }

                std::process::exit(0);
            }
        }
    }

    pub async fn heartbeat_watchdog(master: &Arc<Mutex<MasterNode>>) {
        let master_node = Arc::clone(master);
        tokio::spawn(async move {
            let mut m_node = master_node.lock().await;
            let inactive_workers: Vec<_> = m_node
                .workers
                .iter()
                .filter(|(_, worker)| worker.heartbeat.elapsed() > Duration::from_secs(10))
                .map(|(w, _)| *w)
                .collect();

            for worker_addr in inactive_workers {
                let mut worker = { m_node.workers.get(&worker_addr).cloned().unwrap() };
                let task = worker.set_as_inactive();
                m_node.workers.insert(worker_addr, worker);
                match task {
                    Some(task) => {
                        m_node.tasks.push(task);
                    }
                    None => continue,
                };

                info!("Worker {} set as inactive", worker_addr);
            }
        });
    }
}

pub fn read_task_dir(path: &str) -> Vec<String> {
    let paths = fs::read_dir(path).unwrap();
    let mut files = vec![];
    for entry in paths {
        let file = entry.unwrap().path();
        files.push(file.to_string_lossy().to_string());
    }

    files
}
