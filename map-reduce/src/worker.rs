use rand::{Rng, distr::Alphanumeric};
use std::collections::hash_map::DefaultHasher;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::{collections::HashMap, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::Instant,
};
use tracing::info;

use crate::master::MAP_OUT_BASE_PATH;
use crate::types::KeyValue;
use crate::{
    master::{MASTER_NODE_PORT, MasterStatus},
    types::{Message, MessageType},
};

pub struct WorkerNode {
    port: u16,
}

impl WorkerNode {
    pub fn new(port: u16) -> WorkerNode {
        WorkerNode { port }
    }

    pub async fn run(&self) {
        let addr = format!("127.0.0.1:{}", MASTER_NODE_PORT);

        let mut stream = loop {
            match TcpStream::connect(&addr).await {
                Ok(stream) => {
                    info!("✅ Connected to master at {}", addr);
                    break stream;
                }
                Err(e) => {
                    eprintln!(
                        "⚠️ Failed to connect to master: {:?}. Retrying in 10s...",
                        e
                    );
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
        };

        let msg = Message {
            message_type: MessageType::ConnectionMessage,
            payload: Some("Hello from worker!".to_string()),
            task_status: None,
        };

        let bytes = serde_json::to_vec(&msg).unwrap();

        stream.write_all(&bytes).await.expect("Failed to write");

        loop {
            let mut buf = [0; 1024];
            let n = match stream.read(&mut buf).await {
                Ok(0) => {
                    info!("Master closed the connection, exiting worker.");
                    return;
                }
                Ok(n) => n,
                Err(e) => {
                    eprintln!("Failed to read from master: {e}");
                    break;
                }
            };

            info!(
                "Received from master: {}",
                String::from_utf8_lossy(&buf[..n])
            );

            let msg: Message = serde_json::from_slice(&buf[..n]).expect("Failed to parse message");

            match msg.message_type {
                MessageType::TaskMessage => {
                    let time_recieved = Instant::now();
                    if msg.task_status.is_none() || msg.payload.is_none() {
                        eprintln!("Invalid message format, no task status specified");
                        return;
                    }

                    let path = msg.payload.unwrap();
                    let status = msg.task_status.unwrap();

                    match status {
                        MasterStatus::Map => {
                            let task = self.map(&path).expect("msg");
                            let payload_str = serde_json::to_string(&task).unwrap();

                            let msg = Message {
                                message_type: MessageType::ResponseMessage,
                                payload: Some(payload_str),
                                task_status: None,
                            };

                            let bytes = serde_json::to_vec(&msg).unwrap();

                            stream.write_all(&bytes).await.expect("Failed to write");
                        }
                        MasterStatus::Reduce => {
                            let task = self.reduce(&path);
                            let msg = Message {
                                message_type: MessageType::ResponseMessage,
                                payload: Some(task),
                                task_status: None,
                            };

                            let bytes = serde_json::to_vec(&msg).unwrap();

                            stream.write_all(&bytes).await.expect("Failed to write");
                        }
                        _ => {}
                    }

                    info!(
                        "Task completed in {:?}ms",
                        time_recieved.elapsed().as_millis()
                    )
                }
                MessageType::CompletionMessage => {
                    info!("🎉 Master reported completion. Exiting worker.");

                    break;
                }
                _ => {}
            }
        }

        std::process::exit(0);
    }

    pub fn map(&self, input_file: &str) -> Result<Vec<String>, String> {
        let task_number = self.extract_task_number(input_file);
        let n_reduce = 3;

        println!("Starting map task for file: {}", input_file);

        let contents = fs::read_to_string(input_file)
            .map_err(|e| format!("Failed to read input file {}: {}", input_file, e))?;

        let intermediate_kvs = self.word_count_map(&contents);

        let mut buckets: Vec<Vec<KeyValue>> = vec![Vec::new(); n_reduce];

        for kv in intermediate_kvs {
            let bucket = self.hash_key(&kv.key) % n_reduce;
            buckets[bucket].push(kv);
        }

        let mut output_files = Vec::new();
        for (reduce_task, bucket) in buckets.iter().enumerate() {
            let filename = format!("mr-{}-{}", task_number, reduce_task);
            self.write_intermediate_file(&filename, bucket)?;
            output_files.push(filename);
        }

        println!(
            "Map task completed. Created {} intermediate files",
            output_files.len()
        );
        Ok(output_files)
    }

    pub fn reduce(&self, path: &str) -> String {
        let file = File::open(path).expect("Should have been able to read the file");
        let reader = BufReader::new(file);

        let mut final_counts: HashMap<String, usize> = HashMap::new();

        for line in reader.lines() {
            let line = line.expect("Failed to read line");
            let kv: KeyValue = serde_json::from_str(&line).expect("Failed to parse KeyValue");
            let count: usize = kv.value.parse().unwrap_or(0);

            *final_counts.entry(kv.key).or_insert(0) += count;
        }

        let random_file_name = random_name();
        let output_path = format!(
            "./reduce/out/worker-{}-{}.json",
            self.port, random_file_name
        );

        let json = serde_json::to_string_pretty(&final_counts).unwrap();
        fs::write(&output_path, json).expect("Failed to write reduce output");

        info!("Reduce task completed: output written to {}", output_path);

        output_path
    }

    fn word_count_map(&self, text: &str) -> Vec<KeyValue> {
        let mut results = Vec::new();

        for word in text.split_whitespace() {
            let clean_word = word
                .chars()
                .filter(|c| c.is_alphabetic())
                .collect::<String>()
                .to_lowercase();

            if !clean_word.is_empty() {
                results.push(KeyValue {
                    key: clean_word,
                    value: "1".to_string(),
                });
            }
        }

        results
    }

    fn hash_key(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize
    }

    fn extract_task_number(&self, filename: &str) -> usize {
        filename
            .split('-')
            .last()
            .and_then(|s| s.split('.').next())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    fn write_intermediate_file(&self, filename: &str, kvs: &[KeyValue]) -> Result<(), String> {
        let path = format!("{}{}", MAP_OUT_BASE_PATH, filename);
        let mut file = File::create(path)
            .map_err(|e| format!("Failed to create intermediate file {}: {}", filename, e))?;

        for kv in kvs {
            let json_line = serde_json::to_string(kv)
                .map_err(|e| format!("Failed to serialize KeyValue: {}", e))?;
            writeln!(file, "{}", json_line)
                .map_err(|e| format!("Failed to write to file {}: {}", filename, e))?;
        }

        Ok(())
    }

    // fn word_count_reduce(&self, values: &[String]) -> String {
    //     let count: usize = values.iter().map(|v| v.parse::<usize>().unwrap_or(0)).sum();
    //     count.to_string()
    // }

    // fn read_intermediate_file(&self, filename: &str) -> Result<Vec<KeyValue>, String> {
    //     let file = File::open(filename)
    //         .map_err(|e| format!("Failed to open intermediate file {}: {}", filename, e))?;

    //     let reader = BufReader::new(file);
    //     let mut kvs = Vec::new();

    //     for line in reader.lines() {
    //         let line = line.map_err(|e| format!("Failed to read line from {}: {}", filename, e))?;
    //         let kv: KeyValue = serde_json::from_str(&line)
    //             .map_err(|e| format!("Failed to deserialize KeyValue from {}: {}", filename, e))?;
    //         kvs.push(kv);
    //     }

    //     Ok(kvs)
    // }

    // fn find_intermediate_files(&self, reduce_task_num: usize) -> Result<Vec<String>, String> {
    //     let mut files = Vec::new();
    //     let current_dir = std::env::current_dir()
    //         .map_err(|e| format!("Failed to get current directory: {}", e))?;

    //     let entries =
    //         fs::read_dir(&current_dir).map_err(|e| format!("Failed to read directory: {}", e))?;

    //     for entry in entries {
    //         let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
    //         let filename = entry.file_name().to_string_lossy().to_string();

    //         if filename.starts_with("mr-") && filename.ends_with(&format!("-{}", reduce_task_num)) {
    //             files.push(filename);
    //         }
    //     }

    //     if files.is_empty() {
    //         return Err(format!(
    //             "No intermediate files found for reduce task {}",
    //             reduce_task_num
    //         ));
    //     }

    //     Ok(files)
    // }

    // fn write_final_output(&self, filename: &str, results: &[KeyValue]) -> Result<(), String> {
    //     let mut file = File::create(filename)
    //         .map_err(|e| format!("Failed to create output file {}: {}", filename, e))?;

    //     for kv in results {
    //         writeln!(file, "{} {}", kv.key, kv.value)
    //             .map_err(|e| format!("Failed to write to output file {}: {}", filename, e))?;
    //     }

    //     Ok(())
    // }
}

fn random_name() -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(6)
        .map(char::from)
        .collect()
}
