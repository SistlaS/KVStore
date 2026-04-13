// #![allow(unused)]
use crate::raft_rpc::LogEntry;
use crc32fast::Hasher;
use log::{debug, error, info};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex, RwLock};

#[derive(Debug, PartialEq, Eq, Clone)]

pub enum Role {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MyLogEntry {
    idx: u64,
    term: u64,
    command: String,
}

#[derive(Debug)]
pub struct RaftState {
    // Raft state START
    // persistent state
    pub current_term: u64,
    pub voted_for: Option<u32>,
    // log: Vec<MyLogEntry>, // in-memory log for fast perf
    // volatile state
    pub commit_idx: u64,
    pub last_applied: u64,
    pub last_log_term: u64, // log term starts from 1
    pub last_log_idx: u64,  // log index starts from 1
    // volatile state in leader
    pub next_idx: Option<Vec<u64>>,
    pub match_idx: Option<Vec<u64>>,
    // Raft state END
    _dir_path: String,
    config_filename: String,
    log_filename: String,
    applied_filename: String,
    pub applied_lock: Mutex<()>,            // applied log file
    pub logfile: Arc<Mutex<std::fs::File>>, // log file
    pub state_machine: Arc<RwLock<BTreeMap<String, String>>>, // state machine
    pub in_mem_log: Arc<Mutex<Vec<(u64, u64, u64)>>>, // in-memory log (log_idx, log_term, log_) for fast perf
    // additional state
    pub role: Role,
    pub current_leader: Option<u32>,
}

impl RaftState {
    pub fn new(dir_path: String, id: u32) -> Self {
        let config_filename = format!("{}/raft_{}.config", dir_path, id);
        let log_filename = format!("{}/raft_{}.log", dir_path, id);
        let applied_filename = format!("{}/raft_{}_applied.log", dir_path, id);
        // create the directory if it doesn't exist
        std::fs::create_dir_all(&dir_path).unwrap_or_else(|_| {
            panic!("Failed to create directory: {}", dir_path);
        });

        // create the config file if it doesn't exist
        let mut logfile_exists = false;
        if std::path::Path::new(&log_filename).exists() {
            logfile_exists = true;
        }
        // create the log file if it doesn't exist
        let logfile = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&log_filename)
            .expect("Failed to open log file");
        let logfile = Arc::new(Mutex::new(logfile));

        let (current_term, voted_for) = Self::read_config_file(&config_filename);
        let state_machine = Arc::new(RwLock::new(BTreeMap::new()));
        let mut node = Self {
            // Raft state START
            current_term: current_term,
            voted_for: voted_for,
            // log: Vec::new(),
            commit_idx: 0,
            last_applied: 0,
            last_log_term: 0,
            last_log_idx: 0,
            next_idx: None,
            match_idx: None,
            // Raft state END
            _dir_path: dir_path,
            config_filename,
            log_filename,
            applied_filename,
            applied_lock: Mutex::new(()), // applied log file
            logfile: logfile.clone(),
            state_machine: state_machine,
            in_mem_log: Arc::new(Mutex::new(Vec::new())),
            // additional state
            role: Role::Follower,
            current_leader: None,
        };
        node.write_config();
        if logfile_exists {
            node.recover_log();
        }
        node.print_state();
        node
    }

    pub fn log_applied_to_file(&self, msg: &str) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.applied_filename.clone())
            .expect("Failed to open log file");
        writeln!(
            file,
            "{} {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            msg
        )
        .expect("Failed to write to log file");
    }

    pub fn get_state_machine(&self) -> Arc<RwLock<BTreeMap<String, String>>> {
        self.state_machine.clone()
    }

    pub fn print_state(&self) {
        let debug_str = format!(
            "RaftState: current_term: {}, voted_for: {:?}, commit_idx: {}, last_applied: {}, last_log_term: {}, last_log_idx: {}, next_idx: {:?}, match_idx: {:?}",
            self.current_term,
            self.voted_for,
            self.commit_idx,
            self.last_applied,
            self.last_log_term,
            self.last_log_idx,
            self.next_idx,
            self.match_idx
        );
        debug!("{}", debug_str);
    }

    pub fn apply_pending_commands(&mut self) {
        if self.commit_idx == 0 || self.last_applied == self.commit_idx {
            return;
        }
        info!(
            "apply_pending_commands: last_applied: {}, commit_idx: {}",
            self.last_applied, self.commit_idx
        );
        // apply pending commands from the log
        let mut count = 0;
        while self.last_applied < self.commit_idx {
            let success = self.apply_command_from_log();
            if !success {
                break;
            }
            count += 1;
        }
        if count > 0 {
            info!(
                "apply_pending_commands: last_applied: {}, commit_idx: {}, #{}",
                self.last_applied, self.commit_idx, count
            );
        }
    }

    pub fn apply_command_from_log(&mut self) -> bool {
        debug!("apply_command_from_log");
        let next_apply_idx = self.last_applied + 1;
        // get next applied log entry position
        let next_apply_filepos = {
            // get lock on in_mem_log
            let in_mem_log = self.in_mem_log.lock().unwrap();
            if next_apply_idx > 0 {
                let in_mem_entry = &in_mem_log[(next_apply_idx - 1) as usize];
                in_mem_entry.2
            } else {
                error!("apply_command_from_log: no log entry to apply");
                return false;
            }
            // drop lock
        };

        let mut file = self.logfile.lock().unwrap();

        // seek to the log entry position
        file.seek(SeekFrom::Start(next_apply_filepos))
            .expect("Failed to seek");

        let mut reader = BufReader::new(&mut *file);
        let mut size_buf = [0u8; 4];
        if reader.read_exact(&mut size_buf).is_err() {
            error!("apply_command_from_log: incomplete log entry: cannot read size");
            return false;
        }
        let size = u32::from_le_bytes(size_buf) as usize;
        let mut buffer = vec![0; size];
        if reader.read_exact(&mut buffer).is_err() {
            error!("apply_command_from_log: incomplete log entry: cannot read entry");
            return false;
        }
        let mut checksum_buf = [0u8; 4];
        if reader.read_exact(&mut checksum_buf).is_err() {
            error!("apply_command_from_log: incomplete log entry: cannot read checksum");
            return false;
        }
        // drop the lock before applying the command
        drop(file);

        // verify checksum
        let stored_checksum = u32::from_le_bytes(checksum_buf);
        let mut hasher = Hasher::new();
        hasher.update(&buffer);
        let computed_checksum = hasher.finalize();
        if computed_checksum != stored_checksum {
            error!("apply_command_from_log: checksum mismatch");
            return false;
        }
        let entry = bincode::deserialize::<MyLogEntry>(&buffer);
        match entry {
            Ok(entry) => {
                if entry.idx != next_apply_idx {
                    error!(
                        "apply_command_from_log: index mismatch: expected {}, got {}, cmd {:?}",
                        self.last_applied + 1,
                        entry.idx,
                        entry
                    ); // TODO
                    return false;
                }
                let cmd_idx = entry.idx;
                let cmd_command = entry.command.clone();

                info!(
                    "\tCMD TO APPLY {:?}, next_app_idx {} last_applied {}",
                    entry, cmd_idx, self.last_applied
                );
                return self.apply_command(cmd_idx, cmd_command); // apply cmd will increment last_applied
            }
            Err(e) => {
                println!("Failed to deserialize log entry: {}", e);
            }
        }
        return false;
    }

    pub fn apply_command(&mut self, cmd_idx: u64, command: String) -> bool {
        info!("Apply_command: idx {} {}", cmd_idx, command);
        if cmd_idx > self.commit_idx {
            debug!("cannot apply uncommitted command: {}", command);
            return false;
        }
        if self.last_applied + 1 != cmd_idx {
            debug!(
                "cannot apply command: {}. last_applied: {}, cmd_idx: {}",
                command, self.last_applied, cmd_idx
            );
            return false;
        }

        // apply the command to the state machine
        let mut success = false;
        let applied_lock = self.applied_lock.lock().unwrap();
        let parts: Vec<&str> = command.trim().split_whitespace().collect();
        if parts.len() == 2 {
            // command = "del key"
            let key = parts[1].to_string();
            if let Ok(mut state_machine) = self.state_machine.write() {
                state_machine.remove(&key);
                success = true;
            } else {
                error!("Failed to acquire write lock on state machine");
            }
        } else if parts.len() == 3 {
            // command = "put key value"
            let key = parts[1].to_string();
            let value = parts[2].to_string();
            if let Ok(mut state_machine) = self.state_machine.write() {
                state_machine.insert(key.clone(), value.clone());
                success = true;
            } else {
                error!("Failed to acquire write lock on state machine");
            }
        } else {
            debug!("Invalid command format: {}", command);
        }

        if success {
            self.last_applied += 1; // increment last_applied
            info!(
                "APPLIED COMMAND: last_applied {}: {}. ",
                self.last_applied, command
            );
            self.log_applied_to_file(&format!(
                "APPLIED COMMAND: last_applied {}: {}. ",
                self.last_applied, command
            ));
        }
        drop(applied_lock);
        return success;
    }

    pub fn read_config_file(config_filename: &str) -> (u64, Option<u32>) {
        // try to read the config file
        let mut current_term = 0;
        let mut voted_for = None;
        if let Ok(mut file) = std::fs::File::open(&config_filename) {
            let mut contents = String::new();
            // config file format:
            // 9 -1
            if file.read_to_string(&mut contents).is_ok() {
                let mut lines = contents.lines();
                if let Some(line) = lines.next() {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() == 2 {
                        current_term = parts[0].parse().unwrap_or(0);
                        voted_for = if parts[1] == "-1" {
                            None
                        } else {
                            Some(parts[1].parse::<i32>().unwrap() as u32)
                        };
                        debug!(
                            "read config: current_term: {}, voted_for: {:?}",
                            current_term, voted_for
                        );
                    }
                }
            }
        }
        (current_term, voted_for)
    }

    pub fn write_config(&self) {
        // write voted_for and current_term to config file
        let temp_config_filename = format!("{}.tmp", self.config_filename);
        let mut file = std::fs::File::create(&temp_config_filename).unwrap();
        let voted_for: i32 = self.voted_for.map_or(-1, |v| v as i32);
        let config = format!("{} {}\n", self.current_term, voted_for);
        file.write_all(config.as_bytes()).unwrap();
        std::fs::rename(temp_config_filename, self.config_filename.clone()).unwrap();
    }

    pub fn verify_in_mem_log(&self, in_mem_log: Vec<(u64, u64, u64)>) {
        // pass clone
        let mut i = 0;
        for (idx, _, _) in in_mem_log.clone().iter() {
            if *idx != i + 1 {
                error!(
                    "verify_in_mem_log: index mismatch: expected {}, got {}",
                    i + 1,
                    idx
                );
                // print whole in_mem_log
                for (i, term, pos) in in_mem_log.iter() {
                    error!(
                        "\t\t\tin_mem_log trace: idx: {}, term: {}, pos: {}",
                        i, term, pos
                    );
                }
                std::process::exit(1);
            }
            i += 1;
        }
    }

    pub fn append_to_log_multi(&mut self, given_entries: Vec<LogEntry>) -> bool {
        info!("Append_to_log_multi: {:?}", given_entries);
        // write [len][data][checksum] to disk


        // SKIP OLD ENTRIES
        let mut old_entries = 0;
        let mut entries_to_append = Vec::new();
        for entry in given_entries.clone() {
            if entry.idx < 1 {
                error!("Invalid log entry index: {}", entry.idx);
                continue;
            }
            if entry.idx <= self.last_log_idx {
                // check if entry idx and term match our index and term
                {
                    let in_mem_log = self.in_mem_log.lock().unwrap();
                    let local_term: u64 = if entry.idx > 0 {
                        in_mem_log[(entry.idx - 1) as usize].1
                    } else {
                        0
                    };
                    if entry.term == local_term {
                        // duplicate
                        continue;
                    }
                    info!(
                        "log mismatch: local_term: {}, entry.term: {}",
                        local_term, entry.term
                    );
                }
                // log mismatch -> truncate log, update last_log_idx and last_log_term
                // get lock
                let mut file = self.logfile.lock().unwrap();
                let mut in_mem_log = self.in_mem_log.lock().unwrap();
                let entry_idx = entry.idx;
                let file_pos = {
                    let in_mem_entry = in_mem_log[(entry_idx - 1) as usize];
                    in_mem_entry.2
                };
                file.set_len(file_pos).expect("Failed to truncate log file");
                in_mem_log.truncate((entry_idx - 1) as usize);
                self.last_log_idx = entry_idx - 1;
                self.last_log_term = in_mem_log.last().map_or(0, |(_, term, _)| *term);
                file.seek(SeekFrom::End(0)).expect("Failed to seek to end");
                info!("Truncated log file to {}, {:?}", file_pos, self);
                // flush and sync
                file.flush().expect("Failed to flush log file");
                file.sync_all().expect("Failed to sync log file");
                // drop lock
                
                old_entries += 1;
            }
            // u have to apply all new entries
            entries_to_append.push(entry);
        }

        if old_entries > 0 {
            eprintln!("INFO: {} old entries recved, truncated log file", old_entries);
        }
        
        // get lock
        let mut file = self.logfile.lock().unwrap();
        for entry in entries_to_append.clone() {
            if entry.idx <= self.last_log_idx {
                // not possible
                error!("Impossible log entry index: {}", entry.idx);
                return false;
            }
            let entry = MyLogEntry {
                idx: entry.idx,
                term: entry.term,
                command: entry.command,
            };

            // APPEND A NEW ENTRY
            // serialize entry
            let buffer = bincode::serialize(&entry).expect("Failed to serialize LogEntry");
            // compute checksum
            let mut hasher = Hasher::new();
            hasher.update(&buffer);
            let checksum = hasher.finalize();
            // get current file position
            let entry_filepos = file.seek(SeekFrom::End(0)).expect("Failed to seek to end");
            file.write_all(&(buffer.len() as u32).to_le_bytes())
                .expect("Failed to write size");
            file.write_all(&buffer).expect("Failed to write Log entry");
            file.write_all(&checksum.to_le_bytes())
                .expect("Failed to write checksum");
            info!("APPENDED_MULTI: {:?}", entry);
            // UPDATE LAST LOG INDEX AND TERM
            self.last_log_idx = entry.idx;
            self.last_log_term = entry.term;
            // UPDATE IN MEM LOG
            let mut in_mem_log = self.in_mem_log.lock().unwrap();
            if (in_mem_log.len() as u64) + 1 != entry.idx {
                self.verify_in_mem_log(in_mem_log.clone()); // todo: temporary
                print!("\t\tentry: {:?}, {:?}", entry, self);
                error!("\t\t\tgiven_entries: ");
                for entry in given_entries.iter() {
                    error!("\t\t\ttrace: {:?}", entry);
                }
                std::process::exit(1);
            }
            let in_mem_entry = (entry.idx, entry.term, entry_filepos);
            in_mem_log.push(in_mem_entry);
            info!(
                "APPENDED_MULTI: in_mem_entry: {:?}, log_entry: {:?}",
                in_mem_entry, entry
            );
        }

        file.flush().expect("Failed to flush Log");
        file.sync_all().expect("Failed to sync log file");
        return true;
        // drop lock
    }

    pub fn append_to_log(&mut self, command: String) -> (u64, u64) {
        // get lock
        // write [len][data][checksum] to disk
        let mut file = self.logfile.lock().unwrap();

        info!("Append_to_log: {}", command);
        let new_idx = self.last_log_idx + 1;
        let new_term = self.current_term;
        let entry = MyLogEntry {
            idx: new_idx,
            term: new_term,
            command: command.clone(),
        };
        // serialize entry
        let buffer = bincode::serialize(&entry).expect("Failed to serialize LogEntry");
        // compute checksum
        let mut hasher = Hasher::new();
        hasher.update(&buffer);
        let checksum = hasher.finalize();

        // seek to the end of the file
        let entry_filepos = file.seek(SeekFrom::End(0)).expect("Failed to seek to end");
        file.write_all(&(buffer.len() as u32).to_le_bytes())
            .expect("Failed to write size");
        file.write_all(&buffer).expect("Failed to write Log entry");
        file.write_all(&checksum.to_le_bytes())
            .expect("Failed to write checksum");

        file.flush().expect("Failed to flush Log");
        file.sync_all().expect("Failed to sync log file");
        info!("APPENDED: {:?}", entry);
        if let Ok(mut in_mem_log) = self.in_mem_log.lock() {
            if (in_mem_log.len() as u64) + 1 != entry.idx {
                self.verify_in_mem_log(in_mem_log.clone()); // todo: temporary
                print!("\t\tentry: {:?}, {:?}", entry, self);
                std::process::exit(1);
            }
            let in_mem_entry = (entry.idx, entry.term, entry_filepos);
            in_mem_log.push(in_mem_entry);
            info!(
                "APPENDED: in_mem_entry: {:?}, log_entry: {:?}",
                in_mem_entry, entry
            );
        } else {
            error!("Failed to acquire lock on in_mem_log");
        }
        self.last_log_idx = new_idx;
        self.last_log_term = new_term;
        return (new_idx, new_term);
        // drop lock
    }

    pub fn recover_log(&mut self) -> Option<MyLogEntry> {
        info!("RECOVERING LOG from {}", self.log_filename);

        let mut file = self.logfile.lock().unwrap();
        file.seek(SeekFrom::Start(0)).expect("Failed to seek");

        let mut reader = BufReader::new(&mut *file);
        let mut position: u64;
        let mut i: u64 = 0;
        let mut cmd_term = 0;

        let mut error_encountered = false;
        loop {
            position = reader.seek(SeekFrom::Current(0)).unwrap();
            let cmd_idx = i + 1;

            // check eof
            if reader.fill_buf().expect("Failed to read buffer").is_empty() {
                debug!("\treached EOF at {}", position);
                break;
            }

            // read size
            let mut size_buf = [0u8; 4];
            if reader.read_exact(&mut size_buf).is_err() {
                debug!("\tincomplete log entry: size");
                error_encountered = true;
                break;
            }
            let size = u32::from_le_bytes(size_buf) as usize;

            // read entry
            let mut buffer = vec![0; size];
            if reader.read_exact(&mut buffer).is_err() {
                debug!("\tincomplete log entry: entry");
                error_encountered = true;
                break;
            }

            // read checksum
            let mut checksum_buf = [0u8; 4];
            if reader.read_exact(&mut checksum_buf).is_err() {
                debug!("\tincomplete log entry: checksum");
                error_encountered = true;
                break;
            }

            let stored_checksum = u32::from_le_bytes(checksum_buf);
            // verify checksum
            let mut hasher = Hasher::new();
            hasher.update(&buffer);
            let computed_checksum = hasher.finalize();
            if computed_checksum != stored_checksum {
                debug!("\tchecksum mismatch");
                error_encountered = true;
                break;
            }

            // debug
            let entry = bincode::deserialize::<MyLogEntry>(&buffer);
            match entry {
                Ok(cmd) => {
                    if cmd_idx != cmd.idx {
                        error!("Log entry index mismatch: expected {}, got {}", i, cmd.idx);
                        error_encountered = true;
                        break;
                    }
                    cmd_term = cmd.term;
                    if let Ok(mut in_mem_log) = self.in_mem_log.lock() {
                        if (in_mem_log.len() as u64) + 1 != cmd.idx {
                            error!(
                                "in_mem_log length mismatch: expected {}, got {}",
                                cmd.idx,
                                in_mem_log.len()
                            );
                            error_encountered = true;
                            break;
                        }
                        in_mem_log.push((cmd.idx, cmd.term, position));
                        info!("Recovered {}: pos {}, {:?} ", cmd_idx, position, cmd);
                    } else {
                        error!("Failed to acquire lock on in_mem_log");
                    }
                }
                Err(e) => {
                    error!("Failed to deserialize log entry: {}", e);
                }
            }
            i += 1;
        }
        if error_encountered {
            // Truncate log file to current position
            file.set_len(position).expect("Failed to truncate log");
            debug!("Truncated log file to {}", position);
        }
        debug!("Log replay complete.");
        self.last_log_idx = i;
        self.last_log_term = cmd_term;
        None
    }

    pub async fn check_for_conflicts(&mut self, target_idx: u64, target_term: u64) -> u64 {
        debug!(
            "Verifying log entry at index {} with term {}",
            target_idx, target_term
        );
        let mut file = self.logfile.lock().unwrap();
        file.seek(SeekFrom::Start(0)).expect("Failed to seek");

        let mut reader = BufReader::new(&mut *file);
        let mut position: u64;
        let mut idx: u64 = 1;

        loop {
            position = reader.seek(SeekFrom::Current(0)).unwrap();
            debug!("position {}", position);

            // check eof
            if reader.fill_buf().expect("Failed to read buffer").is_empty() {
                debug!("Reached EOF at {}", position);
                return idx - 1;
            }

            // read size
            let mut size_buf = [0u8; 4];
            if reader.read_exact(&mut size_buf).is_err() {
                debug!("incomplete log entry: cannot read size");
                break;
            }
            let size = u32::from_le_bytes(size_buf) as usize;
            debug!("len {}", size);

            // read entry
            let mut buffer = vec![0; size];
            if reader.read_exact(&mut buffer).is_err() {
                debug!("incomplete log entry: cannot read entry");
                break;
            }

            // read checksum
            let mut checksum_buf = [0u8; 4];
            if reader.read_exact(&mut checksum_buf).is_err() {
                debug!("incomplete log entry: cannot read checksum");
                break;
            }
            let stored_checksum = u32::from_le_bytes(checksum_buf);

            // verify checksum
            let mut hasher = Hasher::new();
            hasher.update(&buffer);
            let computed_checksum = hasher.finalize();
            if computed_checksum != stored_checksum {
                debug!("checksum mismatch");
                break;
            }

            // check for conflicts
            if idx == target_idx {
                let cmd = bincode::deserialize::<MyLogEntry>(&buffer).ok();
                if let Some(cmd) = cmd {
                    if cmd.term == target_term {
                        return target_idx;
                    }
                }
                return idx - 1;
            }

            idx += 1;
        }
        // this should not happen
        error!("truncating log at {}", position);
        file.set_len(position).expect("Failed to truncate log");
        0
    }

    pub async fn check_for_conflicts_fast(&mut self, target_idx: u64, target_term: u64) -> u64 {
        debug!(
            "\tcheck_for_conflicts_fast at index {} with term {}",
            target_idx, target_term
        );
        let mut file = self.logfile.lock().unwrap();
        file.seek(SeekFrom::Start(0)).expect("Failed to seek");

        let mut reader = BufReader::new(&mut *file);
        let mut position: u64;
        let mut idx: u64 = 1;

        loop {
            position = reader.seek(SeekFrom::Current(0)).unwrap();
            // debug!("position {}", position);

            // check eof
            if reader.fill_buf().expect("Failed to read buffer").is_empty() {
                debug!("Reached EOF at {}", position);
                return idx - 1;
            }

            // read size
            let mut size_buf = [0u8; 4];
            if reader.read_exact(&mut size_buf).is_err() {
                debug!("incomplete log entry: cannot read size");
                break;
            }
            let size = u32::from_le_bytes(size_buf) as usize;
            // debug!("len {}", size);

            if idx < target_idx {
                // skip entry + checksum
                reader.seek_relative((size + 4) as i64).unwrap(); // +4 for checksum
                idx += 1;
                continue;
            }

            // if idx == target_idx {

            // read entry
            let mut buffer = vec![0; size];
            if reader.read_exact(&mut buffer).is_err() {
                debug!("incomplete log entry: cannot read entry");
                break;
            }
            // read checksum
            let mut checksum_buf = [0u8; 4];
            if reader.read_exact(&mut checksum_buf).is_err() {
                debug!("incomplete log entry: cannot read checksum");
                break;
            }
            let stored_checksum = u32::from_le_bytes(checksum_buf);
            // verify checksum
            let mut hasher = Hasher::new();
            hasher.update(&buffer);
            let computed_checksum = hasher.finalize();
            if computed_checksum != stored_checksum {
                debug!("checksum mismatch");
                break;
            }
            let cmd = bincode::deserialize::<MyLogEntry>(&buffer).ok();
            if let Some(cmd) = cmd {
                if cmd.term == target_term {
                    return target_idx;
                }
            }
            return idx - 1;
        }
        // this should not happen
        error!("truncating log at {}", position);
        file.set_len(position).expect("Failed to truncate log");
        0
    }

    pub async fn check_for_conflicts_fastest(&mut self, target_idx: u64, target_term: u64) -> u64 {
        let in_mem_log = self.in_mem_log.lock().unwrap();
        // for (i, term) in in_mem_log.iter() {
        //     if *i == target_idx {
        //         if *term == target_term {
        //             return target_idx;
        //         } else {
        //             return target_idx - 1;
        //         }
        //     }
        // }
        let in_mem_idx = target_idx - 1;
        let target_entry = in_mem_log[in_mem_idx as usize];
        if target_entry.0 == target_idx {
            if target_entry.1 == target_term {
                return target_idx;
            } else {
                return target_idx - 1;
            }
        }
        0 // Return 0 if no match is found
    }

    pub async fn get_log_entries_from(&self, target_idx: u64) -> Vec<LogEntry> {
        debug!("\tget_log_entries_from at index {}", target_idx);
        if target_idx > self.last_log_idx {
            return vec![];
        }

        let mut file = self.logfile.lock().unwrap();
        file.seek(SeekFrom::Start(0)).expect("Failed to seek");

        let mut reader = BufReader::new(&mut *file);
        // let mut position: u64;
        let mut idx: u64 = 1;

        let mut entries = Vec::new();
        loop {
            // position = reader.seek(SeekFrom::Current(0)).unwrap();
            // debug!("position {}", position);

            // check eof
            if reader.fill_buf().expect("Failed to read buffer").is_empty() {
                // debug!("Reached EOF at {}", position);
                return entries;
            }

            // read size
            let mut size_buf = [0u8; 4];
            if reader.read_exact(&mut size_buf).is_err() {
                debug!("incomplete log entry: cannot read size");
                break;
            }
            let size = u32::from_le_bytes(size_buf) as usize;
            // debug!("len {}", size);

            // read entry
            let mut buffer = vec![0; size];
            if reader.read_exact(&mut buffer).is_err() {
                debug!("incomplete log entry: cannot read entry");
                break;
            }

            // read checksum
            let mut checksum_buf = [0u8; 4];
            if reader.read_exact(&mut checksum_buf).is_err() {
                debug!("incomplete log entry: cannot read checksum");
                break;
            }
            // verify checksum
            let stored_checksum = u32::from_le_bytes(checksum_buf);
            let mut hasher = Hasher::new();
            hasher.update(&buffer);
            let computed_checksum = hasher.finalize();
            if computed_checksum != stored_checksum {
                debug!("checksum mismatch");
                break;
            }

            if idx >= target_idx {
                let entry = bincode::deserialize::<MyLogEntry>(&buffer).unwrap();
                let log_entry = LogEntry {
                    idx: entry.idx,
                    term: entry.term,
                    command: entry.command,
                };
                entries.push(log_entry);
            }

            idx += 1;
        }
        entries
    }

    pub fn get_term_at_index(&self, index: u64) -> Option<u64> {
        // check if index is valid
        if index < 1 || index > self.last_log_idx {
            return None;
        }
        let in_mem_idx = index - 1;
        let in_mem_log = self.in_mem_log.lock().unwrap();
        if in_mem_idx < in_mem_log.len() as u64 {
            let entry = in_mem_log[in_mem_idx as usize];
            return Some(entry.1);
        }
        None
    }
}
