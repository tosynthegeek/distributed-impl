use std::time::Instant;

use colored::*;
use libp2p::identity;
use sha2::{Digest, Sha256};
use tracing::info;

pub struct ProgressIndicator {
    phase: String,
    start_time: std::time::Instant,
}

impl ProgressIndicator {
    pub fn start(phase: &str) -> Self {
        let message = format!(
            "{} {}",
            "▶".bright_yellow().bold(),
            phase.bright_white().bold()
        );
        info!("{}", message);
        println!();
        Self {
            phase: phase.to_string(),
            start_time: Instant::now(),
        }
    }

    pub fn update(&self, message: &str) {
        info!(phase = %self.phase, "{}", message);
        let msg = format!("{}   └─ {}", "│".bright_blue(), message.dimmed());
        info!("{}", msg);
        println!();
    }

    pub fn finish(self) {
        let duration = self.start_time.elapsed();

        let msg = format!(
            "{} {} {} {}",
            "✓".bright_green().bold(),
            self.phase.bright_white().bold(),
            "completed in".dimmed(),
            format!("{:.2?}", duration).bright_cyan()
        );
        info!("{}", msg);

        println!();
    }

    pub fn error(self, error: &str) {
        let msg = format!(
            "{} {} {}: {}",
            "✗".bright_red().bold(),
            self.phase.bright_white().bold(),
            "failed".bright_red(),
            error.bright_red().bold()
        );
        info!("{}", msg);
        println!();
    }
}

pub fn keypair_from_name(name: &str) -> identity::Keypair {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    let result = hasher.finalize();

    let seed: [u8; 32] = result.into();

    let secret = identity::ed25519::SecretKey::try_from_bytes(seed)
        .expect("Valid 32 bytes for Ed25519 secret key");
    let kp = identity::ed25519::Keypair::from(secret);

    identity::Keypair::from(kp)
}
