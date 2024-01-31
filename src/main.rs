use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use awaitgroup::WaitGroup;
use clap::Parser;
use paho_mqtt::{CreateOptions, CreateOptionsBuilder};

static STOP: AtomicBool = AtomicBool::new(false);

#[tokio::main]
async fn main() {
    let c: Config = Config::parse();

    tokio::spawn(async {
        if tokio::signal::ctrl_c().await.is_ok() {
            STOP.store(true, Ordering::SeqCst);
            println!("exit.")
        }
    });

    println!("config:{:?}", c);
    let benchmark = Benchmark::new(c);
    benchmark.run().await
}

#[derive(Clone, Debug, Parser)]
#[clap(version = "0.1.0", author = "chen quan <chenquan.dev@gmail.com>")]
#[command(name = "mqtt-benchmark")]
#[command(about = "A simple MQTT (broker) benchmarking tool.", long_about = None)]
struct Config {
    #[arg(long)]
    client_id: String,
    #[arg(long)]
    broker: String,
    #[arg(long)]
    topic: String,
    #[arg(long)]
    username: Option<String>,
    #[arg(long)]
    password: Option<String>,
    #[arg(long)]
    qos: i32,
    #[arg(long)]
    payload: Option<String>,
    #[arg(long)]
    size: Option<usize>,
    #[arg(long)]
    client_num: usize,
}

struct Mqtt {
    client: paho_mqtt::Client,
    c: Config,
}

#[derive(Clone, Debug)]
struct Stat {
    cli: usize,
    num: i64,
    duration: time::Duration,
}

impl From<&Config> for CreateOptions {
    fn from(value: &Config) -> Self {
        let builder = CreateOptionsBuilder::new();
        builder
            .client_id(&value.client_id)
            .server_uri(&value.broker)
            .finalize()
    }
}

impl Mqtt {
    fn new(c: Config) -> Self {
        Mqtt {
            client: paho_mqtt::Client::new(&c).unwrap(),
            c,
        }
    }

    async fn init_conn(&self) -> bool {
        let mut connect_opts_builder = paho_mqtt::ConnectOptionsBuilder::new();
        connect_opts_builder
            .keep_alive_interval(Duration::from_secs(60))
            .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(60))
            .clean_session(true);

        if let Some(username) = &self.c.username {
            connect_opts_builder.user_name(username.clone());
        }
        if let Some(password) = &self.c.password {
            connect_opts_builder.password(password.clone());
        }
        let connect_opts = connect_opts_builder.finalize();

        let result = self.client.connect(connect_opts);
        result.is_ok()
    }

    async fn do_benchmark(&self, client_num: usize) -> Option<Stat> {
        let start = time::Instant::now();
        let mut i = 0usize;
        while self.client.is_connected() {
            if STOP.load(Ordering::SeqCst) {
                break;
            }

            let mut msg_builder = paho_mqtt::MessageBuilder::new()
                .topic(&self.c.topic)
                .qos(self.c.qos);

            if let Some(data) = &self.c.payload {
                msg_builder = msg_builder.payload(data.as_bytes());
            }
            let message = msg_builder.finalize();
            if let Err(err) = self.client.publish(message) {
                println!("client: {}, publish failed:{}", client_num, err);
            }
            if let Some(size) = self.c.size {
                if i >= size {
                    break;
                }
            }

            i += 1
        }

        let duration = start.elapsed();
        if self.client.is_connected() {
            if let Err(err) = self.client.disconnect_after(Duration::from_secs(1)) {
                println!("client: {}, disconnect failed:{}", client_num, err);
            }
        } else {
            println!("client: {}, connection is closed earl.", client_num);
            return None;
        }

        let speed = (i as f64) / duration.as_seconds_f64();
        let stat = Stat {
            cli: client_num,
            num: i as i64,
            duration,
        };
        println!(
            "client: {}, num: {}, duration: {}, speed: {} (msg/sec)",
            stat.cli, stat.num, stat.duration, speed
        );

        Some(stat)
    }
}

// ----------------

struct Benchmark {
    config: Config,
}

impl Benchmark {
    fn new(config: Config) -> Self {
        Benchmark { config }
    }

    async fn run(&self) {
        let mut wg = WaitGroup::new();

        let clients = self.init_conn().await;
        let clients = Arc::new(clients);

        let lock = Arc::new(Mutex::new(Vec::<Stat>::new()));
        let start = time::Instant::now();

        for i in 0..self.config.client_num {
            let worker = wg.worker();
            let lock = lock.clone();
            let clients = clients.clone();

            tokio::spawn(async move {
                if let Some(client) = clients.get(i) {
                    let stat_op = client.do_benchmark(i).await;
                    let mut lock = lock.lock().await;
                    if let Some(stat) = stat_op {
                        lock.push(stat);
                    }
                }
                worker.done();
            });
        }
        wg.wait().await;

        {
            let guard = lock.lock().await;
            let mut num = 0i64;
            let duration = start.elapsed();

            for x in guard.iter() {
                num += x.num;
            }

            println!(
                "{} client, total: {}, duration: {}, avg speed: {} (msg/sec)",
                self.config.client_num,
                num,
                duration,
                (num as f64) / duration.as_seconds_f64()
            );
        }
    }
    
    async fn init_conn(&self) -> Vec<Arc<Mqtt>> {
        let start = time::Instant::now();

        let mut wg = WaitGroup::new();
        let mut clients = vec![];
        for i in 0..self.config.client_num {
            let worker = wg.worker();
            let mut config = self.config.clone();
            config.client_id = format!("{}_{}", config.client_id, i);
            config.topic = format!("{}/{}", config.topic, i);

            let mqtt = Arc::new(Mqtt::new(config));

            clients.push(mqtt.clone());

            let mqtt = mqtt.clone();
            tokio::spawn(async move {
                if !mqtt.init_conn().await {
                    println!("client: {}, connect failed", i);
                }
                worker.done();
            });
        }
        wg.wait().await;
        let duration = start.elapsed();
        println!(
            "{} client connected. duration: {}, speed: {} (client/sec)",
            self.config.client_num,
            duration,
            self.config.client_num as f64 / duration.as_seconds_f64()
        );
        clients
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn client() {
        let mqtt = Mqtt::new(Config {
            client_id: "x".to_string(),
            broker: "localhost:1883".to_string(),
            topic: "iot/".to_string(),
            username: None,
            password: None,
            qos: 1,
            payload: Some("1".to_string()),
            size: None,
            client_num: 0,
        });
        mqtt.do_benchmark(1).await;
    }

    #[tokio::test]
    async fn benchmark() {
        let benchmark = Benchmark::new(Config {
            client_id: "x".to_string(),
            broker: "localhost:1883".to_string(),
            topic: "iot/".to_string(),
            username: None,
            password: None,
            qos: 1,
            payload: Some("1".to_string()),
            size: None,
            client_num: 0,
        });
        benchmark.run().await
    }
}
