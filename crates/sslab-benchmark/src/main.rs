pub mod workloads;
use std::sync::Arc;

// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use clap::{crate_name, crate_version, App, AppSettings};
use ethers_providers::{Http, Provider, ProviderExt};
use eyre::Context;
use futures::{future::join_all, StreamExt};
use tokio::{
    net::TcpStream,
    time::{interval, sleep, Duration, Instant},
};
use tracing::{info, subscriber::set_global_default, warn};
use tracing_subscriber::filter::EnvFilter;
use narwhal_types::{TransactionProto, TransactionsClient};
use url::Url;
use crate::workloads::handlers::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID};


#[tokio::main]
async fn main() -> Result<(), eyre::Report> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Narwhal and Tusk.")
        .long_about("To run the benchmark client following are required:\n\
        * the size of the transactions via the --size property\n\
        * the worker address <ADDR> to send the transactions to. A url format is expected ex http://127.0.0.1:7000\n\
        * the rate of sending transactions via the --rate parameter\n\
        \n\
        Optionally the --nodes parameter can be passed where a list (comma separated string) of worker addresses\n\
        should be passed. The benchmarking client will first try to connect to all of those nodes before start sending\n\
        any transactions. That confirms the system is up and running and ready to start processing the transactions.")
        .args_from_usage("<ADDR> 'The network address of the node where to send txs. A url format is expected ex http://127.0.0.1:7000'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses, comma separated, that must be reachable before starting the benchmark.'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    cfg_if::cfg_if! {
        if #[cfg(feature = "benchmark")] {
            let timer = tracing_subscriber::fmt::time::UtcTime::rfc_3339();
            let subscriber_builder = tracing_subscriber::fmt::Subscriber::builder()
                                     .with_env_filter(env_filter)
                                     .with_timer(timer).with_ansi(false);
        } else {
            let subscriber_builder = tracing_subscriber::fmt::Subscriber::builder().with_env_filter(env_filter);
        }
    }
    let subscriber = subscriber_builder.with_writer(std::io::stderr).finish();

    set_global_default(subscriber).expect("Failed to set subscriber");

    let target_str = matches.value_of("ADDR").unwrap();
    let target = target_str.parse::<Url>().with_context(|| {
        format!(
            "Invalid url format {target_str}. Should provide something like http://127.0.0.1:7000"
        )
    })?;
    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let nodes = matches
        .values_of("nodes")
        .unwrap_or_default()
        .map(|x| x.parse::<Url>())
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("Invalid url format {target_str}"))?;

    info!("Node address: {target}");

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {rate} tx/s");

    let client = MultipleClient::new(target, rate, nodes);
    // let client = Client {
    //     target,
    //     rate,
    //     nodes,
    // };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct MultipleClient {
    clients: Vec<Arc<Client>>,
}

impl MultipleClient {

    const MAX_RATE_PER_CLIENT: u64 = 1000;

    pub fn new (target: Url, rate: u64, nodes: Vec<Url>) -> MultipleClient {
        let num_of_clients = std::cmp::max((rate + Self::MAX_RATE_PER_CLIENT - 1) / Self::MAX_RATE_PER_CLIENT, 1);

        info!("Number of clients: {num_of_clients}");

        let mut clients = Vec::new();

        for _ in 0..num_of_clients {
            let client = Client {
                target: target.clone(),
                rate: rate / num_of_clients,
                nodes: nodes.clone(),
            };
            clients.push(Arc::new(client));
        }
        MultipleClient {
            clients
        }
    }

    pub async fn wait(&self) {
        join_all(
            self.clients.iter().cloned().map(|client| {
                tokio::spawn(async move {
                    client.wait().await;
                })
            })
        ).await;
    }

    pub async fn send(&self) -> Result<(), eyre::Report> {
        let results = join_all(
            self.clients.iter().cloned().map(|client| {
                tokio::spawn(async move {
                    client.send().await
                })
            })
        ).await;

        for result in results {
            if let Err(e) = result {
                return Err(e.into());
            }
        }

        Ok(())
    }
}


struct Client {
    target: Url,
    rate: u64,
    nodes: Vec<Url>,
}

impl Client {
    pub async fn send(&self) -> Result<(), eyre::Report> {
        // We are distributing the transactions that need to be sent
        // within a second to sub-buckets. The precision here represents
        // the number of such buckets within the period of 1 second.
        const PRECISION: u64 = 20;
        // The BURST_DURATION represents the period for each bucket we
        // have split. For example if precision is 20 the 1 second (1000ms)
        // will be split in 20 buckets where each one will be 50ms apart.
        // Basically we are looking to send a list of transactions every 50ms.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        let burst = self.rate / PRECISION;

        if burst == 0 {
            return Err(eyre::Report::msg(format!(
                "Transaction rate is too low, should be at least {} tx/s and multiples of {}",
                PRECISION, PRECISION
            )));
        }

        // Connect to the mempool.
        let mut client = TransactionsClient::connect(self.target.as_str().to_owned())
            .await
            .context(format!("failed to connect to {}", self.target))?;

        // Submit all transactions.
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        let provider = Provider::<Http>::connect(self.target.as_str()).await;
        let mut handler = SmallBankTransactionHandler::new(provider, client.clone(), DEFAULT_CHAIN_ID);
        if let Err(e) = handler.init().await {
            warn!("Failed to initialize workload handler: {e}");
            return Err(e.into());
        }
        let handler = Arc::new(handler);


        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            let handler_copy = handler.clone();

            let stream = tokio_stream::iter(0..burst).map(move |_| {

                let raw_tx = handler_copy.create_random_request();
            
                // NOTE: This log entry is used to compute performance.
                let tx_id = u64::from_be_bytes(raw_tx[2..10].try_into().unwrap());
                info!("Sending sample transaction {tx_id}"); //, {}", hex::encode(&raw_tx));
                
                TransactionProto { transaction: raw_tx.into() }
            });

            if let Err(e) = client.submit_transaction_stream(stream).await {
                warn!("Failed to send transaction: {e}");
                break 'main;
            }

            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
        }
        Ok(())
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(&*address.socket_addrs(|| None).unwrap())
                    .await
                    .is_err()
                {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }
}
