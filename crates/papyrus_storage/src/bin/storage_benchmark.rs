use std::fs::read_to_string;
use std::time::Duration;

use clap::{Arg, Command};
use papyrus_storage::db::DbConfig;
use papyrus_storage::state::StateStorageReader;
use papyrus_storage::{StorageConfig, StorageQuery};
use serde::{Deserialize, Serialize};
use starknet_api::core::ChainId;

// TODO(dvir): consider add logger and use it for the prints.

fn main() {
    let cli_params = get_cli_params();

    // Creates List of queries to be executed.
    println!("Creating queries");
    let mut queries: Vec<StorageQuery> = Vec::new();
    for line in
        read_to_string(cli_params.queries_file_path).expect("Fail to read queries file").lines()
    {
        queries.push(serde_json::from_str(line).expect("Failed to parse query"));
    }

    // Open storage to execute the queries.
    println!("Opening storage");
    let db_config = DbConfig {
        path_prefix: cli_params.db_file_path.into(),
        chain_id: ChainId(cli_params.chain_id),
        ..Default::default()
    };
    let config = StorageConfig { db_config, ..Default::default() };

    let (reader, mut _writer) =
        papyrus_storage::open_storage(config).expect("Failed to open storage");
    let txn = reader.begin_ro_txn().expect("Failed to begin read only transaction");
    let state_reader = txn.get_state_reader().expect("Failed to get state reader");

    let mut times = Times::default();

    // Execute the queries and measure the time it takes to execute them.
    println!("Executing queries");
    for q in queries {
        match q {
            StorageQuery::GetClassHashAt(state_number, contract_address) => {
                let now = std::time::Instant::now();
                let _class_hash = state_reader.get_class_hash_at(state_number, &contract_address);
                let exec_time = now.elapsed();
                times.get_class_hash_at.push(exec_time);
                println!(
                    " - get_class_hash_at({state_number:?}, {contract_address:?})\n - time: {:?}",
                    exec_time.as_nanos()
                );
            }
            StorageQuery::GetNonceAt(state_number, contract_address) => {
                let now = std::time::Instant::now();
                let _nonce = state_reader.get_nonce_at(state_number, &contract_address);
                let exec_time = now.elapsed();
                times.get_nonce_at.push(exec_time);
                println!(
                    " - get_nonce_at({state_number:?}, {contract_address:?})\n - time: {:?}",
                    exec_time.as_nanos()
                );
            }
            StorageQuery::GetStorageAt(state_number, contract_address, storage_key) => {
                let now = std::time::Instant::now();
                let _storage =
                    state_reader.get_storage_at(state_number, &contract_address, &storage_key);
                let exec_time = now.elapsed();
                times.get_storage_at.push(exec_time);
                println!(
                    " - get_storage_at({state_number:?}, {contract_address:?}, {storage_key:?})\n \
                     - time: {:?}",
                    exec_time.as_nanos()
                );
            }
        }
    }

    println!("Finished executing queries");

    print_times(times);
}

// Records the time it takes to execute the queries.
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Times {
    get_class_hash_at: Vec<Duration>,
    get_nonce_at: Vec<Duration>,
    get_storage_at: Vec<Duration>,
}

fn print_times(times: Times) {
    let get_class_hash_at_time_sum = times.get_class_hash_at.iter().sum::<Duration>();
    let get_nonce_at_time_sum = times.get_nonce_at.iter().sum::<Duration>();
    let get_storage_at_time_sum = times.get_storage_at.iter().sum::<Duration>();

    println!("Times:");
    println!(" - GetClassHashAt: {:?}", get_class_hash_at_time_sum.as_nanos());
    println!(" - GetNonceAt: {:?}", get_nonce_at_time_sum.as_nanos());
    println!(" - GetStorageAt: {:?}", get_storage_at_time_sum.as_nanos());
    println!(
        " - total time: {:?}",
        (get_class_hash_at_time_sum + get_nonce_at_time_sum + get_storage_at_time_sum).as_nanos()
    );
}

struct CliParams {
    queries_file_path: String,
    db_file_path: String,
    chain_id: String,
}

fn get_cli_params() -> CliParams {
    let matches = Command::new("Storage benchmark")
        .arg(
            Arg::new("queries_file_path")
                .short('q')
                .long("queries_file_path")
                .required(true)
                .help("The path to a file with the queries"),
        )
        .arg(
            Arg::new("db_file_path")
                .short('d')
                .long("db_file_path")
                .required(true)
                .help("The path to the database file"),
        )
        .arg(
            Arg::new("chain_id")
                .short('c')
                .long("chain_id")
                .required(true)
                .help("The chain id SN_MAIN/SN_GOERLI for example"),
        )
        .get_matches();

    let queries_file_path = matches
        .get_one::<String>("queries_file_path")
        .expect("Failed parsing queries_file_path")
        .to_string();
    let db_file_path =
        matches.get_one::<String>("db_file_path").expect("Failed parsing db_file_path").to_string();
    let chain_id =
        matches.get_one::<String>("chain_id").expect("Failed parsing chain_id").to_string();
    CliParams { queries_file_path, db_file_path, chain_id }
}
