use azure_rust_tinkering::PathClient;
use azure_storage_datalake::file_system::Path;
use azure_storage_datalake::{self};
use std::sync::Arc;
use tokio::sync::mpsc;
extern crate dotenv;
use dotenv::dotenv;
use std::env;

#[tokio::main]
async fn main() -> azure_core::Result<()> {
    dotenv().ok();

    let account = env::var("ACCOUNT").expect("Datalake account name seems to be missing...");
    let sas_token = env::var("SAS_TOKEN").expect("Guess the sas token is missing...");
    let file_system_name =
        env::var("FILE_SYSTEM_NAME").expect("Dont forget file system name either...");

    let (paths_sender, mut paths_receiver) = mpsc::channel::<Option<Path>>(10000);
    let paths_sender = Arc::new(paths_sender);

    let path_client = PathClient::new(account.to_string(), sas_token.to_string());

    let read_task = tokio::spawn(async move {
        let mut read_count = 0;
        loop {
            match paths_receiver.recv().await {
                Some(_) => {
                    read_count += 1;

                    if read_count % 1000 == 0 {
                        // println!("Read {} files so far", read_count);
                    }
                }
                None => break,
            }
        }

        println!("Read all paths \\o/");
    });

    let count = path_client
        .list_paths_parallel(
            file_system_name.to_string(),
            "/".to_string(),
            paths_sender,
            256,
        )
        .await;

    read_task.await.expect("durr...");

    println!("Found {} files", count);
    Ok(())
}
