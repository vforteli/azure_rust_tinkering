use azure_storage::StorageCredentials;
use azure_storage_datalake::{self, clients::DataLakeClient};
use futures::stream::StreamExt;
use std::iter::Iterator;
use std::sync::{Arc, Mutex};
use tokio::task::JoinSet;
extern crate dotenv;
use dotenv::dotenv;
use std::env;

#[tokio::main]
async fn main() -> azure_core::Result<()> {
    dotenv().ok();

    let account = env::var("ACCOUNT").expect("Datalake account name seems to be missing...");
    let sas_token = env::var("SAS_TOKEN").expect("Guess the sas token is missing...");

    let paths = Arc::new(Mutex::new(Vec::new()));
    let mut file_system_tasks = JoinSet::new();

    let data_lake_client = Arc::new(DataLakeClient::new(
        account,
        StorageCredentials::sas_token(sas_token).expect("hu? check your token"),
    ));

    let mut filesystems = data_lake_client.list_file_systems().into_stream();

    while let Some(fs_response) = filesystems.next().await {
        fs_response.unwrap().into_iter().for_each(|f| {
            let client = Arc::clone(&data_lake_client);
            let pathssss = Arc::clone(&paths);

            file_system_tasks.spawn(async move {
                let file_system_client = client.file_system_client(f.name.to_string());
                println!("Listing files in path {} started", f.name);

                let mut paths = file_system_client
                    .list_paths()
                    .recursive(false)
                    .into_stream();

                while let Some(path_response) = paths.next().await {
                    pathssss
                        .lock()
                        .unwrap()
                        .append(&mut path_response.unwrap().paths);
                }
            });
        })
    }

    while let Some(_) = file_system_tasks.join_next().await {
        // eh.. its fine
    }

    Arc::try_unwrap(paths)
        .unwrap()
        .into_inner()
        .unwrap()
        .iter()
        .for_each(|path| {
            println!("{}", path.name);
        });

    Ok(())
}
