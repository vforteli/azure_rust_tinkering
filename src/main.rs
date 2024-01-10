use azure_rust_tinkering::path_client::PathClient;
use azure_rust_tinkering::path_index_client::{ListPathsOptions, PathIndexClient};
use azure_rust_tinkering::path_index_model::PathIndexModel;
use azure_rust_tinkering::search_indexer::SearchIndexer;
use azure_rust_tinkering::test_index_model::TestIndexModel;
use azure_storage_datalake::file_system::Path;
use azure_storage_datalake::{self};
use azure_svc_search::package_2023_11_searchindex::search_extensions;
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
    let azure_search_key = env::var("AZURE_SEARCH_KEY").expect("No azure search key found...");
    let azure_search_account_name =
        env::var("AZURE_SEARCH_ACCOUNT_NAME").expect("No azure search key found...");

    run_list_paths_index_test(
        &azure_search_key,
        &azure_search_account_name,
        account,
        sas_token,
    )
    .await;

    // testing list paths...
    // run_list_paths_test().await;

    Ok(())
}

// Test listing paths from path index
async fn run_list_paths_index_test(
    azure_search_key: &str,
    azure_search_account_name: &str,
    account: String,
    sas_token: String,
) {
    let client = PathIndexClient::new(
        azure_search_account_name,
        search_extensions::SearchAuthenticationMethod::ApiKey(azure_search_key.to_string()),
    );

    let (paths_sender, paths_receiver) = mpsc::channel::<Option<PathIndexModel>>(10000);
    let paths_sender = Arc::new(paths_sender);

    let (documents_sender, mut documents_receiver) = mpsc::channel::<Option<TestIndexModel>>(10000);
    let documents_sender = Arc::new(documents_sender);

    let process_documents_task = tokio::spawn(async move {
        let indexer = SearchIndexer::new(account.to_string(), sas_token.to_string());
        let result = indexer
            .read_documents(paths_receiver, documents_sender, 128)
            .await;

        println!("Read {} documents... done...", result.unwrap());
    });

    let upload_documents_task = tokio::spawn(async move {
        let mut dummy_upload_count = 0;
        loop {
            match documents_receiver.recv().await {
                Some(_) => {
                    dummy_upload_count += 1;

                    if dummy_upload_count % 1000 == 0 {
                        println!("Uploaded {} files so far", dummy_upload_count);
                    }
                }
                None => break,
            }
        }

        println!("Uploaded all documents \\o/");
    });

    let count = client
        .list_paths(
            ListPathsOptions {
                filter: None, //Some("search.ismatch('partition_9*')".to_string()),
                from_last_modified: None,
            },
            paths_sender,
        )
        .await;

    process_documents_task.await.expect("durr...");
    upload_documents_task.await.expect("hu?");

    println!("Found {} paths in index", count.unwrap());
}

// Test listing paths from datalake
async fn run_list_paths_test(account: &str, sas_token: &str, file_system_name: &str) {
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
}
