use azure_core::base64::encode;
use azure_rust_tinkering::batch_uploader::BatchUploader;
use azure_rust_tinkering::document_model::DocumentModel;
use azure_rust_tinkering::path_client::PathClient;
use azure_rust_tinkering::path_index_client::{ListPathsOptions, PathIndexClient};
use azure_rust_tinkering::path_index_model::PathIndexModel;
use azure_rust_tinkering::search_indexer::SearchIndexer;
use azure_rust_tinkering::test_index_model::TestIndexModel;
use azure_storage_datalake::file_system::Path;
use azure_storage_datalake::operations::HeadPathResponse;
use azure_storage_datalake::{self};
use azure_svc_search::package_2023_11_searchindex::search_extensions;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Instant;
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
    azure_search_url: &str,
    account: String,
    sas_token: String,
) {
    let max_upload_threads = 4;
    let upload_batch_size = 1000;

    let client = PathIndexClient::new(
        azure_search_url,
        search_extensions::SearchAuthenticationMethod::ApiKey(azure_search_key.to_string()),
    );

    let batch_uploader = BatchUploader::new(
        &azure_search_url,
        search_extensions::SearchAuthenticationMethod::ApiKey(azure_search_key.to_string()),
    );

    let (paths_sender, paths_receiver) =
        mpsc::channel::<Option<PathIndexModel>>(upload_batch_size * max_upload_threads * 2);
    let paths_sender = Arc::new(paths_sender);

    let (documents_sender, documents_receiver) =
        mpsc::channel::<TestIndexModel>(upload_batch_size * (max_upload_threads + 2));
    let documents_sender = Arc::new(documents_sender);

    let start_time = Instant::now();

    let process_documents_task = tokio::spawn(async move {
        fn mapping_func(
            path: PathIndexModel,
            document: DocumentModel,
            properties: HeadPathResponse,
        ) -> Option<TestIndexModel> {
            let index_model = TestIndexModel {
                booleanvalue: document.booleanvalue,
                etag: properties.etag,
                last_modified: properties.last_modified,
                numbervalue: document.numbervalue,
                stringvalue: document.stringvalue,
                path_base64: encode(format!("{}%2f{}", path.file_system, path.path_url_encoded)),
                path_url_encoded: path.path_url_encoded,
            };

            Some(index_model)
        }

        let indexer = SearchIndexer::new(account.to_string(), sas_token.to_string());
        let result = indexer
            .index_documents(
                paths_receiver,
                documents_sender,
                Arc::new(mapping_func),
                128,
            )
            .await;

        println!("Read {} documents... done...", result.unwrap());
    });

    let upload_documents_task = tokio::spawn(async move {
        batch_uploader
            .upload_batches(documents_receiver, max_upload_threads)
            .await;

        println!("Uploaded all documents \\o/");
    });

    let list_count = client
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

    println!(
        "Done after {}ms",
        Instant::now()
            .checked_duration_since(start_time)
            .unwrap()
            .as_millis()
    );
    println!("Found {} paths in index", list_count.unwrap());
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
