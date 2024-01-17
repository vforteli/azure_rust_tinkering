use crate::test_index_model::TestIndexModel;
use azure_core::Url;
use azure_svc_search::package_2023_11_searchindex::models::index_action::SearchAction;
use azure_svc_search::package_2023_11_searchindex::models::{IndexAction, IndexBatch};
use azure_svc_search::package_2023_11_searchindex::{self, search_extensions, Client};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::usize;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

pub struct BatchUploader {
    search_client: Client,
}

pub const TEST_INDEX_NAME: &str = "someindex-large";

impl BatchUploader {
    pub fn new(
        search_service_url: &str,
        credential: search_extensions::SearchAuthenticationMethod,
    ) -> Self {
        let search_client = package_2023_11_searchindex::ClientBuilder::new(credential)
            .endpoint(
                Url::from_str(&format!("{}/{}", search_service_url, TEST_INDEX_NAME))
                    .expect("Invalid search service url probably"),
            )
            .build()
            .expect("Something went haywire creating client?!");

        Self { search_client }
    }

    pub async fn upload_batches(
        &self,
        mut documents_receiver: Receiver<TestIndexModel>,
        max_threads: usize,
    ) {
        let document_client = Arc::new(self.search_client.documents_client());
        let mut tasks = JoinSet::<()>::new();
        let semaphore = Arc::new(Semaphore::new(max_threads));

        let processed_counter = Arc::new(AtomicU64::new(0));

        let mut buffer = Vec::<TestIndexModel>::with_capacity(1000);

        loop {
            let document = documents_receiver.recv().await;
            let has_more = document.is_some();

            if let Some(d) = document {
                buffer.push(d);
            }

            // todo this should also send the batch if above size limit
            if (buffer.len()) % 1000 == 0 || (!has_more && buffer.len() > 0) {
                println!("Sending batch");

                let processed_counter = Arc::clone(&processed_counter);
                let semaphore = Arc::clone(&semaphore);
                let document_client = Arc::clone(&document_client);
                let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();

                let items = buffer
                    .drain(..)
                    .map(|d| IndexAction::new(SearchAction::MergeOrUpload(d)))
                    .collect::<Vec<_>>();

                tasks.spawn(async move {
                    let result = document_client.index(IndexBatch::new(items)).await.unwrap();

                    let current =
                        processed_counter.fetch_add(result.value.len() as u64, Ordering::SeqCst);
                    println!("Uploaded {} documents", current + result.value.len() as u64);
                    drop(permit);
                });
            }

            if buffer.len() == 0 && !has_more {
                break;
            }
        }

        // wait for remaining tasks...
        println!("Waiting for all upload tasks to complete...");
        while let Some(_) = tasks.join_next().await {}

        println!("Upload done");
        println!("Uploaded {:?} documents", processed_counter);
    }
}
