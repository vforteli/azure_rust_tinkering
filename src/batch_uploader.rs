use crate::test_index_model::TestIndexModel;
use azure_core::Url;
use azure_svc_search::package_2023_11_searchindex::models::index_action::SearchAction;
use azure_svc_search::package_2023_11_searchindex::models::{IndexAction, IndexBatch};
use azure_svc_search::package_2023_11_searchindex::{self, search_extensions, Client};
use std::str::FromStr;
use std::usize;
use tokio::sync::mpsc::Receiver;

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
        mut documents_receiver: Receiver<Option<TestIndexModel>>,
        max_threads: usize,
    ) {
        let document_client = self.search_client.documents_client();

        let mut buffer = Vec::<TestIndexModel>::with_capacity(1000);

        // todo spawn...
        while let Some(document) = documents_receiver.recv().await {
            if let Some(document) = document {
                buffer.push(document)
            }

            if (buffer.len()) % 1000 == 0 && buffer.len() > 0 {
                println!("Sending batch");
                let items = buffer
                    .drain(..)
                    .map(|d| IndexAction::new(SearchAction::MergeOrUpload(d)))
                    .collect::<Vec<_>>();

                let batch = IndexBatch::new(items);
                let result = document_client.index(batch).await;
                println!(
                    "Did something with {} documents",
                    result.unwrap().value.len()
                );
            }
        }
    }
}
