use azure_storage::StorageCredentials;
use azure_storage_datalake::clients::FileSystemClient;
use azure_storage_datalake::{self, clients::DataLakeClient};
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::usize;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;
use urlencoding::decode;

use crate::document_model::DocumentModel;
use crate::path_index_model::PathIndexModel;
use crate::test_index_model::TestIndexModel;

pub struct SearchIndexer {
    storage_account_name: String,
    sas_token: String,
}

impl SearchIndexer {
    pub fn new(storage_account_name: String, sas_token: String) -> Self {
        Self {
            storage_account_name,
            sas_token,
        }
    }

    pub async fn read_documents(
        &self,
        mut paths_receiver: Receiver<Option<PathIndexModel>>,
        documents_sender: Arc<Sender<Option<TestIndexModel>>>, // todo this should actually be the target index model TIndex
        // todo also.. this thingy should take a mapping function from TDocument to TIndex
        max_threads: usize,
    ) -> Result<u64, Box<dyn Error>> {
        let data_lake_client = Arc::new(DataLakeClient::new(
            self.storage_account_name.to_string(),
            StorageCredentials::sas_token(self.sas_token.to_string())
                .expect("hu? check your token"),
        ));

        let semaphore = Arc::new(Semaphore::new(max_threads));
        let processed_counter = Arc::new(AtomicU64::new(0));

        loop {
            match paths_receiver.recv().await {
                Some(path) => {
                    if let Some(path) = path {
                        let processed_counter = Arc::clone(&processed_counter);
                        let data_lake_client = Arc::clone(&data_lake_client);
                        let documents_sender = Arc::clone(&documents_sender);

                        let semaphore = Arc::clone(&semaphore);
                        let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();

                        tokio::spawn(async move {
                            // todo cache file system clients / file system?
                            let file_system_client: FileSystemClient =
                                data_lake_client.file_system_client(path.file_system);

                            let file_client = file_system_client
                                .get_file_client(decode(&path.path_url_encoded).unwrap());

                            let properties = file_client
                                .get_properties()
                                .await
                                .expect("Failed reading properties...");

                            let document = serde_json::from_slice::<DocumentModel>(
                                &file_client.read().await.unwrap().data,
                            )
                            .expect("Unable to read document?!");

                            let index_model = TestIndexModel {
                                booleanvalue: document.booleanvalue,
                                etag: properties.etag,
                                last_modified: properties.last_modified,
                                numbervalue: document.numbervalue,
                                stringvalue: document.stringvalue,
                                path_base64: "blabla".to_string(),
                                path_url_encoded: "blabla".to_string(),
                            };

                            documents_sender
                                .send(Some(index_model))
                                .await
                                .expect("document sender wat u doin?");

                            let current_count = processed_counter
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                                + 1;

                            if (current_count) % 1000 == 0 {
                                println!("Read {} files so far", current_count);
                            }

                            drop(permit);
                        });
                    } else {
                        break;
                    }
                }
                None => break,
            }
        }

        println!("Read all paths \\o/");

        Ok(processed_counter.load(std::sync::atomic::Ordering::SeqCst))
    }
}
