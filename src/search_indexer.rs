use crate::document_model::DocumentModel;
use crate::path_index_model::PathIndexModel;
use crate::test_index_model::TestIndexModel;
use azure_core::base64::encode;
use azure_storage::StorageCredentials;
use azure_storage_datalake::clients::FileSystemClient;
use azure_storage_datalake::{self, clients::DataLakeClient};
use futures::FutureExt;
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::usize;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use urlencoding::decode;

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
        documents_sender: Arc<Sender<TestIndexModel>>, // todo this should actually be the target index model TIndex
        // todo also.. this thingy should take a mapping function from TDocument to TIndex
        max_threads: usize,
    ) -> Result<u64, Box<dyn Error>> {
        let data_lake_client = Arc::new(DataLakeClient::new(
            self.storage_account_name.to_string(),
            StorageCredentials::sas_token(self.sas_token.to_string())
                .expect("hu? check your token"),
        ));

        let mut tasks = JoinSet::<()>::new();
        let semaphore = Arc::new(Semaphore::new(max_threads));
        let processed_counter = Arc::new(AtomicU64::new(0));

        // todo this is here to validate if caching the filesystemclient makes sense...
        let file_system_client = Arc::new(data_lake_client.file_system_client("stuff-large-files"));

        loop {
            match paths_receiver.recv().await {
                Some(path) => {
                    // this is here to clean up completed tasks from the joinset. Otherwise there will at some point be potentially millions of them hanging around
                    while let Some(Some(_)) =
                        tokio::task::unconstrained(tasks.join_next()).now_or_never()
                    {
                    }

                    if let Some(path) = path {
                        let processed_counter = Arc::clone(&processed_counter);
                        // let data_lake_client = Arc::clone(&data_lake_client);
                        let documents_sender = Arc::clone(&documents_sender);

                        let semaphore = Arc::clone(&semaphore);
                        let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
                        let file_system_client = Arc::clone(&file_system_client);

                        tasks.spawn(async move {
                            // todo cache file system clients / file system?
                            let file_client = &file_system_client.get_file_client(
                                decode(&path.path_url_encoded)
                                    .expect("Failed creating file client?!"),
                            );

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
                                path_base64: encode(format!(
                                    "{}%2f{}",
                                    path.file_system, path.path_url_encoded
                                )),
                                path_url_encoded: path.path_url_encoded,
                            };

                            documents_sender
                                .send(index_model)
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

        // wait for remaining tasks...
        while let Some(_) = tasks.join_next().await {}

        println!("Read all paths \\o/");

        Ok(processed_counter.load(std::sync::atomic::Ordering::SeqCst))
    }
}
