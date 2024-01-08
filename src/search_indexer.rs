use azure_storage::StorageCredentials;
use azure_storage_datalake::{self, clients::DataLakeClient};
use std::error::Error;
use std::sync::Arc;
use std::usize;
use tokio::sync::mpsc::{Receiver, Sender};
use urlencoding::decode;

use crate::document_model::DocumentModel;
use crate::path_index_model::PathIndexModel;

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
        documents_sender: Arc<Sender<Option<DocumentModel>>>, // todo this should actually be the target index model TIndex
        // todo also.. this thingy should take a mapping function from TDocument to TIndex
        max_threads: usize,
    ) -> Result<u64, Box<dyn Error>> {
        let data_lake_client = Arc::new(DataLakeClient::new(
            self.storage_account_name.to_string(),
            StorageCredentials::sas_token(self.sas_token.to_string())
                .expect("hu? check your token"),
        ));

        let mut read_count = 0;
        loop {
            match paths_receiver.recv().await {
                Some(path) => {
                    let path = path.unwrap();
                    read_count += 1;

                    let file_system_client: azure_storage_datalake::prelude::FileSystemClient =
                        data_lake_client.file_system_client(path.file_system);

                    let file_client =
                        file_system_client.get_file_client(decode(&path.path_url_encoded).unwrap());

                    let properties = file_client.get_properties().await;

                    let document = serde_json::from_slice::<DocumentModel>(
                        &file_client.read().await.unwrap().data,
                    )
                    .unwrap();

                    if read_count % 10 == 0 {
                        println!("Read {} files so far", read_count);
                        println!("{:?}", path.file_last_modified);
                        println!("Properties: {}", properties.unwrap().etag);
                        println!("Document something: {:?}", document.numbervalue);
                    }
                }
                None => break,
            }
        }

        println!("Read all paths \\o/");

        Ok(42)
    }
}
