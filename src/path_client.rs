use azure_storage::StorageCredentials;
use azure_storage_datalake::file_system::Path;
use azure_storage_datalake::{self, clients::DataLakeClient};
use futures::stream::StreamExt;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::usize;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::Instant;

pub struct PathClient {
    storage_account_name: String,
    sas_token: String,
}

impl PathClient {
    pub fn new(storage_account_name: String, sas_token: String) -> Self {
        Self {
            storage_account_name,
            sas_token,
        }
    }

    pub async fn list_paths_parallel(
        &self,
        file_system_name: String,
        path: String,
        paths_sender: Arc<Sender<Option<Path>>>,
        max_threads: usize,
    ) -> u64 {
        let (directory_sender, mut directory_receiver) = mpsc::channel::<Option<String>>(100000);
        let directory_sender = Arc::new(directory_sender);

        let semaphore = Arc::new(Semaphore::new(max_threads));

        let data_lake_client = Arc::new(DataLakeClient::new(
            self.storage_account_name.to_string(),
            StorageCredentials::sas_token(self.sas_token.to_string())
                .expect("hu? check your token"),
        ));

        let file_system_client: Arc<azure_storage_datalake::prelude::FileSystemClient> =
            Arc::new(data_lake_client.file_system_client(file_system_name));

        let directory_receiver_count = Arc::new(AtomicUsize::new(1)); // ugh, why doesnt tokio channels expose a count?
        directory_sender
            .send(Some(path))
            .await
            .expect("hu? channel what u doin?");

        let paths_counter = Arc::new(AtomicU64::new(0));

        let start_time = Instant::now();

        loop {
            match directory_receiver.recv().await {
                Some(value) => {
                    if let Some(value) = value {
                        directory_receiver_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

                        let semaphore = Arc::clone(&semaphore);
                        let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
                        let file_system_client = Arc::clone(&file_system_client);
                        let paths_sender = Arc::clone(&paths_sender);
                        let directory_sender = Arc::clone(&directory_sender);
                        let paths_counter = Arc::clone(&paths_counter);
                        let receiver_count = Arc::clone(&directory_receiver_count);

                        tokio::spawn(async move {
                            let mut list_paths_response = file_system_client
                                .list_paths()
                                .directory(value)
                                .max_results(NonZeroU32::new(10).unwrap())
                                .recursive(false)
                                .into_stream();

                            while let Some(path_response) = list_paths_response.next().await {
                                for path in path_response.unwrap().paths.into_iter() {
                                    let count = paths_counter
                                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                                    if count % 1000 == 0 {
                                        let duration = Instant::now()
                                            .checked_duration_since(start_time)
                                            .unwrap()
                                            .as_secs();

                                        println!(
                                            "Found {} files so far, fps: {}",
                                            count,
                                            count.checked_div(duration as u64).unwrap_or(1)
                                        );
                                    }

                                    if path.is_directory {
                                        receiver_count
                                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                        directory_sender
                                            .send(Some(path.name.clone()))
                                            .await
                                            .expect("hu? channel what u doin?");
                                    } else {
                                        paths_sender
                                            .send(Some(path))
                                            .await
                                            .expect("hu? channel what u doin?");
                                    }
                                }
                            }

                            drop(permit);

                            let current_count =
                                receiver_count.load(std::sync::atomic::Ordering::SeqCst);

                            if semaphore.available_permits() == max_threads && current_count == 0 {
                                println!("Guess we got to the end?!");
                                directory_sender
                                    .send(None)
                                    .await
                                    .expect("hu? channel what u doin?");
                            }
                        });
                    } else {
                        break;
                    }
                }
                None => {
                    break;
                }
            }
        }

        drop(paths_sender);
        drop(directory_sender);

        let count = Arc::try_unwrap(paths_counter).unwrap().into_inner();

        println!(
            "Found {} files, took {} ms",
            count,
            Instant::now()
                .checked_duration_since(start_time)
                .unwrap()
                .as_millis()
        );

        count
    }
}
