use std::{error::Error, str::FromStr, sync::Arc};

use crate::{path_index_model::PathIndexModel, utils::concat_filter_and};
use azure_core::Url;
use azure_svc_search::package_2023_11_searchindex::{
    self,
    models::{index_action::SearchAction, IndexAction, IndexBatch, SearchRequest},
    search_extensions,
};
use tokio::sync::mpsc::Sender;

pub struct PathIndexClient {
    search_client: package_2023_11_searchindex::Client,
}

const SEARCH_PAGE_SIZE: i32 = 5000;
const PATH_INDEX_NAME: &str = "path-created-index";

pub struct ListPathsOptions {
    pub from_last_modified: Option<String>, // should be a datetime of some sort
    pub filter: Option<String>,
}

impl PathIndexClient {
    pub fn new(
        search_service_url: &str,
        credential: search_extensions::SearchAuthenticationMethod,
    ) -> Self {
        let client = package_2023_11_searchindex::ClientBuilder::new(credential)
            .endpoint(
                Url::from_str(&format!("{}/{}", search_service_url, PATH_INDEX_NAME))
                    .expect("Invalid search service url probably"),
            )
            .build()
            .expect("Something went haywire creating client?!");

        Self {
            search_client: client,
        }
    }

    pub async fn list_paths(
        &self,
        options: ListPathsOptions,
        paths_sender: Arc<Sender<Option<PathIndexModel>>>,
    ) -> Result<u64, Box<dyn Error>> {
        let mut path_count = 0;

        let filter = options.filter.unwrap_or("".to_string());
        let last_modified_filter = options
            .from_last_modified
            .map(|f| format!("lastModified ge {}", f))
            .unwrap_or("".to_string());

        let mut order_by_filter = "".to_string();
        let documents_client = self.search_client.documents_client();

        // let foo = TestIndexModel {
        //     booleanvalue: true,
        //     etag: "foo".to_string(),
        //     last_modified: chrono::DateTime::<Utc>::MAX_UTC,
        //     numbervalue: 42,
        //     path_base64: "".to_string(),
        //     path_url_encoded: "".to_string(),
        //     stringvalue: "foo".to_string(),
        // };

        // let action = IndexAction::new(SearchAction::MergeOrUpload(foo));

        // let batch = IndexBatch::new(vec![action]);
        // documents_client.index(batch);

        loop {
            let combined_filter =
                concat_filter_and(&[&order_by_filter, &filter, &last_modified_filter]);

            println!("Running query with filter: {}", combined_filter);

            let mut search_request = SearchRequest::new();
            search_request.top = Some(SEARCH_PAGE_SIZE);
            search_request.filter = Some(combined_filter);
            search_request.orderby = Some("key".to_string());

            let mut previous_key = None;

            loop {
                let search_response = documents_client
                    .search_post::<PathIndexModel>(search_request)
                    .send()
                    .await;

                let mut search_result = search_response?.into_body::<PathIndexModel>().await?;

                for path in search_result.value.drain(..) {
                    if let Some(path_item) = path.index_model {
                        path_count += 1;
                        previous_key = Some(path_item.key.to_string());
                        paths_sender.send(Some(path_item)).await?;
                    }
                }

                if let Some(next) = search_result.search_next_page_parameters {
                    search_request = next;
                } else {
                    break;
                }
            }

            if let Some(key) = previous_key {
                order_by_filter = format!("key gt '{}'", key);
            } else {
                break;
            }
        }

        Ok(path_count)
    }
}
