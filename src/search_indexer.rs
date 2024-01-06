use std::str::FromStr;

use azure_core::Url;
use azure_svc_search::package_2023_11_searchindex::{
    self, models::SearchRequest, search_extensions,
};

use crate::path_index_model::PathIndexModel;

pub async fn foo(search_url: &str, search_key: &str) {
    let index_name = "path-created-index";

    let client = package_2023_11_searchindex::ClientBuilder::new(
        search_extensions::SearchAuthenticationMethod::ApiKey(search_key.to_string()),
    )
    .endpoint(Url::from_str(&format!("{}/{}", search_url, index_name)).unwrap())
    .build()
    .expect("Something went haywire creating client?!");

    let something = client.documents_client().count().await;
    println!("Found something... maybe {:?}", something);

    let search_request = SearchRequest::new();

    let search_results = client
        .documents_client()
        .search_post::<PathIndexModel>(search_request)
        .send()
        .await;

    let foo = search_results
        .unwrap()
        .into_body::<PathIndexModel>()
        .await
        .unwrap();

    foo.value.iter().for_each(|result| {
        println!("{:?}", result);
    });
}
