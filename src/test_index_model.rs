// todo this probably requires some macros for creating keys, searchable fields etc
// todo figure out if we need all derives... Default for example.. or PartialEq, Getting rid of clone could be nice..

use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct TestIndexModel {
    #[serde(rename = "pathbase64")]
    pub path_base64: String,

    pub stringvalue: String,

    pub numbervalue: i32,

    pub booleanvalue: bool,

    #[serde(rename = "eTag")]
    pub etag: String,

    #[serde(rename = "pathUrlEncoded")]
    pub path_url_encoded: String,

    #[serde(rename = "lastModified")]
    pub last_modified: chrono::DateTime<Utc>,
}
