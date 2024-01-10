// todo this probably requires some macros for creating keys, searchable fields etc
// todo figure out if we need all derives... Default for example.. or PartialEq, Getting rid of clone could be nice..

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

    #[serde(rename = "lastModified", with = "time::serde::rfc3339")]
    pub last_modified: OffsetDateTime,
}
