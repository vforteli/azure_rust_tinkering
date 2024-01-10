use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PathIndexModel {
    pub key: String,

    #[serde(rename = "pathUrlEncoded")]
    pub path_url_encoded: String,

    #[serde(rename = "filesystem")]
    pub file_system: String,

    #[serde(rename = "fileLastModified", with = "time::serde::rfc3339")]
    pub file_last_modified: OffsetDateTime,

    #[serde(rename = "lastModified", with = "time::serde::rfc3339")]
    pub last_modified: OffsetDateTime,
}
