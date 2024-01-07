use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PathIndexModel {
    pub key: String,

    #[serde(rename = "pathUrlEncoded")]
    pub path_url_encoded: String,

    #[serde(rename = "filesystem")]
    pub file_system: String,

    #[serde(rename = "fileLastModified")]
    pub file_last_modified: chrono::DateTime<Utc>,

    #[serde(rename = "lastModified")]
    pub last_modified: chrono::DateTime<Utc>,
}
