// todo this probably requires some macros for creating keys, searchable fields etc
// todo figure out if we need all derives... Default for example.. or PartialEq, Getting rid of clone could be nice..

use serde::{Deserialize, Serialize};
use time::{Date, OffsetDateTime, Time};

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

impl Default for TestIndexModel {
    fn default() -> Self {
        Self {
            path_base64: Default::default(),
            stringvalue: Default::default(),
            numbervalue: Default::default(),
            booleanvalue: Default::default(),
            etag: Default::default(),
            path_url_encoded: Default::default(),
            last_modified: OffsetDateTime::new_utc(Date::MIN, Time::MIDNIGHT),
        }
    }
}
