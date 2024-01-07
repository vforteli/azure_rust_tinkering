// todo this probably requires some macros for creating keys, searchable fields etc

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndexModelTest {
    #[serde(rename = "pathbase64")]
    pub path_base64: Option<String>,

    pub stringvalue: Option<String>,

    pub numbervalue: Option<i32>,

    pub booleanvalue: Option<bool>,

    #[serde(rename = "eTag")]
    pub etqg: Option<String>,

    #[serde(rename = "pathUrlEncoded")]
    pub path_url_encoded: Option<String>,

    #[serde(rename = "lastModified")]
    pub last_modified: chrono::DateTime<Utc>,
}
