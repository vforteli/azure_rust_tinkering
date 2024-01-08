use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DocumentModel {
    pub stringvalue: String,

    pub numbervalue: i32,

    pub booleanvalue: bool,
}
