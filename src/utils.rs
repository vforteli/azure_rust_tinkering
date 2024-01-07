// todo if this is ever used for something with security filters, this must validate the individual parts as proper odata filters to ensure no bobby tables happen
pub fn concat_filter_and(filters: &[&str]) -> String {
    filters
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| format!("({})", s))
        .collect::<Vec<_>>()
        .join(" and ")
}

#[cfg(test)]
mod utils_tests {
    use super::*;

    #[test]
    fn test_concat_filter_and_empty() {
        let actual = concat_filter_and(&[]);

        assert_eq!(actual, "");
    }

    #[test]
    fn test_concat_filter_and_single() {
        let actual = concat_filter_and(&["someproperty eq 'somevalue"]);

        assert_eq!(actual, "(someproperty eq 'somevalue)");
    }

    #[test]
    fn test_concat_filter_and_multiple() {
        let actual = concat_filter_and(&[
            "someproperty eq 'somevalue",
            "someotherproperty eq 'someothervalue or foo eq 'bar'",
        ]);

        assert_eq!(actual, "(someproperty eq 'somevalue) and (someotherproperty eq 'someothervalue or foo eq 'bar')");
    }
}
