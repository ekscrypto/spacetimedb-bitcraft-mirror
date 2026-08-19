// SPDX-License-Identifier: MIT

//! Database name → public port / source key helpers.

/// Public frontend port for a mirrored database name.
///
/// `bitcraft-live-global` → 3000; `bitcraft-live-N` → 3000+N.
pub fn public_port_for_database(database: &str) -> u16 {
    if database == "bitcraft-live-global" || database.ends_with("-global") {
        return 3000;
    }
    if let Some(n) = database.strip_prefix("bitcraft-live-") {
        if let Ok(id) = n.parse::<u16>() {
            return 3000 + id;
        }
    }
    3000
}

/// Source key for a database: global is `"global"`, regions keep full name.
pub fn source_name_for_database(database: &str) -> String {
    if database == "bitcraft-live-global" {
        "global".to_string()
    } else {
        database.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_port_and_source_name_from_database() {
        assert_eq!(public_port_for_database("bitcraft-live-global"), 3000);
        assert_eq!(public_port_for_database("bitcraft-live-14"), 3014);
        assert_eq!(source_name_for_database("bitcraft-live-global"), "global");
        assert_eq!(source_name_for_database("bitcraft-live-14"), "bitcraft-live-14");
    }
}
