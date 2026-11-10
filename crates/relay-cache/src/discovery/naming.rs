// SPDX-License-Identifier: MIT

//! Database name → public port / source key helpers.

/// Region-database name prefixes, in recognition order.
///
/// `idc-<N>` is the 1.0 release fleet; `bitcraft-live-<N>` is the EA2 fleet,
/// still accepted so a post-migration binary runs the pre-cutover unit file
/// unchanged (rollback restores only the unit file).
pub const REGION_DB_PREFIXES: &[&str] = &["idc-", "bitcraft-live-"];

/// Public frontend port for a mirrored database name.
///
/// `<any>-global` → 3000; `idc-N` / `bitcraft-live-N` → 3000+N.
pub fn public_port_for_database(database: &str) -> u16 {
    if database.ends_with("-global") {
        return 3000;
    }
    for prefix in REGION_DB_PREFIXES {
        if let Some(id) = database
            .strip_prefix(prefix)
            .and_then(|n| n.parse::<u16>().ok())
        {
            return 3000 + id;
        }
    }
    3000
}

/// Source key for a database: any `*-global` is `"global"`, regions keep full name.
pub fn source_name_for_database(database: &str) -> String {
    if database.ends_with("-global") {
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
        assert_eq!(public_port_for_database("idc-global"), 3000);
        assert_eq!(public_port_for_database("idc-14"), 3014);
        assert_eq!(public_port_for_database("idc-10"), 3010);
        assert_eq!(public_port_for_database("bitcraft-live-global"), 3000);
        assert_eq!(public_port_for_database("bitcraft-live-14"), 3014);
        assert_eq!(source_name_for_database("idc-global"), "global");
        assert_eq!(source_name_for_database("idc-14"), "idc-14");
        assert_eq!(source_name_for_database("bitcraft-live-global"), "global");
        assert_eq!(source_name_for_database("bitcraft-live-14"), "bitcraft-live-14");
    }
}
