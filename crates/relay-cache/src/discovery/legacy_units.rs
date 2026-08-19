// SPDX-License-Identifier: MIT

//! Legacy systemd unit discovery for the per-region relay fleet.

use std::path::Path;

use super::naming::source_name_for_database;

/// One row parsed from a `relay-*.service` unit file.
#[derive(Clone, Debug)]
pub struct DiscoveredSource {
    pub name: String,
    pub database: String,
    pub frontend_port: u16,
    pub dashboard_port: u16,
}

/// How a unit stem is projected into a source name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NamingSpec {
    pub template: Option<String>,
    pub stem_prefix: Option<String>,
}

impl NamingSpec {
    pub fn project(&self, stem: &str) -> String {
        let trimmed = match &self.stem_prefix {
            Some(prefix) if !prefix.is_empty() && stem.starts_with(prefix) => {
                &stem[prefix.len()..]
            }
            _ => stem,
        };
        match &self.template {
            Some(tpl) => tpl.replace("{stem}", trimmed),
            None => trimmed.to_string(),
        }
    }
}

/// Discover legacy relay mirror units under `unit_dir`.
pub fn discover(unit_dir: &Path, naming: &NamingSpec) -> Vec<DiscoveredSource> {
    let entries = match std::fs::read_dir(unit_dir) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };
    let mut found: Vec<(String, DiscoveredSource)> = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".service") else {
            continue;
        };
        if !is_mirror_unit(stem) {
            continue;
        }
        let body = match std::fs::read_to_string(&path) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if let Some(src) = parse_unit(&body, stem, naming) {
            found.push((src.name.clone(), src));
        }
    }
    found.sort_by(|a, b| a.0.cmp(&b.0));
    found.into_iter().map(|(_, s)| s).collect()
}

fn is_mirror_unit(stem: &str) -> bool {
    let Some(rest) = stem.strip_prefix("relay-") else {
        return false;
    };
    if rest.is_empty() {
        return false;
    }
    !matches!(
        stem,
        "relay-stdb"
            | "relay-coordinator"
            | "relay-fleet-sequencer"
            | "relay-fleet-start"
            | "relay-staleness-monitor"
            | "relay-mirror-health"
    )
}

pub fn parse_unit(body: &str, unit_stem: &str, naming: &NamingSpec) -> Option<DiscoveredSource> {
    let frontend_port = parse_bind_port(body, "--frontend-bind")?;
    let dashboard_port = parse_bind_port(body, "--dashboard-bind")?;
    let mirror_database = parse_flag_value(body, "--mirror-database")?;
    let upstream_database = parse_flag_value(body, "--database");
    let name = match upstream_database {
        Some(upstream) => source_name_for_database(&upstream),
        None => naming.project(unit_stem),
    };
    Some(DiscoveredSource {
        name,
        database: mirror_database,
        frontend_port,
        dashboard_port,
    })
}

fn parse_bind_port(body: &str, flag: &str) -> Option<u16> {
    let pat_space = format!("{flag} ");
    let pat_eq = format!("{flag}=");
    for line in body.lines() {
        for raw in [pat_space.as_str(), pat_eq.as_str()] {
            if let Some(idx) = line.find(raw) {
                let rest = &line[idx + raw.len()..];
                let tok = rest.split_whitespace().next().unwrap_or(rest);
                if let Some(port_str) = tok.rsplit(':').next() {
                    if let Ok(p) = port_str.parse::<u16>() {
                        return Some(p);
                    }
                }
            }
        }
    }
    None
}

fn parse_flag_value(body: &str, flag: &str) -> Option<String> {
    let pat_space = format!("{flag} ");
    let pat_eq = format!("{flag}=");
    for line in body.lines() {
        for raw in [pat_space.as_str(), pat_eq.as_str()] {
            if let Some(idx) = line.find(raw) {
                let rest = &line[idx + raw.len()..];
                let tok = rest.split_whitespace().next().unwrap_or(rest);
                let cleaned = tok.trim_end_matches('\\');
                if !cleaned.is_empty() {
                    return Some(cleaned.to_string());
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_unit_file_extracts_ports_and_database() {
        let body = "[Service]\nExecStart=/relay --mirror-database relay-mirror-bc14 \
             --frontend-bind 127.0.0.1:3014 --dashboard-bind 127.0.0.1:3114\n";
        let naming = NamingSpec {
            template: Some("bitcraft-live-{stem}".into()),
            stem_prefix: Some("relay-bc".into()),
        };
        let src = parse_unit(&body, "relay-bc14", &naming).expect("parsed");
        assert_eq!(src.name, "bitcraft-live-14");
        assert_eq!(src.database, "relay-mirror-bc14");
        assert_eq!(src.frontend_port, 3014);
        assert_eq!(src.dashboard_port, 3114);
    }
}
