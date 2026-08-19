// SPDX-License-Identifier: MIT

fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(
        &[
            "proto/relay_cache.proto",
            "proto/roads_cache.proto",
            "proto/roads.proto",
        ],
        &["proto/"],
    )?;
    Ok(())
}
