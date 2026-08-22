// SPDX-License-Identifier: MIT

//! Roads terrain cache: dense per-region grids fed by mirror batches.

pub mod apply;
pub mod catalog;
pub mod coords;
pub mod decode;
pub mod grid;
pub mod harvestable;
pub mod index;
pub mod join;
pub mod meta;
pub mod store;

pub use catalog::{GlobalRoadsCatalog, RoadsFleet};
pub use grid::{get_claim_index, set_claim_index, set_paving};
pub use store::{RoadsRegionGrid, RoadsRegionHandle};
