use serde::{Deserialize, Serialize};

pub mod Intervals;
pub mod Quantiles;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DistributionType {
    ByQuantile,
    ByInterval,
}
