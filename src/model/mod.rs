use serde::{Deserialize, Serialize, Serializer};
use Quantiles::Quantile;

pub mod AxumAPIModel;
pub mod Intervals;
pub mod Quantiles;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DistributionType {
    ByQuantile,
    ByInterval,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DistributionChartData<T: Default> {
    pub axisLabel: String,
    pub dataLabel: String,
    pub axisValues: Vec<T>,
    pub min: T,
    pub max: T,
    pub median: T,
    pub count: i32,
    #[serde(serialize_with = "serialize_f64_as_int")]
    pub mean: f64,
    #[serde(serialize_with = "serialize_f64_as_int")]
    pub rsd: f64,
    pub data: Vec<IntervalData<T>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IntervalData<T> {
    pub column: String,
    pub category: String,
    pub min: T,
    pub max: T,
    pub median: T,
    pub count: i32,
    #[serde(serialize_with = "serialize_f64_as_int")]
    pub mean: f64,
    #[serde(serialize_with = "serialize_f64_as_int")]
    pub rsd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Statistics {
    pub count: u32,
    pub min: i32,
    pub max: i32,
    #[serde(serialize_with = "serialize_f64_as_int")]
    pub mean: f64,
    #[serde(serialize_with = "serialize_f64_as_int")]
    pub median: f64,
    #[serde(serialize_with = "serialize_f64_as_int")]
    pub rsd: f64,
    pub quantiles: Vec<Quantile>,
}

fn serialize_f64_as_int<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_i64(*value as i64)
}
