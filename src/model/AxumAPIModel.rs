use serde::{Deserialize, Serialize};

use super::DistributionType;

#[derive(Deserialize)]
pub struct DataToBinsRequest {
    pub column: String,
    pub filter: StatisticSearchPayload,
    pub all: bool,
    pub distribution_type: DistributionType,
    pub number_of_bins: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct StatisticSearchPayload {
    pub search: Option<String>,
    pub make: Option<String>,
    pub model: Option<String>,

    pub engine: Option<Vec<String>>,
    pub gearbox: Option<String>,

    pub yearFrom: Option<i32>,
    pub yearTo: Option<i32>,
    pub year: Option<i32>,

    pub powerFrom: Option<i32>,
    pub powerTo: Option<i32>,
    pub power: Option<i32>,

    pub mileageFrom: Option<i32>,
    pub mileageTo: Option<i32>,
    pub mileage: Option<i32>,

    pub ccFrom: Option<i32>,
    pub ccTo: Option<i32>,
    pub cc: Option<i32>,

    pub saveDiffFrom: Option<i32>,
    pub saveDiffTo: Option<i32>,

    pub discountFrom: Option<i32>,
    pub discountTo: Option<i32>,

    pub createdOnFrom: Option<i32>,
    pub createdOnTo: Option<i32>,

    pub group: Vec<String>,
    pub aggregators: Vec<String>,
    pub order: Vec<Order>,
    pub stat_column: Option<String>,
    pub estimated_price: Option<i32>,
    pub price: Option<i32>,
    pub priceFrom: Option<i32>,
    pub priceTo: Option<i32>,
}
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct Order {
    pub column: String,
    pub asc: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeErrorResponse {
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum Dimension {
    #[default]
    D1,
    D2,
    D3,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DimensionData<T> {
    pub label: String,
    pub column_name: String,
    pub column_index: u8,
    pub data: Vec<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatisticData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sum: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub median: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rsd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantile: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Metadata {
    pub column: String,
    pub column_index: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatisticResponse {
    pub metadata: Vec<Metadata>,
    pub dimensions: Vec<DimensionData<String>>,
    pub data: Vec<StatisticData>,
    pub total_count: u32,
}
