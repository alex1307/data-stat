#![allow(non_snake_case)]
pub mod services;
use std::{
    path::PathBuf,
    sync::{Arc, Once},
};

use lazy_static::lazy_static;
use log::info;
use polars::{
    lazy::frame::{LazyCsvReader, LazyFileListReader, LazyFrame},
    prelude::{Field, Schema},
};

pub const VEHICLE_DATA_VIEW_FILE: &str = "./resources/Vehicles.csv";
pub const STAT_PRICE_DATA_FILE: &str = "./resources/Prices.csv";
pub const ESTIMATED_PRICES_DATA_FILE: &str = "./resources/EstimatedPrices.csv";
pub const VEHICLE_STATISTIC_DATA_FILE: &str = "./resources/VehicleStatistic.csv";

lazy_static! {
    static ref INIT_LOGGER: Once = Once::new();
    pub static ref VEHICLE_DATA_VIEW_SCHEMA: Arc<Schema> = {
        let mut schema = Schema::from_iter(vec![
            Field::new("advert_id".into(),  polars::datatypes::DataType::String),
            Field::new("source".into(), polars::datatypes::DataType::String),
        ]);
        schema.with_column("title".into(), polars::datatypes::DataType::String);
        schema.with_column("make".into(), polars::datatypes::DataType::String);
        schema.with_column("model".into(), polars::datatypes::DataType::String);
        schema.with_column("year".into(), polars::datatypes::DataType::Int32);
        schema.with_column("engine".into(), polars::datatypes::DataType::String);
        schema.with_column("gearbox".into(), polars::datatypes::DataType::String);
        schema.with_column("cc".into(), polars::datatypes::DataType::Int32);
        schema.with_column("power".into(), polars::datatypes::DataType::Int32);
        schema.with_column("power_kw".into(), polars::datatypes::DataType::Int32);
        schema.with_column("mileage".into(), polars::datatypes::DataType::Int32);
        schema.with_column("currency".into(), polars::datatypes::DataType::String);
        schema.with_column("price".into(), polars::datatypes::DataType::Int32);
        schema.with_column("estimated_price".into(), polars::datatypes::DataType::Int32);
        schema.with_column("save_diff".into(), polars::datatypes::DataType::Int32);
        schema.with_column("extra_charge".into(), polars::datatypes::DataType::Int32);
        schema.with_column("discount".into(), polars::datatypes::DataType::Float32);
        schema.with_column("increase".into(), polars::datatypes::DataType::Float32);
        schema.with_column("price_in_eur".into(), polars::datatypes::DataType::Int32);
        schema.with_column("estimated_price_in_eur".into(), polars::datatypes::DataType::Int32);
        schema.with_column("save_diff_in_eur".into(), polars::datatypes::DataType::Int32);
        schema.with_column("extra_charge_in_eur".into(), polars::datatypes::DataType::Int32);
        schema.with_column("equipment".into(), polars::datatypes::DataType::String);
        schema.with_column("url".into(), polars::datatypes::DataType::String);
        schema.with_column("created_on".into(), polars::datatypes::DataType::Date);
        schema.with_column("updated_on".into(), polars::datatypes::DataType::Date);

        Arc::new(schema)
    };

    pub static ref PRICES_SCHEMA: Arc<Schema> = {
        let mut schema = Schema::from_iter(vec![
            Field::new("make".into(),  polars::datatypes::DataType::String),
            Field::new("model".into(), polars::datatypes::DataType::String),
        ]);
        schema.with_column("year".into(), polars::datatypes::DataType::Int32);
        schema.with_column("engine".into(), polars::datatypes::DataType::String);
        schema.with_column("gearbox".into(), polars::datatypes::DataType::String);
        schema.with_column("power".into(), polars::datatypes::DataType::Int32);
        schema.with_column("mileage".into(), polars::datatypes::DataType::Int32);
        schema.with_column("cc".into(), polars::datatypes::DataType::Int32);
        schema.with_column("mileage_breakdown".into(), polars::datatypes::DataType::String);
        schema.with_column("power_breakdown".into(), polars::datatypes::DataType::String);
        schema.with_column("price_in_eur".into(), polars::datatypes::DataType::Int32);
        schema.with_column("estimated_price_in_eur".into(), polars::datatypes::DataType::Int32);
        schema.with_column("save_diff_in_eur".into(), polars::datatypes::DataType::Int32);
        schema.with_column("extra_charge_in_eur".into(), polars::datatypes::DataType::Int32);
        schema.with_column("discount".into(), polars::datatypes::DataType::Float32);
        schema.with_column("increase".into(), polars::datatypes::DataType::Float32);
        Arc::new(schema)
    };

    pub static ref ESTIMATED_PRICES_SCHEMA: Arc<Schema> = {
        let mut schema = Schema::from_iter(vec![
            Field::new("make".into(),  polars::datatypes::DataType::String),
            Field::new("model".into(), polars::datatypes::DataType::String),
        ]);
        schema.with_column("title".into(), polars::datatypes::DataType::String);
        schema.with_column("equipment".into(), polars::datatypes::DataType::String);
        schema.with_column("year".into(), polars::datatypes::DataType::Int32);
        schema.with_column("engine".into(), polars::datatypes::DataType::String);
        schema.with_column("gearbox".into(), polars::datatypes::DataType::String);
        schema.with_column("power".into(), polars::datatypes::DataType::Int32);
        schema.with_column("mileage".into(), polars::datatypes::DataType::Int32);
        schema.with_column("cc".into(), polars::datatypes::DataType::Int32);
        schema.with_column("mileage_breakdown".into(), polars::datatypes::DataType::String);
        schema.with_column("power_breakdown".into(), polars::datatypes::DataType::String);
        schema.with_column("price_in_eur".into(), polars::datatypes::DataType::Int32);
        schema.with_column("estimated_price_in_eur".into(), polars::datatypes::DataType::Int32);
        Arc::new(schema)
    };

    pub static ref VEHICLE_STATISTIC_SCHEMA: Arc<Schema> = {
        let mut schema = Schema::from_iter(vec![
            Field::new("advert_id".into(),  polars::datatypes::DataType::String),
            Field::new("make".into(),  polars::datatypes::DataType::String),
            Field::new("model".into(), polars::datatypes::DataType::String),
        ]);
        schema.with_column("year".into(), polars::datatypes::DataType::Int32);
        schema.with_column("engine".into(), polars::datatypes::DataType::String);
        schema.with_column("gearbox".into(), polars::datatypes::DataType::String);
        schema.with_column("price_in_eur".into(), polars::datatypes::DataType::Int32);
        schema.with_column("mileage_breakdown".into(), polars::datatypes::DataType::String);
        schema.with_column("cc_breakdown".into(), polars::datatypes::DataType::String);
        schema.with_column("power_breakdown".into(), polars::datatypes::DataType::String);
        schema.with_column("price__breakdown".into(), polars::datatypes::DataType::String);
        schema.with_column("mileage_breakdown_order".into(), polars::datatypes::DataType::Int32);
        schema.with_column("cc_breakdown_order".into(), polars::datatypes::DataType::Int32);
        schema.with_column("power_breakdown_order".into(), polars::datatypes::DataType::String);
        schema.with_column("price__breakdown_order".into(), polars::datatypes::DataType::Int32);
        schema.with_column("year_created_on".into(), polars::datatypes::DataType::Int32);
        schema.with_column("year_changed_on".into(), polars::datatypes::DataType::Int32);
        schema.with_column("week_created_on".into(), polars::datatypes::DataType::Int32);
        schema.with_column("week_changed_on".into(), polars::datatypes::DataType::Int32);
        schema.with_column("days_in_sale".into(), polars::datatypes::DataType::Int32);
        schema.with_column("sold_since".into(), polars::datatypes::DataType::Int32);

        Arc::new(schema)
    };


    // };
    pub static ref VEHICLES_DATA: polars::prelude::LazyFrame = {
        INIT_LOGGER.call_once(|| {
            // Initialize logging or any other one-time setup here
            info!("SUCCESS: Loggers are configured with dir: _log/*");
        });
        let path = vec![PathBuf::from(VEHICLE_DATA_VIEW_FILE)];
        let param = Arc::from(path);
        LazyCsvReader::new(VEHICLE_DATA_VIEW_FILE)
            .with_paths(param)
            .with_try_parse_dates(true)
            .with_separator(b';')
            .with_schema(Some(VEHICLE_DATA_VIEW_SCHEMA.clone()))
            .finish()
            .unwrap()

    };

    pub static ref PRICE_DATA: polars::prelude::LazyFrame = {
        let path = vec![PathBuf::from(STAT_PRICE_DATA_FILE)];
        let param = Arc::from(path);
        LazyCsvReader::new(STAT_PRICE_DATA_FILE)
            .with_paths(param)
            .with_try_parse_dates(true)
            .with_separator(b';')
            .with_schema(Some(PRICES_SCHEMA.clone()))
            .finish()
            .unwrap()
    };

    pub static ref ESTIMATED_PRICES_DATA: polars::prelude::LazyFrame = {
        let path = vec![PathBuf::from(ESTIMATED_PRICES_DATA_FILE)];
        let param = Arc::from(path);
        LazyCsvReader::new(ESTIMATED_PRICES_DATA_FILE)
            .with_paths(param)
            .with_try_parse_dates(true)
            .with_separator(b';')
            .with_schema(Some(ESTIMATED_PRICES_SCHEMA.clone()))
            .finish()
            .unwrap()
    };

    pub static ref VEHICLE_STATIC_DATA: polars::prelude::LazyFrame = {
        INIT_LOGGER.call_once(|| {
            // Initialize logging or any other one-time setup here
            info!("SUCCESS: Loggers are configured with dir: _log/*");
        });
        let path = vec![PathBuf::from(VEHICLE_STATISTIC_DATA_FILE)];
        let param = Arc::from(path);
        LazyCsvReader::new(VEHICLE_STATISTIC_DATA_FILE)
            .with_paths(param)
            .with_try_parse_dates(true)
            .with_separator(b';')
            .with_schema(Some(VEHICLE_STATISTIC_SCHEMA.clone()))
            .finish()
            .unwrap()

    };

    pub static ref HIDDEN_COLUMNS: Vec<String> = vec![
        "id".to_string(),
        "advert_id".to_string(),
        "published_on".to_string(),
        "dealer".to_string(),];
}

pub fn configure_log4rs(file: &str) {
    INIT_LOGGER.call_once(|| {
        log4rs::init_file(file, Default::default()).unwrap();
        info!("SUCCESS: Loggers are configured with dir: _log/*");
    });
}

pub struct Payload {
    pub source: String,
}

impl Payload {
    pub fn get_dataframe(&self) -> &LazyFrame {
        // match self.source.to_uppercase().trim() {
        //     "ESTIMATED_PRICE" => &ESTIMATE_PRICE_DATA,
        //     _ => &PRICE_DATA,
        // }

        &VEHICLES_DATA
    }
}
