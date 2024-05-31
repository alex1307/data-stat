#![allow(non_snake_case)]
pub mod services;
use std::sync::{Arc, Once};

use lazy_static::lazy_static;
use log::info;
use polars::{
    lazy::frame::{LazyCsvReader, LazyFileListReader},
    prelude::Schema,
};

pub const PRICE_DATA_FILE: &str = "./resources/VehiclePriceView.csv";

lazy_static! {
    static ref INIT_LOGGER: Once = Once::new();
    pub static ref PRICE_SCHEMA: Arc<Schema> = {
        let mut schema = Schema::new();
        schema.with_column("id".into(), polars::datatypes::DataType::String);
        schema.with_column("advert_id".into(), polars::datatypes::DataType::String);
        schema.with_column("source".into(), polars::datatypes::DataType::String);
        schema.with_column("title".into(), polars::datatypes::DataType::String);
        schema.with_column("make".into(), polars::datatypes::DataType::String);
        schema.with_column("model".into(), polars::datatypes::DataType::String);
        schema.with_column("year".into(), polars::datatypes::DataType::Int32);
        schema.with_column("mileage".into(), polars::datatypes::DataType::Int32);
        schema.with_column("engine".into(), polars::datatypes::DataType::String);
        schema.with_column("gearbox".into(), polars::datatypes::DataType::String);
        schema.with_column("cc".into(), polars::datatypes::DataType::Int32);
        schema.with_column("power_ps".into(), polars::datatypes::DataType::Int32);
        schema.with_column("power_kw".into(), polars::datatypes::DataType::Int32);
        schema.with_column("created_on".into(), polars::datatypes::DataType::Date);
        schema.with_column("last_updated_on".into(), polars::datatypes::DataType::Date);
        schema.with_column("currency".into(), polars::datatypes::DataType::String);
        schema.with_column("price".into(), polars::datatypes::DataType::Int32);
        schema.with_column("estimated_price".into(), polars::datatypes::DataType::Int32);
        Arc::new(schema)
    };
    pub static ref PRICE_DATA: polars::prelude::LazyFrame = {
        let df_csv = LazyCsvReader::new(PRICE_DATA_FILE)
            .with_path((&PRICE_DATA_FILE).into())
            .with_separator(b';')
            .with_schema(Some(PRICE_SCHEMA.clone()))
            .finish()
            .unwrap();
        df_csv
    };
}

pub fn configure_log4rs() {
    INIT_LOGGER.call_once(|| {
        // Initialize log4rs
        info!("SUCCESS: Loggers are configured with dir: _log/*");
    });
}
