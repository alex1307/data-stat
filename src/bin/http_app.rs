//! Run with
//!
//! ```not_rust
//! cargo run -p example-readme
//! ```

use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use axum::{
    body::Body,
    extract::{Host, Path, Query, Request},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use axum_server::tls_rustls::RustlsConfig;
use clap::Parser;
use data_statistics::{
    configure_log4rs,
    services::PriceStatistic::{apply_filter, to_generic_json, FilterPayload},
    Payload, ESTIMATE_PRICE_DATA,
};
use log::info;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tower_http::cors::{Any, CorsLayer};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to the directory containing the certificate files
    #[clap(short, long, default_value = "/etc/letsencrypt/live/ehomeho.com")]
    cert_dir: PathBuf,
}

#[tokio::main]
async fn main() {
    //tracing_subscriber::fmt::init();
    configure_log4rs("resources/log4rs.yml");
    info!("Starting server...");
    let args = Args::parse();
    let cert_dir = args.cert_dir;
    info!("Cert dir: {:?}", cert_dir);
    tracing_subscriber::fmt::format()
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        .with_source_location(true)
        .with_thread_ids(true);

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);
    let cert_path = cert_dir.join("server.crt");
    let key_path = cert_dir.join("server.key");
    let config = RustlsConfig::from_pem_file(cert_path, key_path)
        .await
        .unwrap();

    // build our application with a route
    let app = Router::new()
        .route("/", get(root))
        // `POST /users` goes to `create_user`
        .route("/users", post(create_user))
        .route("/filter", post(filter))
        .route("/data", post(data))
        .route("/json", post(json))
        .route("/enums/:name", get(enums))
        .route("/enums/:make/models", get(models))
        .layer(cors);
    // `TraceLayer` is provided by tower-http so you have to add that as a dependency.
    // It provides good defaults but is also very customizable.
    //
    // See https://docs.rs/tower-http/0.1.1/tower_http/trace/index.html for more details.;

    // build our application with a route
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    // run our app with hyper
    axum_server::bind_rustls(addr, config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// basic handler that responds with a static string
async fn root() -> &'static str {
    "Hello, World!"
}

async fn filter(Json(payload): Json<FilterPayload>) -> impl IntoResponse {
    tracing::info!("Filter: {:?}", payload);
    // insert your application logic here
    if let Some(aggregate) = payload.aggregate {
        tracing::info!("Aggregate: {:?}", aggregate);
    }

    if !payload.sort.is_empty() {
        tracing::info!("Sort: {:?}", payload.sort);
    }

    if !&payload.filter_string.is_empty() {
        tracing::info!("Filter string: {:?}", payload.filter_string);
    }

    if !&payload.filter_i32.is_empty() {
        tracing::info!("Filter i32: {:?}", payload.filter_i32);
    }

    if !&payload.filter_f64.is_empty() {
        tracing::info!("Filter f64: {:?}", payload.filter_f64);
    }

    (StatusCode::CREATED, {})
}

async fn data(Json(payload): Json<FilterPayload>) -> impl IntoResponse {
    let dataframe = Payload {
        source: payload.source.clone().unwrap_or("".to_string()),
    };
    let df = apply_filter(dataframe.get_dataframe(), payload);
    let json = json!({"data": df.to_string()});

    (StatusCode::OK, Json(json))
}

async fn json(Json(payload): Json<FilterPayload>) -> impl IntoResponse {
    let dataframe = Payload {
        source: payload.source.clone().unwrap_or("".to_string()),
    };
    let df = apply_filter(dataframe.get_dataframe(), payload);
    let json = to_generic_json(&df);

    (StatusCode::OK, Json(json))
}
async fn models(
    Path(make): Path<String>,
    Host(hostname): Host,
    Query(source): Query<HashMap<String, String>>,
    request: Request<Body>,
) -> impl IntoResponse {
    request.extensions().get::<String>();
    info!("Query: {:?}", request.uri().query());
    info!("Host: {:?}", hostname);
    info!("Make: {:?}", make);
    let map = if source.is_empty() {
        data_statistics::services::EnumService::models(&make, &ESTIMATE_PRICE_DATA)
    } else {
        let source = source.get("source");
        let found = if let Some(source) = source {
            source
        } else {
            ""
        };
        let dataframe = Payload {
            source: found.to_string(),
        };
        let df = dataframe.get_dataframe();
        data_statistics::services::EnumService::models(&make, df)
    };

    (StatusCode::OK, Json(map))
}
async fn enums(
    Path(name): Path<String>,
    Host(hostname): Host,
    Query(source): Query<HashMap<String, String>>,
    request: Request<Body>,
) -> impl IntoResponse {
    request.extensions().get::<String>();
    info!("Query: {:?}", request.uri().query());
    info!("Host: {:?}", hostname);
    let map = if source.is_empty() {
        info!("source is empty: Name: {:?}", name);
        data_statistics::services::EnumService::select(&name, &ESTIMATE_PRICE_DATA)
    } else {
        let source = source.get("source");
        let found = if let Some(src) = source { src } else { "" };
        let dataframe = Payload {
            source: found.to_string(),
        };
        let df = dataframe.get_dataframe();
        data_statistics::services::EnumService::select(&name, df)
    };

    (StatusCode::OK, Json(map))
}

async fn create_user(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    Json(payload): Json<CreateUser>,
) -> impl IntoResponse {
    // insert your application logic here
    let user = User {
        id: 1337,
        username: payload.username,
    };

    // this will be converted into a JSON response
    // with a status code of `201 Created`
    (StatusCode::CREATED, Json(user))
}

// the input to our `create_user` handler
#[derive(Deserialize)]
struct CreateUser {
    username: String,
}

// the output to our `create_user` handler
#[derive(Serialize)]
struct User {
    id: u64,
    username: String,
}
