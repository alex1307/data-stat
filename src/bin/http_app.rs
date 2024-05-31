//! Run with
//!
//! ```not_rust
//! cargo run -p example-readme
//! ```

use std::time::Duration;

use axum::{
    body::{Body, Bytes},
    extract::{Host, Path, Request},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};

use data_statistics::{
    configure_log4rs,
    services::PriceStatistic::{apply_filter, to_generic_json, FilterPayload},
    PRICE_DATA,
};
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower_http::cors::CorsLayer;

use tower_http::{classify::ServerErrorsFailureClass, trace::TraceLayer};
use tracing::Span;

#[tokio::main]
async fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "http_app=debug,tower_http=debug")
    }
    tracing_subscriber::fmt::init();
    configure_log4rs();
    info!("Starting server...");

    tracing_subscriber::fmt::format()
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        .with_source_location(true)
        .with_thread_ids(true);

    // build our application with a route
    let app = Router::new()
        .route("/", get(root))
        // `POST /users` goes to `create_user`
        .route("/users", post(create_user))
        .route("/filter", post(filter))
        .route("/data", post(data))
        .route("/json", post(json))
        .route("/enums/:name", get(enums))
        .layer(CorsLayer::permissive())
        // `TraceLayer` is provided by tower-http so you have to add that as a dependency.
        // It provides good defaults but is also very customizable.
        //
        // See https://docs.rs/tower-http/0.1.1/tower_http/trace/index.html for more details.
        .layer(TraceLayer::new_for_http())
        // If you want to customize the behavior using closures here is how
        //
        // This is just for demonstration, you don't need to add this middleware twice
        .layer(
            TraceLayer::new_for_http()
                .on_request(|_request: &Request<_>, _span: &Span| {
                    // ...
                })
                .on_response(|_response: &Response, _latency: Duration, _span: &Span| {
                    // ...
                })
                .on_body_chunk(|_chunk: &Bytes, _latency: Duration, _span: &Span| {
                    // ..
                })
                .on_eos(
                    |_trailers: Option<&HeaderMap>, _stream_duration: Duration, _span: &Span| {
                        // ...
                    },
                )
                .on_failure(
                    |_error: ServerErrorsFailureClass, _latency: Duration, _span: &Span| {
                        // ...
                    },
                ),
        );

    // build our application with a route

    // run our app with hyper
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    tracing::info!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
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

    (StatusCode::CREATED, Json({}))
}

async fn data(Json(payload): Json<FilterPayload>) -> impl IntoResponse {
    let df = apply_filter(&PRICE_DATA, payload);
    let json = json!({"data": df.to_string()});

    (StatusCode::CREATED, Json(json))
}

async fn json(Json(payload): Json<FilterPayload>) -> impl IntoResponse {
    let df = apply_filter(&PRICE_DATA, payload);
    let json = to_generic_json(&df);

    (StatusCode::CREATED, Json(json))
}
async fn enums(
    Path(name): Path<String>,
    Host(hostname): Host,
    request: Request<Body>,
) -> impl IntoResponse {
    request.extensions().get::<String>();
    info!("Query: {:?}", request.uri().query());
    info!("Host: {:?}", hostname);
    let map = data_statistics::services::EnumService::select(&name);
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
