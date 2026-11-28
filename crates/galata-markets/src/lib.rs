//! **galata-markets: the one door for market data** (`add-galata-markets`).
//!
//! History from the tape as Arrow IPC, candles at a period, the live window
//! as server-sent events with every declared hole filled from the record, and
//! the facts about what exists — over HTTP, to clients that never connect to
//! the bus. It reads the tape and the archive and never writes either, reads
//! the bus as `markets` and publishes nothing, and links no venue transport.
//!
//! **And account data** (`add-markets-accounts`), to the clients granted it:
//! the accounts held, each one's state now, its history and its live stream,
//! by alias, from the ledger's projection and the account stream, never
//! changing either.

pub mod access;
pub mod accounts;
pub mod fill;
pub mod health;
pub mod history;
pub mod live;
pub mod metadata;
pub mod periods;
pub mod prices;
pub mod rows;
pub mod status;

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::routing::get;

/// What every handler reads from.
#[derive(Debug, Clone)]
pub struct Markets {
    /// The tape's root, read-only.
    pub tape: PathBuf,
    /// The archive's root, read-only: where a declared hole is filled from.
    pub archive: PathBuf,
    /// The bus, as `markets`; `None` until it is reached.
    pub bus: Option<async_nats::Client>,
    /// How many events wait for a live client before it is called behind.
    pub queue: usize,
    /// Each venue's normaliser, for filling a declared hole from the record.
    pub normalisers: fill::Normalisers,
    /// Fills, by venue and span.
    pub fills: fill::FillCache,
    /// How long a fill waits for the record, and how large one may be.
    pub fill_limits: fill::FillLimits,
    /// How much one answer may hold.
    pub limits: history::Limits,
    /// Each venue's latest status, followed from the bus.
    pub statuses: status::Statuses,
    /// What is held per instrument, kept a while.
    pub held: metadata::HeldCache,
    /// Where the deployment's health is read from, and what to expect.
    pub health: health::Sources,
    /// The ledger's projection (`[ledger] tape`), read-only; `None` where the
    /// deployment declares none, and account history then comes from the
    /// stream alone.
    pub ledger_tape: Option<PathBuf>,
    /// How much one account answer may hold.
    pub account_limits: accounts::Limits,
}

impl Markets {
    /// Over a tape, with the default limits and nothing yet heard.
    pub fn over(tape: PathBuf) -> Markets {
        Markets {
            archive: tape.clone(),
            tape,
            bus: None,
            queue: live::QUEUE,
            normalisers: fill::Normalisers::default(),
            fills: fill::FillCache::default(),
            fill_limits: fill::FillLimits::default(),
            limits: history::Limits::default(),
            statuses: status::Statuses::default(),
            held: metadata::HeldCache::default(),
            health: health::Sources::default(),
            ledger_tape: None,
            account_limits: accounts::Limits::default(),
        }
    }
}

/// **The one clock read** of the service: now, in micros since the epoch.
pub fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

/// The state every handler shares.
pub type Shared = Arc<Markets>;

/// **Every route behind a token**, on their state: the account routes behind
/// the grant of account data as well.
pub fn routes(markets: Shared) -> Router {
    routes_granting(markets, access::AccountGrant::default())
}

/// [`routes`], the account routes answering the clients `grant` names: a test
/// narrows it to have a client without the grant.
pub fn routes_granting(markets: Shared, grant: access::AccountGrant) -> Router {
    let accounts = accounts::routes(markets.clone(), grant);
    Router::new()
        .route("/v1/tape/{kind}", get(history::tape))
        .route("/v1/candles", get(periods::candles))
        .route("/v1/prices", get(prices::prices_route))
        .route("/v1/live", get(live::live))
        .route("/v1/venues", get(metadata::venues_route))
        .route("/v1/instruments", get(metadata::instruments_route))
        .route(
            "/v1/instruments/{venue}/{ticker}",
            get(metadata::instrument_route),
        )
        .with_state(markets)
        .merge(accounts)
}

/// **The service as it is served**: `/health` and `/v1/health` open, every
/// other route behind a client's token. `/v1/health` is a health check, which
/// `market-data-access` lets answer without one (`add-service-health-monitoring`,
/// design D9).
pub fn service(tokens: access::Tokens, markets: Shared) -> Router {
    app(tokens, routes(markets.clone())).merge(
        Router::new()
            .route("/v1/health", get(health::health_route))
            .with_state(markets),
    )
}

/// **The service's routes**: `/health` open, every other route behind a
/// client's token.
pub fn app(tokens: access::Tokens, guarded: Router) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok\n" }))
        .merge(guarded.layer(axum::middleware::from_fn_with_state(
            tokens,
            access::require_token,
        )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    /// A route behind the token, answering with the client it was served to.
    fn probe() -> Router {
        Router::new().route(
            "/v1/probe",
            get(|axum::Extension(client): axum::Extension<access::Client>| async move { client.0 }),
        )
    }

    async fn ask(path: &str, token: Option<&str>) -> (StatusCode, String) {
        let app = app(access::tests::stand_in(), probe());
        let mut request = Request::get(path);
        if let Some(token) = token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        let response = app
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn no_token_and_an_unknown_token_are_refused_alike_naming_no_client() {
        let none = ask("/v1/probe", None).await;
        let unknown = ask("/v1/probe", Some("not-a-token-anybody-was-issued")).await;
        assert_eq!(none.0, StatusCode::UNAUTHORIZED);
        assert_eq!(none, unknown, "the same refusal, whatever was presented");
        for client in access::CLIENTS {
            assert!(!none.1.contains(client), "{}", none.1);
        }
    }

    #[tokio::test]
    async fn a_known_token_is_served_and_named_to_the_handler() {
        assert_eq!(
            ask("/v1/probe", Some("stand-in-token-for-operator")).await,
            (StatusCode::OK, "operator".to_string())
        );
    }

    #[tokio::test]
    async fn the_tape_route_answers_arrow_behind_the_token() {
        let root = tempfile::tempdir().unwrap();
        let markets = Arc::new(Markets::over(root.path().to_path_buf()));
        let app = app(access::tests::stand_in(), routes(markets));
        let path = "/v1/tape/funding?venue=hyperliquid&ticker=BTC&from=0&to=3600000000";
        let refused = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::UNAUTHORIZED);
        let served = app
            .oneshot(
                Request::get(path)
                    .header("Authorization", "Bearer stand-in-token-for-operator")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(served.status(), StatusCode::OK);
        assert_eq!(served.headers()["content-type"], history::ARROW_STREAM);
    }

    #[tokio::test]
    async fn health_needs_no_token() {
        assert_eq!(
            ask("/health", None).await,
            (StatusCode::OK, "ok\n".to_string())
        );
    }
}
