//! **`varcov`'s EWMA horizons**: Σ at the newest close, and what is read
//! from it, as `py/signals/varcov.py` writes them.
//!
//! Per horizon, from its long log returns:
//!
//! 1. The joint sample of the returns as they are (nulls missing). Refused,
//!    or empty: skipped and said. Its last close is the `asof`; not later
//!    than the stored one: nothing new.
//! 2. The walk: the joint sample of the returns without their nulls, and
//!    the EWMA over it (`gr.models.corr.walk_forward` with one origin, the
//!    last close). Refused — under the warm-up — every row is written absent
//!    with the reason, the turbulence still written.
//! 3. Present: `covariance` for i ≤ j and `correlation` for i < j at the
//!    origin, `n_eff` the EWMA's effective sample over the walk's rows.
//! 4. Derived from Σ: beta and idiosyncratic share to BTC, absorption, the
//!    last bar's surprise against the Σ forecast at the previous close, and
//!    the historical turbulence of the last bar.

use galata_quant::{ewma, joint, matrix};
use nalgebra::DMatrix;

use crate::VARCOV;
use crate::bars::Returns;
use crate::declared::Horizon;
use crate::rows::{Row, py_float};

/// The reference instrument beta is taken against.
pub const REFERENCE: &str = "BTC";
/// `ticker_i` of a figure about the whole universe.
pub const EVERYONE: &str = "*";
const SURPRISE: [&str; 4] = [
    "mahalanobis",
    "chi2_percentile",
    "magnitude_surprise",
    "correlation_surprise",
];

/// One run's identity.
#[derive(Debug, Clone)]
pub struct Run {
    /// The run's clock reading.
    pub computed_micros: i64,
    /// Which galata-quant computed it.
    pub code: String,
}

impl Run {
    /// `varcov-<computed>`, as `py/signals` names a run.
    pub fn run_id(&self) -> String {
        format!("{VARCOV}-{}", self.computed_micros)
    }
}

/// What one horizon came to.
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome {
    /// Its rows; none when skipped or nothing new.
    pub rows: Vec<Row>,
    /// What to say of it.
    pub said: String,
}

struct Base<'a> {
    hz: &'a Horizon,
    run: &'a Run,
    asof: i64,
}

impl Base<'_> {
    fn row(
        &self,
        signal: &str,
        measure: &str,
        i: &str,
        j: Option<&str>,
        value: Result<f64, String>,
        target: i64,
    ) -> Row {
        let (value, absent) = match value {
            Ok(v) => (Some(v), None),
            Err(why) => (None, Some(why)),
        };
        Row {
            signal: signal.to_string(),
            horizon: self.hz.name.clone(),
            measure: measure.to_string(),
            ticker_i: i.to_string(),
            ticker_j: j.map(str::to_string),
            h: 1,
            value,
            absent,
            n_eff: None,
            asof_micros: self.asof,
            target_micros: target,
            computed_micros: self.run.computed_micros,
            fitted_through_micros: None,
            fit_from_micros: None,
            model: "ewma".to_string(),
            params: format!("{{\"lam\": {}}}", py_float(self.hz.lam)),
            fitted: false,
            after_gap: false,
            code: self.run.code.clone(),
            run_id: self.run.run_id(),
        }
    }
}

fn joint_of(r: &Returns) -> Result<joint::Joint, String> {
    joint::joint(&r.ticker, &r.ts, &r.close_ts, &r.value).map_err(|e| e.reason)
}

/// The walk's Σ path over a joint sample: `walk_forward(corr="ewma")` with
/// its one origin at the sample's last close, refused as that refuses.
fn walk(j: &joint::Joint, lam: f64) -> Result<ewma::EwmaFit, String> {
    let w = ewma::warmup(lam, None).map_err(|e| e.reason)?;
    if j.rows() < w {
        return Err(format!(
            "the first origin has {} joint returns, under the warm-up {w} at lam={}: it starts at {}'s first return",
            j.rows(),
            py_float(lam),
            j.youngest
        ));
    }
    ewma::ewma(j, lam, Some(w)).map_err(|e| e.reason)
}

fn when(micros: i64) -> String {
    jiff::Timestamp::from_microsecond(micros)
        .map(|t| t.strftime("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|_| micros.to_string())
}

/// One horizon's rows, given its returns and the newest asof stored for it.
pub fn compute(hz: &Horizon, returns: &Returns, stored: Option<i64>, run: &Run) -> Outcome {
    let skipped = |why: String| Outcome {
        rows: Vec::new(),
        said: format!("{}: skipped, {why}", hz.name),
    };
    let full = match joint_of(returns) {
        Ok(j) => j,
        Err(why) => return skipped(why),
    };
    if full.rows() == 0 {
        return skipped("no joint returns".to_string());
    }
    let last = full.rows() - 1;
    let asof = full.close_ts[last];
    if stored.is_some_and(|s| asof <= s) {
        return Outcome {
            rows: Vec::new(),
            said: format!("{}: nothing new since {}", hz.name, when(asof)),
        };
    }
    let base = Base { hz, run, asof };
    let tickers = full.tickers.clone();
    let bar_open = full.ts[last];

    let walked = joint_of(&returns.present()).and_then(|j| walk(&j, hz.lam).map(|fit| (j, fit)));
    let (walk_joint, fit) = match walked {
        Ok(found) => found,
        Err(why) => {
            let mut rows = absent(&base, &tickers, &why);
            rows.extend(turbulence(&base, &full, bar_open));
            return Outcome {
                rows,
                said: format!("{}: absent, {why}", hz.name),
            };
        }
    };

    let sigma = fit
        .sigma
        .last()
        .expect("a walk past its warm-up has a row")
        .clone();
    let rho = ewma::correlation(&sigma);
    let n_eff = ewma::n_eff(hz.lam, Some(walk_joint.rows() as f64)).ok();
    let after_gap = *walk_joint
        .after_gap
        .last()
        .expect("a walked sample has rows");
    let mut rows = Vec::new();
    for (i, a) in tickers.iter().enumerate() {
        for (j, b) in tickers.iter().enumerate().skip(i) {
            let mut cov = base.row(VARCOV, "covariance", a, Some(b), Ok(sigma[(i, j)]), asof);
            cov.n_eff = n_eff;
            cov.after_gap = after_gap;
            rows.push(cov);
            if i != j {
                let mut cor = base.row(VARCOV, "correlation", a, Some(b), Ok(rho[(i, j)]), asof);
                cor.n_eff = n_eff;
                cor.after_gap = after_gap;
                rows.push(cor);
            }
        }
    }
    rows.extend(derived(&base, &tickers, &sigma, returns, &full, bar_open));
    rows.extend(turbulence(&base, &full, bar_open));
    Outcome {
        rows,
        said: format!(
            "{}: {} instruments at {}",
            hz.name,
            tickers.len(),
            when(asof)
        ),
    }
}

fn derived(
    base: &Base,
    tickers: &[String],
    sigma: &DMatrix<f64>,
    returns: &Returns,
    full: &joint::Joint,
    bar_open: i64,
) -> Vec<Row> {
    let asof = base.asof;
    let mut rows = Vec::new();
    if tickers.iter().any(|t| t == REFERENCE) {
        match matrix::beta(sigma, tickers, REFERENCE) {
            Ok(betas) => {
                for (t, b, idio) in betas {
                    rows.push(base.row("beta", "beta", &t, Some(REFERENCE), Ok(b), asof));
                    rows.push(base.row(
                        "beta",
                        "idiosyncratic_share",
                        &t,
                        Some(REFERENCE),
                        Ok(idio),
                        asof,
                    ));
                }
            }
            Err(why) => {
                for t in tickers.iter().filter(|t| *t != REFERENCE) {
                    for m in ["beta", "idiosyncratic_share"] {
                        rows.push(base.row(
                            "beta",
                            m,
                            t,
                            Some(REFERENCE),
                            Err(why.reason.clone()),
                            asof,
                        ));
                    }
                }
            }
        }
    }
    match matrix::absorption(sigma, 1) {
        Ok((cov_ar, cor_ar)) => {
            rows.push(base.row(
                "absorption",
                "covariance_ar",
                EVERYONE,
                None,
                Ok(cov_ar),
                asof,
            ));
            rows.push(base.row(
                "absorption",
                "correlation_ar",
                EVERYONE,
                None,
                Ok(cor_ar),
                asof,
            ));
        }
        Err(why) => {
            for m in ["covariance_ar", "correlation_ar"] {
                rows.push(base.row(
                    "absorption",
                    m,
                    EVERYONE,
                    None,
                    Err(why.reason.clone()),
                    asof,
                ));
            }
        }
    }
    // The last bar against the Σ forecast before it: a second walk, through
    // the previous close.
    let found = if full.rows() < 2 {
        Err("no bar before the last to forecast from".to_string())
    } else {
        let prev = full.close_ts[full.rows() - 2];
        joint_of(&returns.through(prev).present())
            .and_then(|j| walk(&j, base.hz.lam))
            .and_then(|before| {
                let s = before
                    .sigma
                    .last()
                    .expect("a walk past its warm-up has a row");
                matrix::surprise(full.row(full.rows() - 1), s).map_err(|e| e.reason)
            })
    };
    match found {
        Ok(s) => {
            let values = [
                Ok(s.mahalanobis),
                Ok(s.chi2_percentile),
                Ok(s.magnitude_surprise),
                s.correlation_surprise
                    .ok_or_else(|| "no magnitude to divide by: no instrument moved".to_string()),
            ];
            for (m, v) in SURPRISE.iter().zip(values) {
                rows.push(base.row("surprise", m, EVERYONE, None, v, bar_open));
            }
        }
        Err(why) => {
            for m in SURPRISE {
                rows.push(base.row("surprise", m, EVERYONE, None, Err(why.clone()), bar_open));
            }
        }
    }
    rows
}

/// Every row of a horizon whose Σ is absent, carrying the same reason.
fn absent(base: &Base, tickers: &[String], why: &str) -> Vec<Row> {
    let asof = base.asof;
    let gone = || Err(why.to_string());
    let mut rows = Vec::new();
    for (i, a) in tickers.iter().enumerate() {
        for b in &tickers[i..] {
            rows.push(base.row(VARCOV, "covariance", a, Some(b), gone(), asof));
            if a != b {
                rows.push(base.row(VARCOV, "correlation", a, Some(b), gone(), asof));
            }
        }
    }
    if tickers.iter().any(|t| t == REFERENCE) {
        for t in tickers.iter().filter(|t| *t != REFERENCE) {
            for m in ["beta", "idiosyncratic_share"] {
                rows.push(base.row("beta", m, t, Some(REFERENCE), gone(), asof));
            }
        }
    }
    for m in ["covariance_ar", "correlation_ar"] {
        rows.push(base.row("absorption", m, EVERYONE, None, gone(), asof));
    }
    for m in SURPRISE {
        rows.push(base.row("surprise", m, EVERYONE, None, gone(), asof));
    }
    rows
}

/// Kritzman and Li's historical turbulence of the last bar: no model, so
/// written whatever the model said.
fn turbulence(base: &Base, full: &joint::Joint, bar_open: i64) -> Vec<Row> {
    let found = matrix::turbulence(&full.values, full.rows(), full.width()).map_err(|e| e.reason);
    let mut rows = Vec::new();
    for (k, m) in ["turbulence", "percentile"].iter().enumerate() {
        let v = found.clone().map(|(t, p)| if k == 0 { t } else { p });
        let mut row = base.row("turbulence", m, EVERYONE, None, v, bar_open);
        row.model = "sample-covariance".to_string();
        row.params = format!("{{\"n\": {}}}", full.rows());
        rows.push(row);
    }
    rows
}
