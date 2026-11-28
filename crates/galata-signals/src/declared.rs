//! **The declared horizons**: `py/signals/signals.toml`'s `[varcov.<name>]`
//! tables, the one file both calculators read. Slice 1 computes the EWMA ones
//! (`corr = "ewma"`); a fitted horizon is named as skipped, never computed
//! here and never written absent on its behalf.

use std::collections::BTreeMap;

/// One EWMA horizon.
#[derive(Debug, Clone, PartialEq)]
pub struct Horizon {
    /// `30m`, `1d`, `1w`.
    pub name: String,
    /// The captured bars it is built from: `1m`, `1d`.
    pub bars: String,
    /// The decay.
    pub lam: f64,
}

/// What `signals.toml` declares for `varcov`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Declared {
    /// The horizons this slice computes.
    pub ewma: Vec<Horizon>,
    /// The fitted horizons, by name: later slices'.
    pub skipped: Vec<String>,
}

/// The widths a horizon may have, in microseconds, as `py/signals/bars.py`'s.
pub const WIDTH: [(&str, i64); 7] = [
    ("1m", 60_000_000),
    ("5m", 300_000_000),
    ("30m", 1_800_000_000),
    ("1h", 3_600_000_000),
    ("4h", 14_400_000_000),
    ("1d", 86_400_000_000),
    ("1w", 604_800_000_000),
];

/// The intervals the tape captures.
pub const CAPTURED: [&str; 4] = ["1m", "1h", "4h", "1d"];

/// A width's microseconds.
pub fn width(name: &str) -> Option<i64> {
    WIDTH.iter().find(|(n, _)| *n == name).map(|(_, w)| *w)
}

/// Read `signals.toml`'s varcov horizons.
pub fn read(text: &str) -> Result<Declared, String> {
    let doc: toml::Table =
        toml::from_str(text).map_err(|e| format!("signals.toml does not parse: {e}"))?;
    let Some(varcov) = doc.get("varcov").and_then(|v| v.as_table()) else {
        return Ok(Declared::default());
    };
    let known = [
        "bars",
        "corr",
        "model",
        "dist",
        "lam",
        "deseasonalise",
        "min_obs",
    ];
    let mut out = Declared::default();
    let tables: BTreeMap<&String, &toml::Value> = varcov.iter().collect();
    for (name, table) in tables {
        let table = table
            .as_table()
            .ok_or_else(|| format!("varcov.{name} is not a table"))?;
        if let Some(k) = table.keys().find(|k| !known.contains(&k.as_str())) {
            return Err(format!("varcov.{name}: unknown key {k}"));
        }
        let corr = table.get("corr").and_then(|v| v.as_str()).unwrap_or("");
        if !matches!(corr, "dcc" | "cdcc" | "ccc" | "ewma") {
            return Err(format!(
                "varcov.{name}: corr must be dcc, cdcc, ccc or ewma"
            ));
        }
        if corr != "ewma" {
            out.skipped.push(name.clone());
            continue;
        }
        let bars = table
            .get("bars")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("varcov.{name}: no bars"))?;
        if !CAPTURED.contains(&bars) {
            return Err(format!(
                "varcov.{name}: bars={bars} is not one the tape captures ({})",
                CAPTURED.join(", ")
            ));
        }
        let (Some(coarse), Some(fine)) = (width(name), width(bars)) else {
            return Err(format!("varcov.{name}: {name} is not a width"));
        };
        if coarse < fine || coarse % fine != 0 {
            return Err(format!(
                "varcov.{name}: {name} is not a whole multiple of {bars}"
            ));
        }
        let lam = match table.get("lam") {
            None => 0.94,
            Some(v) => v
                .as_float()
                .or_else(|| v.as_integer().map(|i| i as f64))
                .ok_or_else(|| format!("varcov.{name}: lam is not a number"))?,
        };
        out.ewma.push(Horizon {
            name: name.clone(),
            bars: bars.to_string(),
            lam,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ewma_horizons_are_taken_and_the_fitted_named() {
        let d = read(
            r#"
[varcov.5m]
bars = "1m"
model = "garch"
dist = "t"
corr = "ccc"
[varcov.30m]
bars = "1m"
corr = "ewma"
lam = 0.94
[varcov.1w]
bars = "1d"
corr = "ewma"
"#,
        )
        .unwrap();
        assert_eq!(
            d.ewma.iter().map(|h| h.name.as_str()).collect::<Vec<_>>(),
            vec!["1w", "30m"]
        );
        assert_eq!(d.ewma[0].lam, 0.94);
        assert_eq!(d.skipped, vec!["5m"]);
    }

    #[test]
    fn the_repositorys_signals_toml_reads() {
        let text = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../py/signals/signals.toml"
        ))
        .unwrap();
        let d = read(&text).unwrap();
        let names: Vec<&str> = d.ewma.iter().map(|h| h.name.as_str()).collect();
        assert_eq!(names, vec!["1d", "1w", "30m"]);
        assert_eq!(d.skipped.len(), 3);
    }
}
