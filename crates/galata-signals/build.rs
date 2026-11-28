//! Which galata-quant computed a row: its version and commit, read from the
//! lockfile cargo resolved, and stamped into every row's `code`
//! (`galata-quant@0.1.0+f1c95f8`) so the record says which arithmetic it holds.
fn main() {
    let lock = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock");
    println!("cargo:rerun-if-changed={}", lock.display());
    let text = std::fs::read_to_string(&lock).unwrap_or_default();
    let mut code = "galata-quant@unknown".to_string();
    let mut in_quant = false;
    let mut version = String::new();
    for line in text.lines() {
        if line == "[[package]]" {
            in_quant = false;
        } else if line == "name = \"galata-quant\"" {
            in_quant = true;
        } else if in_quant && let Some(v) = line.strip_prefix("version = \"") {
            version = v.trim_end_matches('"').to_string();
        } else if in_quant && let Some(source) = line.strip_prefix("source = \"") {
            let commit = source
                .trim_end_matches('"')
                .rsplit('#')
                .next()
                .unwrap_or("");
            code = format!("galata-quant@{version}+{}", &commit[..commit.len().min(7)]);
        }
    }
    println!("cargo:rustc-env=GALATA_QUANT_CODE={code}");
}
