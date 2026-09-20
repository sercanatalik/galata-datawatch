//! The civil calendar, defined once, with its inverse beside it.
//!
//! [`date_of`] and [`midnight_of`] must agree for a partition name to round
//! trip, and **a second implementation of the calendar does not fail when it
//! drifts — it disagrees**, producing a partition nobody can find. So they sit
//! together, and the validation of one is a round trip through the other.
//!
//! Howard Hinnant's `days_from_civil` / `civil_from_days`: exact, and needing
//! no dependency and no clock. The input is always a timestamp the caller
//! already holds.

/// Microseconds in a day.
const MICROS_PER_DAY: i64 = 86_400_000_000;

/// The UTC date a receipt time falls on, as `YYYY-MM-DD`.
pub fn date_of(micros: i64) -> String {
    // Euclidean, not truncating: a negative timestamp belongs to the day
    // BEFORE the epoch, and `/` would round it toward zero and name the wrong
    // one.
    let days = micros.div_euclid(MICROS_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Midnight UTC of a `YYYY-MM-DD` date, in microseconds. The exact inverse of
/// [`date_of`].
///
/// `None` for anything that is not exactly ten characters of `YYYY-MM-DD` with
/// a real month and day.
pub fn midnight_of(date: &str) -> Option<i64> {
    let bytes = date.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let y: i64 = date[0..4].parse().ok()?;
    let m: u32 = date[5..7].parse().ok()?;
    let d: u32 = date[8..10].parse().ok()?;
    if !(1..=12).contains(&m) || d < 1 {
        return None;
    }
    let days = days_from_civil(y, m, d);
    // Round trip, which is what rejects 2026-02-30 **without a month-length
    // table here to disagree with the one already inside `civil_from_days`**.
    if civil_from_days(days) != (y, m, d) {
        return None;
    }
    Some(days * MICROS_PER_DAY)
}

/// Days since the epoch, from a civil date. The exact inverse of the below.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = m as i64;
    let d = d as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// A civil date, from days since the epoch. Exact, and needing nothing.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_date_round_trips_to_its_own_midnight() {
        for micros in [
            0,
            1_758_326_400_000_000,
            1_758_326_460_123_456,
            -1,
            -86_400_000_000,
        ] {
            let date = date_of(micros);
            let midnight = midnight_of(&date).expect("a date we produced must parse");
            assert_eq!(
                date_of(midnight),
                date,
                "{micros} -> {date} did not survive"
            );
            assert!(midnight <= micros, "midnight is at or before the moment");
            assert!(
                micros - midnight < MICROS_PER_DAY,
                "and within the same day"
            );
        }
    }

    #[test]
    fn an_impossible_date_is_refused_rather_than_normalised() {
        // No month-length table here. The round trip through civil_from_days
        // is what rejects these, so there is no second table to drift from the
        // first.
        for bad in [
            "2026-02-30",
            "2026-13-01",
            "2026-00-01",
            "2026-01-00",
            "2026-01-32",
            "2026-1-01",
            "2026/01/01",
            "20260101",
            "",
        ] {
            assert_eq!(midnight_of(bad), None, "{bad:?} must be refused");
        }
        // And a real leap day is not.
        assert!(midnight_of("2024-02-29").is_some());
        assert_eq!(midnight_of("2023-02-29"), None, "2023 is not a leap year");
    }

    #[test]
    fn a_time_before_the_epoch_still_names_its_date() {
        // Truncating division would name 1970-01-01 for the microsecond
        // before the epoch. Euclidean division names the day it belongs to.
        assert_eq!(date_of(0), "1970-01-01");
        assert_eq!(date_of(-1), "1969-12-31");
        assert_eq!(date_of(-MICROS_PER_DAY), "1969-12-31");
        assert_eq!(date_of(-MICROS_PER_DAY - 1), "1969-12-30");
    }

    #[test]
    fn known_dates_are_what_they_should_be() {
        assert_eq!(date_of(1_758_326_400_000_000), "2025-09-20");
        assert_eq!(midnight_of("1970-01-01"), Some(0));
        assert_eq!(midnight_of("2000-03-01"), Some(951_868_800_000_000));
    }
}
