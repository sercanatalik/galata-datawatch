//! A service token is raised before the morning it lapses, not found as a
//! refusal at restart.

use galata_datawatch_vault::{TOKEN_WARNING_DAYS, TokenNotice, token_notice, utc_date};

const DAY: i64 = 86_400;
const NOW: i64 = 1_790_000_000;

#[test]
fn a_token_inside_thirty_days_is_a_warning() {
    assert_eq!(
        token_notice(NOW + 10 * DAY, NOW),
        TokenNotice::Soon { days: 10 }
    );
    assert_eq!(token_notice(NOW + 29 * DAY + 1, NOW).word(), "WARN");
    // The last day is still a warning, not yet an expiry.
    assert_eq!(token_notice(NOW + 60, NOW), TokenNotice::Soon { days: 0 });
}

#[test]
fn a_token_a_year_out_says_nothing() {
    assert_eq!(
        token_notice(NOW + 365 * DAY, NOW),
        TokenNotice::Ok { days: 365 }
    );
    assert_eq!(
        token_notice(NOW + TOKEN_WARNING_DAYS * DAY, NOW).word(),
        "ok"
    );
}

#[test]
fn a_lapsed_token_is_expired_not_negative_days() {
    assert_eq!(token_notice(NOW, NOW), TokenNotice::Expired);
    let lapsed = token_notice(NOW - 40 * DAY, NOW);
    assert_eq!((lapsed.word(), lapsed.days()), ("EXPIRED", 0));
}

#[test]
fn the_date_is_the_utc_calendar_day() {
    assert_eq!(utc_date(0), "1970-01-01");
    assert_eq!(utc_date(-1), "1969-12-31");
    assert_eq!(utc_date(951_782_400), "2000-02-29");
    // The four service tokens minted 2026-09-25 07:14Z, as `gv token ls` shows them.
    assert_eq!(utc_date(1_821_856_440), "2027-09-25");
    assert_eq!(utc_date(1_821_916_799), "2027-09-25");
    assert_eq!(utc_date(1_821_916_800), "2027-09-26");
}
