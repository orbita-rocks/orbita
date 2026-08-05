//! RFC 3339 timestamps to Unix milliseconds.
//!
//! Both credential sources report expiry as an RFC 3339 instant in UTC, and
//! the refresh logic compares it against `Clock::now_millis()`. That is the
//! only conversion needed, in one direction, with no time zones, so this is
//! thirty lines of civil-calendar arithmetic rather than a date library. It is
//! the inverse of the algorithm `orbita-objectstore` already uses to format
//! signing timestamps: Howard Hinnant's `days_from_civil`
//! (<https://howardhinnant.github.io/date_algorithms.html>).
//!
//! A timestamp that does not parse is `None` rather than an error, and the
//! caller treats an unparseable expiry as "expires immediately". Guessing that
//! a credential lasts forever because its deadline was unreadable is how a
//! node ends up signing with a dead key at three in the morning.

/// Parses `YYYY-MM-DDTHH:MM:SS[.fff]Z` into Unix milliseconds.
///
/// Only the UTC `Z` form is accepted, because it is the only form AWS emits
/// here and accepting an offset would mean carrying code that no deployment
/// exercises.
pub(crate) fn parse_rfc3339_millis(text: &str) -> Option<u64> {
    let text = text.trim();
    let text = text.strip_suffix('Z').or_else(|| text.strip_suffix('z'))?;
    // Fractional seconds are dropped: the refresh margin is minutes wide, so
    // sub-second precision on an expiry cannot change a decision.
    let (text, _) = text.split_once('.').unwrap_or((text, ""));

    let (date, time) = text.split_once('T').or_else(|| text.split_once('t'))?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: u64 = date_parts.next()?.parse().ok()?;
    let day: u64 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    let mut time_parts = time.split(':');
    let hour: u64 = time_parts.next()?.parse().ok()?;
    let minute: u64 = time_parts.next()?.parse().ok()?;
    let second: u64 = time_parts.next()?.parse().ok()?;
    // A leap second reports 60, which is a real value on the wire.
    if time_parts.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    let days = days_from_civil(year, month, day);
    let seconds = days.checked_mul(86_400)? + (hour * 3600 + minute * 60 + second) as i64;
    // A pre-epoch expiry is already long gone, and the caller only compares
    // this against a Unix millisecond clock, so clamping at zero says the same
    // thing without needing a signed type everywhere.
    u64::try_from(seconds.max(0)).ok()?.checked_mul(1000)
}

/// Days since 1970-01-01 for a proleptic Gregorian date.
fn days_from_civil(year: i64, month: u64, day: u64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = (year - era * 400) as u64;
    let shifted = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * shifted + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_epoch_parses_as_zero() {
        assert_eq!(parse_rfc3339_millis("1970-01-01T00:00:00Z"), Some(0));
    }

    #[test]
    fn an_expiry_round_trips_against_the_signing_formatter() {
        // 2013-05-24T00:00:00Z, the instant orbita-objectstore's signing tests
        // pin, so the two calendars are checked against each other.
        assert_eq!(
            parse_rfc3339_millis("2013-05-24T00:00:00Z"),
            Some(1_369_353_600_000)
        );
    }

    #[test]
    fn a_leap_day_is_a_real_day() {
        assert_eq!(
            parse_rfc3339_millis("2024-02-29T12:34:56Z"),
            Some(1_709_210_096_000)
        );
    }

    #[test]
    fn fractional_seconds_are_accepted_and_ignored() {
        assert_eq!(
            parse_rfc3339_millis("2024-02-29T12:34:56.789Z"),
            Some(1_709_210_096_000)
        );
    }

    #[test]
    fn an_unparseable_expiry_is_none_rather_than_forever() {
        for text in [
            "",
            "not a timestamp",
            "2024-02-29T12:34:56+01:00",
            "2024-13-01T00:00:00Z",
            "2024-02-29T25:00:00Z",
            "2024-02-29",
        ] {
            assert_eq!(parse_rfc3339_millis(text), None, "{text:?}");
        }
    }
}
