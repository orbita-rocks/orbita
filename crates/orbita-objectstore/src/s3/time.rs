//! Unix milliseconds to the two timestamp forms SigV4 wants.
//!
//! This is forty lines of civil-calendar arithmetic instead of a `chrono`
//! dependency because signing needs exactly one conversion, UTC only, no
//! parsing, no time zones. The algorithm is Howard Hinnant's `civil_from_days`
//! (<https://howardhinnant.github.io/date_algorithms.html>), which is exact
//! over the whole useful range.

/// `(YYYYMMDD, YYYYMMDDTHHMMSSZ)` for a Unix-epoch timestamp in milliseconds.
pub(crate) fn amz_timestamp(unix_millis: u64) -> (String, String) {
    let secs = unix_millis / 1000;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let date = format!("{year:04}{month:02}{day:02}");
    let stamp = format!("{date}T{hour:02}{minute:02}{second:02}Z");
    (date, stamp)
}

/// Parses the `LastModified` an S3 listing puts on every entry into Unix
/// milliseconds, or `None` when the text is not the ISO 8601 UTC form S3 emits.
///
/// The sweep needs a write time it can compare against a clock, and the
/// listing already carries one, so this reads it rather than issuing a HEAD per
/// object. Anything unparseable becomes `None` rather than a guessed instant,
/// because a wrong time here is a deleted-live-object bug: see the `None`
/// contract on [`ObjectMeta`](crate::ObjectMeta).
///
/// The accepted shape is `YYYY-MM-DDTHH:MM:SS[.fff]Z`, which is what
/// `ListObjectsV2` returns. Fractional seconds are read to millisecond
/// precision; any offset other than `Z` is rejected, because S3 lists in UTC
/// and honouring a zone would mean pulling in a parser this crate exists to
/// avoid.
pub(crate) fn parse_iso8601_millis(text: &str) -> Option<u64> {
    let text = text.trim();
    let bytes = text.as_bytes();
    // The fixed-width prefix up to the seconds field is exactly 19 characters:
    // `YYYY-MM-DDTHH:MM:SS`. Anything shorter cannot be a timestamp.
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    if bytes[10] != b'T' && bytes[10] != b' ' {
        return None;
    }
    let year: i64 = text.get(0..4)?.parse().ok()?;
    let month: u64 = text.get(5..7)?.parse().ok()?;
    let day: u64 = text.get(8..10)?.parse().ok()?;
    let hour: u64 = text.get(11..13)?.parse().ok()?;
    let minute: u64 = text.get(14..16)?.parse().ok()?;
    let second: u64 = text.get(17..19)?.parse().ok()?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }

    // Whatever follows the seconds is an optional `.fff` fraction and a `Z`.
    let mut millis_of_second: u64 = 0;
    let rest = &text[19..];
    let rest = if let Some(frac) = rest.strip_prefix('.') {
        let digits: String = frac.chars().take_while(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() {
            return None;
        }
        // Pad or truncate to exactly three digits of millisecond precision.
        let mut padded = digits.clone();
        padded.truncate(3);
        while padded.len() < 3 {
            padded.push('0');
        }
        millis_of_second = padded.parse().ok()?;
        &frac[digits.len()..]
    } else {
        rest
    };
    if rest != "Z" {
        return None;
    }

    let days = days_from_civil(year, month, day);
    let secs = days * 86_400 + (hour * 3600 + minute * 60 + second) as i64;
    let millis = secs
        .checked_mul(1000)?
        .checked_add(millis_of_second as i64)?;
    u64::try_from(millis).ok()
}

/// A proleptic Gregorian `(year, month, day)` to days since 1970-01-01, the
/// inverse of [`civil_from_days`]. Howard Hinnant's `days_from_civil`.
fn days_from_civil(year: i64, month: u64, day: u64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy =
        (153 * (if month > 2 { month - 3 } else { month + 9 }) as i64 + 2) / 5 + day as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Days since 1970-01-01 to a proleptic Gregorian `(year, month, day)`.
fn civil_from_days(days: i64) -> (i64, u64, u64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_epoch_formats_as_1970() {
        assert_eq!(
            amz_timestamp(0),
            ("19700101".to_string(), "19700101T000000Z".to_string())
        );
    }

    #[test]
    fn the_sigv4_documentation_date_round_trips() {
        // 2013-05-24T00:00:00Z, the timestamp in AWS's published SigV4 test
        // vectors.
        assert_eq!(amz_timestamp(1_369_353_600_000).1, "20130524T000000Z");
    }

    #[test]
    fn a_leap_day_lands_on_february_29() {
        // 2024-02-29T12:34:56Z.
        assert_eq!(amz_timestamp(1_709_210_096_000).1, "20240229T123456Z");
    }

    #[test]
    fn a_last_modified_with_millis_parses_to_unix_millis() {
        // S3 lists this exact form, fractional seconds and all.
        assert_eq!(
            parse_iso8601_millis("2026-01-01T00:00:00.000Z"),
            Some(1_767_225_600_000)
        );
    }

    #[test]
    fn a_last_modified_round_trips_against_the_formatter() {
        // Whatever the formatter emits at second precision, the parser reads
        // back, so the two civil-calendar routines agree.
        for millis in [
            0u64,
            1_369_353_600_000,
            1_709_210_096_000,
            1_767_225_600_000,
        ] {
            let stamp = amz_timestamp(millis).1;
            let iso = format!(
                "{}-{}-{}T{}:{}:{}Z",
                &stamp[0..4],
                &stamp[4..6],
                &stamp[6..8],
                &stamp[9..11],
                &stamp[11..13],
                &stamp[13..15],
            );
            assert_eq!(parse_iso8601_millis(&iso), Some(millis), "for {iso}");
        }
    }

    #[test]
    fn fractional_seconds_are_read_to_millisecond_precision() {
        // Sub-millisecond digits are dropped, not rounded, and short fractions
        // are padded rather than misread as a larger number.
        assert_eq!(
            parse_iso8601_millis("2026-01-01T00:00:00.123456Z"),
            Some(1_767_225_600_123)
        );
        assert_eq!(
            parse_iso8601_millis("2026-01-01T00:00:00.1Z"),
            Some(1_767_225_600_100)
        );
    }

    #[test]
    fn a_non_utc_or_malformed_time_is_none_rather_than_a_guess() {
        assert_eq!(parse_iso8601_millis("2026-01-01T00:00:00+01:00"), None);
        assert_eq!(parse_iso8601_millis("not a time"), None);
        assert_eq!(parse_iso8601_millis(""), None);
        assert_eq!(parse_iso8601_millis("2026-13-01T00:00:00Z"), None);
    }
}
