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
}
