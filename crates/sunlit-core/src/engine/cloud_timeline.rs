//! The cloud timeline's pure halves.
//!
//! The engine's cloud block keeps only a handful of memory fields; the
//! decisions themselves are these functions of (instant, archive, interval),
//! which is what makes them testable without a worker, a renderer or a
//! network: the round an instant wears, the round that would cover it, and
//! the line the UI reads from the state those two leave behind.

use std::time::Duration;

use time::OffsetDateTime;

/// The round that would cover the instant `at`: the instant floored onto the
/// source's publication interval, in UTC — the zone the upstream names its
/// rounds in, whatever offset the scene instant carries. `None` where the
/// interval is degenerate.
pub(super) fn floor_for(at: OffsetDateTime, interval: Duration) -> Option<OffsetDateTime> {
    let at = at.to_offset(OffsetDateTime::UNIX_EPOCH.offset());
    let interval_secs = i64::try_from(interval.as_secs()).ok()?;
    if interval_secs <= 0 {
        return None;
    }
    let midnight = at.replace_time(time::Time::MIDNIGHT);
    let into_day = (at - midnight).whole_seconds();
    let floored = (into_day / interval_secs) * interval_secs;
    Some(midnight + time::Duration::seconds(floored))
}

/// The round of `rounds` that covers the instant `at`, under the strict
/// binding: the newest round at or before `at`, and only while `at` sits
/// inside that round's own interval. A nearest round older than the window
/// does not speak for the instant, and nothing outside the archive's span is
/// covered at all.
pub(super) fn covering_round(
    rounds: &[OffsetDateTime],
    at: OffsetDateTime,
    interval: Duration,
) -> Option<OffsetDateTime> {
    let window = time::Duration::try_from(interval).ok()?;
    let index = rounds.partition_point(|round| *round <= at);
    if index == 0 {
        return None;
    }
    let round = rounds[index - 1];
    (at - round < window).then_some(round)
}

/// A round as the round line spells it, in UTC — the zone the upstream
/// names its rounds in: `2026-08-16 09:00`.
pub(super) fn round_text(round: OffsetDateTime) -> String {
    let format = time::format_description::parse("[year]-[month]-[day] [hour]:[minute]")
        .expect("the round text format parses");
    round
        .to_offset(OffsetDateTime::UNIX_EPOCH.offset())
        .format(&format)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(hour: u8, minute: u8) -> OffsetDateTime {
        time::macros::datetime!(2026-08-16 00:00 UTC)
            + time::Duration::hours(i64::from(hour))
            + time::Duration::minutes(i64::from(minute))
    }

    fn hourly_rounds(days_back: i64, hours: &[u8]) -> Vec<OffsetDateTime> {
        let mut rounds = Vec::new();
        for day in 0..days_back {
            for hour in hours {
                rounds.push(at(*hour, 0) - time::Duration::days(day));
            }
        }
        rounds.sort();
        rounds
    }

    #[test]
    fn floors_onto_the_whole_hour() {
        assert_eq!(
            floor_for(at(9, 41), Duration::from_hours(1)),
            Some(at(9, 0))
        );
        assert_eq!(floor_for(at(0, 0), Duration::from_hours(1)), Some(at(0, 0)));
    }

    #[test]
    fn floors_onto_the_three_hour_mark() {
        assert_eq!(
            floor_for(at(7, 59), Duration::from_hours(3)),
            Some(at(6, 0))
        );
        assert_eq!(floor_for(at(6, 0), Duration::from_hours(3)), Some(at(6, 0)));
    }

    #[test]
    fn a_degenerate_interval_floors_to_nothing() {
        assert_eq!(floor_for(at(9, 0), Duration::ZERO), None);
    }

    /// The upstream names its rounds in UTC, whatever offset the scene
    /// instant carries: a 17:00+08:00 instant is the 09:00 UTC round, and a
    /// floor that answered in wall time would ask the upstream for a round
    /// eight hours away from the one the user is looking at.
    #[test]
    fn the_floor_and_the_text_answer_in_utc_not_wall_time() {
        // 09:41+08:00 IS the 01:41 UTC instant in Beijing wall-time dress:
        // the round it belongs to is 01:00 UTC, not 09:00.
        let instant = at(1, 41).to_offset(time::macros::offset!(+8));
        assert_eq!(floor_for(instant, Duration::from_hours(1)), Some(at(1, 0)));
        assert_eq!(round_text(instant), "2026-08-16 01:41");
    }

    #[test]
    fn the_covering_round_is_the_newest_at_or_before_the_instant() {
        let rounds = hourly_rounds(1, &[8, 9, 10]);
        assert_eq!(
            covering_round(&rounds, at(9, 41), Duration::from_hours(1)),
            Some(at(9, 0))
        );
    }

    #[test]
    fn an_instant_outside_the_span_is_uncovered() {
        let rounds = hourly_rounds(1, &[8, 9]);
        assert_eq!(
            covering_round(&rounds, at(20, 0), Duration::from_hours(1)),
            None,
            "before the first round is uncovered"
        );
        assert_eq!(
            covering_round(
                &rounds,
                at(0, 0) - time::Duration::days(2),
                Duration::from_hours(1)
            ),
            None,
            "after the last round is uncovered"
        );
    }

    #[test]
    fn a_nearest_round_past_the_window_does_not_cover() {
        let rounds = hourly_rounds(1, &[8]);
        assert_eq!(
            covering_round(&rounds, at(9, 1), Duration::from_hours(1)),
            None,
            "61 minutes past the round is outside its hour"
        );
        assert_eq!(
            covering_round(&rounds, at(8, 59), Duration::from_hours(1)),
            Some(at(8, 0)),
            "the last minute inside the hour is covered"
        );
        assert_eq!(
            covering_round(&rounds, at(9, 0), Duration::from_hours(1)),
            None,
            "the hour's own start belongs to the next round, not to this one"
        );
    }

    #[test]
    fn an_empty_archive_covers_nothing() {
        assert_eq!(covering_round(&[], at(9, 0), Duration::from_hours(1)), None);
    }

    #[test]
    fn the_round_line_spells_the_round_compactly() {
        assert_eq!(round_text(at(9, 5)), "2026-08-16 09:05");
    }
}
