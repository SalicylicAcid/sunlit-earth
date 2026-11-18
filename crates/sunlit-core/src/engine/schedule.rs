//! Deadline bookkeeping for the engine loop's periodic jobs.
//!
//! Its own file because the loop's own schedule is the contract this crate
//! documents most carefully: the loop never sleeps on wall time, it asks this
//! what is due against the injected clock.

use std::time::Duration;

/// Deadline bookkeeping for one periodic job.
pub(super) struct Schedule {
    pub(super) interval: Duration,
    pub(super) next: Duration,
}

impl Schedule {
    pub(super) fn new(interval: Duration, now: Duration) -> Self {
        Self {
            interval,
            next: now + interval,
        }
    }

    /// Whether the job is due, rolling the deadline forward if so.
    ///
    /// The deadline is recomputed from `now` rather than accumulated, so a long
    /// stall (a sleeping laptop, a mock clock jumping a day) produces one run
    /// rather than a burst of catch-up runs.
    pub(super) fn due(&mut self, now: Duration) -> bool {
        if now >= self.next {
            self.next = now + self.interval;
            true
        } else {
            false
        }
    }

    pub(super) fn set_interval(&mut self, interval: Duration, now: Duration) {
        self.interval = interval;
        self.next = now + interval;
    }
}

#[derive(Default)]
pub(super) struct RoundSchedule {
    next: Option<time::OffsetDateTime>,
    in_flight: bool,
    last_tick: Option<time::OffsetDateTime>,
}

impl RoundSchedule {
    pub(super) fn started(&mut self) {
        self.in_flight = true;
    }

    pub(super) fn due(
        &mut self,
        now: time::OffsetDateTime,
        fetching: bool,
        publication: crate::assets::cloud_source::CloudPublication,
    ) -> bool {
        let resumed = self
            .last_tick
            .replace(now)
            .is_some_and(|last| (now - last).abs() >= time::Duration::minutes(1));
        if self.in_flight && !fetching {
            self.in_flight = false;
            self.next = Some(
                publication
                    .latest
                    .and_then(|round| {
                        round.checked_add(
                            time::Duration::try_from(publication.interval)
                                .expect("publication interval fits"),
                        )
                    })
                    .filter(|next| *next > now)
                    .unwrap_or(now + time::Duration::minutes(5)),
            );
        }
        if resumed && !fetching {
            self.next = Some(now);
        }
        !fetching && self.next.is_some_and(|next| now >= next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publication(
        hours: u64,
        latest: time::OffsetDateTime,
    ) -> crate::assets::cloud_source::CloudPublication {
        crate::assets::cloud_source::CloudPublication {
            interval: Duration::from_hours(hours),
            latest: Some(latest),
        }
    }

    #[test]
    fn native_deadlines_are_the_next_round_not_the_download_time() {
        for hours in [1, 3] {
            let latest = time::macros::datetime!(2026-11-18 21:00 UTC);
            let now = latest + time::Duration::minutes(38);
            let mut schedule = RoundSchedule::default();
            schedule.started();
            assert!(!schedule.due(now, false, publication(hours, latest)));
            assert_eq!(schedule.next, Some(latest + Duration::from_hours(hours)));
            let boundary = schedule.next.unwrap();
            schedule.last_tick = Some(boundary - time::Duration::seconds(1));
            assert!(schedule.due(boundary, false, publication(hours, latest)));
            schedule.started();
            assert!(!schedule.due(boundary, true, publication(hours, latest)));
            assert!(!schedule.due(boundary, false, publication(hours, latest)));
            assert_eq!(schedule.next, Some(boundary + time::Duration::minutes(5)));
        }
    }

    #[test]
    fn delayed_publication_retries_until_caught_up_then_waits_for_the_next_round() {
        let now = time::macros::datetime!(2026-11-18 06:00 UTC);
        let previous = now - time::Duration::hours(1);
        let mut schedule = RoundSchedule::default();
        for minutes in [0, 5, 10] {
            let check = now + time::Duration::minutes(minutes);
            schedule.last_tick = Some(check);
            schedule.started();
            assert!(!schedule.due(check, false, publication(1, previous)));
            assert_eq!(schedule.next, Some(check + time::Duration::minutes(5)));
        }
        schedule.started();
        assert!(!schedule.due(
            now + time::Duration::minutes(10),
            false,
            publication(1, now)
        ));
        assert_eq!(schedule.next, Some(now + time::Duration::hours(1)));
    }

    #[test]
    fn resume_and_clock_jumps_check_once_without_overlapping_a_fetch() {
        let now = time::macros::datetime!(2026-11-18 06:38 UTC);
        let published = publication(3, time::macros::datetime!(2026-11-18 06:00 UTC));
        for jump in [
            time::Duration::minutes(10),
            time::Duration::days(2),
            -time::Duration::minutes(10),
        ] {
            let mut schedule = RoundSchedule::default();
            schedule.started();
            assert!(!schedule.due(now, false, published));
            assert!(schedule.due(now + jump, false, published));
            schedule.started();
            assert!(!schedule.due(now + jump, true, published));
            assert!(!schedule.due(now + jump, false, published));
        }
    }

    #[test]
    fn a_finished_attempt_without_a_round_waits_before_checking_again() {
        let now = time::macros::datetime!(2026-11-18 06:00 UTC);
        let mut schedule = RoundSchedule::default();
        schedule.started();
        assert!(!schedule.due(
            now,
            false,
            crate::assets::cloud_source::CloudPublication {
                interval: Duration::from_hours(1),
                latest: None,
            }
        ));
        assert_eq!(schedule.next, Some(now + time::Duration::minutes(5)));
    }

    #[test]
    fn schedule_fires_at_its_interval_and_not_before() {
        let mut s = Schedule::new(Duration::from_secs(10), Duration::ZERO);
        assert!(!s.due(Duration::from_secs(9)));
        assert!(s.due(Duration::from_secs(10)));
    }

    #[test]
    fn schedule_does_not_burst_after_a_long_stall() {
        let mut s = Schedule::new(Duration::from_secs(10), Duration::ZERO);
        // One jump of a simulated day must produce one run, not 8640.
        assert!(s.due(Duration::from_hours(24)));
        assert!(!s.due(Duration::from_hours(24)));
        assert!(s.due(Duration::from_secs(86_410)));
    }

    #[test]
    fn schedule_interval_change_restarts_the_countdown() {
        let mut s = Schedule::new(Duration::from_mins(10), Duration::ZERO);
        s.set_interval(Duration::from_mins(1), Duration::from_secs(30));
        assert!(!s.due(Duration::from_secs(89)));
        assert!(s.due(Duration::from_secs(90)));
    }
}
