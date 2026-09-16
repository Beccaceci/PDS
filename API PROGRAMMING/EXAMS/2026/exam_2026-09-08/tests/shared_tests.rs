use exam_2026_09_08::{ClientId, RateLimiter};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Timing slack. Sleeps and thread scheduling are never exact, so every
/// assertion about elapsed time is a band rather than a point.
const SLACK: Duration = Duration::from_millis(60);

/// Window used by most tests: long enough that SLACK is small next to it,
/// short enough that the suite stays fast.
const WINDOW: Duration = Duration::from_millis(200);

fn timed<F: FnOnce()>(f: F) -> Duration {
    let start = Instant::now();
    f();
    start.elapsed()
}

fn assert_near(actual: Duration, expected: Duration, what: &str) {
    let lo = expected.saturating_sub(SLACK);
    let hi = expected + SLACK;
    assert!(
        actual >= lo && actual <= hi,
        "{what}: expected ~{expected:?} (±{SLACK:?}), got {actual:?}"
    );
}

fn assert_no_delay(actual: Duration, what: &str) {
    assert!(
        actual < SLACK,
        "{what}: expected no delay (<{SLACK:?}), got {actual:?}"
    );
}

// ---- requests below the limit are never delayed ----------------------

#[test]
fn a_single_request_is_not_delayed() {
    let r = RateLimiter::new(WINDOW, 3);
    let c = ClientId::new(1);

    assert_no_delay(timed(|| r.acquire(c)), "first request");
}

#[test]
fn a_full_burst_up_to_the_limit_is_not_delayed() {
    let r = RateLimiter::new(WINDOW, 3);
    let c = ClientId::new(1);

    let elapsed = timed(|| {
        for _ in 0..3 {
            r.acquire(c);
        }
    });
    assert_no_delay(elapsed, "burst of exactly max_requests");
}

// ---- the request past the limit is delayed ---------------------------

#[test]
fn the_request_past_the_limit_is_delayed_until_the_window_frees_up() {
    let r = RateLimiter::new(WINDOW, 3);
    let c = ClientId::new(1);

    for _ in 0..3 {
        r.acquire(c);
    }
    // The oldest of the three just happened, so it leaves the window a
    // full WINDOW from now.
    assert_near(timed(|| r.acquire(c)), WINDOW, "4th request");
}

#[test]
fn the_delay_is_measured_from_the_oldest_request_not_from_now() {
    let r = RateLimiter::new(WINDOW, 2);
    let c = ClientId::new(1);

    r.acquire(c);
    thread::sleep(WINDOW / 2);
    r.acquire(c);

    // The oldest request is already half a window old, so only the
    // remaining half should be waited out.
    assert_near(timed(|| r.acquire(c)), WINDOW / 2, "3rd request");
}

#[test]
fn a_limit_of_one_serializes_requests_one_per_window() {
    let r = RateLimiter::new(WINDOW, 1);
    let c = ClientId::new(1);

    assert_no_delay(timed(|| r.acquire(c)), "1st request");
    assert_near(timed(|| r.acquire(c)), WINDOW, "2nd request");
    assert_near(timed(|| r.acquire(c)), WINDOW, "3rd request");
}

// ---- the window actually slides --------------------------------------

#[test]
fn requests_that_fell_out_of_the_window_no_longer_count() {
    let r = RateLimiter::new(WINDOW, 3);
    let c = ClientId::new(1);

    for _ in 0..3 {
        r.acquire(c);
    }
    thread::sleep(WINDOW + SLACK);

    // The first burst has expired, so a whole new burst is free.
    let elapsed = timed(|| {
        for _ in 0..3 {
            r.acquire(c);
        }
    });
    assert_no_delay(elapsed, "burst after the window expired");
}

#[test]
fn a_delayed_request_occupies_the_window_from_when_it_was_admitted() {
    let r = RateLimiter::new(WINDOW, 1);
    let c = ClientId::new(1);

    r.acquire(c);
    r.acquire(c); // waits ~WINDOW, then is admitted "now"

    // The second request is fresh, so the third pays another full window
    // rather than being let straight through.
    assert_near(timed(|| r.acquire(c)), WINDOW, "3rd request");
}

#[test]
fn sustained_traffic_is_capped_at_max_requests_per_window() {
    let max = 2usize;
    let total = 6usize;
    let r = RateLimiter::new(WINDOW, max);
    let c = ClientId::new(1);

    let elapsed = timed(|| {
        for _ in 0..total {
            r.acquire(c);
        }
    });

    // 6 requests at 2 per window cannot finish faster than 2 windows:
    // the first 2 go through immediately, then each further pair costs a
    // window of waiting.
    let floor = WINDOW * ((total / max) as u32 - 1);
    assert!(
        elapsed + SLACK >= floor,
        "sustained traffic finished too fast: {elapsed:?} < {floor:?}, \
         which means more than {max} requests passed in some window"
    );
}

// ---- clients are limited independently -------------------------------

#[test]
fn a_saturated_client_does_not_delay_a_different_client() {
    let r = RateLimiter::new(WINDOW, 2);
    let busy = ClientId::new(1);
    let idle = ClientId::new(2);

    for _ in 0..2 {
        r.acquire(busy);
    }
    assert_no_delay(timed(|| r.acquire(idle)), "other client's request");
}

#[test]
fn each_client_gets_its_own_budget() {
    let r = RateLimiter::new(WINDOW, 1);

    // One request each for three clients: all free.
    let elapsed = timed(|| {
        for id in 0..3 {
            r.acquire(ClientId::new(id));
        }
    });
    assert_no_delay(elapsed, "one request per client");

    // A second request for one of them is not.
    assert_near(
        timed(|| r.acquire(ClientId::new(0))),
        WINDOW,
        "client 0's second request",
    );
}

// ---- concurrent callers ----------------------------------------------

#[test]
fn concurrent_clients_do_not_block_each_other() {
    let r = Arc::new(RateLimiter::new(WINDOW, 1));

    let elapsed = timed(|| {
        let handles: Vec<_> = (0..8)
            .map(|id| {
                let r = Arc::clone(&r);
                thread::spawn(move || r.acquire(ClientId::new(id)))
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    });

    assert_no_delay(elapsed, "8 distinct clients acquiring concurrently");
}

#[test]
fn concurrent_requests_for_one_client_respect_the_limit() {
    let max = 2usize;
    let threads = 6usize;
    let r = Arc::new(RateLimiter::new(WINDOW, max));
    let c = ClientId::new(1);
    let admissions = Arc::new(Mutex::new(Vec::new()));

    let start = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let r = Arc::clone(&r);
            let admissions = Arc::clone(&admissions);
            thread::spawn(move || {
                r.acquire(c);
                admissions.lock().unwrap().push(start.elapsed());
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let mut admissions = admissions.lock().unwrap().clone();
    admissions.sort();
    let as_ms: Vec<u128> = admissions.iter().map(|d| d.as_millis()).collect();

    // Slide a window over the admissions: no window may contain more than
    // max_requests of them.
    for (i, t0) in admissions.iter().enumerate() {
        let in_window = admissions[i..]
            .iter()
            .take_while(|t| **t - *t0 < WINDOW)
            .count();
        assert!(
            in_window <= max,
            "{in_window} requests were admitted within one {WINDOW:?} window \
             starting at {t0:?}, but max_requests is {max}. \
             Admission times (ms): {as_ms:?}"
        );
    }
}
