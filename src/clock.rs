use crate::proto;
use failure::Error;
use hybrid_clocks::{Clock as HybridClock, ClockSource, Timestamp};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

pub type HybridTimestamp = Timestamp<WallT>;

#[derive(Clone)]
pub struct Clock {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    clock: HybridClock<Wall>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Wall;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Nanoseconds since unix epoch
pub struct WallT(u64);

impl Clock {
    pub fn new() -> Self {
        let inner = Arc::new(Mutex::new(Inner::new()));
        Self { inner }
    }

    pub fn now(&self) -> HybridTimestamp {
        self.inner.lock().unwrap().clock.now()
    }

    pub fn for_expiration_in(&self, diff: Duration) -> HybridTimestamp {
        let mut timestamp = self.now();
        timestamp.count = 0;
        timestamp.time = WallT(timestamp.time.0 + nanos(&diff));
        timestamp
    }

    pub fn update(&self, msg: &HybridTimestamp) -> Result<(), Error> {
        let mut locked = self.inner.lock().unwrap();
        let now = locked.clock.now();
        if now > *msg {
            return Ok(());
        }
        locked.clock.observe(msg).map_err(Error::from)
    }
}

impl Inner {
    pub fn new() -> Self {
        let clock = HybridClock::new_with_max_diff(Wall, Duration::from_millis(500));
        Self { clock }
    }
}

impl ClockSource for Wall {
    type Time = WallT;
    type Delta = Duration;

    fn now(&mut self) -> Self::Time {
        let duration = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Failed to get current time");
        WallT(nanos(&duration))
    }
}

impl std::ops::Sub for WallT {
    type Output = Duration;

    fn sub(self, other: Self) -> Duration {
        let dt = self.0 - other.0;
        Duration::from_nanos(dt)
    }
}

impl From<&proto::Timestamp> for HybridTimestamp {
    fn from(message: &proto::Timestamp) -> Self {
        Timestamp {
            time: WallT(message.wall_time),
            count: message.logical_time,
            epoch: message.epoch,
        }
    }
}

impl From<proto::Timestamp> for HybridTimestamp {
    fn from(message: proto::Timestamp) -> Self {
        Self::from(&message)
    }
}

impl From<HybridTimestamp> for proto::Timestamp {
    fn from(timestamp: HybridTimestamp) -> Self {
        let mut message = proto::Timestamp::new();
        message.set_wall_time(timestamp.time.0);
        message.set_logical_time(timestamp.count);
        message.set_epoch(timestamp.epoch);
        message
    }
}

fn nanos(duration: &Duration) -> u64 {
    duration.as_secs() * 1_000_000_000 + duration.subsec_nanos() as u64
}
