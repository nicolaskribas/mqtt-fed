use crate::federator::{Context, Id};
use crate::message::CoreAnn;
use std::cmp::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) struct CoreAnnCollection {
    inner: Vec<CoreAnnEntry>,
    latest_seqn: u32,
    latest_time: Instant,
    redundancy: usize,
    core_ann_timeout: Duration,
}

#[derive(Debug)]
pub(crate) struct SeqNumberTooOld;

impl CoreAnnCollection {
    pub(crate) fn new(ctx: &Arc<Context>, core_ann: &CoreAnn) -> Self {
        Self {
            latest_seqn: core_ann.seq_number,
            inner: Vec::from([CoreAnnEntry::from(core_ann)]),
            redundancy: ctx.redundancy,
            core_ann_timeout: 3 * ctx.core_ann_interval,
            latest_time: Instant::now(),
        }
    }

    pub(crate) fn insert(&mut self, core_ann: &CoreAnn) -> Result<(), SeqNumberTooOld> {
        let new_entry = CoreAnnEntry::from(core_ann);

        let maybe_current = self.inner.iter_mut().find(|entry| entry.id == new_entry.id);

        if let Some(current) = maybe_current {
            if new_entry.seq <= current.seq {
                Err(SeqNumberTooOld)
            } else {
                *current = new_entry;
                self.inner.sort_unstable();
                Ok(())
            }
        } else {
            self.inner.push(new_entry);
            self.inner.sort_unstable();
            Ok(())
        }
    }

    pub(crate) fn latest(&self) -> Instant {
        self.latest_time
    }

    pub(crate) fn get_latest_seqn(&self) -> u32 {
        self.latest_seqn
    }

    pub(crate) fn is_parent(&self, id: Id) -> bool {
        self.get_parents_id().any(|parent_id| *parent_id == id)
    }

    pub(crate) fn get_parents(&mut self) -> impl Iterator<Item = &mut CoreAnnEntry> {
        let mut min_dist = None;

        self.inner
            .iter_mut()
            .filter(|entry| entry.time.elapsed() < self.core_ann_timeout)
            .take_while(move |entry| entry.dis == *min_dist.get_or_insert(entry.dis))
            .take(self.redundancy)
    }

    pub(crate) fn get_parents_id(&self) -> impl Iterator<Item = &Id> {
        let mut min_dist = None;

        self.inner
            .iter()
            .filter(|entry| entry.time.elapsed() < self.core_ann_timeout)
            .take_while(move |entry| entry.dis == *min_dist.get_or_insert(entry.dis))
            .take(self.redundancy)
            .map(|parent| &parent.id)
    }

    pub(crate) fn get_min_dist(&self) -> u32 {
        self.inner
            .iter()
            .filter(|entry| entry.time.elapsed() < self.core_ann_timeout)
            .next()
            .unwrap()
            .dis
    }
}

#[derive(PartialEq, Eq, PartialOrd)]
pub(crate) struct CoreAnnEntry {
    id: Id,
    dis: u32,
    seq: u32,
    time: Instant,
    ans: bool,
}

impl CoreAnnEntry {
    pub(crate) fn get_id(&self) -> Id {
        self.id
    }

    pub(crate) fn get_seq_number(&self) -> u32 {
        self.seq
    }

    pub(crate) fn was_answered(&self) -> bool {
        self.ans
    }

    pub(crate) fn set_answered(&mut self) {
        self.ans = true;
    }
}

impl From<&CoreAnn> for CoreAnnEntry {
    fn from(core_ann: &CoreAnn) -> Self {
        Self {
            id: core_ann.sender_id,
            dis: core_ann.dist_to_core + 1,
            seq: core_ann.seq_number,
            time: Instant::now(),
            ans: false,
        }
    }
}

impl Ord for CoreAnnEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let dis_ord = self.dis.cmp(&other.dis);
        if dis_ord == Ordering::Equal {
            self.id.cmp(&other.id)
        } else {
            dis_ord
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parent_ord_ne_dis() {
        let less_parent = CoreAnnEntry {
            id: 2,
            dis: 1,
            seq: 999,
            time: Instant::now(),
            ans: false,
        };

        let greater_parent = CoreAnnEntry {
            id: 1,
            dis: 2,
            ..less_parent
        };

        assert_eq!(less_parent.cmp(&greater_parent), Ordering::Less);
    }

    #[test]
    fn parent_ord_eq_dis() {
        let less_parent = CoreAnnEntry {
            id: 1,
            dis: 2,
            seq: 101,
            time: Instant::now(),
            ans: false,
        };

        let greater_parent = CoreAnnEntry {
            id: 2,
            ..less_parent
        };

        assert_eq!(less_parent.cmp(&greater_parent), Ordering::Less);
    }
}
