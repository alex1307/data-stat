use std::{cmp::Ordering, collections::HashSet, fmt::Debug, hash::Hash};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Interval<T: Ord + Clone + Debug + Hash> {
    pub column: String,
    pub start: T,
    pub end: T,
    pub category: String,
    pub index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SortedIntervals<T: Ord + Clone + Debug + Hash> {
    pub column: String,
    pub intervals: Vec<Interval<T>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatInterval {
    pub column: String,
    pub orig_start: i32,
    pub orig_end: i32,
    pub rsd: f64,
    pub count: i32,
    pub start: i32,
    pub end: i32,
}

impl<T: Ord + Clone + Debug + Hash> SortedIntervals<T> {
    /// Adds a new interval and then sorts and validates intervals in the set.
    /// Returns an error if there is an overlap.
    pub fn add_interval(&mut self, interval: Interval<T>) -> Result<(), String> {
        if self.intervals.is_empty() {
            self.intervals.push(interval);
            return Ok(());
        }

        if interval.start > interval.end {
            return Err(format!("Invalid interval {:?}", interval));
        }

        let min = self
            .intervals
            .iter()
            .min_by(|a, b| a.start.cmp(&b.start))
            .unwrap()
            .start
            .clone();
        let max = self
            .intervals
            .iter()
            .max_by(|a, b| a.end.cmp(&b.end))
            .unwrap()
            .end
            .clone();
        if interval.end <= min {
            let mut cloned = interval.clone();
            cloned.index = 0;
            self.intervals.insert(0, cloned);
        } else if interval.start >= max {
            let mut cloned = interval.clone();
            cloned.index = self.intervals.len();
            self.intervals.push(cloned);
        } else {
            let left = self
                .intervals
                .iter()
                .filter(|i| i.end <= interval.start)
                .max_by(|a, b| a.end.cmp(&b.end));

            let right = self
                .intervals
                .iter()
                .filter(|i| i.start >= interval.end)
                .min_by(|a, b| a.start.cmp(&b.start));

            if right.is_none() {
                let mut cloned = left.unwrap().clone();
                cloned.index = self.intervals.len();
                self.intervals.push(cloned);
                return Ok(());
            }

            if left.is_none() || right.is_none() {
                return Err(format!(
                    "Overlap between for {:?}. left: {:?}, right: {:?} ",
                    interval, left, right
                ));
            } else {
                let left = left.unwrap();
                let right = right.unwrap();
                if interval.start >= left.end && interval.end <= right.start {
                    let mut cloned = interval.clone();
                    let index = right.index;
                    cloned.index = index;
                    self.intervals.insert(index, cloned);
                    for i in index + 1..self.intervals.len() {
                        self.intervals[i].index += 1;
                    }
                } else {
                    return Err(format!("Overlap between {:?} and {:?}", interval, right));
                }
            }
        }
        Ok(())
        // Add the new interval
    }

    pub fn sort_intervals<F>(&mut self, comparator: Option<F>)
    where
        F: Fn(&Interval<T>, &Interval<T>) -> Ordering,
    {
        match comparator {
            Some(cmp) => self.intervals.sort_by(cmp),
            None => self.intervals.sort_by(|a, b| a.start.cmp(&b.start)),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.intervals.is_empty()
    }

    pub fn len(&self) -> usize {
        self.intervals.len()
    }

    pub fn get_intervals(&self) -> Vec<Interval<T>> {
        self.intervals.clone()
    }

    pub fn min(&self) -> Option<&Interval<T>> {
        self.intervals.iter().min_by(|a, b| a.start.cmp(&b.start))
    }

    pub fn max(&self) -> Option<&Interval<T>> {
        self.intervals.iter().max_by(|a, b| a.end.cmp(&b.end))
    }

    pub fn get_interval(&self, index: usize) -> Option<&Interval<T>> {
        if index < self.intervals.len() {
            Some(&self.intervals[index])
        } else {
            None
        }
    }

    pub fn values(&self) -> Vec<T> {
        let unique = self
            .intervals
            .iter()
            .flat_map(|i| vec![i.start.clone(), i.end.clone()])
            .collect::<HashSet<T>>();

        let mut values = unique.into_iter().collect::<Vec<T>>();
        values.sort(); // Sort to maintain order
        values // Return the sorted values
    }
}

impl From<Vec<Interval<i32>>> for SortedIntervals<i32> {
    fn from(intervals: Vec<Interval<i32>>) -> Self {
        let column = intervals[0].column.clone();
        let mut sorted_intervals = SortedIntervals::<i32> {
            column,
            intervals: vec![],
        };
        for interval in intervals {
            sorted_intervals.add_interval(interval).unwrap();
        }
        sorted_intervals
    }
}
