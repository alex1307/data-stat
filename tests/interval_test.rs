#[cfg(test)]
mod intervals_test {

    use data_statistics::{
        configure_log4rs,
        model::Intervals::{Interval, SortedIntervals},
    };
    use log::info;

    #[test]
    fn test_add_interval() {
        configure_log4rs("resources/log4rs.yml");
        let mut intervals = SortedIntervals::<i32> {
            column: "test".to_string(),
            intervals: vec![],
        };
        let interval = Interval {
            column: "test".to_string(),
            start: 1,
            end: 2,
            category: "test".to_string(),
            index: 0,
        };
        let ok = intervals.add_interval(interval);
        assert!(ok.is_ok());
        assert_eq!(intervals.intervals.len(), 1);
        let interval = Interval {
            column: "test".to_string(),
            start: 3,
            end: 4,
            category: "test".to_string(),
            index: 0,
        };
        let ok = intervals.add_interval(interval);
        assert!(ok.is_ok());
        assert_eq!(intervals.intervals.len(), 2);
        let interval = Interval {
            column: "test".to_string(),
            start: 2,
            end: 3,
            category: "test".to_string(),
            index: 0,
        };
        let ok = intervals.add_interval(interval);
        assert!(ok.is_ok());
        assert_eq!(intervals.intervals.len(), 3);
        let interval = Interval {
            column: "test".to_string(),
            start: 1,
            end: 2,
            category: "test".to_string(),
            index: 0,
        };
        let result = intervals.add_interval(interval);
        assert!(result.is_err());
        for i in intervals.intervals.iter() {
            info!("{:?}", i);
        }
        let interval = Interval {
            column: "test".to_string(),
            start: 10,
            end: 15,
            category: "test".to_string(),
            index: 0,
        };
        let ok = intervals.add_interval(interval);
        assert!(ok.is_ok());
        assert_eq!(intervals.intervals.len(), 4);
        let interval = Interval {
            column: "test".to_string(),
            start: 5,
            end: 10,
            category: "test".to_string(),
            index: 0,
        };
        let ok = intervals.add_interval(interval);
        assert!(ok.is_ok());
        assert_eq!(intervals.intervals.len(), 5);
        let mut counter = 0;
        for i in intervals.intervals.iter() {
            info!("{:?}", i);
            assert!(i.start <= i.end);
            assert_eq!(i.column, "test");
            assert_eq!(i.index, counter);
            counter += 1;
        }
        info!("{:?}", intervals.values());
    }

    #[test]
    fn test_add_overlapping_intervals() {
        configure_log4rs("resources/log4rs.yml");
        let mut intervals = SortedIntervals::<i32> {
            column: "test".to_string(),
            intervals: vec![],
        };
        let interval = Interval {
            column: "test".to_string(),
            start: 1,
            end: 2,
            category: "test".to_string(),
            index: 0,
        };
        let ok = intervals.add_interval(interval);
        assert!(ok.is_ok());
        assert_eq!(intervals.intervals.len(), 1);
        let interval = Interval {
            column: "test".to_string(),
            start: 2,
            end: 3,
            category: "test".to_string(),
            index: 0,
        };
        let ok = intervals.add_interval(interval);
        assert!(ok.is_ok());
        assert_eq!(intervals.intervals.len(), 2);
        let interval = Interval {
            column: "test".to_string(),
            start: 4,
            end: 5,
            category: "test".to_string(),
            index: 0,
        };
        let ok = intervals.add_interval(interval);
        assert!(ok.is_ok());
        assert_eq!(intervals.intervals.len(), 3);
        let interval = Interval {
            column: "test".to_string(),
            start: 1,
            end: 2,
            category: "test".to_string(),
            index: 0,
        };
        let result = intervals.add_interval(interval);
        assert!(result.is_err());
        for i in intervals.intervals.iter() {
            info!("{:?}", i);
        }
        let interval = Interval {
            column: "test".to_string(),
            start: 3,
            end: 4,
            category: "test".to_string(),
            index: 0,
        };
        let ok = intervals.add_interval(interval);
        assert!(ok.is_ok());
        assert_eq!(intervals.intervals.len(), 4);
        let interval = Interval {
            column: "test".to_string(),
            start: 5,
            end: 10,
            category: "test".to_string(),
            index: 0,
        };
        let ok = intervals.add_interval(interval);
        assert!(ok.is_ok());
        assert_eq!(intervals.intervals.len(), 5);
        let mut counter = 0;
        for i in intervals.intervals.iter() {
            info!("{:?}", i);
            assert!(i.start <= i.end);
            assert_eq!(i.column, "test");
            assert_eq!(i.index, counter);
            counter += 1;
        }
        info!("{:?}", intervals.values());
    }
}
