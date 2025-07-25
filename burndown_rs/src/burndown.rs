use pyo3::prelude::*;
use pyo3::types::{ PyList, PyTuple };
use pyo3::Python;
use chrono::{ DateTime, Utc, Duration };

// debug struct
#[derive(Debug)]
struct BurnResult {
    hits: i64,
    date: DateTime::<Utc>,
}



fn iter_burndown_dates(resolution_hours: i64, rust_start: &Vec::<DateTime::<Utc>>, rust_end: &Vec::<DateTime::<Utc>>) -> (Vec::<DateTime::<Utc>>, Vec::<i64>) {
    let min_val = rust_start.iter().min().cloned().unwrap();
    let max_val = rust_end.iter().max().cloned().unwrap();

    let mut current_val = min_val;
    let step = Duration::hours(resolution_hours);
    let mut dates: Vec::<DateTime::<Utc>> = Vec::new();
    let mut hits_vec: Vec::<i64> = Vec::new();
    while current_val <= max_val {
        let start = current_val;
        let end = start + step;

        let start_points: Vec::<&DateTime::<Utc>> = rust_start.iter().filter(|dt| **dt >= start && **dt < end).collect();  // filter gives &&DateTime so **
        let end_points: Vec::<&DateTime::<Utc>> = rust_end.iter().filter(|dt| **dt >= start && **dt < end).collect();

        hits_vec.push(start_points.len() as i64 - end_points.len() as i64);
        dates.push(start);
        current_val = end;
    }
    (dates, hits_vec)
}


#[pyfunction]
pub fn process_timestamp(resolution_hours: i64, start_dates: &PyList, end_dates: &PyList) -> PyResult::<(Vec::<String>, Vec::<i64>)> {
    fn timestamp64_to_naive(nanosecs: i64) -> DateTime::<Utc> {
        let secs = nanosecs / 1_000_000_000;
        let nsecs = (nanosecs % 1_000_000_000) as u32;
        DateTime::<Utc>::from_timestamp(secs, nsecs).unwrap()
    }
    let rust_start: Vec::<DateTime::<Utc>> = start_dates.iter()
        .map(|dt| dt.extract::<i64>().unwrap())
        .map(timestamp64_to_naive)
        .collect();
    let rust_end: Vec::<DateTime::<Utc>> = end_dates.iter()
        .map(|dt| dt.extract::<i64>().unwrap())
        .map(timestamp64_to_naive)
        .collect();

    // for (i, (t1, t2)) in rust_start.iter().zip(rust_end.iter()).enumerate() {
    //     println!("Issue no: {} has start: {:?} and end: {:?}", i, t1, t2);
    //     if i == 1 {
    //         break;
    //     }
    // }
    let (dates, hits_vec): (Vec::<DateTime::<Utc>>, Vec::<i64>) = iter_burndown_dates(resolution_hours, &rust_start, &rust_end);
    // for (date, hits) in dates.iter().zip(hits_vec.iter()) {
    //     if *hits != 0 {
    //         println!("Date {:?} has {} hits", date, hits);
    //     }
    // }
    let py_dates: Vec::<String> = dates.iter().map(|dt| dt.to_rfc3339()).collect();
    Ok((py_dates, hits_vec))
}