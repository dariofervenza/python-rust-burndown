use pyo3::prelude::*;
use pyo3::types::{ PyList };
use chrono::{ DateTime, Utc, Duration };
use std::str::FromStr;

#[derive(Debug)]
pub enum Resolution {
    Minutes,
    Hours,
    Days,
    Weeks,
}


impl FromStr for Resolution {
    type Err = String;

    fn from_str(input: &str) -> Result::<Self, Self::Err> {
        match input.to_lowercase().as_str() {
            "m" | "min" | "minute" | "minutes" => Ok(Resolution::Minutes),
            "h" | "hour" | "hours" => Ok(Resolution::Hours),
            "d" | "day" | "days" => Ok(Resolution::Days),
            "w" | "week" | "weeks" => Ok(Resolution::Weeks),
            _ => Err(format!("Invalid resolution: {}", input)),
        }
    }
}


impl Resolution {
    fn to_duration(&self, value: i64) -> Duration {
        match self {
            Resolution::Minutes => Duration::minutes(value),
            Resolution::Hours => Duration::hours(value),
            Resolution::Days => Duration::days(value),
            Resolution::Weeks => Duration::weeks(value),
        }
    }
}



fn iter_burndown_dates(resolution_type: Resolution, resolution_val: i64, rust_start: &Vec::<DateTime::<Utc>>, rust_end: &Vec::<DateTime::<Utc>>) -> (Vec::<DateTime::<Utc>>, Vec::<i64>) {
    let min_val = rust_start.iter().min().cloned().unwrap();
    let max_val = rust_end.iter().max().cloned().unwrap();

    let mut current_val = min_val;
    let step = resolution_type.to_duration(resolution_val);
    let mut dates: Vec::<DateTime::<Utc>> = Vec::new();
    let mut hits_vec: Vec::<i64> = Vec::new();
    let mut accumulated: i64 = 0;
    while current_val <= max_val {
        let start = current_val;
        let end = start + step;

        hits_vec.push(accumulated);
        dates.push(start);

        let start_points: Vec::<&DateTime::<Utc>> = rust_start.iter().filter(|dt| **dt >= start && **dt < end).collect();  // filter gives &&DateTime so **
        let end_points: Vec::<&DateTime::<Utc>> = rust_end.iter().filter(|dt| **dt >= start && **dt < end).collect();

        accumulated += start_points.len() as i64 - end_points.len() as i64;

        hits_vec.push(accumulated);
        dates.push(start);
        current_val = end;
    }
    (dates, hits_vec)
}


#[pyfunction]
pub fn compute_burndown(resolution_type: &str, resolution_val: i64, start_dates: &PyList, end_dates: &PyList) -> PyResult::<(Vec::<String>, Vec::<i64>)> {
    fn timestamp64_to_naive(nanosecs: i64) -> DateTime::<Utc> {
        let secs = nanosecs / 1_000_000_000;
        let nsecs = (nanosecs % 1_000_000_000) as u32;
        DateTime::<Utc>::from_timestamp(secs, nsecs).unwrap()
    }
    let resolution = resolution_type.parse::<Resolution>()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;
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
    
    let (dates, hits_vec): (Vec::<DateTime::<Utc>>, Vec::<i64>) = iter_burndown_dates(resolution, resolution_val, &rust_start, &rust_end);
    // for (date, hits) in dates.iter().zip(hits_vec.iter()) {
    //     if *hits != 0 {
    //         println!("Date {:?} has {} hits", date, hits);
    //     }
    // }
    let py_dates: Vec::<String> = dates.iter().map(|dt| dt.to_rfc3339()).collect();
    Ok((py_dates, hits_vec))
}
