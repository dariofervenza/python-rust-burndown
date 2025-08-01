use pyo3::prelude::*;

pub mod burndown; 


#[pymodule]
fn burndown_rs(_py: Python, m: &PyModule) -> PyResult::<()> {
    m.add_function(wrap_pyfunction!(burndown::compute_burndown, m)?)?;
    Ok(())
}