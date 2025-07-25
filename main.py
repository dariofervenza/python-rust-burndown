import random
import pandas as pd
from datetime import datetime, timedelta
from time import time

from concurrent.futures import ProcessPoolExecutor, as_completed

import burndown_rs


def generate_df(no_of_issues: int) -> pd.DataFrame:
    n_issue = []
    start_dates = []
    end_dates = []
    for num in range(no_of_issues):
        n_issue.append(num)
        start_date = datetime.now() + timedelta(weeks=random.randint(-50, 50), days=random.randint(-50, 50), hours=random.randint(-50, 50), minutes=random.randint(-50, 50), seconds=random.randint(-50, 50)) 
        start_dates.append(start_date)
        end_date = start_date + timedelta(weeks=random.randint(0, 50), days=random.randint(0, 50), hours=random.randint(10, 50), minutes=random.randint(0, 50), seconds=random.randint(0, 50)) 
        end_dates.append(end_date)
    return pd.DataFrame(data={
        "num_issue": n_issue,
        "start_date": start_dates,
        "end_date": end_dates,
    })


def generate_df_pool(n_jobs: int = 24, no_of_issues: int = 300_000) -> list[pd.DataFrame]:
    return [generate_df(no_of_issues) for _ in range(n_jobs)]


def process_burndown_rs(resolution: int, df: pd.DataFrame, thread_no: int):
    dates, hits = burndown_rs.process_timestamp(resolution, df.start_date.astype("int64").to_list(), df.end_date.astype("int64").to_list())
    return thread_no


def main(n_jobs: int, no_of_issues: int, resolution: int, max_workers: int = 24):
    df_pool = generate_df_pool(n_jobs, no_of_issues=no_of_issues)
    print("Pool generated")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_burndown_rs, resolution, df, thread) for thread, df in enumerate(df_pool)]
        for future in as_completed(futures):
            res = future.result()
            print(f"Thread number {res} as completed its task")


if __name__ == "__main__":
    main(
        n_jobs=500,
        no_of_issues=5_000,
        resolution=1
    )
