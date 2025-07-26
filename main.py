import random
import pandas as pd
from datetime import datetime, timedelta
from time import time

from concurrent.futures import ProcessPoolExecutor, as_completed

import burndown_rs


def generate_points(no_of_issues: int, low1=35, high1=55, w1=300, d1=1500, h1=150000, m1=15000, s1=300000) -> tuple:
    n_issue = []
    start_dates = []
    end_dates = []
    for num in range(no_of_issues):
        n_issue.append(num)
        start_low = random.randint(0, low1)
        start_high = random.randint(0, high1)
        start_date = datetime.now() + timedelta(
            weeks=random.randint(-start_low, start_high),
            days=random.randint(-start_low, start_high),
            hours=random.randint(-start_low, start_high),
            minutes=random.randint(-start_low, start_high),
            seconds=random.randint(-start_low, start_high)
            ) 
        start_dates.append(start_date)
        end_date = start_date + timedelta(
            weeks=random.randint(0, random.randint(0, w1)),
            days=random.randint(0, random.randint(0, d1)),
            hours=random.randint(0, random.randint(0, h1)),
            minutes=random.randint(0, random.randint(0, m1)),
            seconds=random.randint(0, random.randint(0, s1))) 
        end_dates.append(end_date)
    return tuple(n_issue), tuple(start_dates), tuple(end_dates)


def tuple_to_df(n_issue, start_dates, end_dates):
    return pd.DataFrame(data={
        "num_issue": n_issue,
        "start_date": start_dates,
        "end_date": end_dates,
    })


def generate_df(no_of_issues: int) -> pd.DataFrame:
    return tuple_to_df(*generate_points(no_of_issues))


def generate_df_pool(n_jobs: int = 24, no_of_issues: int = 300_000) -> list[pd.DataFrame]:
    return [generate_df(no_of_issues) for _ in range(n_jobs)]


def generate_df_pool_process(n_jobs: int = 24, no_of_issues: int = 1_000_000, max_workers: int = 23, *args) -> list[pd.DataFrame]:
    df_pool = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(generate_points, no_of_issues, *args) for _ in range(n_jobs)]
        for idx, future in enumerate(as_completed(futures)):
            df = tuple_to_df(*future.result())
            df_pool.append(df)
            if idx % 5 == 0:
                print(f"Pool generation {idx} finished")
    return df_pool


def process_burndown_rs(resolution_type: str, resolution_val: int, df: pd.DataFrame, thread_no: int):
    dates, hits = burndown_rs.compute_burndown(
        resolution_type, resolution_val, df.start_date.astype("int64").to_list(), df.end_date.astype("int64").to_list())
    
    return thread_no, dates, hits


def main(n_jobs: int, no_of_issues: int, resolution_type: str, resolution_val: int, max_workers: int = 20):
    print("Generating pool")
    df_pool = generate_df_pool_process(n_jobs, no_of_issues=no_of_issues, max_workers=max_workers * 5)
    print("Pool generated")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                process_burndown_rs, resolution_type, resolution_val, df, thread)
            for thread, df in enumerate(df_pool)]
        for future in as_completed(futures):
            res, dates, hits = future.result()
            print(f"Thread number {res} as completed its task")
    for date, hit in zip(dates, hits):
        print(f"{date}: {hit}")


if __name__ == "__main__":
    main(
        n_jobs=3,
        no_of_issues=5_000,
        resolution_type="days",
        resolution_val=1,
        max_workers=18
    )
