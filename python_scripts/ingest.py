from extract import *
import os
from datetime import datetime


start_year = os.environ.get("START_YEAR")
end_year = os.environ.get("END_YEAR")

#@flow(log_prints=True)
def etl_flow(year_start, year_end):
    years = list(range(year_start, year_end + 1))

    _ = list(map(lambda year: extract(year), years))

    for year in years:
        p = Path(cwd / raw_dir_prefix / f"{year}")
        for file in p.glob("*.txt"):
            print(f"Processing {file} into section files...")
            try:
                split_into_section_files(file)
                print(f"Processed {file} into section files successfully.")
            except:
                print(f"Error processing {file} into section files. Skipping.")
                continue

    print(f"Processing section files into parquet...")
    try:
        process_section_files_into_parquet()
    except Exception as e:
        print(f"Error processing section files into parquet: {e}")

    loading_parquet_into_raw_tables()

    load_stations_data()

if __name__ == "__main__":
    if start_year in [None, ""] or end_year in [None, ""]:
        print("START_YEAR and END_YEAR environment variables must be set.")
    else:
        if start_year.isdigit() and end_year.isdigit():
            if start_year < 2003 or end_year > datetime.now().year:
                print("START_YEAR must be >= 2003 and END_YEAR must be <= current year.")
            elif start_year > end_year:
                print("START_YEAR must be less than or equal to END_YEAR.")
            else:
                etl_flow(int(start_year), int(end_year))
