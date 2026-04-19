from prefect import flow
from extract import *


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
    etl_flow(2020, 2022)
