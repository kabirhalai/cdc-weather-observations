from urllib.request import urlretrieve
from pathlib import Path
import pandas as pd
from prefect import task

import duckdb

cwd = Path.cwd()
raw_dir_prefix = f"raw/CDC"
processed_dir_prefix = f"processed"

keys = ["year", "month", "IIiii"]
s1 = [
    "G1",
    "Po",
    "G1.1",
    "P",
    "G1.2",
    "sn",
    "T",
    "st",
    "G1.3",
    "sn.1",
    "Tx",
    "sn.2",
    "Tn",
    "G1.4",
    "e",
    "G1.5",
    "R1",
    "Rd",
    "nr",
    "G1.6",
    "S1",
    "ps",
    "G1.7",
    "mp",
    "mT",
    "mTx",
    "mTn",
    "G1.8",
    "me",
    "mR",
    "mS",
]
s2 = [
    "G2",
    "Yb",
    "Yc",
    "G2.1",
    "Po.1",
    "G2.2",
    "P.1",
    "G2.3",
    "sn.3",
    "T.1",
    "st.1",
    "G2.4",
    "sn.4",
    "Tx.1",
    "sn.5",
    "Tn.1",
    "G2.5",
    "e.1",
    "G2.6",
    "R1.1",
    "nr.1",
    "G2.7",
    "S1.1",
    "G2.8",
    "YP",
    "YT",
    "YTx",
    "G2.9",
    "Ye",
    "YR",
    "YS",
]
s3 = [
    "G3",
    "T25",
    "T30",
    "G3.1",
    "T35",
    "T40",
    "G3.2",
    "Tn0",
    "Tx0",
    "G3.3",
    "R01",
    "R05",
    "G3.4",
    "R10",
    "R50",
    "G3.5",
    "R100",
    "R150",
    "G3.6",
    "s00",
    "s01",
    "G3.7",
    "s10",
    "s50",
    "G3.8",
    "f10",
    "f20",
    "f30",
    "G3.9",
    "V1",
    "V2",
    "V3",
]
s4 = [
    "G4",
    "sn.6",
    "Txd",
    "yx",
    "G4.1",
    "sn.7",
    "Tnd",
    "yn",
    "G4.2",
    "sn.8",
    "Tax",
    "yax",
    "G4.3",
    "sn.9",
    "Tan",
    "yan",
    "G4.4",
    "Rx",
    "yr",
    "G4.5",
    "iw",
    "fx",
    "yfx",
    "G4.6",
    "Dts",
    "Dgr",
    "G4.7",
    "iy",
    "Gx",
    "Gn",
]

WEATHER_OBSERVATIONS_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_global/CLIMAT/monthly/raw/"
FILE_PREFIX = "CLIMAT_RAW_"

STATIONS_URL = (
    "https://opendata.dwd.de/climate_environment/CDC/help/stations_list_CLIMAT_data.txt"
)


#@task
def extract(year: int, month: int = None) -> str:
    """
    Extracts the CLIMAT data for the given year and month.

    Args:
        year (int): The year to extract data for.
        month (int): The month to extract data for.

    Returns:
        str: The extracted data as a string.
    """
    try:
        if month is None:
            month = range(1, 13)  # If month is None, extract data for all months
        _ = list(
            map(
                lambda x: extract_for_year_and_month(year, x),
                month if isinstance(month, range) else [month],
            )
        )
        return "Data extracted successfully."
    except Exception as e:
        print(f"Error extracting data for {year}-{month:02d}: {e}")
        return ""


def extract_for_year_and_month(year: int, month: int) -> str:
    """
    Extracts the CLIMAT data for the given year and month.

    Args:
        year (int): The year to extract data for.
        month (int): The month to extract data for.

    Returns:
        str: The extracted data as a string.
    """
    print(f"Extracting data for {year}-{month:02d}...")
    # Format the month to be two digits
    month_str = f"{month:02d}"
    # Construct the file name
    file_name = f"{FILE_PREFIX}{year}{month_str}.txt"
    # Construct the full URL
    url = f"{WEATHER_OBSERVATIONS_URL}{file_name}"

    Path.mkdir(cwd / raw_dir_prefix / f"{year}", parents=True, exist_ok=True)

    try:
        urlretrieve(url, f"{cwd}/{raw_dir_prefix}/{year}/{file_name}")
        print(f"Data for {year}-{month_str} extracted successfully.")
    except Exception as e:
        print(f"Error extracting data for {year}-{month_str}: {e}")

    return ""


#@task
def split_into_section_files(file: Path) -> None:
    try:
        df = pd.read_csv(file, sep=";")

        parquet_files = {
            "section_1": df[keys + s1],
            "section_2": df[keys + s2],
            "section_3": df[keys + s3],
            "section_4": df[keys + s4],
        }

        parquet_path = Path(cwd / processed_dir_prefix)
        if not parquet_path.exists():
            parquet_path.mkdir(parents=True, exist_ok=True)

        for p_file in parquet_files:
            parquet_files[p_file].to_csv(
                cwd / processed_dir_prefix / f"{file.stem}_{p_file}.csv", index=False
            )
            print(f"Processed {file} into {p_file} csv file.")
    except Exception as e:
        print(f"Error processing {file} into section files: {e}")
        raise e


def cleaning_col_values(section, df_list):
    combined_df = pd.concat(df_list, ignore_index=True)
    col_dict = {
        "section_1": [
            "Po",
            "P",
            "sn",
            "T",
            "st",
            "Tx",
            "Tn",
            "e",
            "R1",
            "Rd",
            "nr",
            "S1",
            "ps",
            "mp",
            "mT",
            "mTx",
            "mTn",
            "me",
            "mR",
            "mS",
        ],
        "section_2": [
            "Po.1",
            "P.1",
            "sn.3",
            "T.1",
            "st.1",
            "sn.4",
            "Tx.1",
            "sn.5",
            "Tn.1",
            "e.1",
            "R1.1",
            "nr.1",
            "S1.1",
            "Yb",
            "Yc",
            "YTx",
            "YR",
        ],
        "section_3": [
            "T25",
            "T30",
            "T35",
            "T40",
            "Tn0",
            "Tx0",
            "R01",
            "R05",
            "R10",
            "R50",
            "R100",
            "R150",
            "s00",
            "s01",
            "s10",
            "s50",
            "f10",
            "f20",
            "f30",
            "V1",
            "V2",
            "V3",
        ],
        "section_4": [
            "Txd",
            "Tnd",
            "Tax",
            "Tan",
            "Rx",
            "iw",
            "fx",
            "Dts",
            "Dgr",
            "Gx",
            "Gn",
            "yn",
            "yr",
            "yan",
            "yfx",
        ],
    }

    to_numeric_cols = col_dict.get(section, [])
    for col in to_numeric_cols:
        if col in combined_df.columns:
            combined_df[col] = pd.to_numeric(combined_df[col], errors="coerce")
    return combined_df


#@task
def process_section_files_into_parquet() -> None:
    try:
        p = Path(cwd / processed_dir_prefix)
        sections = ["section_1", "section_2", "section_3", "section_4"]
        for section in sections:
            section_files = list(p.glob(f"*_{section}.csv"))
            if not section_files:
                print(f"No files found for {section}. Skipping.")
                continue

            df_list = list(map(lambda file: pd.read_csv(file), section_files))

            combined_df = cleaning_col_values(section, df_list)

            combined_df.to_parquet(
                cwd / processed_dir_prefix / f"{section}.parquet",
                index=False,
                compression="snappy",
            )
            print(f"Processed {len(section_files)} files into {section}.parquet.")

            _ = list(map(lambda file: file.unlink(), section_files))

    except Exception as e:
        print(f"Error processing {section} files into parquet: {e}")
        raise e


#@task
def loading_parquet_into_raw_tables():
    _ = run_in_duckdb("CREATE SCHEMA IF NOT EXISTS raw;")

    for file in Path(cwd / processed_dir_prefix).glob("*.parquet"):
        print(f"Loading {file} into duckdb...")
        try:
            _ = run_in_duckdb(
                f"CREATE VIEW IF NOT EXISTS {file.stem} AS SELECT * FROM read_parquet('{file}');",
                'raw'
            )
        except Exception as e:
            print(f"Error loading {file} into duckdb: {e}")
            continue
    return


#@task
def load_stations_data():
    # The file uses Western European encoding
    stations_df = pd.read_csv(STATIONS_URL, sep=";", encoding="latin-1")

    conn=duckdb.connect(Path.cwd() / "data" / "warehouse.duckdb")
    conn.register("stations_df", stations_df)
    conn.execute("USE raw; CREATE OR REPLACE TABLE stations AS SELECT * FROM stations_df;")

    return


#@task
def run_in_duckdb(query: str, schema: str=None):
    try:
        con = duckdb.connect(Path.cwd() / "data" / "warehouse.duckdb")
        query = f"USE {schema}; {query}" if schema else query
        result = con.sql(query)
        if result is not None:
            return result.fetchdf()
        print(f"Query executed successfully: {query}")
        return result
    except Exception as e:
        print(f"Error executing query: {query}, error: {e}")
        raise e
