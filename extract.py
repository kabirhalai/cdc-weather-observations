from urllib.request import urlretrieve
from pathlib import Path

cwd = Path.cwd()
dir_prefix = f"raw/CDC"

MAIN_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_global/CLIMAT/monthly/raw/"
FILE_PREFIX = "CLIMAT_RAW_"

def extract(year: int, month: int=None) -> str:
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
        _ = list(map(lambda x: extract_for_year_and_month(year, x), month if isinstance(month, range) else [month]))
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
    url = f"{MAIN_URL}{file_name}"

    Path.mkdir(cwd / dir_prefix / f"{year}", parents=True, exist_ok=True)

    try:
        urlretrieve(url, f"{cwd}/{dir_prefix}/{year}/{file_name}")
        print(f"Data for {year}-{month_str} extracted successfully.")
    except Exception as e:
        print(f"Error extracting data for {year}-{month_str}: {e}")

    return ""
    
if __name__ == "__main__":
    year = 2022
    data = extract(year)
    # process into parquet
    