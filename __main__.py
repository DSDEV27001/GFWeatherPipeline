import pandas as pd
import logging
import functools
from pandas_schema import Column, Schema
from pandas_schema.validation import (
    CanConvertValidation,
    MatchesPatternValidation,
    InRangeValidation,
    InListValidation,
)

FILENAMES = ["Data\weather.20160201", "Data\weather.20160301"]

logger = logging.getLogger(__name__)
fh = logging.FileHandler("error.log")
logger.addHandler(fh)


def log_error(logger):
    """
    Decorator function to log errors
    """

    def decorated(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Exception as e:
                if logger:
                    logger.exception(e)
                raise

        return wrapped

    return decorated


@log_error(logger)
def import_monthly_weather_csv(fpath: str) -> pd.DataFrame:
    """
    Imports a monthly weather .csv into a DataFrame
    Converts -99 to null
    Removes leading and trailing whitespace
    """
    frame_out = pd.read_csv(f"{fpath}.csv", na_values=-99).applymap(
        lambda x: x.strip() if isinstance(x, str) else x
    )
    return frame_out


def validate_weather_df(frame_in: pd.DataFrame) -> pd.DataFrame:
    return frame_in


def transform_weather_df(frame_in: pd.DataFrame) -> pd.DataFrame:
    """
    Merge date and time into one field use standard ISO format
    Transform SiteName into Proper Case
    Ensure reverse geocode data generated for all weather stations
    Use reverse geocode generated data to populate Region and Country with more accurate data
    Remove duplicates
    """
    return frame_in


def export_weather_to_parquet(frame_in: pd.DataFrame):
    frame_in.to_parquet()


def main():
    raw_weather_frame = pd.concat(
        import_monthly_weather_csv(filename) for filename in FILENAMES
    )
    export_weather_to_parquet(raw_weather_frame)

# main()


frame = import_monthly_weather_csv(f"Data\weather.20160201")
