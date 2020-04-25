import pandas as pd
import numpy as np
import logging
import functools
from pandas_schema import Column, Schema
from pandas_schema.validation import (
    MatchesPatternValidation,
    InRangeValidation,
    IsDtypeValidation,
)

FILENAMES = ["Data\weather.20160201", "Data\weather.20160301"]

logger = logging.getLogger(__name__)
fh = logging.FileHandler("error.log")
logger.addHandler(fh)

weather_file_schema = Schema(
    [
        Column("ForecastSiteCode", [IsDtypeValidation(np.dtype(int))]),
        Column(
            "ObservationTime",
            [InRangeValidation(0, 23), IsDtypeValidation(np.dtype(int))],
        ),
        Column("ObservationDate"),
        Column(
            "WindDirection",
            [InRangeValidation(0, 16), IsDtypeValidation(np.dtype(int))],
        ),  # 16 points of the compass (N is 0 and 16 since it is 0 and 360 degrees)
        Column("WindSpeed", [IsDtypeValidation(pd.Int64Dtype())], allow_empty=True),
        Column("WindGust", [IsDtypeValidation(pd.Int64Dtype())], allow_empty=True),
        Column("Visibility", [IsDtypeValidation(pd.Int64Dtype())], allow_empty=True),
        Column(
            "ScreenTemperature",
            [InRangeValidation(-50, +50), IsDtypeValidation(np.dtype(float))],
            allow_empty=True,
        ),
        Column("Pressure", [IsDtypeValidation(pd.Int64Dtype())], allow_empty=True),
        Column("SignificantWeatherCode", [InRangeValidation(0, 30)], allow_empty=True),
        Column("SiteName", [IsDtypeValidation(np.dtype(str))],),
        Column(
            "Latitude", [InRangeValidation(-90, 90), IsDtypeValidation(np.dtype(float))]
        ),  # Signed latitude range
        Column(
            "Longitude",
            [InRangeValidation(-180, 80), IsDtypeValidation(np.dtype(float))],
        ),  # Signed longtitude range
        Column("Region", [IsDtypeValidation(np.dtype(str))], allow_empty=True),
        Column(
            "Country",
            [
                MatchesPatternValidation(r"\d{4}[A-Z]{4}"),
                IsDtypeValidation(np.dtype(str)),
            ],
            allow_empty=True,
        ),
    ]
)


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
    frame_out = pd.read_csv(
        f"{fpath}.csv", na_values=-99, float_precision="round_trip"
    ).applymap(lambda x: x.strip() if isinstance(x, str) else x)
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
