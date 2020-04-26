import pandas as pd
import numpy as np
import logging
import functools
import datetime
from pandas_schema import Column, Schema, validation as val

FILENAMES = ["weather.20160201", "weather.20160301"]

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
    frame_out = pd.read_csv(
        f"Data/{fpath}.csv", na_values=-99, float_precision="round_trip"
    ).applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return frame_out


def validate_weather_data(frame_in: pd.DataFrame):
    # TODO Min only range and length
    weather_file_schema = Schema(
        [
            Column(
                "ForecastSiteCode",
                [
                    val.InRangeValidation(1000, 100000),
                    val.IsDtypeValidation(np.dtype("int64")),
                ],
            ),
            Column(
                "ObservationTime",
                [
                    val.InRangeValidation(0, 24),
                    val.IsDtypeValidation(np.dtype("int64")),
                ],
            ),
            Column("ObservationDate", [val.DateFormatValidation("%Y-%m-%dT%H:%M:%S")]),
            Column(
                "WindDirection",
                [
                    val.InRangeValidation(0, 17),
                    val.IsDtypeValidation(np.dtype("int64")),
                ],
            ),  # 16 points of the compass (N is 0 and 16 since it is 0 and 360 degrees)
            Column(
                "WindSpeed",
                [val.InRangeValidation(0, 255), val.CanConvertValidation(int)],
                allow_empty=True,
            ),
            Column(
                "WindGust",
                [val.InRangeValidation(0, 255), val.CanConvertValidation(int)],
                allow_empty=True,
            ),
            Column("Visibility", [val.CanConvertValidation(int)], allow_empty=True),
            Column(
                "ScreenTemperature",
                [
                    val.InRangeValidation(-50, +50),
                    val.IsDtypeValidation(np.dtype(float)),
                ],
                allow_empty=True,
            ),
            Column(
                "Pressure",
                [val.InRangeValidation(870, 1085), val.CanConvertValidation(int)],
                allow_empty=True,
            ),
            Column(
                "SignificantWeatherCode",
                [val.InRangeValidation(0, 31)],
                allow_empty=True,
            ),
            Column("SiteName", [val.CanConvertValidation(str)],),
            Column(
                "Latitude",
                [
                    val.InRangeValidation(-90, 90),
                    val.IsDtypeValidation(np.dtype(float)),
                ],
            ),  # Signed latitude range
            Column(
                "Longitude",
                [
                    val.InRangeValidation(-180, 80),
                    val.IsDtypeValidation(np.dtype(float)),
                ],
            ),  # Signed longitude range
            Column("Region", [val.CanConvertValidation(str)], allow_empty=True),
            Column("Country", [val.CanConvertValidation(str)], allow_empty=True),
        ]
    )

    errors = weather_file_schema.validate(frame_in)
    if len(errors) > 0:
        for error in errors:
            logger.exception(error)
        # TODO: Create exception class to handle error
        raise Exception(
            "Error: Weather files data validation failed. Please refer to the log file for detailed information."
        )


def transform_weather_df(frame_in: pd.DataFrame) -> pd.DataFrame:
    """
    Merge date and time into one field use standard ISO format
    Transform SiteName into Proper Case
    Ensure reverse geocode data generated for all weather stations
    Use reverse geocode generated data to populate Region and Country with more accurate data
    Remove duplicates
    """

    frame_out = (
        frame_in.astype({"ObservationTime": np.dtype(str)})
        .assign(
            ObservationDateTime=lambda df: df.ObservationDate.str.slice(0, 11)
            + df.ObservationTime.str.zfill(2)
            + df.ObservationDate.str.slice(13,),
            SiteName=lambda df: np.where(
                df.ForecastSiteCode < 10000,
                df.SiteName.str[:-7].str.title(),
                df.SiteName.str[:-8].str.title(),
            ),
        )
        .drop(columns=["ObservationTime", "ObservationDate"])
    )

    return frame_out


def export_weather_to_parquet(frame_in: pd.DataFrame):
    frame_in.to_parquet("Data/weather.parquet", index=False)


def main():
    raw_weather_frame = pd.concat(
        import_monthly_weather_csv(filename) for filename in FILENAMES
    )

    validate_weather_data(raw_weather_frame)

    weather_frame_out = transform_weather_df(raw_weather_frame)

    export_weather_to_parquet(weather_frame_out)


main()


# frame = import_monthly_weather_csv("weather.20160201")
#
# errors = weather_file_schema.validate(frame)
#
# for error in errors:
#     print(error)
