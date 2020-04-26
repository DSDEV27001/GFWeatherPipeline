import pandas as pd
import numpy as np
import logging
import functools
import textwrap
from pandas_schema import Column, Schema, validation as val
from pydrill import client, exceptions

FILENAMES = ["weather.20160201", "weather.20160301"]

clogger = logging.getLogger(__name__)
fh = logging.FileHandler("error.log")
clogger.addHandler(fh)


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



def import_monthly_weather_csv(fpath: str) -> pd.DataFrame:
    """
    Imports a monthly weather .csv into a DataFrame
    Converts -99 to null
    Removes leading and trailing whitespace
    """
    try:
        frame_out = pd.read_csv(f"Data/{fpath}.csv", na_values=-99).applymap(
            lambda x: x.strip() if isinstance(x, str) else x
        )
    except Exception as e:
        if clogger:
            clogger.exception(e)
        raise
    # except IOError:
    # except FileNotFoundError:
    # except TypeError:
    # except NameError:

    return frame_out


def validate_weather_data(frame_in: pd.DataFrame):
    """
    Uses a schema to validate the input dataframe
    :param frame_in:
    """
    # TODO length of text fields and alphanumeric checks
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
            clogger.exception(error)
        raise DataValidationError(
            "Error: Data validation failed. Please refer to the log file for detailed information."
        )


class DataValidationError(Exception):
    """
    Exception raised when data validation produces errors.
    """


# TODO Add additional categorical data types e.g compass points, weather types and visibility codes
@log_error(clogger)
def transform_weather_df(frame_in: pd.DataFrame) -> pd.DataFrame:
    """
    Merge date and time into one field use standard ISO format
    Transform SiteName into Proper Case
    Ensure reverse geocode data generated for all weather stations
    Use reverse geocode generated data to populate Region and Country with more accurate data
    Remove duplicates
    """

    frame_out = (
        frame_in.assign(
            ObservationDateTime=lambda df: df.ObservationDate.str.slice(0, 11)
            + df.ObservationTime.apply(str).str.zfill(2)
            + df.ObservationDate.str.slice(13,),
            SiteName=lambda df: np.where(
                df.ForecastSiteCode < 10000,
                df.SiteName.str[:-7].str.title(),
                df.SiteName.str[:-8].str.title(),
            ),
        )
        .drop_duplicates()
        .astype(
            {
                "ObservationDate": np.dtype("M"),
                "ObservationDateTime": np.dtype("M"),
                "Pressure": pd.Int64Dtype(),
                "WindDirection": pd.Int64Dtype(),
                "WindSpeed": pd.Int64Dtype(),
                "SignificantWeatherCode": pd.Int64Dtype(),
                "WindGust": pd.Int64Dtype(),
                "Visibility": pd.Int64Dtype(),
            }
        )
        .sort_values(["ObservationDate", "ObservationTime", "Region", "SiteName"])
    )

    return frame_out

@log_error(clogger)
def export_weather_to_parquet(frame_in: pd.DataFrame):
    frame_in.to_parquet(
        "Data/weather.parquet", index=False, engine="pyarrow", row_group_size=10000
    )

@log_error(clogger)
def max_daily_average_temperature(frame_in: pd.DataFrame):
    sql = textwrap.dedent(
        """select
            *
        from
        (
            select
                ObservationDate,Region, SiteName,round(AVG(ScreenTemperature),2) as DailyAverageTemperature
            from
                dfs.`C:\\Users\\Michael\\PycharmProjects\\GFWeatherPipelineTask\\Data\\weather.parquet`
            group by
                ObservationDate,Region,SiteName
        )
    where
        DailyAverageTemperature =(select
                                    max(DailyAverageTemperature)
                                from
                                    (select
                                        ROUND(AVG(ScreenTemperature),2) as DailyAverageTemperature
                                    from
                                        dfs.`C:\\Users\\Michael\\PycharmProjects\\GFWeatherPipelineTask\\Data\\weather.parquet`
                                    group by
                                        ObservationDate,Region,SiteName))"""
    )

    drill_query_parquet(frame_in, sql)

@log_error(clogger)
def drill_query_parquet(frame_in: pd.DataFrame, sql):
    """

    Note: drill must be running on the specified host
    """
    drill = client.PyDrill(host="localhost", port=8047)
    max_daily_temp_results = drill.query(sql, timeout=10)

    if not drill.is_active():
        raise exceptions.ImproperlyConfigured("Please run Drill first")

    header = ["ObservationDate", "Region", "SiteName", "DailyAverageTemperature"]
    # print(max_daily_temp_results.data)

    row = list(max_daily_temp_results.rows[0].values())
    width = (
        max(
            max(len(column) for column in row),
            max(len(column_name) for column_name in header),
        )
        + 2
    )

    print(
        "\nData for weather station site with the hottest day (maximum daily average temperature): \n\n"
        + "".join(column_name.ljust(width) for column_name in header)
        + "\n"
        + "".join(
            column.ljust(width) for column in [row[1], row[2], row[0], row[3]]
        )  # specified rows corrects PyDrill random column output order
    )

@log_error(clogger)
def main():
    raw_weather_frame = pd.concat(
        import_monthly_weather_csv(filename) for filename in FILENAMES
    )

    validate_weather_data(raw_weather_frame)

    weather_frame_out = transform_weather_df(raw_weather_frame)

    export_weather_to_parquet(weather_frame_out)

    max_daily_average_temperature(weather_frame_out)


main()
