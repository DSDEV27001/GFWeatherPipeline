import pandas as pd
import numpy as np
import logging
import functools
import textwrap
import retrying
import mappings
import re
from pandas_schema import Column, Schema, validation
from pydrill import client, exceptions

FILENAMES = ["Data/weather.20160201.csv", "Data/weather.20160301.csv"]

clogger = logging.getLogger(__name__)
fh = logging.FileHandler("error.log")
clogger.addHandler(fh)


def log_error(logger):
    """
    Decorator function to log errors generically
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


@retrying.retry(
    retry_on_exception=FileNotFoundError or IOError, stop_max_attempt_number=5
)
def import_monthly_weather_csv(fpath: str, export_cords: bool = False) -> pd.DataFrame:
    """
    Imports a monthly weather .csv into a DataFrame
    Converts -99 to null
    Removes leading and trailing whitespace
    Optionally exports weather station code and latitude and longitude for reverse geo-coding
    """
    try:
        frame_out = pd.read_csv(f"{fpath}", na_values=-99).applymap(
            lambda x: x.strip() if isinstance(x, str) else x
        )
        if export_cords:
            frame_out[
                ["ForecastSiteCode", "Latitude", "Longitude"]
            ].drop_duplicates().to_csv("Data/ForecastSiteCords.csv", index=False)
        return frame_out
    except Exception as e:
        if clogger:
            clogger.exception(e)
        raise


@log_error(clogger)
def validate_weather_data(frame_in: pd.DataFrame):
    """
    Uses a schema to validate the input dataframe
    """
    string_check_regex = re.compile(r"^(?=[A-Za-z0-9 &,./\-()\"']{1,50}$)")

    weather_file_schema = Schema(
        [
            Column(
                "ForecastSiteCode",
                [
                    validation.InRangeValidation(1000, 100000),
                    validation.IsDtypeValidation(np.dtype("int64")),
                ],
            ),
            Column(
                "ObservationTime",
                [
                    validation.InRangeValidation(0, 24),
                    validation.IsDtypeValidation(np.dtype("int64")),
                ],
            ),
            Column(
                "ObservationDate",
                [validation.DateFormatValidation("%Y-%m-%dT%H:%M:%S")],
            ),
            Column(
                "WindDirection",
                [
                    validation.InRangeValidation(0, 17),
                    validation.IsDtypeValidation(np.dtype("int64")),
                ],
            ),  # 16 points of the compass (N is 0 and 16 since it is 0 and 360 degrees)
            Column(
                "WindSpeed",
                [
                    validation.InRangeValidation(0, 255),
                    validation.CanConvertValidation(int),
                ],
                allow_empty=True,
            ),
            Column(
                "WindGust",
                [
                    validation.InRangeValidation(0, 255),
                    validation.CanConvertValidation(int),
                ],
                allow_empty=True,
            ),
            Column(
                "Visibility",
                [
                    validation.CanConvertValidation(int),
                    validation.InRangeValidation(0, 125000),
                ],
                allow_empty=True,
            ),
            Column(
                "ScreenTemperature",
                [
                    validation.InRangeValidation(-50, +50),
                    validation.IsDtypeValidation(np.dtype(float)),
                ],
                allow_empty=True,
            ),
            Column(
                "Pressure",
                [
                    validation.InRangeValidation(870, 1085),
                    validation.CanConvertValidation(int),
                ],
                allow_empty=True,
            ),
            Column(
                "SignificantWeatherCode",
                [validation.InRangeValidation(0, 31)],
                allow_empty=True,
            ),
            Column(
                "SiteName", [validation.MatchesPatternValidation(string_check_regex)],
            ),
            Column(
                "Latitude",
                [
                    validation.InRangeValidation(-90, 90),
                    validation.IsDtypeValidation(np.dtype(float)),
                ],
            ),  # Signed latitude range
            Column(
                "Longitude",
                [
                    validation.InRangeValidation(-180, 80),
                    validation.IsDtypeValidation(np.dtype(float)),
                ],
            ),  # Signed longitude range
            Column(
                "Region",
                [validation.MatchesPatternValidation(string_check_regex)],
                allow_empty=True,
            ),
            Column(
                "Country",
                [validation.MatchesPatternValidation(string_check_regex)],
                allow_empty=True,
            ),
        ]
    )

    errors = weather_file_schema.validate(frame_in)
    if len(errors) > 0:
        if clogger:
            for error in errors:
                clogger.exception(error)
        raise DataValidationError(
            "Error: Data validation failed. Please refer to the log file for detailed information."
        )


class DataValidationError(Exception):
    """
    Exception raised when data validation produces errors.
    """


@log_error(clogger)
def transform_weather_df(frame_in: pd.DataFrame) -> pd.DataFrame:
    """
    Merges date and time into one field using standard ISO format
    Transforms SiteName into proper case
    Uses maps to populate Country with more accurate and complete data
    (eg Glasgow and Strathclyde are not in England!)
    Enriches data with human readable information
    Corrects wrongly inferred types
    Removes data duplicates
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
            # Creates a categorical field for visibility
            VisibilityDescription=lambda df: pd.cut(
                df.Visibility,
                [0, 1000, 4000, 10000, 20000, 40000, np.inf],
                labels=[
                    "Very poor",
                    "Poor",
                    "Moderate",
                    "Good",
                    "Very good",
                    "Excellent",
                ],
                include_lowest=True,
                right=False,
            ),
            Region=lambda df: np.where(
                df.ForecastSiteCode == 3204, "Isle of Man", df.Region
            ),
            # Corrects capitalised and blank countries
            Country=lambda df: df.Region.map(
                mappings.REGION_TO_COUNTRY, na_action="ignore"
            ),
            # Enriches data with human readable information
            WindCompass=lambda df: df.WindDirection.map(
                mappings.COMPASS_16_PT, na_action="ignore"
            ),
            WeatherType=lambda df: df.SignificantWeatherCode.map(
                mappings.WEATHER_TYPES, na_action="ignore"
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
    """
    Exports weather data to a parquet file
    """
    frame_in.to_parquet(
        "Data/weather.parquet", index=False, engine="pyarrow", row_group_size=10000
    )
    frame_in.to_csv("Data/weather.csv", index=False)


@log_error(clogger)
def max_daily_average_temperature(frame_in: pd.DataFrame):
    """
    SQL Query text designed to answer task questions
    Passes the sql string and the DataFrame to another function to execute in drill
    """
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

    query_out = query_parquet(frame_in, sql)
    format_task_query_output(query_out)


@log_error(clogger)
def query_parquet(frame_in: pd.DataFrame, sql: str):
    """
    Uses Apache Drill to interrogate the weather parquet file
    with a specified SQL query
    Note: drill must be running on the specified host
    """
    drill = client.PyDrill(host="localhost", port=8047)

    if not drill.is_active():
        raise exceptions.ImproperlyConfigured(
            "Apache Drill must be running to allow querying of parquet data"
        )

    return drill.query(sql, timeout=10)


def format_task_query_output(query_output):
    header = ["ObservationDate", "Region", "SiteName", "DailyAverageTemperature"]

    row = list(query_output.rows[0].values())
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
            # corrects PyDrill random column output order bug/feature
            column.ljust(width)
            for column in [row[1], row[2], row[0], row[3]]
        )
    )


def main():
    try:
        raw_weather_frame = pd.concat(
            import_monthly_weather_csv(filename) for filename in FILENAMES
        )

        validate_weather_data(raw_weather_frame)

        weather_frame_out = transform_weather_df(raw_weather_frame)

        export_weather_to_parquet(weather_frame_out)

        max_daily_average_temperature(weather_frame_out)

    except Exception as e:
        if clogger:
            clogger.exception(e)
        raise
    finally:
        logging.shutdown()


main()
