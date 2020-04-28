import pytest
import weather_pipeline
import pandas as pd
from pandas.testing import assert_frame_equal
from contextlib import contextmanager

# exists in eval string
# noinspection PyUnresolvedReferences
from pydrill import exceptions

EXAMPLE_INPUT_DATA_PATH = "Data/example-input-data.csv"
PARQUET_FILE_DFS_ABS_PATH = (
    "dfs.`C:/Users/Michael/PycharmProjects/GFWeatherPipelineTask/Data/weather.parquet`"
)


@contextmanager
def not_raises(expected_exception):
    try:
        yield

    except expected_exception as error:
        raise AssertionError(f"Raised exception {error} when it should not!")

    except Exception as error:
        raise AssertionError(f"An unexpected exception {error} raised.")


# import_monthly_weather_csv unit tests

df_func_example_out = pd.read_csv("Data/CSVTestFiles/test-out.csv")


@pytest.mark.parametrize(
    "test_path, expected_output",
    [("Data/example-input-data.csv", df_func_example_out)],
)
def test_import_monthly_weather_csv(test_path, expected_output):
    assert_frame_equal(
        weather_pipeline.import_monthly_weather_csv(test_path), expected_output
    )


@pytest.mark.parametrize(
    "test_path, expected_exception",
    [
        ("", FileNotFoundError),
        ("Data/CSVTestFiles/blank.csv", pd.errors.EmptyDataError),
        ("Data/CSVTestFiles/header-only.csv", pd.errors.EmptyDataError),
        ("Data/CSVTestFiles/random-chas.csv", UnicodeDecodeError),
        ("Data/CSVTestFiles/no-header.csv", weather_pipeline.DataValidationError),
        ("Data/CSVTestFiles/extra-data.csv", pd.errors.ParserError),
        (
            "Data/CSVTestFiles/missing-column-names.csv",
            weather_pipeline.DataValidationError,
        ),
        (
            "Data/CSVTestFiles/extra-column-names.csv",
            weather_pipeline.DataValidationError,
        ),
    ],
)
def test2_import_monthly_weather_csv(test_path, expected_exception):
    with pytest.raises(expected_exception):
        weather_pipeline.import_monthly_weather_csv(test_path)


# Validation passes with standard data and picks up missing data

df_example_data_for_validation = weather_pipeline.import_monthly_weather_csv(
    EXAMPLE_INPUT_DATA_PATH
)
df_missing_data = weather_pipeline.import_monthly_weather_csv(
    "Data/CSVTestFiles/missing-data.csv"
)


@pytest.mark.parametrize(
    "frame_in, expected_exception",
    [
        (
            df_example_data_for_validation,
            "not_raises(weather_pipeline.DataValidationError)",
        ),
        (df_missing_data, "pytest.raises(weather_pipeline.DataValidationError)"),
    ],
)
def test_validate_weather_data(frame_in, expected_exception):
    with eval(expected_exception):
        weather_pipeline.validate_weather_data(frame_in)


@pytest.mark.parametrize(
    "frame_in, expected_exception",
    [
        (
            df_example_data_for_validation,
            "not_raises(pd.errors.ParserError or pd.errors.UnsupportedFunctionCall)",
        )
    ],
)
def test_transform_weather_df(frame_in, expected_exception):
    with eval(expected_exception):
        weather_pipeline.transform_weather_df(frame_in)


@pytest.mark.parametrize(
    "drill_file_path, expected_exception",
    [
        (PARQUET_FILE_DFS_ABS_PATH, "not_raises(exceptions.ImproperlyConfigured)"),
        (
            "dfs.`C:/Users/Michael/PycharmProjects/GFWeatherPipelineTask/Data/wether.parquet`",
            "pytest.raises(exceptions.TransportError)",
        ),
    ],
)
def test_max_daily_average_temperature(drill_file_path, expected_exception):
    with eval(expected_exception):
        weather_pipeline.max_daily_average_temperature(drill_file_path)


WELL_FORMED_SQL = """select
                ObservationDate,Region, SiteName,round(AVG(ScreenTemperature),2) as DailyAverageTemperature
            from
                dfs.`C:/Users/Michael/PycharmProjects/GFWeatherPipelineTask/Data/weather.parquet`  
            group by
                ObservationDate,Region,SiteName"""

MALFORMED_SQL = """select
                ObservatinDate,Region, SiteName,round(AVG(ScreenTemperature),2) as DailyAverageTemperature
            from
                dfs.`C:/Users/Michael/PycharmProjects/GFWeatherPipelineTask/Data/weather.parquet`  
            group by
                ObservationDate,Region,SitName"""


@pytest.mark.parametrize(
    "sql, expected_exception",
    [
        (WELL_FORMED_SQL, "not_raises(exceptions.QueryError)"),
        (MALFORMED_SQL, "pytest.raises(exceptions.TransportError)",),
    ],
)
def test_query_parquet(sql, expected_exception):
    with eval(expected_exception):
        weather_pipeline.query_parquet(sql)
