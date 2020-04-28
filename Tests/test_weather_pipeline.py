import pytest
import weather_pipeline
import pandas as pd
from pandas.testing import assert_frame_equal
from contextlib import contextmanager

EXAMPLE_INPUT_DATA_PATH = "Data/example-input-data.csv"


@contextmanager
def not_raises(ExpectedException):
    try:
        yield

    except ExpectedException as error:
        raise AssertionError(f"Raised exception {error} when it should not!")

    except Exception as error:
        raise AssertionError(f"An unexpected exception {error} raised.")


# import_monthly_weather_csv unit tests

df_func_example_out = pd.read_csv("Data/CSVTestFiles/test-out.csv")


@pytest.mark.parametrize(
    "test_path, expectation", [("Data/example-input-data.csv", df_func_example_out)],
)
def test_import_monthly_weather_csv(test_path, expectation):
    assert_frame_equal(
        weather_pipeline.import_monthly_weather_csv(test_path), expectation
    )


@pytest.mark.parametrize(
    "test_path, expectation",
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
def test2_import_monthly_weather_csv(test_path, expectation):
    with pytest.raises(expectation):
        weather_pipeline.import_monthly_weather_csv(test_path)


# Validation picks up missing data and passes with standard data

df_example_data_for_validation = weather_pipeline.import_monthly_weather_csv(
    EXAMPLE_INPUT_DATA_PATH
)
df_missing_data = weather_pipeline.import_monthly_weather_csv("Data/CSVTestFiles/missing-data.csv")


@pytest.mark.parametrize(
    "frame_in, expectation",
    [
        (
            df_example_data_for_validation,
            "not_raises(weather_pipeline.DataValidationError)",
        ),
        (
            df_missing_data, "pytest.raises(weather_pipeline.DataValidationError)"
        )
    ],
)
def test_validate_weather_data(frame_in, expectation):
    with eval(expectation):
        weather_pipeline.validate_weather_data(frame_in)


# def test_max_daily_average_temperature(drill_file_path: str):
#
# # malformed SQL + wrong path in above
# def query_parquet(sql: str)
