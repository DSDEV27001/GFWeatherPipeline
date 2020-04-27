import pytest
import weather_pipeline
import pandas as pd
from pandas.testing import assert_frame_equal

df_test_out = pd.read_csv("Data/CSVTestFiles/test-out.csv")


@pytest.mark.parametrize(
    "test_path, expectation", [("Data/test-data.csv", df_test_out)],
)
def test_import_monthly_weather_csv(test_path, expectation):
    assert_frame_equal(
        weather_pipeline.import_monthly_weather_csv(test_path), expectation
    )


@pytest.mark.parametrize(
    "test_path, expectation",
    [
        ("", FileNotFoundError),
        ("Data/blank.csv", pd.errors.EmptyDataError),
        ("Data/header-only.csv", pd.errors.EmptyDataError),
        ("Data/random-chas.csv", UnicodeDecodeError),
        ("Data/no-header.csv", weather_pipeline.DataValidationError),
        # ("Data/missing-data.csv", weather_pipeline.DataValidationError),
        ("Data/extra-data.csv", pd.errors.ParserError),
        ("Data/missing-column-names.csv", weather_pipeline.DataValidationError),
        ("Data/extra-column-names.csv", weather_pipeline.DataValidationError),
    ],
)
def test2_import_monthly_weather_csv(test_path, expectation):
    with pytest.raises(expectation):
        weather_pipeline.import_monthly_weather_csv(test_path)
