import pytest
import weather_pipeline
import pandas as pd
from pandas.testing import assert_frame_equal
from contextlib import contextmanager, nullcontext as does_not_raise

df_test_out = pd.read_csv("Data/test-out.csv")


@pytest.mark.parametrize(
    "test_path, expectation", [("Data/test-data.csv", df_test_out)],
)
def test_import_monthly_weather_csv(test_path, expectation):
    # with does_not_raise:
    assert_frame_equal(
        weather_pipeline.import_monthly_weather_csv(test_path), expectation
    )


@pytest.mark.parametrize(
    "test_path, expectation",
    [
        ("", FileNotFoundError),
        ("Data/blank.csv", EmptyDataError),
        ("Data/header-only.csv", UnicodeDecodeError),
        ("Data/random-chas.csv", UnicodeDecodeError),
    ],
)
def test2_import_monthly_weather_csv(test_path, expectation):
    with pytest.raises(expectation):
        weather_pipeline.import_monthly_weather_csv(test_path)


# def test_sum():
#     assert sum([1, 2, 3]) == 6, "Should be 6"
#
#
# def test_sum_tuple():
#     assert sum((1, 2, 2)) == 6, "Should be 6"
