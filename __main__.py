import weather_pipeline as wp
import logging
import pandas as pd

FILENAMES = ["Data/weather.20160201.csv", "Data/weather.20160301.csv"]

clogger = logging.getLogger(__name__)
fh = logging.FileHandler("error.log")
clogger.addHandler(fh)


def main():
    try:
        raw_weather_frame = pd.concat(
            wp.import_monthly_weather_csv(filename) for filename in FILENAMES
        )

        wp.validate_weather_data(raw_weather_frame)

        weather_frame_out = wp.transform_weather_df(raw_weather_frame)

        wp.export_weather_to_parquet(weather_frame_out)

        wp.max_daily_average_temperature(weather_frame_out)

    except Exception as e:
        if clogger:
            clogger.exception(e)
        raise
    finally:
        logging.shutdown()


main()
