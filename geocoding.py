# using reverse geo-coding of longitude and latitude
import time
import pandas as pd
import locationiq.geocoder as gc
import os
import numpy as np
from tqdm import tqdm


def get_reverse_geocodes(fpath: str) -> pd.DataFrame:

    frame_codes = pd.read_csv(fpath, index_col=None)
    geocoder = gc.LocationIQ(os.environ.get("APIKey"), "json")
    results_list_dict = []

    for index, row in tqdm(
        frame_codes.iterrows(),
        desc="Retrieving address information",
        leave=True,
        unit="addresses",
        total=frame_codes.shape[0],
    ):
        dict_in = geocoder.reverse_geocode(row["Latitude"], row["Longitude"]).get(
            "address"
        )
        dict_in["ForecastSiteCode"] = int(row["ForecastSiteCode"])
        results_list_dict.append(dict_in)
        time.sleep(0.9)  # Avoids reaching rate limit for LocationIQ geocoding service

    frame_out = pd.DataFrame(results_list_dict)

    return frame_out


def get_column(frame_in: pd.DataFrame, col: str):

    return frame_in.reindex(columns=[col]).squeeze()


def transform_reverse_geocodes(frame_in: pd.DataFrame()):

    state_district = get_column(frame_in, "state_district")
    county = get_column(frame_in, "county")

    frame_out = (
        frame_in.assign(
            Region=np.where(
                state_district.isnull(),
                np.where(county.isnull(), "", county),
                state_district,
            ),
            Locality=lambda df: get_column(df, "town").fillna("")
            + get_column(df, "city").fillna("")
            + get_column(df, "village").fillna(""),
        )
        .reindex(
            columns=["ForecastSiteCode", "Locality", "Region", "state", "postcode"]
        )
        .rename(columns={"state": "Country", "postcode": "Postcode"})
    )

    frame_out.to_csv("Data/ForecastSiteAddresses.csv", index=False)


transform_reverse_geocodes(get_reverse_geocodes("Data/ForecastSiteCords.csv"))

# TODO: Error handling eg see codes below and also
# LocationIqNoPlacesFound if there are no matching results
# LocationIqInvalidKey If the provided api_key is invalid.
# LocationIqInvalidRequest If you go past your rate limit.
# LocationIqRequestLimitExeceeded If you go past ratelimits.
# LocationIqServerError occurs basically when there's server error
