import pandas as pd

def solar_time_to_hms(solar_time, gmt=False, longitude=None):

    if pd.isna(solar_time):
        return pd.Series([None, None, None])

    if gmt is True:
        assert longitude is not None, "If GMT solar time is given, longitude must be provided"
        assert isinstance(longitude, (int, float)), "Longitude must be numeric"

    if isinstance(solar_time, (int, float)):
        hhmmss = float(solar_time) * 24.0
        hours = int(hhmmss)
        mmss = (hhmmss - hours) * 60.0
        minutes = int(mmss)
        ss = (mmss - minutes) * 60.0
        seconds = int(ss)
        if gmt:
            offset = hours-(longitude/15)
            previous_day = offset < 0
            return pd.Series([offset%24, minutes, seconds, previous_day])
        else:
            return pd.Series([hours, minutes, seconds])
    else:
        print(f"Provided solar time, {solar_time} is not numeric. Returning none-series")
        return pd.Series([None, None, None])
