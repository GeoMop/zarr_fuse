import polars as pl
from datetime import datetime

def get_3day_forecast(cache, lat: float, lon: float) -> pl.DataFrame:
    """
    Fetch the complete forecast data from yr.no and flatten it into a Polars DataFrame.

    This function queries the 'complete' endpoint from the Norwegian Meteorological Institute.
    It processes the returned JSON to build a DataFrame with:
      - 'date_time': forecast timestamp (as a datetime object).
      - All key-value pairs from the "instant" -> "details" section.
      - All key-value pairs from the "next_1_hours" -> "details" section, with keys prefixed by "n1h_".

    Parameters:
        lat (float): Latitude of the location.
        lon (float): Longitude of the location.

    Returns:
        pl.DataFrame: A flattened DataFrame containing the forecast data.
    """
    url = "https://api.met.no/weatherapi/locationforecast/2.0/complete"
    headers = {
        "User-Agent": "MyWeatherApp/1.0 (your_email@example.com)"  # Replace with your details.
    }
    params = {
        "lat": lat,
        "lon": lon
    }

    response = cache.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()

    timeseries = data.get("properties", {}).get("timeseries", [])

    records = []
    for entry in timeseries:
        # Parse the forecast time. Adjust "Z" to an ISO-compatible offset.
        time_str = entry.get("time")
        # Convert the time string to a datetime object.
        dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        record = {"date_time": dt}

        # Flatten the "instant" details.
        instant_details = entry.get("data", {}).get("instant", {}).get("details", {})
        record.update(instant_details)

        # Flatten the "next_1_hours" details, if available, with a "n1h_" prefix.
        n1h_details = entry.get("data", {}).get("next_1_hours", {}).get("details", {})
        for key, value in n1h_details.items():
            record[f"{key}"] = value

        records.append(record)

    # Create a Polars DataFrame from the list of records.
    df = pl.DataFrame(records)
    return df
