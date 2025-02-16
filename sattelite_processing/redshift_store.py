import os
import pandas as pd
from datetime import datetime
from Utilities.lossless_geohash import encode_lossless_geohash

# Define output directory and ensure it exists
OUTPUT_DIR = "../Datasets/sattelite_data/"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def safe_encode(row):
    """Encodes latitude and longitude using lossless_geohash."""
    val = encode_lossless_geohash(row["latitude"], row["longitude"], 6)
    if isinstance(val, (list, tuple, pd.Series)):
        return ".".join(map(str, val))
    return str(val)


def process_chunk_file(file_path: str):
    """
    Processes a single chunk CSV file, cleans and transforms the data,
    and updates the corresponding aggregated Parquet file based on the data's
    timestamp (grouped by year and month).

    Parameters:
        file_path (str): Path to the chunk CSV file.
    """
    print(f"Processing file: {file_path}")

    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return

    # Add a geohash column using the safe_encode helper
    df["geohash"] = df.apply(safe_encode, axis=1)

    # Extract timestamp from the 'filename' column
    df["timestamp"] = df["filename"].apply(
        lambda x: datetime.strptime(x.split(".")[1], "%Y%m%dT%H%M%S") if "." in x else None
    )

    # Rename and reorder columns
    df.rename(columns={"Kd_490": "kd_490"}, inplace=True)
    df.rename(
        columns={
            "kd_490": "modis_kd_490",
            "sst": "modis_sst",
            "chlor_a": "modis_chlor_a"
        },
        inplace=True
    )
    df = df[["geohash", "modis_sst", "modis_chlor_a", "modis_kd_490", "timestamp"]]

    # If there's no data, nothing to process
    if df.empty:
        print(f"No data found in {file_path}.")
        return

    # Add year and month columns for aggregation
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month

    # Group by year and month and update corresponding aggregated files
    for (year, month), group in df.groupby(['year', 'month']):
        output_filename = os.path.join(OUTPUT_DIR, f"satellite_raw_{year}_{str(month).zfill(2)}.parquet")
        if os.path.exists(output_filename):
            try:
                existing = pd.read_parquet(output_filename, engine='pyarrow')
                group = pd.concat([existing, group], ignore_index=True)
            except Exception as e:
                print(f"Error reading existing file {output_filename}: {e}")

        # Drop duplicate rows based on geohash and timestamp
        group = group.drop_duplicates(subset=["geohash", "timestamp"])
        group.drop(columns=["year", "month"], inplace=True)

        try:
            group.to_parquet(output_filename, index=False, engine='pyarrow')
            print(f"✅ Saved aggregated file: {output_filename}")
        except Exception as e:
            print(f"Error saving {output_filename}: {e}")

    print("🚀 File processed and aggregated successfully!")

# Example usage:
# process_chunk_file("./input_chunks/output_chunk_example.csv")
