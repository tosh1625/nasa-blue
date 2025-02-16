import os
import requests
from tqdm import tqdm
import netCDF4 as nc
import pandas as pd
import numpy as np
import json
from shapely.geometry import shape, Point
from concurrent.futures import ProcessPoolExecutor, as_completed
from queue import Queue, Empty
import threading
import time
from redshift_store import process_chunk_file  # our previously defined function

base_url = "https://oceandata.sci.gsfc.nasa.gov/getfile/"
with open("urls.txt") as file:
    filenames = [line.strip() for line in file if line.strip()]
with open("polygons.geojson") as f:
    geojson = json.load(f)
polygons = [shape(feature["geometry"]) for feature in geojson["features"]]


def try_get(var, container):
    if var in container.variables:
        return container.variables[var][:]
    return None


def download_file(filename):
    filepath = os.path.join("docs", filename)
    if os.path.exists(filepath):
        return
    file_url = base_url + filename
    try:
        response = requests.get(file_url, stream=True, timeout=30)
    except Exception as e:
        print(f"Error downloading {filename}: {e}")
        return
    total = response.headers.get("content-length")
    total = int(total) if total else 0
    with open(filepath, "wb") as f:
        if total:
            with tqdm(total=total, unit="B", unit_scale=True, desc=filename, leave=False) as progress_bar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        progress_bar.update(len(chunk))
        else:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)


def process_file(filename):
    filepath = os.path.join("docs", filename)
    if not os.path.exists(filepath):
        return None
    try:
        ds = nc.Dataset(filepath, "r")
    except OSError as e:
        print(f"Skipping file {filepath} due to error: {e}")
        os.remove(filepath)
        return None
    nlines = len(ds.dimensions["number_of_lines"])
    npixels = len(ds.dimensions["pixels_per_line"])
    lat_min = getattr(ds, "geospatial_lat_min", -90)
    lat_max = getattr(ds, "geospatial_lat_max", 90)
    lon_min = getattr(ds, "geospatial_lon_min", -180)
    lon_max = getattr(ds, "geospatial_lon_max", 180)
    lat_arr = np.linspace(lat_max, lat_min, nlines)
    lon_arr = np.linspace(lon_min, lon_max, npixels)
    lat_flat = np.repeat(lat_arr, npixels)
    lon_flat = np.tile(lon_arr, nlines)
    data = {
        "latitude": lat_flat,
        "longitude": lon_flat,
        "filename": np.full(nlines * npixels, os.path.basename(filename))
    }
    for var in ["sst", "chlor_a", "Kd_490"]:
        value = None
        if ds.groups:
            for grp in ds.groups.values():
                if value is None:
                    temp = try_get(var, grp)
                    if temp is not None:
                        value = temp.flatten()
                        break
        if value is None:
            temp = try_get(var, ds)
            if temp is not None:
                value = temp.flatten()
        if value is None:
            data[var] = np.full(nlines * npixels, np.nan)
        else:
            data[var] = value
    df = pd.DataFrame(data)
    ds.close()
    return df


def process_chunk(chunk, chunk_index):
    dfs = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_file, filename): filename for filename in chunk}
        for future in as_completed(futures):
            df = future.result()
            if df is not None:
                dfs.append(df)
    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        combined = combined[(~combined["sst"].isna()) | (~combined["chlor_a"].isna()) | (~combined["Kd_490"].isna())]
        filtered_indices = set()
        for poly in tqdm(polygons, desc=f"Filtering chunk {chunk_index}"):
            min_lon, min_lat, max_lon, max_lat = poly.bounds
            candidate = combined[(combined["latitude"] >= min_lat) & (combined["latitude"] <= max_lat) &
                                 (combined["longitude"] >= min_lon) & (combined["longitude"] <= max_lon)]
            for idx, row in candidate.iterrows():
                pt = Point(row["longitude"], row["latitude"])
                if poly.contains(pt):
                    filtered_indices.add(idx)
        filtered_df = combined.loc[list(filtered_indices)].copy()
        filtered_df.sort_values(["latitude", "longitude"], inplace=True)

        if filtered_df.empty:
            print(f"⚠️  Chunk {chunk_index} has no data after filtering. Skipping aggregation.")
        else:
            # Save the filtered data to a CSV file.
            output_csv = f"output_chunk_{chunk_index}.csv"
            filtered_df.to_csv(output_csv, index=False)
            print(f"✅ Saved output CSV: {output_csv}")

            # Pass the CSV file to process_chunk_file to update aggregated data.
            process_chunk_file(output_csv)

    # Clean up downloaded files.
    for filename in chunk:
        filepath = os.path.join("docs", filename)
        if os.path.exists(filepath):
            os.remove(filepath)


if __name__ == '__main__':
    chunk_size = 10
    download_queue = Queue()


    # Downloader thread: continuously downloads files and puts filename in the queue.
    def downloader():
        for filename in filenames:
            download_file(filename)
            download_queue.put(filename)
        # Signal completion.
        download_queue.put(None)


    t = threading.Thread(target=downloader)
    t.start()

    current_chunk = []
    chunk_index = 3002  # Starting index for naming output files.
    # Process files as they are downloaded, without waiting for all downloads to finish.
    while True:
        try:
            # Wait for up to 5 seconds for a new file.
            item = download_queue.get(timeout=5)
        except Empty:
            # If no new file and current chunk is non-empty, process it.
            if current_chunk:
                process_chunk(current_chunk, chunk_index)
                current_chunk = []
                chunk_index += 1
            # If downloader thread is still alive, continue waiting.
            if t.is_alive():
                continue
            else:
                break
        if item is None:
            # Sentinel received: process any remaining files.
            if current_chunk:
                process_chunk(current_chunk, chunk_index)
            break
        current_chunk.append(item)
        if len(current_chunk) >= chunk_size:
            process_chunk(current_chunk, chunk_index)
            current_chunk = []
            chunk_index += 1
    t.join()
    # Final check: if any files remain unprocessed.
    if current_chunk:
        process_chunk(current_chunk, chunk_index)
