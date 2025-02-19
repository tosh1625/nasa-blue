import pandas as pd
import numpy as np
import sys
import re
import SB_support_updated
from datetime import datetime, timedelta
from Utilities.seabass_column_names import seabass_columns
from Utilities.collect_file_paths import collect_file_paths
from Utilities.lossless_geohash import encode_lossless_geohash
from Utilities.convert_time import *

def extract_degree(sb_degree):
    return float(re.search(r'[-]?\d+\.?\d*', sb_degree).group(0))

class Seabass:

    def create_df(self, sb_file_directory, start_year=1900, end_year=2025, chl_all=False, depth_all=False, kd_all=False,
                  rrs_all=False, spm_all=False, additional_columns=None, preview_column_list=False):

        # Create list of columns to import
        import_columns = set()

        # Add date and time columns
        import_columns.update(['serialdate', 'date', 'year', 'month', 'day', 'sdy', 'solar_time', 'solar_time_gmt',
                               'time', 'hour', 'minute', 'second'])
        # Add geolocation columns
        import_columns.update(['lat', 'lon'])

        # Add Chlorophyll columns
        if chl_all:
            import_columns.update(c for c in seabass_columns if c.startswith("chl_") or c == "chl")
        # Add depth columns
        if depth_all:
            import_columns.update(c for c in seabass_columns if "depth" in c)
            import_columns.update(c for c in seabass_columns if c.startswith("z"))
        # Add diffuse attenuation coefficient columns
        if kd_all:
            import_columns.update(c for c in seabass_columns if c.startswith("kd"))
        # Add remote sensing reflectance columns
        if rrs_all:
            import_columns.update(c for c in seabass_columns if c.startswith("rrs"))
        # Add total suspended particulate matter columns
        if spm_all:
            import_columns.update(c for c in seabass_columns if c.startswith("spm_") or c == "spm")

        # Add user supplied additional columns
        if isinstance(additional_columns, list):
            import_columns.update(additional_columns)
        elif additional_columns is None:
            pass
        else:
            raise Exception('additional_columns must be a list')

        # Assert all items in import_columns are in seabass_columns
        for column_name in import_columns:
            assert column_name in seabass_columns, "Invalid column name '{}'".format(column_name)

        # Preview column list
        if preview_column_list:
            print("[PREVIEW ENABLED] This configuration will include the following columns:\n"
                     " {}".format(", ".join(import_columns)))
            sys.exit(0)

        # Generate Seabass file path list
        path_list = collect_file_paths(sb_file_directory, 'sb')
        num_paths = len(path_list)

        # Create main dataframe
        df = pd.DataFrame()

        # Record Seabass file with invalid format
        invalid_format = []

        for n, file_path in enumerate(path_list):

            print(str(n+1) + '/' + str(num_paths) + ' ' + file_path)

            # Get file name
            file_name = file_path.split('/')[-1]

            # Create blank dataframe
            sub_df = pd.DataFrame()

            # Import measurements from Seabass files
            sb_data = SB_support_updated.readSB(filename=file_path, no_warn=True)

            # Terminate current iteration if file's start and end years fall outside requested year range
            if not any(start_year <= int(year) <= end_year for year in
                   [sb_data.headers['start_date'][:4], sb_data.headers['end_date'][:4]]):
                continue

            # Populate sub-dataframe
            for k, v in sb_data.data.items():
                if k in import_columns:
                    sub_df[k] = v

            # If latitude and longitude columns do not exist, copy values from header
            if not {'lat', 'lon'}.issubset(sub_df.columns) and \
                    {'north_latitude', 'east_longitude'}.issubset(sb_data.headers.keys()):
                lat_north = sb_data.headers.get('north_latitude')
                lat_south = sb_data.headers.get('south_latitude')
                lon_east = sb_data.headers.get('east_longitude')
                lon_west = sb_data.headers.get('west_longitude')

                # If geolocation is not area, populate latitude and longitude columns
                if lat_north == lat_south and lon_east == lon_west:
                    sub_df['lat'] = extract_degree(lat_north)
                    sub_df['lon'] = extract_degree(lon_east)
                else:
                    print(f"    Geolocation in {file_name} header is an area, not a point. Populating cells with NaN.")
                    sub_df['lat'] = np.nan
                    sub_df['lon'] = np.nan

            # If dates are not in columns, copy from file header
            if all(c not in sub_df.columns for c in ['date', 'serialdate', 'year', 'month', 'day', 'sdy']):
                start_date = sb_data.headers.get('start_date')
                end_date = sb_data.headers.get('end_date')

                assert len(str(start_date)) == 8, "start_date must be 8 digits long."
                assert len(str(end_date)) == 8, "end_date must be 8 digits long."

                if start_date != end_date:
                    print(f"Start date and end date are not the same: {start_date} and {end_date}")
                else:
                    sub_df['year'] = int(start_date[:4])
                    sub_df['month'] = int(start_date[4:6])
                    sub_df['day'] = int(start_date[6:8])

            # If solar time is used, convert to standard time
            if all(c not in sub_df.columns for c in ['serialdate', 'hour', 'minute', 'second']):

                if 'solar_time' in sub_df.columns:
                    time_series = sub_df['solar_time'].apply(solar_time_to_hms)
                    sub_df['hour'] = time_series[0]
                    sub_df['minute'] = time_series[1]
                    sub_df['day'] = time_series[2]

                if 'solar_time_gmt' in sub_df.columns:
                    time_series = sub_df['solar_time_gmt'].apply(solar_time_to_hms,
                                    args=(True, extract_degree(sb_data.headers.get('east_longitude'))))
                    sub_df['hour'] = time_series[0]
                    sub_df['minute'] = time_series[1]
                    sub_df['day'] = time_series[2]

            # Convert column data types
            for col in sub_df.columns:
                if col not in ['time', 'depth_code']:
                    sub_df[col] = sub_df[col].astype(float)

            # Add original Seabass file name to dataframe
            sub_df['filename'] = file_name

            # Merge dataframe to main dataframe
            if df.empty:
                df = sub_df
            else:
                df = pd.merge(df, sub_df, how='outer')

        # Create date and time columns if missing
        for c in ['serialdate', 'date', 'year', 'month', 'day', 'hour', 'minute', 'second']:
            if c not in df.columns:
                df[c] = pd.NA

        # If serialdate is available, use it to populate date and time columns
        def split_serialdate(sd):
            if pd.notnull(sd):
                str_sd = str(int(sd))
                assert len(str_sd) == 14, 'Invalid serialdate format'
                return pd.Series([int(str_sd[:4]), int(str_sd[4:6]), int(str_sd[6:8]),
                                  int(str_sd[8:10]), int(str_sd[10:12]), int(str_sd[12:14])])
            else:
                return pd.Series([None, None, None, None, None, None])

        date_series = df['serialdate'].apply(split_serialdate)
        df['year'] = df['year'].combine_first(date_series[0])
        df['month'] = df['month'].combine_first(date_series[1])
        df['day'] = df['day'].combine_first(date_series[2])
        df['hour'] = df['hour'].combine_first(date_series[2])
        df['minute'] = df['minute'].combine_first(date_series[2])
        df['second'] = df['second'].combine_first(date_series[2])

        # If dataframe contains SDY column, convert to month and day
        def convert_sdy(row):
            if pd.notnull(row['year']) and pd.notnull(row['sdy']):
                date = datetime(int(row['year']), 1, 1) + timedelta(days=int(row['sdy']) - 1)
                return pd.Series([date.month, date.day])
            else:
                return pd.Series([None, None])

        if 'sdy' in df.columns:
            mmdd = df[['year', 'sdy']].apply(convert_sdy, axis=1)
            df['month'] = df['month'].combine_first(mmdd[0])
            df['day'] = df['day'].combine_first(mmdd[1])

        # If year, month, day columns are missing, generate using date values
        def split_date(row):
            if pd.notna(row['date']) and pd.isna(row['serialdate']):
                str_date = str(int(row['date']))
                return pd.Series([int(str_date[:4]), int(str_date[4:6]), int(str_date[6:8])])
            else:
                return pd.Series([None, None, None])

        date_series = df[['date', 'serialdate']].apply(split_date, axis=1)
        df['year'] = df['year'].combine_first(date_series[0])
        df['month'] = df['month'].combine_first(date_series[1])
        df['day'] = df['day'].combine_first(date_series[2])

        # If date value is missing, generate using year, month, day values
        df['date'] = df.apply(lambda r: f"{int(r['year'])}{int(r['month']):02d}{int(r['day']):02d}"
            if pd.isna(r['date']) and pd.isna(r['serialdate']) and
               all(pd.notna(r[c]) for c in ['year', 'month', 'day']) else r['date'], axis=1)

        # If hour, minute, second columns are missing, generate using time or serialdate values
        def split_time(row):
            if pd.notna(row['time']) and pd.isna(row['serialdate']):
                str_time = row['time'].split(':')
                return pd.Series([int(str_time[0]), int(str_time[1]), int(str_time[2])])
            else:
                return pd.Series([None, None, None])

        time_series = df[['time', 'serialdate']].apply(split_time, axis=1)

        df['hour'] = df['hour'].combine_first(time_series[0])
        df['minute'] = df['minute'].combine_first(time_series[1])
        df['second'] = df['second'].combine_first(time_series[2])

        # If time value is missing, generate using hour, minute, second values
        df['time'] = df.apply(lambda r: f"{int(r['hour'])}:{int(r['minute']):02d}:{int(r['second']):02d}"
            if pd.isna(r['time']) and pd.isna(r['serialdate']) and
               all(pd.notna(r[c]) for c in ['hour', 'minute', 'second']) else r['time'], axis=1)

        # Add lossless Geohash column
        df['geohash_p6'] = df.apply(lambda row: encode_lossless_geohash(lat=row['lat'], lng=row['lon'], precision=6,
                                                                        filename=row['filename']), axis=1)

        # Remove sdy and serialdate columns
        if 'sdy' in df.columns:
            df.drop(columns='sdy', inplace=True)
        if 'serialdate' in df.columns:
            df.drop(columns='serialdate', inplace=True)

        print("\nInvalid values have been replaced by NaNs in the following columns:")
        print(*invalid_format, sep='\n')
        
        print("\nNumber of rows:", df.shape[0])
        print("Number of columns:", df.shape[1])

        return df


    def create_csv(self, sb_file_directory, start_year=1900, end_year=2025, chl_all=False, depth_all=False,
                   kd_all=False, rrs_all=False, spm_all=False, additional_columns=None, preview_column_list=False):

        # Create dataframe
        df = self.create_df(sb_file_directory, start_year, end_year, chl_all, depth_all, kd_all, rrs_all, spm_all,
                            additional_columns, preview_column_list)

        # Set file path and name
        export_location = sb_file_directory[:sb_file_directory.rfind('/') + 1]
        yyyymmdd = datetime.now().strftime('%Y%m%d')
        csv_name = ('seabass_' + yyyymmdd + '_r' + str(df.shape[0])
                    + '_c' + str(df.shape[1]) + '.csv')

        # Export dataframe as CSV
        df.to_csv(export_location + csv_name, index=False)
        print('\n' + csv_name + ' has been created in ' + export_location)
