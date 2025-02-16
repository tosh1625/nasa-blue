import pandas as pd
import numpy as np
import sys
import SB_support_updated
from Utilities.seabass_column_names import seabass_columns
from Utilities.collect_file_paths import collect_file_paths

class Seabass:

    def create_df(self, sb_file_directory, start_year=1900, end_year=2025, chl_all=False, depth_all=False, kd_all=False,
                  spm_all=False, additional_columns=None, preview_column_list=False):

        # Create list of columns to import
        import_columns = set()

        # Add date and time columns
        import_columns.update(['serialdate', 'date', 'year', 'month', 'day', 'time', 'hour', 'minute', 'second'])
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

            # Create blank dataframe
            sub_df = pd.DataFrame()

            # Import measurements from Seabass files
            sb_data = SB_support_updated.readSB(filename=file_path, no_warn=True)

            # Terminate current iteration if file's start and end years fall outside requested year range
            if not any(start_year <= int(year) <= end_year for year in
                   [sb_data.headers['start_date'][:4], sb_data.headers['end_date'][:4]]):
                continue

            # Populate dataframe
            for k, v in sb_data.data.items():

                # Mark invalid values as NaN
                values_to_replace = {'NULL'}
                if any(x in values_to_replace for x in v):
                    v = [np.nan if x in values_to_replace else x for x in v]
                    invalid_format.append((file_path, k))

                if k in import_columns:
                    sub_df[k] = v

            for col in sub_df.columns:
                if col not in ['time', 'depth_code']:
                    sub_df[col] = sub_df[col].astype(float)

            # Add original Seabass file name to dataframe
            sub_df['filename'] = file_path.split('/')[-1]

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

        # If year, month, day columns are missing, generate using date values
        def split_date(row):
            if pd.notna(row['date']) and pd.isna(['serialdate']):
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
            if pd.notna(row['time']) and pd.isna(['serialdate']):
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

        print("\nInvalid values have been replaced by NaNs in the following columns:")
        print(*invalid_format, sep='\n')
        
        print("\nNumber of rows:", df.shape[0])
        print("Number of columns:", df.shape[1])

        return df


    def create_csv(self, sb_file_directory, start_year=1900, end_year=2025, chl_all=False, depth_all=False,
                   kd_all=False, spm_all=False, additional_columns=None, preview_column_list=False):

        # Create dataframe
        df = self.create_df(sb_file_directory, start_year, end_year, chl_all, depth_all, kd_all, spm_all,
                            additional_columns, preview_column_list)

        # Set file path and name
        export_location = sb_file_directory[:sb_file_directory.rfind('/') + 1]
        csv_name = sb_file_directory.split('/')[-1] + '.csv'

        # Export dataframe as CSV
        df.to_csv(export_location + csv_name)
        print('\n' + csv_name + ' has been created in ' + export_location)
