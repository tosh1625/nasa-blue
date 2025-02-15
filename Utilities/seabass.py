import pandas as pd
import numpy as np
import SB_support_updated
from seabass_columns import seabass_columns
from Utilities.collect_file_paths import collect_file_paths

class Seabass:

    def create_df(self, sb_file_directory, start_year=1900, end_year=2025,
                  chl=True, depth=True, kd=True, spm=True, additional_columns=None):

        # Create list of columns to import
        import_columns = set()

        # Add date and time columns
        import_columns.update(['date', 'year', 'month', 'day', 'time', 'hour', 'minute', 'second'])
        # Add geolocation columns
        import_columns.update(['lat', 'lon'])

        # Add Chlorophyll columns
        if chl:
            import_columns.update(c for c in seabass_columns if c.startswith("chl_") or c == "chl")
        # Add depth columns
        if depth:
            import_columns.update(c for c in seabass_columns if "depth" in c)
            import_columns.update(c for c in seabass_columns if c.startswith("z"))
        # Add diffuse attenuation coefficient columns
        if kd:
            import_columns.update(c for c in seabass_columns if c.startswith("kd"))
        # Add total suspended particulate matter columns
        if spm:
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

        # If year, month, day columns are missing, generate using date values
        def split_date(date):
            if pd.notna(date):
                str_date = str(int(date))
                return pd.Series([int(str_date[:4]), int(str_date[4:6]), int(str_date[6:8])])
            else:
                return pd.Series([None, None, None])

        date_series = df['date'].apply(split_date)
        df['year'] = df['year'].combine_first(date_series[0])
        df['month'] = df['month'].combine_first(date_series[1])
        df['day'] = df['day'].combine_first(date_series[2])

        # If date value is missing, generate using year, month, day values
        df['date'] = df.apply(lambda r: f"{int(r['year'])}{int(r['month']):02d}{int(r['day']):02d}"
            if pd.isna(r['date']) and all(pd.notna(r[c]) for c in ['year', 'month', 'day']) else r['date'], axis=1)

        # Create hour, minute, second columns if not exist
        for c in ['hour', 'minute', 'second']:
            if c not in df.columns:
                df[c] = pd.NA

        # If hour, minute, second columns are missing, generate using time values
        def split_time(time):
            if pd.notna(time):
                str_time = time.split(':')
                return pd.Series([int(str_time[0]), int(str_time[1]), int(str_time[2])])
            else:
                return pd.Series([None, None, None])

        time_series = df['time'].apply(split_time)
        df['hour'] = df['hour'].combine_first(time_series[0])
        df['minute'] = df['minute'].combine_first(time_series[1])
        df['second'] = df['second'].combine_first(time_series[2])

        # If time value is missing, generate using hour, minute, second values
        df['time'] = df.apply(lambda r: f"{int(r['hour'])}:{int(r['minute']):02d}:{int(r['second']):02d}"
            if pd.isna(r['time']) and all(pd.notna(r[c]) for c in ['hour', 'minute', 'second']) else r['time'], axis=1)

        print("\nInvalid values have been replaced by NaNs in the following columns:")
        print(*invalid_format, sep='\n')
        
        print("\nNumber of rows:", df.shape[0])
        print("Number of columns:", df.shape[1])

        return df


    def create_csv(self, sb_file_directory, start_year=1900, end_year=2025,
                   chl=True, depth=True, kd=True, spm=True, additional_columns=None):

        # Create dataframe
        df = self.create_df(sb_file_directory, start_year, end_year, chl, depth, kd, spm, additional_columns)

        # Set file path and name
        export_location = sb_file_directory[:sb_file_directory.rfind('/') + 1]
        csv_name = sb_file_directory.split('/')[-1] + '.csv'

        # Export dataframe as CSV
        df.to_csv(export_location + csv_name , index=True)
        print('\n' + csv_name + ' has been created in ' + export_location)
