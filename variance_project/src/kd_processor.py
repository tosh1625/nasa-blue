import pandas as pd
import numpy as np

class KdProcessor:
    """
    A processor class for analyzing Kd_490 measurements over time and space.
    Uses DateHandler and GeohashHandler to process temporal and spatial data.
    """
    
    def __init__(self, date_handler, geohash_handler):
        """
        Initialize the KdProcessor.
        
        Parameters:
        -----------
        date_handler : DateHandler
            Handler for datetime operations
        geohash_handler : GeohashHandler
            Handler for geohash operations
        """
        self.date_handler = date_handler
        self.geohash_handler = geohash_handler

    def load_data(self, data):
        self.df = data
    
    def preprocess_data(self, timestamp_col, geohash_col, kd_col, geohash_precision=5):
        """
        Preprocess the raw data by adding datetime and geospatial information.
        
        Parameters:
        -----------
        df : pandas.DataFrame
            DataFrame containing the raw data
        timestamp_col : str
            Column name for timestamps
        geohash_col : str
            Column name for geohashes
        kd_col : str
            Column name for Kd_490 values
        geohash_precision : int, optional
            Precision for geohash decoding (default: 5)
            
        Returns:
        --------
        pandas.DataFrame
            Preprocessed DataFrame with datetime and geospatial columns
        """
        self.timestamp_col = timestamp_col
        self.geohash_col = geohash_col
        self.kd_col = kd_col
        self.gh_precision = geohash_precision

        df = self.df
        # Process timestamps
        df_time = self.date_handler.process_dataframe(df=df , timestamp_col=timestamp_col)
        
        # Process geohashes
        df_processed = self.geohash_handler.process_dataframe(df = df_time, geohash_col = geohash_col, precision=geohash_precision)
        
        return df_processed
    
    def truncate_geohash(self, geohash_str, precision):
        """
        Truncate a geohash to the specified precision.
        
        Parameters:
        -----------
        geohash_str : str
            Original geohash string
        precision : int
            Desired precision
            
        Returns:
        --------
        str
            Truncated geohash
        """
        # For standard geohashes
        if '.' not in geohash_str:
            return geohash_str[:precision]
        
        # For custom format like "HD9Td.PFTWP"
        parts = geohash_str.split('.')
        if len(parts) == 2:
            return parts[0][:precision]
        
        return geohash_str[:precision]
    
    def calculate_time_window_variances(self, df, window_days=[5, 10], 
                                        geohash_precision=5):
        """
        Calculate variances in Kd_490 values for specified time windows.
        
        Parameters:
        -----------
        df : pandas.DataFrame
            Preprocessed DataFrame with datetime and geospatial columns
        kd_col : str
            Column name for Kd_490 values
        timestamp_col : str
            Column name for timestamps
        window_days : list, optional
            List of time windows in days (default: [5, 10])
        geohash_col : str, optional
            Column to use for geohash grouping (default: 'std_geohash')
        geohash_precision : int, optional
            Precision to truncate geohashes for grouping (default: 5)
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with geohash, bounding box, and variance columns
        """
        # df = self.df
        kd_col = self.kd_col
        timestamp_col = self.timestamp_col
        geohash_col = self.geohash_col

        # Ensure datetime column exists
        if 'datetime' not in df.columns:
            df = self.date_handler.process_dataframe(df, timestamp_col)
        
        # Truncate geohashes to the specified precision for grouping
        df['grouped_geohash'] = df[geohash_col].apply(
            lambda x: self.truncate_geohash(x, geohash_precision)
        )
        
        # Sort by geohash and datetime
        df_sorted = df.sort_values(['grouped_geohash', 'datetime'])
        
        # Calculate variances for each time window
        result_rows = []
        
        # Group by geohash
        for geohash, group in df_sorted.groupby('grouped_geohash'):
            # Sort group by datetime
            group = group.sort_values('datetime')
            
            # Get bounding box info for this geohash (use first row's data)
            bbox_data = {
                'min_lat': group['min_lat'].iloc[0],
                'max_lat': group['max_lat'].iloc[0],
                'min_lng': group['min_lng'].iloc[0],
                'max_lng': group['max_lng'].iloc[0],
                'center_lat': group['center_lat'].iloc[0],
                'center_lng': group['center_lng'].iloc[0],
                'geohash': geohash
            }
            
            # Create a dictionary to hold variance results for this geohash
            variance_data = bbox_data.copy()
            
            # Loop through each window size
            for window in window_days:
                # Calculate pairwise differences for measurements 'window' days apart
                pairs = []
                variance_key = f'kd_variance_{window}day'
                
                for i, row1 in group.iterrows():
                    date1 = row1['datetime']
                    kd1 = row1[kd_col]
                    
                    # Find measurements approximately 'window' days later
                    later_measurements = group[
                        (group['datetime'] > date1) & 
                        (group['datetime'] <= date1 + pd.Timedelta(days=window+0.5)) &
                        (group['datetime'] >= date1 + pd.Timedelta(days=window-0.5))
                    ]
                    
                    for j, row2 in later_measurements.iterrows():
                        kd2 = row2[kd_col]
                        day_diff = (row2['datetime'] - date1).days
                        
                        pairs.append({
                            'kd1': kd1,
                            'kd2': kd2,
                            'diff': abs(kd2 - kd1),
                            'pct_diff': abs((kd2 - kd1) / kd1) * 100 if kd1 != 0 else np.nan,
                            'days_apart': day_diff
                        })
                
                # Calculate variance statistics if we have pairs
                if pairs:
                    pairs_df = pd.DataFrame(pairs)
                    variance_data[variance_key] = pairs_df['diff'].mean()
                    variance_data[f'{variance_key}_median'] = pairs_df['diff'].median()
                    variance_data[f'{variance_key}_std'] = pairs_df['diff'].std()
                    variance_data[f'{variance_key}_count'] = len(pairs)
                    variance_data[f'{variance_key}_pct'] = pairs_df['pct_diff'].mean()
                else:
                    variance_data[variance_key] = np.nan
                    variance_data[f'{variance_key}_median'] = np.nan
                    variance_data[f'{variance_key}_std'] = np.nan
                    variance_data[f'{variance_key}_count'] = 0
                    variance_data[f'{variance_key}_pct'] = np.nan
            
            result_rows.append(variance_data)
        
        # Create result DataFrame
        result_df = pd.DataFrame(result_rows)
        
        return result_df
    
    def prepare_for_visualization(self, variance_df, window_days=5, variance_type='kd_variance'):
        """
        Prepare data for visualization in Dash.
        
        Parameters:
        -----------
        variance_df : pandas.DataFrame
            DataFrame with variance calculations
        window_days : int, optional
            Which window size to use for visualization (default: 5)
        variance_type : str, optional
            Type of variance to visualize (default: 'kd_variance')
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame formatted for Dash visualization
        """
        # Copy input DataFrame to avoid modifying the original
        vis_df = variance_df.copy()
        
        # Create column name based on window size and variance type
        col_name = f'{variance_type}_{window_days}day'
        
        # Filter out rows with missing variance values
        vis_df = vis_df.dropna(subset=[col_name])
        
        # Create a column for hover text in Dash
        vis_df['hover_text'] = vis_df.apply(
            lambda row: (
                f"Geohash: {row['geohash']}<br>"
                f"Variance: {row[col_name]:.4f}<br>"
                f"Sample count: {row[f'{col_name}_count']}"
            ), axis=1
        )
        
        return vis_df