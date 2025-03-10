import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pygeohash as pgh  # For geohash manipulation

class KdProcessor:
    """
    A class to process Kd_490 satellite data, calculate variances at different 
    time intervals, and prepare data for visualization in Dash.
    """
    
    def __init__(self, data_df=None):
        """
        Initialize the processor with optional data.
        
        Parameters:
        -----------
        data_df : pandas.DataFrame, optional
            DataFrame containing the satellite data with columns:
            geohash, timestamp, longitude, latitude, kd_490
        """
        self.data = data_df
        if data_df is not None:
            self._preprocess_data()
    
    def load_data(self, data_df):
        """
        Load data into the processor.
        
        Parameters:
        -----------
        data_df : pandas.DataFrame
            DataFrame containing the satellite data.
        """
        self.data = data_df
        self._preprocess_data()
        return self
    
    def _preprocess_data(self):
        """
        Perform initial data preprocessing:
        - Convert timestamps to datetime
        - Ensure geohash is processed correctly
        - Sort data by geohash and time
        """
        # Convert timestamp to datetime if needed
        if not pd.api.types.is_datetime64_dtype(self.data['timestamp']):
            # For the specific format you mentioned (19 digits) - using nanoseconds
            if len(str(self.data['timestamp'].iloc[0])) >= 19:
                # Convert directly as nanoseconds
                self.data['timestamp'] = pd.to_datetime(self.data['timestamp'], unit='ns')
            elif len(str(self.data['timestamp'].iloc[0])) >= 16:  # Microseconds
                self.data['timestamp'] = pd.to_datetime(self.data['timestamp'], unit='us')
            elif len(str(self.data['timestamp'].iloc[0])) >= 13:  # Milliseconds
                self.data['timestamp'] = pd.to_datetime(self.data['timestamp'], unit='ms')
            else:  # Seconds
                self.data['timestamp'] = pd.to_datetime(self.data['timestamp'], unit='s')
            
        # Add date component for easier grouping
        self.data['date'] = self.data['timestamp'].dt.date
        
        # Sort data
        self.data = self.data.sort_values(['geohash', 'timestamp'])
        
        # Cache the original geohash precision
        self.original_precision = len(self.data['geohash'].iloc[0].split('.')[0])
    
    def truncate_geohash(self, precision):
        """
        Create a new column with truncated geohash at specified precision.
        
        Parameters:
        -----------
        precision : int
            The desired geohash precision level.
            
        Returns:
        --------
        self : KdProcessor
            For method chaining.
        """
        # Create a new column with truncated geohash
        self.data['geohash_p{}'.format(precision)] = self.data['geohash'].apply(
            lambda gh: self._truncate_geohash_string(gh, precision)
        )
        return self
    
    def _truncate_geohash_string(self, geohash, precision):
        """
        Helper method to truncate a geohash string to specified precision.
        
        Parameters:
        -----------
        geohash : str
            Original geohash string (may include a period)
        precision : int
            Desired precision
            
        Returns:
        --------
        str : Truncated geohash
        """
        # Handle geohashes that may contain a period
        if '.' in geohash:
            parts = geohash.split('.')
            main_part = parts[0]
            # Ensure we don't exceed the available length
            if precision <= len(main_part):
                return main_part[:precision]
            else:
                # If asking for more precision than available, return original
                return geohash
        else:
            # Simple truncation for standard geohashes
            return geohash[:precision]
    
    def calculate_interval_differences(self, interval_days, precision=None, start_date=None, end_date=None):
        """
        Calculate the differences in kd_490 values at regular intervals.
        
        Parameters:
        -----------
        interval_days : int
            Number of days between measurements (e.g., 5, 10)
        precision : int, optional
            Geohash precision to use. If None, uses original geohash.
        start_date : datetime or str, optional
            Start date for analysis. If None, uses earliest date.
        end_date : datetime or str, optional
            End date for analysis. If None, uses latest date.
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with interval differences and statistics.
        """
        # Make a copy to avoid modifying original data
        working_data = self.data.copy()
        
        # Filter by date range if specified
        if start_date is not None:
            if isinstance(start_date, str):
                start_date = pd.to_datetime(start_date)
            working_data = working_data[working_data['timestamp'] >= start_date]
            
        if end_date is not None:
            if isinstance(end_date, str):
                end_date = pd.to_datetime(end_date)
            working_data = working_data[working_data['timestamp'] <= end_date]
        
        # Determine which geohash column to use
        if precision is not None:
            # Create truncated geohash if it doesn't exist
            geohash_col = f'geohash_p{precision}'
            if geohash_col not in working_data.columns:
                self.truncate_geohash(precision)
                working_data = self.data.copy()  # Get updated data with new column
                # Apply date filters again if needed
                if start_date is not None:
                    working_data = working_data[working_data['timestamp'] >= start_date]
                if end_date is not None:
                    working_data = working_data[working_data['timestamp'] <= end_date]
        else:
            geohash_col = 'geohash'
        
        # Create a date range with specified interval
        if start_date is None:
            start_date = working_data['timestamp'].min()
        if end_date is None:
            end_date = working_data['timestamp'].max()
            
        # Generate the dates at interval_days apart
        interval_dates = pd.date_range(start=start_date, end=end_date, freq=f'{interval_days}D')
        
        # Group by geohash and find nearest value for each interval date
        results = []
        
        for gh in working_data[geohash_col].unique():
            # Get data for this geohash
            gh_data = working_data[working_data[geohash_col] == gh]
            
            interval_values = []
            for interval_date in interval_dates:
                # Find the closest measurement to this date
                closest_idx = (gh_data['timestamp'] - interval_date).abs().idxmin()
                closest_record = gh_data.loc[closest_idx]
                
                # Only use if within 1 day of the target date
                time_diff = abs((closest_record['timestamp'] - interval_date).total_seconds())
                if time_diff <= 86400:  # Within 1 day (86400 seconds)
                    interval_values.append({
                        'geohash': gh,
                        'interval_date': interval_date,
                        'actual_date': closest_record['timestamp'],
                        'kd_490': closest_record['kd_490'],
                        'longitude': closest_record['longitude'],
                        'latitude': closest_record['latitude']
                    })
            
            # Calculate differences between consecutive intervals
            if len(interval_values) >= 2:
                for i in range(1, len(interval_values)):
                    diff = interval_values[i]['kd_490'] - interval_values[i-1]['kd_490']
                    results.append({
                        'geohash': gh,
                        'start_date': interval_values[i-1]['interval_date'],
                        'end_date': interval_values[i]['interval_date'],
                        'start_kd': interval_values[i-1]['kd_490'],
                        'end_kd': interval_values[i]['kd_490'],
                        'diff': diff,
                        'abs_diff': abs(diff),
                        'pct_change': (diff / interval_values[i-1]['kd_490']) * 100 if interval_values[i-1]['kd_490'] != 0 else np.nan,
                        'longitude': interval_values[i]['longitude'],
                        'latitude': interval_values[i]['latitude']
                    })
        
        # Convert to DataFrame
        if not results:
            return pd.DataFrame()
            
        diff_df = pd.DataFrame(results)
        
        # Calculate summary statistics for each geohash
        summary = diff_df.groupby('geohash').agg({
            'diff': ['mean', 'std', 'min', 'max', 'count'],
            'abs_diff': ['mean', 'median'],
            'pct_change': ['mean', 'std', 'min', 'max']
        }).reset_index()
        
        # Flatten the column names
        summary.columns = ['_'.join(col).strip() for col in summary.columns.values]
        
        # Rename the geohash column back to just 'geohash'
        summary.rename(columns={'geohash_': 'geohash'}, inplace=True)
        
        # Calculate the average latitude and longitude for each geohash
        geo_info = diff_df.groupby('geohash').agg({
            'longitude': 'mean',
            'latitude': 'mean'
        }).reset_index()
        
        # Merge the summary with geo info
        summary = pd.merge(summary, geo_info, on='geohash')
        
        # Calculate variance of differences
        summary['variance'] = diff_df.groupby('geohash')['diff'].var().values
        
        return {
            'differences': diff_df,
            'summary': summary
        }
    
    def get_variance_by_location(self, interval_days, precision=None, start_date=None, end_date=None):
        """
        Get the variance of kd_490 differences by location, suitable for mapping.
        
        Parameters:
        -----------
        interval_days : int
            Number of days between measurements
        precision : int, optional
            Geohash precision to use
        start_date, end_date : datetime or str, optional
            Date range for analysis
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with variance statistics by location.
        """
        results = self.calculate_interval_differences(
            interval_days=interval_days,
            precision=precision,
            start_date=start_date,
            end_date=end_date
        )
        
        if 'summary' in results:
            return results['summary']
        return pd.DataFrame()
    
    def get_time_series_data(self, geohash, precision=None, start_date=None, end_date=None):
        """
        Get time series data for a specific geohash or geohash prefix.
        
        Parameters:
        -----------
        geohash : str
            Geohash or geohash prefix to filter by
        precision : int, optional
            If provided, will match based on truncated geohash
        start_date, end_date : datetime or str, optional
            Date range for filtering
            
        Returns:
        --------
        pandas.DataFrame
            Time series data for the specified geohash(es).
        """
        # Make a copy to avoid modifying original data
        working_data = self.data.copy()
        
        # Filter by date range if specified
        if start_date is not None:
            if isinstance(start_date, str):
                start_date = pd.to_datetime(start_date)
            working_data = working_data[working_data['timestamp'] >= start_date]
            
        if end_date is not None:
            if isinstance(end_date, str):
                end_date = pd.to_datetime(end_date)
            working_data = working_data[working_data['timestamp'] <= end_date]
        
        # Determine how to filter based on geohash
        if precision is not None:
            # Create truncated geohash if it doesn't exist
            geohash_col = f'geohash_p{precision}'
            if geohash_col not in working_data.columns:
                self.truncate_geohash(precision)
                working_data = self.data.copy()
                # Apply date filters again
                if start_date is not None:
                    working_data = working_data[working_data['timestamp'] >= start_date]
                if end_date is not None:
                    working_data = working_data[working_data['timestamp'] <= end_date]
            
            # Filter by truncated geohash
            filtered_data = working_data[working_data[geohash_col] == geohash]
        else:
            # Filter by exact or prefix match on original geohash
            filtered_data = working_data[working_data['geohash'].str.startswith(geohash)]
        
        # Sort by timestamp
        filtered_data = filtered_data.sort_values('timestamp')
        
        return filtered_data

# Usage example:
# processor = KdProcessor()
# processor.load_data(df)
# 
# # Calculate differences at 10-day intervals using 5-digit geohash precision
# results = processor.calculate_interval_differences(
#     interval_days=10,
#     precision=5,
#     start_date='2023-01-01',
#     end_date='2023-03-31'
# )
# 
# # Get time series for a specific location
# ts_data = processor.get_time_series_data(
#     geohash='9q8yy',
#     precision=5
# )