#%%
import pygeohash as pgh
import pandas as pd
import numpy as np
import geohash2
# from .lossless_geohash import decode_lossless_geohash

#%%

######################################################## Handles GeoHash Data and provides bounding box ######################################################################
class GeohashHandler:
    """
    A simple utility class for handling geohash operations.
    This class is stateless and provides methods to get coordinates and bounding boxes.
    It assumes access to teammate's decode_lossless_geohash function.
    """
    
    # def __init__(self, custom_decoder=None):
    #     """Initialize the GeohashHandler."""
    #     if custom_decoder is None:
    #         # Import here to avoid circular imports
    #         from .lossless_geohash import decode_lossless_geohash
    #         self.custom_decoder = decode_lossless_geohash
    #     else:
    #         self.custom_decoder = custom_decoder
    def __init__(self):
        pass
    
    def get_coordinates(self, geohash_str, precision=5):
        """
        Get center coordinates from geohash.
        
        Parameters:
        -----------
        geohash_str : str
            Geohash string (custom or standard)
        precision : int, optional
            Precision for custom decoder (default: 5)
            
        Returns:
        --------
        dict
            Dictionary with latitude and longitude
        """
        # if self.custom_decoder and '.' in geohash_str:
        #     # Use custom decoder for format like "HD9Td.PFTWP"
        #     lat, lng = self.custom_decoder(geohash_str, precision)
        # else:
            # Use standard geohash2 for regular geohashes
            # If there's a period, take only the part before it
        if '.' in geohash_str:
            geohash_str = geohash_str.split('.')[0]
        lat, lng = geohash2.decode(geohash_str)
            
        return {
            'latitude': lat,
            'longitude': lng
        }
    
    def get_bounding_box(self, geohash_str, standard_precision=6):
        """
        Get bounding box for a location.
        
        Parameters:
        -----------
        geohash_str : str
            Geohash string (custom or standard)
        standard_precision : int, optional
            Precision for standard geohash (default: 6)
            
        Returns:
        --------
        dict
            Dictionary with min_lat, max_lat, min_lng, max_lng
        """
        # Get center coordinates first
        coords = self.get_coordinates(geohash_str)
        
        # Create a standard geohash at the desired precision
        std_geohash = geohash2.encode(
            float(coords['latitude']), 
            float(coords['longitude']), 
            standard_precision
        )
        
        # Get the bounding box
        min_lng, min_lat, max_lng, max_lat = geohash2.decode_exactly(std_geohash)
        
        return {
            'min_lat': min_lat,
            'max_lat': max_lat,
            'min_lng': min_lng,
            'max_lng': max_lng,
            'center_lat': coords['latitude'],
            'center_lng': coords['longitude'],
            'std_geohash': std_geohash
        }
    
    def process_dataframe(self, df, geohash_col, value_col=None, precision=5, standard_precision=6):
        """
        Process a dataframe to add coordinates and bounding boxes.
        
        Parameters:
        -----------
        df : pandas.DataFrame
            DataFrame containing the data
        geohash_col : str
            Column name containing geohashes
        value_col : str, optional
            Column name containing values to preserve
        precision : int, optional
            Precision for custom decoder (default: 5)
        standard_precision : int, optional
            Precision for standard geohash (default: 6)
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with added columns for coordinates and bounding boxes
        """
        result_df = df.copy()
        
        # Add coordinates and bounding boxes
        coords_and_boxes = []
        for gh in df[geohash_col]:
            try:
                # Get center coordinates
                coords = self.get_coordinates(gh, precision)
                
                # Get bounding box
                bbox = self.get_bounding_box(gh, standard_precision)
                
                coords_and_boxes.append({
                    'latitude': coords['latitude'],
                    'longitude': coords['longitude'],
                    'min_lat': bbox['min_lat'],
                    'max_lat': bbox['max_lat'],
                    'min_lng': bbox['min_lng'],
                    'max_lng': bbox['max_lng'],
                    'center_lat':coords['latitude'],
                    'center_lng':coords['longitude'],
                    'std_geohash': bbox['std_geohash']
                })
            except Exception as e:
                coords_and_boxes.append({
                    'latitude': None,
                    'longitude': None,
                    'min_lat': None,
                    'max_lat': None,
                    'min_lng': None,
                    'max_lng': None,
                    'center_lat':None,
                    'center_lng':None,
                    'std_geohash': None
                })
        
        # Add new columns to the dataframe
        for key in coords_and_boxes[0].keys():
            result_df[key] = [cb[key] for cb in coords_and_boxes]
        
        return result_df


######################################################## Date Handler ######################################################################

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class DateHandler:
    """
    A utility class for handling date and timestamp operations.
    This class is stateless and provides methods to convert between timestamps 
    and datetime objects, and to find nearest dates in a series.
    """
    
    def __init__(self, time_unit='ns'):
        """
        Initialize the DateHandler.
        
        Parameters:
        -----------
        time_unit : str, optional
            Unit of the timestamps ('ns' for nanoseconds, 'ms' for milliseconds, etc.)
            Default is 'ns' for nanosecond precision.
        """
        self.time_unit = time_unit
    
    def timestamp_to_datetime(self, timestamp):
        """
        Convert a timestamp to a datetime object.
        
        Parameters:
        -----------
        timestamp : int or float
            Timestamp in the specified time unit
            
        Returns:
        --------
        datetime
            Converted datetime object
        """
        if self.time_unit == 'ns':
            # Convert nanosecond timestamp to seconds first
            timestamp_seconds = timestamp / 1e9
            return datetime.fromtimestamp(timestamp_seconds)
        else:
            # For other units, use pandas which handles various time units
            return pd.Timestamp(timestamp, unit=self.time_unit).to_pydatetime()
    
    def datetime_to_timestamp(self, dt):
        """
        Convert a datetime object to a timestamp.
        
        Parameters:
        -----------
        dt : datetime
            Datetime object to convert
            
        Returns:
        --------
        int or float
            Timestamp in the specified time unit
        """
        if self.time_unit == 'ns':
            # Convert to nanoseconds
            return int(dt.timestamp() * 1e9)
        else:
            # For other units, use pandas
            return pd.Timestamp(dt).astype('int64')
    
    def create_date_sequence(self, start_date, end_date, interval_days):
        """
        Create a sequence of dates at regular intervals.
        
        Parameters:
        -----------
        start_date : datetime or timestamp
            Start date of the sequence
        end_date : datetime or timestamp
            End date of the sequence
        interval_days : int
            Interval between dates in days
            
        Returns:
        --------
        list
            List of datetime objects at regular intervals
        """
        # Convert to datetime if timestamps were provided
        if isinstance(start_date, (int, float)):
            start_date = self.timestamp_to_datetime(start_date)
        if isinstance(end_date, (int, float)):
            end_date = self.timestamp_to_datetime(end_date)
        
        # Create sequence
        date_sequence = []
        current_date = start_date
        
        while current_date <= end_date:
            date_sequence.append(current_date)
            current_date += timedelta(days=interval_days)
            
        return date_sequence
    
    def find_nearest_date(self, target_date, available_dates):
        """
        Find the nearest date to a target date from a list of available dates.
        
        Parameters:
        -----------
        target_date : datetime or timestamp
            The target date to find the nearest match for
        available_dates : list or array-like
            List of available dates (datetime objects or timestamps)
            
        Returns:
        --------
        datetime or timestamp
            The nearest date from the available dates
        int
            Index of the nearest date in the available_dates list
        """
        # Convert to datetime if timestamps were provided
        if isinstance(target_date, (int, float)):
            target_date = self.timestamp_to_datetime(target_date)
            
        # Convert all available dates to datetime if they are timestamps
        dates_for_comparison = []
        for date in available_dates:
            if isinstance(date, (int, float)):
                dates_for_comparison.append(self.timestamp_to_datetime(date))
            else:
                dates_for_comparison.append(date)
        
        # Calculate time differences
        time_diffs = np.abs([
            (date - target_date).total_seconds() 
            for date in dates_for_comparison
        ])
        
        # Find index of minimum difference
        nearest_idx = np.argmin(time_diffs)
        
        # Return original format
        return available_dates[nearest_idx], nearest_idx
    
    def process_dataframe(self, df, timestamp_col):
        """
        Process a dataframe to add human-readable date columns.
        
        Parameters:
        -----------
        df : pandas.DataFrame
            DataFrame containing the data
        timestamp_col : str
            Column name containing timestamps
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with added date columns
        """
        result_df = df.copy()
        
        # Convert timestamps to datetime
        result_df['datetime'] = result_df[timestamp_col].apply(self.timestamp_to_datetime)
        
        # Add date components
        result_df['date'] = result_df['datetime'].dt.date
        result_df['year'] = result_df['datetime'].dt.year
        result_df['month'] = result_df['datetime'].dt.month
        result_df['day'] = result_df['datetime'].dt.day
        result_df['hour'] = result_df['datetime'].dt.hour
        result_df['minute'] = result_df['datetime'].dt.minute
        
        return result_df
    
    def group_by_time_period(self, df, timestamp_col, period='day'):
        """
        Group data by time periods and calculate statistics.
        
        Parameters:
        -----------
        df : pandas.DataFrame
            DataFrame containing the data
        timestamp_col : str
            Column name containing timestamps
        period : str, optional
            Time period for grouping ('day', 'week', 'month')
            Default is 'day'
            
        Returns:
        --------
        pandas.DataFrame
            Grouped DataFrame with statistics
        """
        # Process dataframe to add datetime columns if they don't exist
        if 'datetime' not in df.columns:
            df = self.process_dataframe(df, timestamp_col)
        
        # Define grouping
        if period == 'day':
            grouped = df.groupby(df['datetime'].dt.date)
        elif period == 'week':
            grouped = df.groupby(df['datetime'].dt.isocalendar().week)
        elif period == 'month':
            grouped = df.groupby([df['datetime'].dt.year, df['datetime'].dt.month])
        else:
            raise ValueError(f"Unsupported period: {period}. Use 'day', 'week', or 'month'.")
            
        return grouped