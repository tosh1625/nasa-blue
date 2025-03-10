import pygeohash as pgh
import pandas as pd
import numpy as np
import geohash2
from lossless_geohash import *



class GeohashHandler:
    """
    A simple utility class for handling geohash operations.
    This class is stateless and provides methods to get coordinates and bounding boxes.
    It assumes access to teammate's decode_lossless_geohash function.
    """
    
    def __init__(self, custom_decoder=decode_lossless_geohash):
        """
        Initialize the GeohashHandler.
        
        Parameters:
        -----------
        custom_decoder : function, optional
            Function to decode custom geohash format.
            Should accept geohash string and precision, and return (lat, lng).
            If None, standard geohash2 decoding will be used.
        """
        self.custom_decoder = custom_decoder
    
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
        if self.custom_decoder and '.' in geohash_str:
            # Use custom decoder for format like "HD9Td.PFTWP"
            lat, lng = self.custom_decoder(geohash_str, precision)
        else:
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
            coords['latitude'], 
            coords['longitude'], 
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
                    'std_geohash': None
                })
        
        # Add new columns to the dataframe
        for key in coords_and_boxes[0].keys():
            result_df[key] = [cb[key] for cb in coords_and_boxes]
        
        return result_df
