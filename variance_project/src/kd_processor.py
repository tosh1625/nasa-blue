######################################################## Processors to handle Kd Values and aggregate across time-scales ######################################################################

class KdProcessor:
    def __init__(self, date_handler, geohash_handler):
        self.date_handler = date_handler
        self.geohash_handler = geohash_handler
        self.df = None


    def load_data(self,data):
        self.df = data

    @staticmethod
    def check_columns(df, columns=None):
        for c in columns:
            assert c in list(df.columns), f'{c} column is not in columns'

    def process_timestamp(self, timestamp):
        """Process a single timestamp value and return datetime components."""
        dt = self.date_handler.timestamp_to_datetime(timestamp)
        return {
            'datetime': dt,
            'year': dt.year,
            'month': dt.month,
            'day': dt.day
        }
    
    
    
    def generate_bbox(self,gh_str, precision=5):
        self.geohash_handler.get_bounding_box(self,gh_str)

    def determine_season(self, lat, datetime_obj):
        """
        Determine season based on latitude and datetime.
        
        Parameters:
        -----------
        lat : float
            Latitude (-90 to 90)
        datetime_obj : datetime
            Date to check
        
        Returns:
        --------
        str
            'summer', 'fall', 'winter', or 'spring'
        """
        # Get month and day
        month = datetime_obj.month
        day = datetime_obj.day
        
        # Northern hemisphere seasons if lat >= 0, otherwise Southern hemisphere
        is_northern = lat >= 0
        
        # Calculate day of year (0-365)
        day_of_year = datetime_obj.timetuple().tm_yday
        
        if is_northern:
            if (month == 3 and day >= 20) or (month > 3 and month < 6) or (month == 6 and day < 21):
                return 'spring'
            elif (month == 6 and day >= 21) or (month > 6 and month < 9) or (month == 9 and day < 22):
                return 'summer'
            elif (month == 9 and day >= 22) or (month > 9 and month < 12) or (month == 12 and day < 21):
                return 'fall'
            else:
                return 'winter'
        else:
            # Southern hemisphere (seasons are reversed)
            if (month == 3 and day >= 20) or (month > 3 and month < 6) or (month == 6 and day < 21):
                return 'fall'
            elif (month == 6 and day >= 21) or (month > 6 and month < 9) or (month == 9 and day < 22):
                return 'winter'
            elif (month == 9 and day >= 22) or (month > 9 and month < 12) or (month == 12 and day < 21):
                return 'spring'
            else:
                return 'summer'
        
    ######################################################## Coordinate stuff ######################################################################
    def process_coordinates(self, lat, lon, precision=5):
        """Process coordinates and return geohash at specified precision."""
        return self.geohash_handler.get_bounding_box(lat, lon, standard_precision=precision)
                
    
    def preprocess(self, df = None, process_dates=True, process_geohashes=True, precision=5):
        """Apply processing to a dataframe."""
        self.precision = precision
        if not df:
            df = self.df
        result = df.copy()
        
        if process_dates and 'timestamp' in df.columns:
            date_data = df['timestamp'].apply(self.process_timestamp)
            result['datetime'] = date_data.apply(lambda x: x['datetime'])
            result['year'] = date_data.apply(lambda x: x['year'])
            result['month'] = date_data.apply(lambda x: x['month'])
            result['day'] = date_data.apply(lambda x: x['day'])

            # Add season calculation
            result['season'] = result.apply(
                lambda row: self.determine_season(row['latitude'], row['datetime']),
                axis=1
        )
            
        if process_geohashes and 'latitude' in df.columns and 'longitude' in df.columns:
        # Apply the function to get dictionaries for each row
            geohash_data = df.apply(
                lambda row: self.process_coordinates(row['latitude'], row['longitude'], precision),
                axis=1
            )
            
            # Extract each field from the dictionaries and create new columns
            result['geohash_std'] = geohash_data.apply(lambda x: x['std_geohash'])
            result['min_lat'] = geohash_data.apply(lambda x: x['min_lat'])
            result['max_lat'] = geohash_data.apply(lambda x: x['max_lat'])
            result['min_lng'] = geohash_data.apply(lambda x: x['min_lng'])
            result['max_lng'] = geohash_data.apply(lambda x: x['max_lng'])
            result['center_lat'] = geohash_data.apply(lambda x: x['center_lat'])
            result['center_lng'] = geohash_data.apply(lambda x: x['center_lng'])
        
        self.processed_df = result
        return result
    

class KdAggregate:
    def __init__(self, data, window_days=10, precision=5):
        """
        Initialize KdAggregate with configuration parameters.
        
        Parameters:
        -----------
        window_days : int
            Rolling window size in days to check for readings
        precision : int
            Geohash precision level to use for aggregation
        """
        self.df = data
        self.window_days = window_days
        self.precision = precision
        self.filtered_data = None
        self.aggregated_data = None
    
    def filter_by_date_range(self, df, start_date=None, end_date=None):
        """
        Filter dataframe by date range.
        
        Parameters:
        -----------
        df : pandas.DataFrame
            DataFrame to process
        start_date : str or datetime, optional
            Start date for filtering (inclusive)
        end_date : str or datetime, optional
            End date for filtering (inclusive)
            
        Returns:
        --------
        self
            Returns self for method chaining
        """
        # Ensure datetime column exists
        if 'datetime' not in df.columns:
            raise ValueError("DataFrame must have 'datetime' column. Run preprocess() first.")
        
        # Apply date filtering if provided
        filtered_df = df.copy()
        
        if start_date is not None:
            if isinstance(start_date, str):
                start_date = pd.to_datetime(start_date)
            filtered_df = filtered_df[filtered_df['datetime'] >= start_date]
        
        if end_date is not None:
            if isinstance(end_date, str):
                end_date = pd.to_datetime(end_date)
            filtered_df = filtered_df[filtered_df['datetime'] <= end_date]
        
        self.filtered_data = filtered_df
        return self


    def calculate_kd_differences(self, value_column='kd_490'):
        """
        Calculate differences between Kd values at intervals specified by window_days.
        
        Parameters:
        -----------
        value_column : str
            Column containing Kd values to analyze
            
        Returns:
        --------
        self
            Returns self for method chaining
        """
        # Use filtered data if available, otherwise use the original data
        df_to_use = self.filtered_data if self.filtered_data is not None else self.df
        
        # Create geohash lookup table (one entry per unique geohash)
        geohash_lookup = df_to_use.drop_duplicates('geohash_std')[
            ['geohash_std', 'min_lat', 'max_lat', 'min_lng', 'max_lng', 'center_lat', 'center_lng']
        ]
        
        # Group by geohash
        grouped = df_to_use.groupby('geohash_std')
        
        # Prepare results container
        diff_results = []
        
        # Process each geohash group
        for geohash, group in grouped:
            # Sort by datetime
            group = group.sort_values('datetime')
            
            # Check if we have enough readings
            if len(group) >= 2:
                # Create a list to store the datetime and value pairs
                time_series = list(zip(group['datetime'], group[value_column]))
                
                # Calculate differences based on window_days
                for i in range(len(time_series) - 1):
                    current_dt, current_value = time_series[i]
                    
                    # Find the next reading that's at least window_days later
                    for j in range(i + 1, len(time_series)):
                        next_dt, next_value = time_series[j]
                        days_diff = (next_dt - current_dt).days
                        
                        # If this reading is approximately window_days later, calculate difference
                        if days_diff >= self.window_days:
                            # Calculate the difference in Kd values
                            kd_diff = next_value - current_value
                            kd_pct_change = (kd_diff / current_value) * 100 if current_value != 0 else float('inf')
                            
                            result = {
                                'geohash_std': geohash,
                                'start_date': current_dt,
                                'end_date': next_dt,
                                'days_between': days_diff,
                                f'{value_column}_start': current_value,
                                f'{value_column}_end': next_value,
                                f'{value_column}_diff': kd_diff,
                                f'{value_column}_pct_change': kd_pct_change
                            }
                            
                            diff_results.append(result)
                            break  # Move to the next starting point
        
        # Convert results to DataFrame
        diff_df = pd.DataFrame(diff_results) if diff_results else pd.DataFrame()
        
        # Merge with the geohash lookup table
        if not diff_df.empty:
            self.aggregated_data = pd.merge(diff_df, geohash_lookup, on='geohash_std')
        else:
            self.aggregated_data = diff_df
        
        return self

    def aggregate_differences(self, value_column='kd_490'):
        """
        Aggregate the calculated differences by geohash.
        
        Parameters:
        -----------
        value_column : str
            Base column name used for difference calculations
            
        Returns:
        --------
        self
            Returns self for method chaining
        """
        # Check if differences have been calculated
        if self.aggregated_data is None:
            self.calculate_kd_differences(value_column = value_column)
        
        diff_column = f'{value_column}_diff'
        pct_change_column = f'{value_column}_pct_change'
        
        if diff_column not in self.aggregated_data.columns:
            raise ValueError(f"Difference column '{diff_column}' not found. Ensure you've calculated differences.")
        
        # Select the columns to aggregate
        columns_to_agg = [diff_column, pct_change_column]
        
        # Group by geohash and calculate mean
        summary = self.aggregated_data.groupby('geohash_std')[columns_to_agg].mean().reset_index()
        
        # Merge with the geospatial data (keeping only one row per geohash)
        geospatial_columns = ['min_lat', 'max_lat', 'min_lng', 'max_lng', 'center_lat', 'center_lng']
        geohash_lookup = self.aggregated_data.drop_duplicates('geohash_std')[
            ['geohash_std'] + geospatial_columns
        ]
        
        # Merge to get the final summary
        self.summary_data = pd.merge(summary, geohash_lookup, on='geohash_std')
        
        return self