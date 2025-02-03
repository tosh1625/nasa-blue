import pandas as pd
import geohash2

def convert_to_geohash(latitude, longitude, precision):
    if -180 <= latitude <= 180 and -90 <= longitude <= 90:
        return geohash2.encode(latitude, longitude, precision)
    else:
        return geohash2.encode(0, 0, precision)

def generate_geohash(spreadsheet, precision_list, remove_column_name_spaces=True):

    # Ensure list does not contain invalid precision
    assert all(3 <= e < 9 for e in precision_list), "Geohash precisions need to be between 3 and 9"

    # Get spreadsheet extension
    file_extension = spreadsheet.split(".")[-1]

    if file_extension == 'xlsx':
        df = pd.read_excel(spreadsheet)
    elif file_extension =='csv':
        df = pd.read_csv(spreadsheet)
    else:
        raise ValueError('Use .xlsx or .csv format')

    # Remove column name spaces
    if remove_column_name_spaces:
        df.columns = df.columns.str.replace(' ', '', regex=False)

    # Check if latitude and longitude columns exist
    assert 'latitude' in df.columns or 'lat' in df.columns, "No latitude column in spreadsheet"
    assert 'longitude' in df.columns or 'lon' in df.columns or 'lng' in df.columns, "No longitude column in spreadsheet"

    for p in precision_list:
        df['geohash_p' + str(p)] = df.apply(lambda row: convert_to_geohash(row['latitude'], row['longitude'], p), axis=1)

    # Export DataFrame as spreadsheet
    if file_extension == 'xlsx':
        df.to_excel('spreadsheet with geohash.' + file_extension, index=False)
    elif file_extension == 'csv':
        df.to_csv('spreadsheet with geohash.' + file_extension, index=False)

generate_geohash('your spreadsheet name', [7, 8, 9])