import math

ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

def int_to_base62(n, width, alphabet=ALPHABET):
    if n < 0:
        raise ValueError("Negative numbers not supported")
    result = ""
    while n:
        n, rem = divmod(n, 62)
        result = alphabet[rem] + result
    result = result or alphabet[0]
    return result.rjust(width, alphabet[0])

def base62_to_int(s, alphabet=ALPHABET):
    n = 0
    for char in s:
        n = n * 62 + alphabet.index(char)
    return n

def encode_lossless_geohash(lat, lng, precision, filename):
    if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
        if not -90 <= lat <= 90:
            print(f"    {filename} has invalid latitude, {str(lat)}. Unable to generate geohash.")
            return None
        if not -180 <= lng <= 180:
            print(f"    {filename} has invalid longitude, {str(lat)}. Unable to generate geohash.")
            return None
        scale = 10 ** precision
        lat_num = round((lat + 90) * scale)
        lng_num = round((lng + 180) * scale)
        width_lat = math.ceil(math.log(180 * scale + 1, 62))
        width_lng = math.ceil(math.log(360 * scale + 1, 62))
        width = max(width_lat, width_lng)
        lat_code = int_to_base62(lat_num, width)
        lng_code = int_to_base62(lng_num, width)
        return f"{lat_code}.{lng_code}"
    else:
        print(f"    Non-numeric latitude({lat}) and/or longitude({lng}). Unable to generate geohash.")
        return None

def decode_lossless_geohash(code, precision):
    if not code or '.' not in code:
        raise ValueError("Invalid code format, expected 'latcode.lngcode'")
    lat_code, lng_code = code.split('.')
    scale = 10 ** precision
    lat_num = base62_to_int(lat_code)
    lng_num = base62_to_int(lng_code)
    lat = lat_num / scale - 90
    lng = lng_num / scale - 180
    return lat, lng

