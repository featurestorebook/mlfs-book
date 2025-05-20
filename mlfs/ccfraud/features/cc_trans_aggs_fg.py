import hopsworks
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import math
import geoip2.database
from datetime import datetime


def prev_ts_transaction(df: pd.DataFrame) -> pd.DataFrame:
    """
    df must have a 'card_id' and 'transaction_time' columns.
    """
    window_spec = Window.partitionBy("card_id").orderBy("transaction_time")

    df = df.withColumn("prev_transaction_time", F.lag(F.col("transaction_time"), 1).over(window_spec))

    return df

def prev_ip_transaction(df: pd.DataFrame) -> pd.DataFrame:
    """
    df must have a 'card_id' and 'transaction_time' columns.
    """
    window_spec = Window.partitionBy("card_id").orderBy("transaction_time")

    df = df.withColumn("prev_transaction_ip", F.lag(F.col("ip_addr"), 1).over(window_spec))

    return df


def haversine_distance(lon1, lat1, timestamp1, lon2, lat2, timestamp2):
    """
    Calculate the Haversine distance between two geographical points.

    Parameters:
    - lon1, lat1: Longitude and latitude of the source point in decimal degrees
    - timestamp1: Timestamp of the source point (unused)
    - lon2, lat2: Longitude and latitude of the destination point in decimal degrees
    - timestamp2: Timestamp of the destination point (unused)

    Returns:
    - Distance in kilometers between the two points.
    """
    # Earth radius in kilometers
    R = 6371.0

    # Convert decimal degrees to radians
    lon1_rad, lat1_rad = math.radians(lon1), math.radians(lat1)
    lon2_rad, lat2_rad = math.radians(lon2), math.radians(lat2)

    # Haversine formula
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c

    return distance



def haversine_distance_transactions(ip_addr1, timestamp1, ip_addr2, timestamp2):
    """
    Calculate the haversine distance between two transactions and their
    timestamps to determine "impossible travel" fraud patterns.

    Args:
        ip_addr1 (str): IP address of the first transaction
        timestamp1 (str): Timestamp of the first transaction (ISO format)
        ip_addr2 (str): IP address of the second transaction
        timestamp2 (str): Timestamp of the second transaction (ISO format)

    Returns:
        tuple: (distance_km, time_diff_hours, speed_kmh) where:
            - distance_km is the haversine distance in kilometers
            - time_diff_hours is the time difference in hours
            - speed_kmh is the implied travel speed in km/h
    """
    # First convert IP addresses to geo coordinates
    lat1, lon1 = ip_to_coordinates(ip_addr1)
    lat2, lon2 = ip_to_coordinates(ip_addr2)

    # Calculate haversine distance
    distance_km = haversine(lat1, lon1, lat2, lon2)

    # Calculate time difference in hours
    dt1 = datetime.fromisoformat(timestamp1)
    dt2 = datetime.fromisoformat(timestamp2)
    time_diff_seconds = abs((dt2 - dt1).total_seconds())
    time_diff_hours = time_diff_seconds / 3600

    # Calculate implied travel speed (km/h)
    speed_kmh = distance_km / time_diff_hours if time_diff_hours > 0 else float('inf')

    return distance_km, time_diff_hours, speed_kmh

def ip_to_coordinates(ip_address):
    """
    Convert an IP address to latitude and longitude using MaxMind GeoIP database.

    Args:
        ip_address (str): IP address to convert

    Returns:
        tuple: (latitude, longitude)
    """
    try:
        # You'll need to download the GeoLite2 database from MaxMind
        # https://dev.maxmind.com/geoip/geoip2/geolite2/
        reader = geoip2.database.Reader('GeoLite2-City.mmdb')
        response = reader.city(ip_address)
        return response.location.latitude, response.location.longitude
    except Exception as e:
        print(f"Error getting location for IP {ip_address}: {e}")
        return 0, 0  # Default to 0,0 (null island) on error

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance in kilometers between two points
    on the earth specified in decimal degrees.

    Args:
        lat1, lon1: Latitude and longitude of point 1 (in decimal degrees)
        lat2, lon2: Latitude and longitude of point 2 (in decimal degrees)

    Returns:
        float: Distance in kilometers
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of Earth in kilometers
    return c * r

def is_impossible_travel(distance_km, time_diff_hours, max_speed=1000):
    """
    Determine if the transactions represent impossible travel.

    Args:
        distance_km (float): Distance in kilometers
        time_diff_hours (float): Time difference in hours
        max_speed (float): Maximum feasible travel speed in km/h
                          (default: 1000 km/h which is approximately passenger jet speed)

    Returns:
        bool: True if the implied travel speed exceeds maximum feasible speed
    """
    if time_diff_hours == 0:
        return distance_km > 0  # Any non-zero distance in zero time is impossible

    speed = distance_km / time_diff_hours
    return speed > max_speed

# Example usage
if __name__ == "__main__":
    # Example transactions
    ip1 = "203.0.113.1"    # New York (example IP)
    time1 = "2023-03-15T10:30:00"

    ip2 = "198.51.100.42"  # Tokyo (example IP)
    time2 = "2023-03-15T11:30:00"  # 1 hour later

    distance, time_diff, speed = haversine_distance_transactions(ip1, time1, ip2, time2)

    print(f"Distance between transactions: {distance:.2f} km")
    print(f"Time between transactions: {time_diff:.2f} hours")
    print(f"Implied travel speed: {speed:.2f} km/h")

    if is_impossible_travel(distance, time_diff):
        print("FRAUD ALERT: Impossible travel detected!")
    else:
        print("Travel pattern is physically possible.")
