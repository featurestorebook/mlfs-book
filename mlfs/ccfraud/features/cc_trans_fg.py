import hopsworks
import pandas as pd
from math import radians
import numpy as np
from datetime import datetime




# @hopsworks.udf(int, drop=['prev_transaction_time'])
# def days_to_card_expiry(expiry_date: pd.Series) -> pd.Series:
#     """
#     """
#     bins = [0, 1, 10, 30, 90, 180, float('inf')]
#     labels = ['0-1', '2-10', '11-30', '31-90', '91-180', '181-']
#     binned_days_to_expiry = pd.cut(expiry_date, bins=bins, labels=labels)
#     return binned_days_to_expiry
    # return expiry_date.dt.date() - datetime.now.date()

@hopsworks.udf(int, hopsworks.training.Statistics)
def amount_deviation_from_avg(amount, statistics=statistics):
    """
    """
    return np.abs(amount - statistics.amount.mean)


@hopsworks.udf(int, drop=['prev_transaction_time'])
def time_since_last_trans(prev_transaction_time: pd.Series, transaction_time: pd.Series) -> pd.Series:
    """
    """
    return (transaction_time - prev_transaction_time).dt.total_seconds().astype(int)


# @hopsworks.udf(int, drop=['prev_ip_addr'])
# def haversine_distance(card_present, prev_card_present, 
#                        ip_addr: pd.Series, prev_ip_addr: pd.Series,
#                        time_since_last_trans: pd.Series) -> pd.Series:
#     """
#     """


#     if card_present != True or prev_card_present != True :
#         return 0
    
#     latitude, longitude = # get from ip_addr
#     prev_latitude, prev_longitude = # get from prev_ip_addr
#     latitude_diff = latitude - prev_latitude
#     longitude_diff = longitude - prev_longitude
#     a = np.sin(latitude_diff/2.0)**2
#     b = np.cos(latitude) * np.cos(prev_latitude) * np.sin(longitude_diff/2.0)**2
#     c = 2*np.arcsin(np.sqrt(a + b))

#     return c



def is_fraud(transactions_df: pd.DataFrame, cc_fraud_df: pd.DataFrame) -> pd.DataFrame:
    """
        if there is a label, set the value to be '1', otherwise '0'
    """
    labels_df = transactions_df.join(cc_fraud_df, on="transaction_id", how="left")
 
    # Make 'is_fraud' a label for identifying if a transaction is fraudulent or not
    labels_df = labels_df.withColumn("is_fraud", F.when(F.col("fraud_type").isNotNull(), 1).otherwise(0))

    # Drop cols that are not needed
    labels_df = labels_df.drop("fraud_report_id", "report_time", "fraud_type")

    return labels_df



# def haversine_distance(lon1, lat1, timestamp1, lon2, lat2, timestamp2):
#     """
#     Calculate the Haversine distance between two geographical points.

#     Parameters:
#     - lon1, lat1: Longitude and latitude of the source point in decimal degrees
#     - timestamp1: Timestamp of the source point (unused)
#     - lon2, lat2: Longitude and latitude of the destination point in decimal degrees
#     - timestamp2: Timestamp of the destination point (unused)

#     Returns:
#     - Distance in kilometers between the two points.
#     """
#     # Earth radius in kilometers
#     R = 6371.0

#     # Convert decimal degrees to radians
#     lon1_rad, lat1_rad = math.radians(lon1), math.radians(lat1)
#     lon2_rad, lat2_rad = math.radians(lon2), math.radians(lat2)

#     # Haversine formula
#     dlon = lon2_rad - lon1_rad
#     dlat = lat2_rad - lat1_rad

#     a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
#     c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

#     distance = R * c

#     return distance



@hopsworks.udf(bool)
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
