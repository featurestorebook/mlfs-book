import hopsworks
import pandas as pd
from math import radians
import numpy as np
from datetime import datetime
import requests
import os



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
def time_since_last_trans(transaction_time: pd.Series, prev_transaction_time: pd.Series) -> pd.Series:
    """
    """
    #TODO, when prev_transaction_time == None, return int.min
    return (transaction_time - prev_transaction_time).dt.total_seconds().astype(int)


@hopsworks.udf(int, drop=['prev_ip_addr'])
def haversine_distance(card_present, prev_card_present, 
                       ip_addr: pd.Series, prev_ip_addr: pd.Series,
                       time_since_last_trans: pd.Series) -> pd.Series:
    """
    """
    if card_present != True or prev_card_present != True :
        return 0

    distance_kms = haversine_distance_transactions(prev_ip_addr, ip_addr)
    
    return is_impossible_travel(distance_kms, time_since_last_trans)



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


def haversine_distance_transactions(ip_addr1: pd.Series , ip_addr2: pd.Series):
    """
    Calculate the haversine distance between two transactions and their
    timestamps to determine "impossible travel" fraud patterns.

    Args:
        ip_addr1 (str): IP address of the first transaction
        ip_addr2 (str): IP address of the second transaction

    Returns:
        - distance_km is the haversine distance in kilometers
    """
    # First convert IP addresses to geo coordinates
    lat1, lon1 = ip_to_coordinates(ip_addr1)
    lat2, lon2 = ip_to_coordinates(ip_addr2)

    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of Earth in kilometers
    return c * r

def ip_to_coordinates(root_dir, ip_address):
    """
    Uses MaxMind Database - 
    EULA: https://www.maxmind.com/en/geolite2/eula
    Convert an IP address to latitude and longitude using MaxMind GeoIP database.

    Args:
        ip_address (str): IP address to convert

    Returns:
        tuple: (latitude, longitude)
    """
    mmdb_path = f"{root_dir}/data/GeoLite2-City.mmdb"
    download_url = "https://repo.hops.works/dev/jdowling/GeoLite2-City.mmdb"
    
    mmdb_path = os.path.normpath(mmdb_path)    
    if not os.path.exists(mmdb_path):
        print(f"MMDB not found at {mmdb_path}. Downloading...")
        response = requests.get(download_url, timeout=30)
        response.raise_for_status()  # fail if download didn't work
        with open(mmdb_path, "wb") as f:
            f.write(response.content)        
        print(f"Downloaded database to: {mmdb_path}")
    
    try:
        # You'll need to download the GeoLite2 database from MaxMind
        # https://dev.maxmind.com/geoip/geoip2/geolite2/
        reader = geoip2.database.Reader(f"{root_dir}/data//GeoLite2-City.mmdb")
        response = reader.city(ip_address)
        return response.location.latitude, response.location.longitude
    except Exception as e:
        print(f"Error getting location for IP {ip_address}: {e}")
        return 0, 0  # Default to 0,0 (null island) on error


def is_impossible_travel(distance_km, time_since_last_trans, max_speed=1100.0):
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
    if time_since_last_trans == 0:
        return distance_km > 0  # Any non-zero distance in zero time is impossible

    speed = distance_km / (time_since_last_trans * 3600)  # Convert time to hours
    return speed > max_speed
