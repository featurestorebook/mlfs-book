import pandas as pd
import numpy as np
import os
import requests
import geoip2.database
from collections import Counter
import hopsworks
from hsfs.transformation_statistics import TransformationStatistics

root_dir=""


def time_since_last_trans(ts: pd.Series, prev_ts: pd.Series) -> pd.Series:
    """
    Calculate time difference in seconds between current and previous transaction.
    Returns 0 when there is no previous transaction (prev_ts is None/NaT).
    """
    # Compute time difference
    delta = (ts - prev_ts).dt.total_seconds()

    # Replace NaN / NaT differences with 0 and cast to int
    return delta.fillna(0).astype(int)

    
@hopsworks.udf(bool, mode="pandas", drop=['card_present', 'prev_card_present', 'ip_address', 'prev_ip_address'])
def haversine_distance(card_present: pd.Series, prev_card_present: pd.Series, 
                       ip_address: pd.Series, prev_ip_address: pd.Series, time_since_last_trans: pd.Series) -> pd.Series:
    """
    Determine if travel between transactions is feasible.
    Returns True if travel is feasible, False if not feasible.
    Only checks for impossible travel when card_present OR prev_card_present is True.
    """

    def _lookup_coordinates(reader, ip_address):
        """
        Lookup coordinates for an IP address using an existing GeoIP reader.
    
        Args:
            reader: An open geoip2.database.Reader instance
            ip_address (str): IP address to lookup
    
        Returns:
            tuple: (latitude, longitude) - returns (0.0, 0.0) if IP cannot be resolved
        """
        # Handle None or empty IP addresses
        if ip_address is None or ip_address == '' or pd.isna(ip_address):
            return (0.0, 0.0)
    
        try:
            response = reader.city(ip_address)
            lat = response.location.latitude
            lon = response.location.longitude
    
            # Handle cases where lat/lon might be None
            if lat is None or lon is None:
                return (0.0, 0.0)
    
            return (float(lat), float(lon))
        except Exception:
            # Silently handle errors for common issues (private IPs, invalid IPs, etc.)
            return (0.0, 0.0)  # Default to 0,0 (null island) on error
    
    
    
    def is_impossible_travel(distance_km: pd.Series, time_since_last_trans: pd.Series, 
                            max_speed: float = 1100.0) -> pd.Series:
        """
        Determine if the transactions represent impossible travel.
        
        Args:
            distance_km (pd.Series): Distance in kilometers
            time_since_last_trans (pd.Series): Time difference in seconds
            max_speed (float): Maximum feasible travel speed in km/h
        
        Returns:
            pd.Series: True if travel is impossible, False if feasible
        """
        # Handle zero time case
        zero_time = time_since_last_trans == 0
        
        # Calculate speed in km/h (time is in seconds, so divide by 3600)
        speed = distance_km / (time_since_last_trans / 3600)
        
        # Impossible if: (zero time AND non-zero distance) OR (speed exceeds max)
        impossible = (zero_time & (distance_km > 0)) | (speed > max_speed)
        
        return impossible

    
    # Create mask for rows where at least one transaction used a card
    check_travel = card_present | prev_card_present
    
    # Initialize all as feasible (True)
    result = pd.Series(True, index=ip_address.index)
    
    # Only check travel for rows where a card was present
    if check_travel.any():
        # Filter to only check relevant rows
        filtered_prev_ip = prev_ip_address[check_travel]
        filtered_ip = ip_address[check_travel]
        filtered_time = time_since_last_trans[check_travel]
        

        # Ensure database is downloaded and get path
        root_dir = "~"
        mmdb_path = f"{root_dir}/data/GeoLite2-City.mmdb"
        download_url = "https://repo.hops.works/dev/jdowling/GeoLite2-City.mmdb"
    
        mmdb_path = os.path.normpath(mmdb_path)
        if not os.path.exists(mmdb_path):
            print(f"MMDB not found at {mmdb_path}. Downloading...")
            try:
                response = requests.get(download_url, timeout=30)
                response.raise_for_status()
                os.makedirs(os.path.dirname(mmdb_path), exist_ok=True)
                with open(mmdb_path, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded database to: {mmdb_path}")
            except Exception as e:
                print(f"Error downloading database: {e}")
                raise
        
    
        # Open the GeoIP reader ONCE for all lookups
        reader = geoip2.database.Reader(mmdb_path)
    
        try:
            # Get coordinates for both IP series using the shared reader
            coords1 = filtered_prev_ip.apply(lambda ip: _lookup_coordinates(reader, ip))
            coords2 = filtered_ip.apply(lambda ip: _lookup_coordinates(reader, ip))
        finally:
            reader.close()  # Always close the reader
    
        # Extract lat/lon into separate arrays - handle None values
        lat1 = np.array([float(c[0]) if c is not None else 0.0 for c in coords1], dtype=np.float64)
        lon1 = np.array([float(c[1]) if c is not None else 0.0 for c in coords1], dtype=np.float64)
        lat2 = np.array([float(c[0]) if c is not None else 0.0 for c in coords2], dtype=np.float64)
        lon2 = np.array([float(c[1]) if c is not None else 0.0 for c in coords2], dtype=np.float64)
    
        # Convert to radians (vectorized)
        lat1_rad = np.radians(lat1)
        lon1_rad = np.radians(lon1)
        lat2_rad = np.radians(lat2)
        lon2_rad = np.radians(lon2)
    
        # Haversine formula (vectorized)
        dlon = lon2_rad - lon1_rad
        dlat = lat2_rad - lat1_rad
        a = np.sin(dlat/2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        r = 6371  # Radius of Earth in kilometers
    
        distance_kms = pd.Series(c * r, index=coords1.index)
     
        
        # Check if travel is impossible
        impossible = is_impossible_travel(distance_kms, filtered_time)
        
        # Mark impossible travel as not feasible (False)
        result.loc[check_travel] = ~impossible
    
    return result
