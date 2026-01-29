import pandas as pd
import numpy as np
import os
import requests
import geoip2.database
from collections import Counter
import hopsworks
from hsfs.transformation_statistics import TransformationStatistics

root_dir=""


@hopsworks.udf(int, mode="pandas")
def days_to_card_expiry(expiry_date: pd.Series) -> pd.Series:
    """
       the number of days until the credit card expires
    """
    bins = [0, 1, 10, 30, 90, 180, float('inf')]
    labels = ['0-1', '2-10', '11-30', '31-90', '91-180', '181-']
    binned_days_to_expiry = pd.cut(expiry_date, bins=bins, labels=labels)
    return binned_days_to_expiry

@hopsworks.udf(int, mode="pandas")
def amount_deviation_from_avg(amount, statistics: TransformationStatistics = None):
    """
       calculates how much a given transaction deviates from avg transaction amounts for a given card
    """
    return np.abs(amount - statistics.amount.mean)


#@hopsworks.udf(int, drop=['prev_ts'], mode="pandas")
def time_since_last_trans(ts: pd.Series, prev_ts: pd.Series) -> pd.Series:
    """
    Calculate time difference in seconds between current and previous transaction.
    Returns 0 when there is no previous transaction (prev_ts is None/NaT).
    """
    # Compute time difference
    delta = (ts - prev_ts).dt.total_seconds()

    # Replace NaN / NaT differences with 0 and cast to int
    return delta.fillna(0).astype(int)

    
@hopsworks.udf(bool, drop=['card_present', 'prev_card_present', 'ip_address', 'prev_ip_address'])
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
        
        # Calculate distances
        # distance_kms = haversine_distance_transactions(
        #     filtered_prev_ip, 
        #     filtered_ip,
        #     root_dir
        # )


        # Ensure database is downloaded and get path
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


# def _ensure_geoip_database(r) -> str:
#     """
#     Ensure the GeoIP database is downloaded and return its path.

#     Returns:
#         str: Path to the GeoIP database file
#     """
#     mmdb_path = f"{root_dir}/data/GeoLite2-City.mmdb"
#     download_url = "https://repo.hops.works/dev/jdowling/GeoLite2-City.mmdb"

#     mmdb_path = os.path.normpath(mmdb_path)
#     if not os.path.exists(mmdb_path):
#         print(f"MMDB not found at {mmdb_path}. Downloading...")
#         try:
#             response = requests.get(download_url, timeout=30)
#             response.raise_for_status()
#             os.makedirs(os.path.dirname(mmdb_path), exist_ok=True)
#             with open(mmdb_path, "wb") as f:
#                 f.write(response.content)
#             print(f"Downloaded database to: {mmdb_path}")
#         except Exception as e:
#             print(f"Error downloading database: {e}")
#             raise

#     return mmdb_path




# def test_ip_resolution(ip_series: pd.Series,  sample_size: int = None) -> dict:
#     """
#     Test what percentage of IP addresses can be resolved to lat/lon coordinates.

#     Args:
#         ip_series (pd.Series): Series of IP addresses to test
#         sample_size (int): Optional - test only a sample of IPs for speed

#     Returns:
#         dict: Statistics about IP resolution success/failure
#     """
#     print("Testing IP address resolution in GeoIP database...")

#     # Ensure database is downloaded
#     mmdb_path = _ensure_geoip_database(root_dir)
    
#     # Remove null/NaN values
#     valid_ips = ip_series.dropna()
    
#     if len(valid_ips) == 0:
#         return {
#             "total_ips": 0,
#             "null_ips": len(ip_series),
#             "error": "No valid IPs to test"
#         }
    
#     # Sample if requested
#     if sample_size and len(valid_ips) > sample_size:
#         test_ips = valid_ips.sample(n=sample_size, random_state=42)
#         print(f"Testing {sample_size} sampled IPs out of {len(valid_ips)} total...")
#     else:
#         test_ips = valid_ips
#         print(f"Testing all {len(test_ips)} IPs...")
    
#     # Initialize counters
#     successful = 0
#     failed = 0
#     null_island = 0  # IPs that returned (0.0, 0.0)
#     failed_ips = []
#     error_types = Counter()
#     countries_found = Counter()
    
#     # Open reader once
#     reader = geoip2.database.Reader(mmdb_path)
    
#     # Test each IP
#     for idx, ip in enumerate(test_ips):
#         if idx % 1000 == 0 and idx > 0:
#             print(f"  Tested {idx}/{len(test_ips)} IPs...")
        
#         try:
#             response = reader.city(ip)
#             lat = response.location.latitude
#             lon = response.location.longitude
            
#             if lat is not None and lon is not None:
#                 if lat == 0.0 and lon == 0.0:
#                     null_island += 1
#                 else:
#                     successful += 1
#                     # Track country distribution
#                     if response.country.name:
#                         countries_found[response.country.name] += 1
#             else:
#                 failed += 1
#                 failed_ips.append(ip)
#                 error_types["No coordinates returned"] += 1
                
#         except geoip2.errors.AddressNotFoundError:
#             failed += 1
#             failed_ips.append(ip)
#             error_types["Address not found in database"] += 1
#         except Exception as e:
#             failed += 1
#             failed_ips.append(ip)
#             error_types[str(type(e).__name__)] += 1
    
#     reader.close()
    
#     # Calculate statistics
#     total_tested = len(test_ips)
#     success_rate = (successful / total_tested * 100) if total_tested > 0 else 0
#     null_island_rate = (null_island / total_tested * 100) if total_tested > 0 else 0
#     failure_rate = (failed / total_tested * 100) if total_tested > 0 else 0
    
#     results = {
#         "total_ips_in_dataset": len(ip_series),
#         "null_or_nan_ips": len(ip_series) - len(valid_ips),
#         "ips_tested": total_tested,
#         "successful_resolutions": successful,
#         "null_island_coords": null_island,
#         "failed_resolutions": failed,
#         "success_rate_percent": round(success_rate, 2),
#         "null_island_rate_percent": round(null_island_rate, 2),
#         "failure_rate_percent": round(failure_rate, 2),
#         "error_types": dict(error_types),
#         "top_10_countries": dict(countries_found.most_common(10)),
#         "sample_failed_ips": failed_ips[:10] if failed_ips else []
#     }
    
#     # Print summary
#     print("\n" + "="*60)
#     print("IP ADDRESS RESOLUTION TEST RESULTS")
#     print("="*60)
#     print(f"Total IPs in dataset: {results['total_ips_in_dataset']}")
#     print(f"Null/NaN IPs: {results['null_or_nan_ips']}")
#     print(f"IPs tested: {results['ips_tested']}")
#     print(f"\nSuccessful resolutions: {results['successful_resolutions']} ({results['success_rate_percent']}%)")
#     print(f"Null Island (0,0) coords: {results['null_island_coords']} ({results['null_island_rate_percent']}%)")
#     print(f"Failed resolutions: {results['failed_resolutions']} ({results['failure_rate_percent']}%)")
    
#     if error_types:
#         print(f"\nError breakdown:")
#         for error_type, count in error_types.most_common():
#             print(f"  - {error_type}: {count}")
    
#     if countries_found:
#         print(f"\nTop 10 countries found:")
#         for country, count in countries_found.most_common(10):
#             print(f"  - {country}: {count}")
    
#     if failed_ips:
#         print(f"\nSample of failed IPs (first 10):")
#         for ip in failed_ips[:10]:
#             print(f"  - {ip}")
    
#     print("="*60)
    
#     return results
