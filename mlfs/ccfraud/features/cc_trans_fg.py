import pandas as pd
import numpy as np
import os
import requests
import geoip2.database
from collections import Counter

# import hopsworks
# import pandas as pd
# from math import radians
# import numpy as np
# from datetime import datetime
# import requests
# import os
# import hopsworks
# from hsfs.transformation_statistics import TransformationStatistics

# @hopsworks.udf(int, drop=['prev_transaction_time'])
# def days_to_card_expiry(expiry_date: pd.Series) -> pd.Series:
#     """
#     """
#     bins = [0, 1, 10, 30, 90, 180, float('inf')]
#     labels = ['0-1', '2-10', '11-30', '31-90', '91-180', '181-']
#     binned_days_to_expiry = pd.cut(expiry_date, bins=bins, labels=labels)
#     return binned_days_to_expiry
    # return expiry_date.dt.date() - datetime.now.date()

# @hopsworks.udf(int, TransformationStatistics)
# def amount_deviation_from_avg(amount, statistics=statistics):
#     """
#     """
#     return np.abs(amount - statistics.amount.mean)


# @hopsworks.udf(int)
def time_since_last_trans(ts: pd.Series, prev_ts: pd.Series) -> pd.Series:
    """
    Calculate time difference in seconds between current and previous transaction.
    Returns 0 when there is no previous transaction (prev_ts is None/NaN).
    """
    # Handle None/NaN in prev_ts - when there's no previous transaction
    # Return 0 to indicate no time has passed (will be filtered out in haversine_distance)
    result = pd.Series(0, index=ts.index, dtype=int)
    
    # Calculate difference only where prev_ts is not null
    valid_mask = prev_ts.notna()
    if valid_mask.any():
        result.loc[valid_mask] = (ts[valid_mask] - prev_ts[valid_mask]).dt.total_seconds().astype(int)
    
    return result


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


# @hopsworks.udf(int, drop=['card_present', 'prev_card_present', 'ip_addr', 'prev_ip_addr', 'prev_card_present', 'prev_ip_addr', 'time_since_last_trans'])
def haversine_distance(card_present: pd.Series, prev_card_present: pd.Series, 
                       ip_addr: pd.Series, prev_ip_addr: pd.Series,
                       time_since_last_trans: pd.Series, root_dir: str) -> pd.Series:
    """
    Determine if travel between transactions is feasible.
    Returns True if travel is feasible, False if not feasible.
    Only checks for impossible travel when card_present OR prev_card_present is True.
    """
    # Create mask for rows where at least one transaction used a card
    check_travel = card_present | prev_card_present
    
    # Initialize all as feasible (True)
    result = pd.Series(True, index=ip_addr.index)
    
    # Only check travel for rows where a card was present
    if check_travel.any():
        # Filter to only check relevant rows
        filtered_prev_ip = prev_ip_addr[check_travel]
        filtered_ip = ip_addr[check_travel]
        filtered_time = time_since_last_trans[check_travel]
        
        # Calculate distances
        distance_kms = haversine_distance_transactions(
            filtered_prev_ip, 
            filtered_ip,
            root_dir
        )
        
        # Check if travel is impossible
        impossible = is_impossible_travel(distance_kms, filtered_time)
        
        # Mark impossible travel as not feasible (False)
        result.loc[check_travel] = ~impossible
    
    return result


def haversine_distance_transactions(ip_addr1: pd.Series, ip_addr2: pd.Series, root_dir: str):
    """
    Calculate the haversine distance between two series of IP addresses.
    
    Args:
        ip_addr1 (pd.Series): IP addresses of first transactions
        ip_addr2 (pd.Series): IP addresses of second transactions
        root_dir (str): Root directory for database file
    
    Returns:
        pd.Series: Haversine distances in kilometers
    """
    # Get coordinates for both IP series
    coords1 = ip_addr1.apply(lambda ip: ip_to_coordinates(root_dir, ip))
    coords2 = ip_addr2.apply(lambda ip: ip_to_coordinates(root_dir, ip))
    
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
    
    return pd.Series(c * r, index=ip_addr1.index)


def ip_to_coordinates(root_dir, ip_address):
    """
    Convert an IP address to latitude and longitude using MaxMind GeoIP database.
    
    Args:
        root_dir (str): Root directory path
        ip_address (str): IP address to convert
    
    Returns:
        tuple: (latitude, longitude) - returns (0.0, 0.0) if IP cannot be resolved
    """
    # Handle None or empty IP addresses
    if ip_address is None or ip_address == '' or pd.isna(ip_address):
        return (0.0, 0.0)
    
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
            return (0.0, 0.0)
    
    try:
        reader = geoip2.database.Reader(mmdb_path)
        response = reader.city(ip_address)
        lat = response.location.latitude
        lon = response.location.longitude
        
        # Handle cases where lat/lon might be None
        if lat is None or lon is None:
            return (0.0, 0.0)
        
        return (float(lat), float(lon))
    except Exception as e:
        # Silently handle errors for common issues (private IPs, invalid IPs, etc.)
        # Only print for debugging if needed
        # print(f"Error getting location for IP {ip_address}: {e}")
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


def test_ip_resolution(ip_series: pd.Series, root_dir: str, sample_size: int = None) -> dict:
    """
    Test what percentage of IP addresses can be resolved to lat/lon coordinates.
    
    Args:
        ip_series (pd.Series): Series of IP addresses to test
        root_dir (str): Root directory for GeoIP database
        sample_size (int): Optional - test only a sample of IPs for speed
    
    Returns:
        dict: Statistics about IP resolution success/failure
    """
    print("Testing IP address resolution in GeoIP database...")
    
    # Download database if needed
    mmdb_path = f"{root_dir}/data/GeoLite2-City.mmdb"
    download_url = "https://repo.hops.works/dev/jdowling/GeoLite2-City.mmdb"
    
    mmdb_path = os.path.normpath(mmdb_path)    
    if not os.path.exists(mmdb_path):
        print(f"MMDB not found at {mmdb_path}. Downloading...")
        response = requests.get(download_url, timeout=30)
        response.raise_for_status()
        os.makedirs(os.path.dirname(mmdb_path), exist_ok=True)
        with open(mmdb_path, "wb") as f:
            f.write(response.content)        
        print(f"Downloaded database to: {mmdb_path}")
    
    # Remove null/NaN values
    valid_ips = ip_series.dropna()
    
    if len(valid_ips) == 0:
        return {
            "total_ips": 0,
            "null_ips": len(ip_series),
            "error": "No valid IPs to test"
        }
    
    # Sample if requested
    if sample_size and len(valid_ips) > sample_size:
        test_ips = valid_ips.sample(n=sample_size, random_state=42)
        print(f"Testing {sample_size} sampled IPs out of {len(valid_ips)} total...")
    else:
        test_ips = valid_ips
        print(f"Testing all {len(test_ips)} IPs...")
    
    # Initialize counters
    successful = 0
    failed = 0
    null_island = 0  # IPs that returned (0.0, 0.0)
    failed_ips = []
    error_types = Counter()
    countries_found = Counter()
    
    # Open reader once
    reader = geoip2.database.Reader(mmdb_path)
    
    # Test each IP
    for idx, ip in enumerate(test_ips):
        if idx % 1000 == 0 and idx > 0:
            print(f"  Tested {idx}/{len(test_ips)} IPs...")
        
        try:
            response = reader.city(ip)
            lat = response.location.latitude
            lon = response.location.longitude
            
            if lat is not None and lon is not None:
                if lat == 0.0 and lon == 0.0:
                    null_island += 1
                else:
                    successful += 1
                    # Track country distribution
                    if response.country.name:
                        countries_found[response.country.name] += 1
            else:
                failed += 1
                failed_ips.append(ip)
                error_types["No coordinates returned"] += 1
                
        except geoip2.errors.AddressNotFoundError:
            failed += 1
            failed_ips.append(ip)
            error_types["Address not found in database"] += 1
        except Exception as e:
            failed += 1
            failed_ips.append(ip)
            error_types[str(type(e).__name__)] += 1
    
    reader.close()
    
    # Calculate statistics
    total_tested = len(test_ips)
    success_rate = (successful / total_tested * 100) if total_tested > 0 else 0
    null_island_rate = (null_island / total_tested * 100) if total_tested > 0 else 0
    failure_rate = (failed / total_tested * 100) if total_tested > 0 else 0
    
    results = {
        "total_ips_in_dataset": len(ip_series),
        "null_or_nan_ips": len(ip_series) - len(valid_ips),
        "ips_tested": total_tested,
        "successful_resolutions": successful,
        "null_island_coords": null_island,
        "failed_resolutions": failed,
        "success_rate_percent": round(success_rate, 2),
        "null_island_rate_percent": round(null_island_rate, 2),
        "failure_rate_percent": round(failure_rate, 2),
        "error_types": dict(error_types),
        "top_10_countries": dict(countries_found.most_common(10)),
        "sample_failed_ips": failed_ips[:10] if failed_ips else []
    }
    
    # Print summary
    print("\n" + "="*60)
    print("IP ADDRESS RESOLUTION TEST RESULTS")
    print("="*60)
    print(f"Total IPs in dataset: {results['total_ips_in_dataset']}")
    print(f"Null/NaN IPs: {results['null_or_nan_ips']}")
    print(f"IPs tested: {results['ips_tested']}")
    print(f"\nSuccessful resolutions: {results['successful_resolutions']} ({results['success_rate_percent']}%)")
    print(f"Null Island (0,0) coords: {results['null_island_coords']} ({results['null_island_rate_percent']}%)")
    print(f"Failed resolutions: {results['failed_resolutions']} ({results['failure_rate_percent']}%)")
    
    if error_types:
        print(f"\nError breakdown:")
        for error_type, count in error_types.most_common():
            print(f"  - {error_type}: {count}")
    
    if countries_found:
        print(f"\nTop 10 countries found:")
        for country, count in countries_found.most_common(10):
            print(f"  - {country}: {count}")
    
    if failed_ips:
        print(f"\nSample of failed IPs (first 10):")
        for ip in failed_ips[:10]:
            print(f"  - {ip}")
    
    print("="*60)
    
    return results
