import polars as pl
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
from geopy.distance import geodesic
import random
import uuid
import hsfs
import hopsworks
import geoip2.database
import os

# seeds
fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)


# ---------------------------
# IP Address Generation with GeoIP Support
# ---------------------------

# Known IP ranges for different countries (these are real public IP ranges)
# Known IP ranges for different countries (these are real public IP ranges that resolve in GeoIP)
COUNTRY_IP_RANGES = {
    "United States": [
        ("8.0.0.0", "8.255.255.255"),       # Level3
        ("12.0.0.0", "12.255.255.255"),     # AT&T
        ("24.0.0.0", "24.255.255.255"),     # Cable/DSL
        ("50.0.0.0", "50.127.255.255"),     # Various ISPs
        ("66.0.0.0", "66.255.255.255"),     # Various ISPs
        ("67.0.0.0", "67.255.255.255"),     # Various ISPs
        ("68.0.0.0", "68.255.255.255"),     # Cable/DSL
        ("69.0.0.0", "69.255.255.255"),     # Various ISPs
        ("71.0.0.0", "71.255.255.255"),     # Cable/DSL
        ("74.0.0.0", "74.255.255.255"),     # Various ISPs
        ("75.0.0.0", "75.255.255.255"),     # Various ISPs
        ("76.0.0.0", "76.255.255.255"),     # Various ISPs
        ("96.0.0.0", "96.255.255.255"),     # Various ISPs
        ("98.0.0.0", "98.255.255.255"),     # Various ISPs
        ("99.0.0.0", "99.255.255.255"),     # Various ISPs
        ("104.0.0.0", "104.255.255.255"),   # Various
        ("107.0.0.0", "107.255.255.255"),   # Various ISPs
        ("108.0.0.0", "108.255.255.255"),   # Various ISPs
    ],
    "United Kingdom": [
        ("81.0.0.0", "81.255.255.255"),     # British Telecom
        ("86.0.0.0", "86.255.255.255"),     # Various UK ISPs
        ("90.0.0.0", "90.255.255.255"),     # Virgin Media
        ("92.0.0.0", "92.255.255.255"),     # Various UK ISPs
        ("94.0.0.0", "94.255.255.255"),     # Various UK ISPs
    ],
    "Germany": [
        ("46.0.0.0", "46.255.255.255"),     # Various EU
        ("78.0.0.0", "78.255.255.255"),     # Deutsche Telekom
        ("91.0.0.0", "91.255.255.255"),     # Various EU
    ],
    "France": [
        ("80.0.0.0", "80.255.255.255"),     # Orange
        ("82.0.0.0", "82.255.255.255"),     # Various FR ISPs
        ("88.0.0.0", "88.255.255.255"),     # Free/SFR
        ("90.0.0.0", "90.127.255.255"),     # Various FR ISPs
    ],
    "China": [
        ("1.0.0.0", "1.63.255.255"),        # China Telecom
        ("27.0.0.0", "27.255.255.255"),     # China networks
        ("36.0.0.0", "36.127.255.255"),     # China networks
        ("58.0.0.0", "58.255.255.255"),     # China networks
        ("106.0.0.0", "106.255.255.255"),   # China networks
        ("110.0.0.0", "110.255.255.255"),   # China networks
        ("111.0.0.0", "111.255.255.255"),   # China networks
        ("112.0.0.0", "112.255.255.255"),   # China networks
        ("113.0.0.0", "113.255.255.255"),   # China networks
        ("114.0.0.0", "114.255.255.255"),   # China networks
        ("115.0.0.0", "115.255.255.255"),   # China networks
        ("116.0.0.0", "116.255.255.255"),   # China networks
        ("117.0.0.0", "117.127.255.255"),   # China networks
        ("118.0.0.0", "118.255.255.255"),   # China networks
        ("119.0.0.0", "119.255.255.255"),   # China networks
        ("120.0.0.0", "120.255.255.255"),   # China networks
        ("121.0.0.0", "121.255.255.255"),   # China networks
        ("122.0.0.0", "122.255.255.255"),   # China networks
        ("123.0.0.0", "123.255.255.255"),   # China networks
    ],
    "Japan": [
        ("60.0.0.0", "60.255.255.255"),     # Japan networks
        ("61.0.0.0", "61.255.255.255"),     # Japan/Korea/Taiwan
        ("126.0.0.0", "126.255.255.255"),   # Japan networks
        ("133.0.0.0", "133.255.255.255"),   # Japan networks
        ("210.0.0.0", "210.255.255.255"),   # Japan/APNIC
        ("211.0.0.0", "211.255.255.255"),   # Japan/APNIC
    ],
    "India": [
        ("14.0.0.0", "14.255.255.255"),     # BSNL
        ("49.0.0.0", "49.255.255.255"),     # Various Indian ISPs
        ("117.128.0.0", "117.255.255.255"), # Indian ISPs
    ],
    "Canada": [
        ("70.0.0.0", "70.255.255.255"),     # Rogers
        ("72.0.0.0", "72.255.255.255"),     # Various Canadian ISPs
        ("142.0.0.0", "142.255.255.255"),   # Various Canadian ISPs
        ("184.0.0.0", "184.127.255.255"),   # Various Canadian ISPs
    ],
    "Australia": [
        ("1.128.0.0", "1.159.255.255"),     # Telstra
        ("27.32.0.0", "27.47.255.255"),     # Various AU ISPs
        ("101.0.0.0", "101.255.255.255"),   # Various AU ISPs
        ("103.0.0.0", "103.127.255.255"),   # APNIC region including AU
    ],
    "Brazil": [
        ("177.0.0.0", "177.255.255.255"),   # Various BR ISPs
        ("179.0.0.0", "179.255.255.255"),   # Various BR ISPs
        ("186.0.0.0", "186.255.255.255"),   # LACNIC - BR ISPs
        ("189.0.0.0", "189.255.255.255"),   # Various BR ISPs
        ("191.0.0.0", "191.255.255.255"),   # Various BR ISPs
    ],
    "Russia": [
        ("5.0.0.0", "5.255.255.255"),       # Various RU networks
        ("31.0.0.0", "31.255.255.255"),     # Various RU networks
        ("37.0.0.0", "37.127.255.255"),     # Various RU networks
        ("46.0.0.0", "46.255.255.255"),     # RIPE region including RU
        ("77.0.0.0", "77.127.255.255"),     # Various RU networks
        ("78.0.0.0", "78.127.255.255"),     # Various RU networks
        ("79.0.0.0", "79.127.255.255"),     # Various RU networks
        ("80.0.0.0", "80.127.255.255"),     # Various RU networks
        ("81.0.0.0", "81.127.255.255"),     # Various RU networks
        ("82.0.0.0", "82.127.255.255"),     # Various RU networks
        ("83.0.0.0", "83.127.255.255"),     # Various RU networks
        ("84.0.0.0", "84.127.255.255"),     # Various RU networks
        ("85.0.0.0", "85.127.255.255"),     # Various RU networks
        ("86.0.0.0", "86.127.255.255"),     # Various RU networks
        ("87.0.0.0", "87.127.255.255"),     # Various RU networks
        ("88.0.0.0", "88.127.255.255"),     # Various RU networks
        ("89.0.0.0", "89.127.255.255"),     # Various RU networks
        ("90.0.0.0", "90.127.255.255"),     # Various RU networks
        ("91.0.0.0", "91.127.255.255"),     # Various RU networks
        ("92.0.0.0", "92.127.255.255"),     # Various RU networks
        ("93.0.0.0", "93.127.255.255"),     # Various RU networks
        ("94.0.0.0", "94.127.255.255"),     # Various RU networks
        ("95.0.0.0", "95.127.255.255"),     # Various RU networks
    ],
    "South Korea": [
        ("1.224.0.0", "1.255.255.255"),     # KT Corp
        ("14.0.0.0", "14.127.255.255"),     # Various KR ISPs
        ("27.0.0.0", "27.127.255.255"),     # Various KR ISPs
        ("58.0.0.0", "58.127.255.255"),     # Various KR ISPs
        ("59.0.0.0", "59.127.255.255"),     # Various KR ISPs
        ("60.0.0.0", "60.127.255.255"),     # Various KR ISPs
        ("61.0.0.0", "61.127.255.255"),     # KT Corp
        ("106.0.0.0", "106.127.255.255"),   # Various KR ISPs
        ("110.0.0.0", "110.127.255.255"),   # Various KR ISPs
        ("112.0.0.0", "112.127.255.255"),   # Various KR ISPs
        ("114.0.0.0", "114.127.255.255"),   # Various KR ISPs
        ("115.0.0.0", "115.127.255.255"),   # Various KR ISPs
        ("116.0.0.0", "116.127.255.255"),   # Various KR ISPs
        ("117.0.0.0", "117.127.255.255"),   # Various KR ISPs
        ("118.0.0.0", "118.127.255.255"),   # Various KR ISPs
        ("119.0.0.0", "119.127.255.255"),   # Various KR ISPs
        ("121.0.0.0", "121.255.255.255"),   # Various KR ISPs
        ("175.0.0.0", "175.255.255.255"),   # Various KR ISPs
    ],
    "Mexico": [
        ("187.0.0.0", "187.255.255.255"),   # Telmex
        ("189.0.0.0", "189.255.255.255"),   # Various MX ISPs
        ("201.0.0.0", "201.255.255.255"),   # LACNIC - MX ISPs
    ],
    "Spain": [
        ("80.0.0.0", "80.127.255.255"),     # Various ES ISPs
        ("81.0.0.0", "81.127.255.255"),     # Various ES ISPs
        ("83.0.0.0", "83.255.255.255"),     # Various ES ISPs
        ("84.0.0.0", "84.255.255.255"),     # Telefonica
        ("85.0.0.0", "85.127.255.255"),     # Various ES ISPs
        ("88.0.0.0", "88.127.255.255"),     # Various ES ISPs
    ],
    "Italy": [
        ("79.0.0.0", "79.255.255.255"),     # Telecom Italia
        ("80.0.0.0", "80.127.255.255"),     # Various IT ISPs
        ("81.0.0.0", "81.127.255.255"),     # Various IT ISPs
        ("82.0.0.0", "82.127.255.255"),     # Various IT ISPs
        ("87.0.0.0", "87.255.255.255"),     # Various IT ISPs
        ("93.0.0.0", "93.255.255.255"),     # Various IT ISPs
        ("151.0.0.0", "151.255.255.255"),   # Various IT ISPs
    ],
    "Netherlands": [
        ("77.0.0.0", "77.255.255.255"),     # KPN
        ("80.0.0.0", "80.127.255.255"),     # Various NL ISPs
        ("81.0.0.0", "81.127.255.255"),     # Various NL ISPs
        ("82.0.0.0", "82.127.255.255"),     # Various NL ISPs
        ("83.0.0.0", "83.127.255.255"),     # Various NL ISPs
        ("84.0.0.0", "84.127.255.255"),     # Various NL ISPs
        ("85.0.0.0", "85.127.255.255"),     # Various NL ISPs
        ("86.0.0.0", "86.127.255.255"),     # Various NL ISPs
        ("145.0.0.0", "145.255.255.255"),   # Various NL ISPs
        ("213.0.0.0", "213.255.255.255"),   # Various NL ISPs
    ],
    "Indonesia": [
        ("36.64.0.0", "36.127.255.255"),    # Telkom Indonesia
        ("103.0.0.0", "103.255.255.255"),   # Various ID ISPs
        ("110.0.0.0", "110.127.255.255"),   # Various ID ISPs
        ("112.0.0.0", "112.127.255.255"),   # Various ID ISPs
        ("114.0.0.0", "114.127.255.255"),   # Various ID ISPs
        ("118.0.0.0", "118.127.255.255"),   # Various ID ISPs
        ("125.0.0.0", "125.127.255.255"),   # Various ID ISPs
    ],
    "Saudi Arabia": [
        ("37.128.0.0", "37.255.255.255"),   # STC
        ("46.0.0.0", "46.127.255.255"),     # Various SA ISPs
        ("80.0.0.0", "80.127.255.255"),     # Various SA ISPs
        ("109.0.0.0", "109.255.255.255"),   # Various SA ISPs
        ("188.0.0.0", "188.255.255.255"),   # Various SA ISPs
    ],
    "Turkey": [
        ("78.160.0.0", "78.191.255.255"),   # Turk Telekom
        ("79.0.0.0", "79.127.255.255"),     # Various TR ISPs
        ("80.0.0.0", "80.127.255.255"),     # Various TR ISPs
        ("81.0.0.0", "81.127.255.255"),     # Various TR ISPs
        ("82.0.0.0", "82.127.255.255"),     # Various TR ISPs
        ("85.0.0.0", "85.127.255.255"),     # Various TR ISPs
        ("88.0.0.0", "88.127.255.255"),     # Various TR ISPs
        ("176.0.0.0", "176.255.255.255"),   # Various TR ISPs
        ("185.0.0.0", "185.255.255.255"),   # Various TR ISPs
    ],
    "Taiwan": [
        ("1.160.0.0", "1.175.255.255"),     # Taiwan networks
        ("27.0.0.0", "27.127.255.255"),     # Taiwan networks
        ("36.224.0.0", "36.239.255.255"),   # Taiwan networks
        ("60.0.0.0", "60.127.255.255"),     # Taiwan networks
        ("61.0.0.0", "61.127.255.255"),     # Taiwan networks
        ("111.0.0.0", "111.127.255.255"),   # Taiwan networks
        ("114.0.0.0", "114.127.255.255"),   # Taiwan networks
        ("118.0.0.0", "118.127.255.255"),   # Taiwan networks
        ("125.0.0.0", "125.127.255.255"),   # Taiwan networks
    ],
    "Belgium": [
        ("77.0.0.0", "77.127.255.255"),     # Belgacom
        ("78.0.0.0", "78.127.255.255"),     # Various BE ISPs
        ("79.0.0.0", "79.127.255.255"),     # Various BE ISPs
        ("80.0.0.0", "80.127.255.255"),     # Various BE ISPs
        ("81.0.0.0", "81.127.255.255"),     # Various BE ISPs
        ("82.0.0.0", "82.127.255.255"),     # Various BE ISPs
        ("83.0.0.0", "83.127.255.255"),     # Various BE ISPs
        ("84.0.0.0", "84.127.255.255"),     # Various BE ISPs
        ("85.0.0.0", "85.127.255.255"),     # Various BE ISPs
    ],
    "Argentina": [
        ("181.0.0.0", "181.255.255.255"),   # Various AR ISPs
        ("186.0.0.0", "186.127.255.255"),   # Various AR ISPs
        ("190.0.0.0", "190.255.255.255"),   # LACNIC - AR ISPs
        ("200.0.0.0", "200.127.255.255"),   # LACNIC - AR ISPs
    ],
    "Thailand": [
        ("1.0.128.0", "1.0.255.255"),       # Thailand networks
        ("27.0.0.0", "27.127.255.255"),     # Thailand networks
        ("58.0.0.0", "58.127.255.255"),     # Thailand networks
        ("101.0.0.0", "101.127.255.255"),   # Thailand networks
        ("103.0.0.0", "103.127.255.255"),   # Thailand networks
        ("110.0.0.0", "110.127.255.255"),   # Thailand networks
        ("111.0.0.0", "111.127.255.255"),   # Thailand networks
        ("112.0.0.0", "112.127.255.255"),   # Thailand networks
        ("113.0.0.0", "113.127.255.255"),   # Thailand networks
        ("114.0.0.0", "114.127.255.255"),   # Thailand networks
        ("115.0.0.0", "115.127.255.255"),   # Thailand networks
        ("116.0.0.0", "116.127.255.255"),   # Thailand networks
        ("117.0.0.0", "117.127.255.255"),   # Thailand networks
        ("118.0.0.0", "118.127.255.255"),   # Thailand networks
        ("119.0.0.0", "119.127.255.255"),   # Thailand networks
        ("125.0.0.0", "125.127.255.255"),   # Thailand networks
    ],
    "Israel": [
        ("2.0.0.0", "2.127.255.255"),       # Various IL ISPs
        ("5.0.0.0", "5.127.255.255"),       # Various IL ISPs
        ("37.0.0.0", "37.127.255.255"),     # Various IL ISPs
        ("46.0.0.0", "46.127.255.255"),     # Various IL ISPs
        ("77.0.0.0", "77.127.255.255"),     # Various IL ISPs
        ("79.0.0.0", "79.127.255.255"),     # Various IL ISPs
        ("80.0.0.0", "80.127.255.255"),     # Various IL ISPs
        ("82.0.0.0", "82.127.255.255"),     # Various IL ISPs
        ("85.0.0.0", "85.127.255.255"),     # Various IL ISPs
    ],
    "Poland": [
        ("5.0.0.0", "5.127.255.255"),       # Various PL ISPs
        ("31.0.0.0", "31.127.255.255"),     # Various PL ISPs
        ("37.0.0.0", "37.127.255.255"),     # Various PL ISPs
        ("46.0.0.0", "46.127.255.255"),     # Various PL ISPs
        ("77.0.0.0", "77.127.255.255"),     # Various PL ISPs
        ("78.0.0.0", "78.127.255.255"),     # Various PL ISPs
        ("79.0.0.0", "79.127.255.255"),     # Various PL ISPs
        ("80.0.0.0", "80.127.255.255"),     # Various PL ISPs
        ("81.0.0.0", "81.127.255.255"),     # Various PL ISPs
        ("83.0.0.0", "83.127.255.255"),     # Various PL ISPs
        ("89.0.0.0", "89.127.255.255"),     # Various PL ISPs
    ],
    "Nigeria": [
        ("41.0.0.0", "41.127.255.255"),     # AfriNIC - NG ISPs
        ("102.0.0.0", "102.127.255.255"),   # AfriNIC - NG ISPs
        ("105.0.0.0", "105.127.255.255"),   # AfriNIC - NG ISPs
        ("154.0.0.0", "154.127.255.255"),   # AfriNIC - NG ISPs
        ("196.0.0.0", "196.127.255.255"),   # AfriNIC - NG ISPs
        ("197.0.0.0", "197.127.255.255"),   # AfriNIC - NG ISPs
    ],
}

# Use well-known public IP ranges as fallback (Google Public DNS and Cloudflare)
DEFAULT_IP_RANGES = [
    ("8.8.8.0", "8.8.8.255"),              # Google DNS (US)
    ("1.1.1.0", "1.1.1.255"),              # Cloudflare (US)
]

# Neighboring countries for realistic local travel patterns
NEIGHBORING_COUNTRIES = {
    "United States": ["Canada", "Mexico"],
    "Canada": ["United States"],
    "Mexico": ["United States"],
    "United Kingdom": ["France", "Germany", "Netherlands", "Belgium", "Spain", "Italy"],
    "France": ["United Kingdom", "Germany", "Spain", "Italy", "Belgium", "Netherlands"],
    "Germany": ["France", "Netherlands", "Belgium", "Poland"],
    "Spain": ["France", "Italy"],
    "Italy": ["France", "Spain", "Germany"],
    "Netherlands": ["Germany", "Belgium", "United Kingdom", "France"],
    "Belgium": ["France", "Netherlands", "Germany", "United Kingdom"],
    "Poland": ["Germany", "Russia"],
    "China": ["Japan", "South Korea", "India", "Russia"],
    "Japan": ["China", "South Korea", "Taiwan"],
    "South Korea": ["Japan", "China"],
    "Taiwan": ["Japan", "China"],
    "India": ["China", "Thailand"],
    "Thailand": ["India", "Indonesia"],
    "Indonesia": ["Thailand", "Australia"],
    "Australia": ["Indonesia"],
    "Brazil": ["Argentina"],
    "Argentina": ["Brazil"],
    "Russia": ["Poland", "China"],
    "Saudi Arabia": ["Turkey", "Israel"],
    "Turkey": ["Saudi Arabia", "Israel"],
    "Israel": ["Turkey", "Saudi Arabia"],
    "Nigeria": [],  # Limited neighbors in our country list
}



def ip_to_int(ip_str: str) -> int:
    """Convert IP address string to integer"""
    parts = ip_str.split('.')
    return (int(parts[0]) << 24) + (int(parts[1]) << 16) + (int(parts[2]) << 8) + int(parts[3])


def int_to_ip(ip_int: int) -> str:
    """Convert integer to IP address string"""
    return f"{(ip_int >> 24) & 255}.{(ip_int >> 16) & 255}.{(ip_int >> 8) & 255}.{ip_int & 255}"


def generate_ip_for_country(country: str, seed: int = None) -> str:
    """
    Generate a random IP address within known ranges for a given country.
    
    Args:
        country: Country name
        seed: Random seed for reproducibility
        
    Returns:
        IP address string that should resolve in GeoIP database
    """
    if seed is not None:
        rng = np.random.default_rng(seed)
    else:
        rng = np.random.default_rng()
    
    # Get IP ranges for this country, or use default
    ip_ranges = COUNTRY_IP_RANGES.get(country, DEFAULT_IP_RANGES)
    
    # Select a random range
    start_ip, end_ip = ip_ranges[rng.integers(0, len(ip_ranges))]
    
    # Convert to integers
    start_int = ip_to_int(start_ip)
    end_int = ip_to_int(end_ip)
    
    # Generate random IP within range
    ip_int = rng.integers(start_int, end_int + 1)
    
    return int_to_ip(ip_int)


def generate_random_ips(count: int, seed: int = None) -> list:
    """
    Generate random IP addresses from various countries.
    
    Args:
        count: Number of IPs to generate
        seed: Random seed for reproducibility
        
    Returns:
        List of IP address strings
    """
    if seed is not None:
        rng = np.random.default_rng(seed)
    else:
        rng = np.random.default_rng()
    
    # Get all available countries
    all_countries = list(COUNTRY_IP_RANGES.keys())
    
    ips = []
    for i in range(count):
        # Pick a random country
        country = all_countries[rng.integers(0, len(all_countries))]
        # Generate IP for that country
        ip = generate_ip_for_country(country, seed=seed + i if seed else None)
        ips.append(ip)
    
    return ips


def generate_ips_for_countries(countries: list, seed: int = None) -> list:
    """
    Generate IP addresses for specific countries (one IP per country in list).

    Args:
        countries: List of country names
        seed: Random seed for reproducibility

    Returns:
        List of IP address strings matching the countries list
    """
    ips = []
    for i, country in enumerate(countries):
        ip = generate_ip_for_country(country, seed=seed + i if seed else None)
        ips.append(ip)

    return ips


def generate_ip_in_same_subnet(previous_ip: str, seed: int = None) -> str:
    """
    Generate an IP address in the same /16 or /24 subnet as the previous IP.
    Simulates transactions from the same ISP/region.

    Args:
        previous_ip: The previous IP address
        seed: Random seed for reproducibility

    Returns:
        New IP address in the same subnet
    """
    if seed is not None:
        rng = np.random.default_rng(seed)
    else:
        rng = np.random.default_rng()

    # Parse previous IP
    parts = previous_ip.split('.')
    if len(parts) != 4:
        return previous_ip

    try:
        # Keep first 2-3 octets, vary last octet(s)
        # 70% chance of /24 subnet (same first 3 octets)
        # 30% chance of /16 subnet (same first 2 octets)
        if rng.random() < 0.7:
            # Same /24 subnet
            new_ip = f"{parts[0]}.{parts[1]}.{parts[2]}.{rng.integers(1, 255)}"
        else:
            # Same /16 subnet
            new_ip = f"{parts[0]}.{parts[1]}.{rng.integers(1, 255)}.{rng.integers(1, 255)}"

        return new_ip
    except Exception:
        return previous_ip


# ---------------------------
# Utility / fixed generators
# ---------------------------


def generate_merchant_details(rows: int, start_date: datetime, end_date: datetime) -> pl.DataFrame:
    """Generate merchant details DataFrame"""
    print("Generating merchant details...")
    if start_date is None or end_date is None:
        raise ValueError("start_date and end_date are required")

    categories = [
        "Groceries", "Travel", "Fashion", "Healthcare", "Restaurants", "Gas Stations",
        "Electronics", "Home Improvement", "Entertainment", "Education", "Insurance",
        "Automotive", "Books", "Sports", "Beauty", "Jewelry", "Pet Supplies",
        "Furniture", "Pharmacy", "Department Stores"
    ] * 25  # repeated to get many options

    countries = [
        "United States", "China", "Japan", "Germany", "India", "United Kingdom",
        "France", "Italy", "Brazil", "Canada", "Russia", "South Korea", "Australia",
        "Spain", "Mexico", "Indonesia", "Netherlands", "Saudi Arabia", "Turkey",
        "Taiwan", "Belgium", "Argentina", "Thailand", "Israel", "Poland", "Nigeria",
        # ... (shortened list for brevity, but you can expand as you wish)
    ]

    delta_days = max((end_date - start_date).days, 0)
    
    # Generate daily chargeback counts
    cnt_chrgeback_prev_day = [round(np.random.exponential(2.5), 2) for _ in range(rows)]
    
    # Compute weekly chargebacks: roughly 7x daily with some variation (±20%)
    cnt_chrgeback_prev_week = [
        round(day_count * 7 * np.random.uniform(0.8, 1.2), 2) 
        for day_count in cnt_chrgeback_prev_day
    ]
    
    # Compute monthly chargebacks: roughly 30x daily with some variation (±20%)
    cnt_chrgeback_prev_month = [
        round(day_count * 30 * np.random.uniform(0.8, 1.2), 2) 
        for day_count in cnt_chrgeback_prev_day
    ]
    
    merchant_data = {
        "merchant_id": [f"MERCH_{i:08d}" for i in range(rows)],
        "category": [random.choice(categories) for _ in range(rows)],
        "country": [random.choice(countries) for _ in range(rows)],
        "cnt_chrgeback_prev_day": cnt_chrgeback_prev_day,
        "cnt_chrgeback_prev_week": cnt_chrgeback_prev_week,
        "cnt_chrgeback_prev_month": cnt_chrgeback_prev_month,
        "last_modified": [start_date + timedelta(days=random.randint(0, delta_days)) for _ in range(rows)],
    }

    return pl.DataFrame(merchant_data)


def generate_bank_details(rows: int, start_date: datetime, end_date: datetime) -> pl.DataFrame:
    """Generate bank details DataFrame"""
    print("Generating bank details...")
    if start_date is None or end_date is None:
        raise ValueError("start_date and end_date are required")

    countries = ["United States", "China", "Japan", "Germany", "India", "United Kingdom",
                 "France", "Italy", "Brazil", "Canada", "Russia", "South Korea", "Australia",
                 "Spain", "Mexico", "Indonesia", "Netherlands", "Saudi Arabia", "Turkey"] * 7

    rows = int(rows)
    credit_ratings = np.random.beta(5, 2, rows) * 9 + 1
    credit_ratings = np.round(credit_ratings).astype(int)
    credit_ratings = np.clip(credit_ratings, 1, 10)

    delta_days = max((end_date - start_date).days, 0)
    
    # Generate days_since_bank_cr_changed using Weibull distribution
    # Shape parameter k=1.5 (slightly increasing hazard rate)
    # Scale parameter lambda=180 (mean around 160 days)
    weibull_shape = 1.5
    weibull_scale = 180
    days_since_cr_changed = np.random.weibull(weibull_shape, rows) * weibull_scale
    days_since_cr_changed = np.round(days_since_cr_changed).astype(int)
    days_since_cr_changed = np.clip(days_since_cr_changed, 0, 1095)  # Cap at 3 years
    
    bank_data = {
        "bank_id": [f"BANK_{i:08d}" for i in range(rows)],
        "country": [random.choice(countries) for _ in range(rows)],
        "credit_rating": credit_ratings.tolist(),
        "last_modified": [end_date - timedelta(days=random.randint(0, delta_days)) for _ in range(rows)],
        "days_since_bank_cr_changed": days_since_cr_changed.tolist(),
    }

    return pl.DataFrame(bank_data)


def generate_account_details(
    rows: int ,
    account_creation_start_date: datetime,
    current_date: datetime,
    account_last_modified_start_date: datetime
) -> pl.DataFrame:
    """Generate account details DataFrame"""
    print("Generating account details...")
    if account_creation_start_date is None or current_date is None or account_last_modified_start_date is None:
        raise ValueError("Please provide account_creation_start_date, current_date and account_last_modified_start_date")

    rows = int(rows)
    delta_days_lm = max((current_date - account_last_modified_start_date).days, 0)
    account_data = {
        "account_id": [f"ACC_{i:08d}" for i in range(rows)],
        "name": [fake.name() for _ in range(rows)],
        "address": [fake.address().replace('\n', ', ') for _ in range(rows)],
        "debt_end_prev_month": [round(np.random.normal(2500, 1500), 2) for _ in range(rows)],
        "last_modified": [current_date - timedelta(days=random.randint(0, delta_days_lm)) for _ in range(rows)]
    }

    # creation & optional end dates
    creation_dates = []
    end_dates = []
    delta_days_creation = max((current_date - account_creation_start_date).days, 0)
    for _ in range(rows):
        creation = current_date - timedelta(days=random.randint(0, max(1, delta_days_creation)))
        creation_dates.append(creation)
        if random.random() < 0.9:
            end_dates.append(None)
        else:
            end_dates.append(creation + timedelta(days=random.randint(1, max(1, delta_days_creation))))

    account_data["creation_date"] = creation_dates
    account_data["end_date"] = end_dates

    return pl.DataFrame(account_data)


def assign_cardholder_home_locations(
    account_df: pl.DataFrame,
    seed: int = 42
) -> pl.DataFrame:
    """
    Assign a home country to each account/cardholder.

    Args:
        account_df: Account details DataFrame
        seed: Random seed for reproducibility

    Returns:
        account_df with additional 'home_country' column
    """
    print("Assigning home locations to cardholders...")
    rng = np.random.default_rng(seed)

    # Weight countries by population/likelihood
    # US/major countries more common than others
    countries = list(COUNTRY_IP_RANGES.keys())

    # Create weights: US and major countries more likely
    weights = []
    for c in countries:
        if c == "United States":
            weights.append(0.35)
        elif c in ["United Kingdom", "Germany", "France", "Canada", "China", "Japan"]:
            weights.append(0.08)
        elif c in ["India", "Brazil", "Australia", "Spain", "Italy"]:
            weights.append(0.04)
        else:
            weights.append(0.01)

    # Normalize weights
    weights = np.array(weights)
    weights = weights / weights.sum()

    num_accounts = account_df.height
    home_countries = rng.choice(countries, size=num_accounts, p=weights)

    return account_df.with_columns(
        pl.Series("home_country", home_countries)
    )


def generate_card_details(
    rows: int,
    num_accounts: int,
    num_banks: int,
    current_date: datetime,
    issue_date: datetime,
    expiry_date: datetime,
    transactions_start_date: datetime = None
) -> pl.DataFrame:
    """
    Generate card details DataFrame.

    Args:
        rows: Number of cards to generate
        num_accounts: Number of accounts
        num_banks: Number of banks
        current_date: Current date
        issue_date: Earliest card issue date
        expiry_date: Latest card expiry date
        transactions_start_date: Start date for transactions. If provided, ensures last_modified is before this date
    """
    print("Generating card details...")
    if current_date is None or issue_date is None or expiry_date is None:
        raise ValueError("Provide current_date, issue_date and expiry_date")

    account_ids = [f"ACC_{i:08d}" for i in range(num_accounts)]
    bank_ids = [f"BANK_{i:08d}" for i in range(num_banks)]

    rows = int(rows)
    card_data = {
        "card_id": [f"CARD_{i:08d}" for i in range(rows)],
        "cc_num": [
            f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
            for _ in range(rows)
        ],
        "account_id": [random.choice(account_ids) for _ in range(rows)],
        "bank_id": [random.choice(bank_ids) for _ in range(rows)],
    }

    delta_issue_days = max((current_date - issue_date).days, 0)
    expiry_days_span = max((expiry_date - current_date).days, 0)

    issue_dates = [(current_date - timedelta(days=random.randint(0, delta_issue_days))).replace(day=1) for _ in range(rows)]
    expiry_dates = [(current_date + timedelta(days=random.randint(0, expiry_days_span))).replace(day=1) for _ in range(rows)]

    card_data["issue_date"] = issue_dates
    card_data["cc_expiry_date"] = expiry_dates

    card_types = ["Credit"] * 60 + ["Debit"] * 35 + ["Prepaid"] * 5
    statuses = ["Active"] * 95 + ["Blocked"] * 4 + ["Lost/Stolen"] * 1
    card_data["card_type"] = [random.choice(card_types) for _ in range(rows)]
    card_data["status"] = [random.choice(statuses) for _ in range(rows)]

    # Ensure last_modified is always before transactions_start_date if provided
    if transactions_start_date:
        # Calculate max days to ensure last_modified is before transactions_start_date
        max_last_modified_date = transactions_start_date - timedelta(days=1)
        delta_last_modified_days = max((max_last_modified_date - issue_date).days, 0)
        card_data["last_modified"] = [issue_date + timedelta(days=random.randint(0, delta_last_modified_days)) for _ in range(rows)]
    else:
        card_data["last_modified"] = [current_date - timedelta(days=random.randint(0, delta_issue_days)) for _ in range(rows)]

    return pl.DataFrame(card_data)


# ---------------------------
# Transactions: From existing card + merchant (preserves FKs)
# ---------------------------

def generate_credit_card_transactions_from_existing(
    card_df: pl.DataFrame,
    merchant_df: pl.DataFrame,
    start_date: datetime,
    end_date: datetime,
    rows: int,
    tid_offset: int = 0,
    seed: int = 42,
) -> pl.DataFrame:
    """
    Create transactions by sampling existing card_df (cc_num, account_id) and merchant_df (merchant_id).
    Returns a Polars DataFrame with columns: t_id, cc_num, account_id, merchant_id, amount, ip_address, card_present, ts
    
    IP addresses are generated to match merchant countries and resolve in GeoIP database.
    """
    print("Generating credit card transactions from existing card + merchant tables...")
    rows = int(rows)
    rng = np.random.default_rng(seed)

    # Ensure required columns exist
    required_card_cols = {"cc_num", "account_id"}
    if not required_card_cols.issubset(set(card_df.columns)):
        raise ValueError(f"card_df must contain columns: {required_card_cols}")

    required_merchant_cols = {"merchant_id", "country"}
    if not required_merchant_cols.issubset(set(merchant_df.columns)):
        raise ValueError("merchant_df must contain 'merchant_id' and 'country' columns")

    # 1) Sample (cc_num, account_id)
    # Polars sampling with replacement
    cards_sample = card_df.select(["cc_num", "account_id"]).sample(n=rows, with_replacement=True, shuffle=True, seed=seed)
    merchants_sample = merchant_df.select(["merchant_id", "country"]).sample(n=rows, with_replacement=True, shuffle=True, seed=seed + 1)

    # 2) Amounts (log-normal) and card_present flag
    amounts = np.round(rng.lognormal(mean=3.5, sigma=1.2, size=rows), 2).tolist()
    card_present = rng.integers(0, 2, size=rows).astype(bool).tolist()

    # 3) IP addresses based on merchant countries - now using GeoIP-valid IPs
    merchant_countries = merchants_sample["country"].to_list()
    ip_address = generate_ips_for_countries(merchant_countries, seed=seed)

    # 4) timestamps uniform in [start_date, end_date)
    total_seconds = max(int((end_date - start_date).total_seconds()), 1)
    offsets = rng.integers(0, total_seconds, size=rows)
    ts = [start_date + timedelta(seconds=int(s)) for s in offsets]

    # 5) t_id
    t_ids = list(range(tid_offset, tid_offset + rows))

    # Build DataFrame using columns from sampled frames (they are Polars Series)
    df = pl.DataFrame({
        "t_id": t_ids,
        "cc_num": cards_sample["cc_num"].to_list(),
        "account_id": cards_sample["account_id"].to_list(),
        "merchant_id": merchants_sample["merchant_id"].to_list(),
        "amount": amounts,
        "ip_address": ip_address,
        "card_present": card_present,
        "ts": ts
    })

    return df


def generate_credit_card_transactions_with_location_continuity(
    card_df: pl.DataFrame,
    account_df: pl.DataFrame,  # Must have 'home_country' column
    merchant_df: pl.DataFrame,
    start_date: datetime,
    end_date: datetime,
    rows: int,
    tid_offset: int = 0,
    seed: int = 42,
) -> pl.DataFrame:
    """
    Generate transactions with realistic location patterns using vectorized Polars operations.
    - Most transactions (~85%) happen in home country
    - Some local travel (~10%) to neighboring countries
    - Rare international travel (~5%) with appropriate time gaps

    Args:
        card_df: Card details DataFrame (cc_num, account_id)
        account_df: Account details DataFrame with 'home_country' column
        merchant_df: Merchant details DataFrame (merchant_id, country)
        start_date: Start date for transactions
        end_date: End date for transactions
        rows: Total number of transactions to generate
        tid_offset: Starting transaction ID offset
        seed: Random seed for reproducibility

    Returns:
        DataFrame with columns: t_id, cc_num, account_id, merchant_id, amount, ip_address, card_present, ts
    """
    print("Generating credit card transactions with location continuity (vectorized)...")
    rng = np.random.default_rng(seed)

    # Join card_df with account_df to get home_country
    cards_with_home = card_df.join(
        account_df.select(["account_id", "home_country"]),
        on="account_id",
        how="left"
    ).with_columns(
        pl.col("home_country").fill_null("United States")
    )

    if "home_country" not in cards_with_home.columns:
        raise ValueError("account_df must have 'home_country' column. Call assign_cardholder_home_locations() first.")

    num_cards = cards_with_home.height
    if num_cards == 0:
        raise ValueError("card_df is empty")

    transactions_per_card = rows // num_cards
    extra_transactions = rows % num_cards
    total_seconds = int((end_date - start_date).total_seconds())

    if total_seconds <= 0:
        raise ValueError("end_date must be after start_date")

    print(f"  Generating {rows} transactions for {num_cards} cards...")
    print(f"  Average {transactions_per_card} transactions per card")

    # Step 1: Create card index for each transaction (vectorized distribution)
    card_indices = []
    for i in range(num_cards):
        count = transactions_per_card + (1 if i < extra_transactions else 0)
        card_indices.extend([i] * count)

    # Step 2: Create base transaction DataFrame with card assignments
    txn_df = pl.DataFrame({
        "t_id": range(tid_offset, tid_offset + rows),
        "card_idx": card_indices,
        "random_seconds": rng.integers(0, total_seconds, size=rows),
    })

    # Step 3: Join with card info
    # Cast card_idx to u32 to match with_row_index dtype
    cards_indexed = cards_with_home.with_row_index("card_idx")
    txn_df = txn_df.with_columns(pl.col("card_idx").cast(pl.UInt32))
    txn_df = txn_df.join(cards_indexed, on="card_idx", how="left")

    # Step 4: Generate timestamps and sort by card and time
    txn_df = txn_df.with_columns([
        (pl.lit(start_date) + pl.duration(seconds=pl.col("random_seconds"))).alias("ts")
    ]).sort(["card_idx", "ts"])

    # Step 5: Add transaction sequence number per card
    txn_df = txn_df.with_columns([
        pl.col("ts").cum_count().over("card_idx").alias("txn_seq")
    ])

    print("  Assigning countries to transactions...")

    # Step 6: Determine location changes
    # For simplicity and speed, we'll assign countries probabilistically
    # without complex state tracking (trade-off for performance)
    location_choices = rng.choice(
        ["home", "local", "international"],
        size=rows,
        p=[0.85, 0.10, 0.05]
    )

    txn_df = txn_df.with_columns([
        pl.Series("location_type", location_choices)
    ])

    # Step 7: Assign merchant country based on location type
    # For home: use home_country
    # For local: use neighboring countries (that exist in merchant list)
    # For international: use random countries

    # Get list of all countries available in merchant data
    available_merchant_countries = set(merchant_df["country"].unique().to_list())
    all_countries = list(available_merchant_countries)

    # Generate neighboring countries for "local" transactions
    # Only use neighbors that actually exist in merchant list
    neighbor_countries = []
    for row in txn_df.iter_rows(named=True):
        home = row["home_country"]
        neighbors = NEIGHBORING_COUNTRIES.get(home, [])
        # Filter to only neighbors that have merchants
        valid_neighbors = [n for n in neighbors if n in available_merchant_countries]

        if valid_neighbors:
            # Pick a random valid neighbor
            neighbor = rng.choice(valid_neighbors)
        elif home in available_merchant_countries:
            # No valid neighbors, but home country has merchants - use home
            neighbor = home
        else:
            # No valid neighbors AND home has no merchants - pick a random country
            # This keeps the transaction "somewhere" rather than falling back badly
            neighbor = rng.choice(all_countries)
        neighbor_countries.append(neighbor)

    # Generate random countries for international transactions
    random_countries = rng.choice(all_countries, size=rows)

    txn_df = txn_df.with_columns([
        pl.Series("neighbor_country", neighbor_countries),
        pl.Series("random_country", random_countries)
    ])

    # Assign merchant_country with fallback logic for countries without merchants
    # - "home" -> home_country (or neighbor if home has no merchants)
    # - "local" -> neighbor_country
    # - "international" -> random_country
    merchant_countries = []
    for row in txn_df.iter_rows(named=True):
        location_type = row["location_type"]
        home = row["home_country"]

        if location_type == "home":
            # For home transactions, prefer home_country
            if home in available_merchant_countries:
                merchant_countries.append(home)
            else:
                # Home has no merchants, use a neighbor or random
                neighbors = NEIGHBORING_COUNTRIES.get(home, [])
                valid_neighbors = [n for n in neighbors if n in available_merchant_countries]
                if valid_neighbors:
                    merchant_countries.append(rng.choice(valid_neighbors))
                else:
                    merchant_countries.append(rng.choice(all_countries))
        elif location_type == "local":
            merchant_countries.append(row["neighbor_country"])
        else:  # international
            merchant_countries.append(row["random_country"])

    txn_df = txn_df.with_columns([
        pl.Series("merchant_country", merchant_countries)
    ])

    print("  Assigning merchants...")

    # Step 8: Assign merchants using groupby for better performance
    # First, add a random value per transaction for sampling within country
    txn_df = txn_df.with_columns([
        pl.Series("rand_for_merchant", rng.random(size=rows))
    ])

    # Create merchant counts per country for modulo-based selection
    merchant_counts = merchant_df.group_by("country").agg([
        pl.col("merchant_id").count().alias("merchant_count")
    ])

    # Join to get merchant count per transaction
    txn_df = txn_df.join(merchant_counts, left_on="merchant_country", right_on="country", how="left")

    # For countries with no merchants, use fallback
    txn_df = txn_df.with_columns([
        pl.col("merchant_count").fill_null(merchant_df.height)
    ])

    # Calculate merchant index to select (deterministic based on random value)
    txn_df = txn_df.with_columns([
        (pl.col("rand_for_merchant") * pl.col("merchant_count")).cast(pl.Int64).alias("merchant_idx")
    ])

    # Now we need to join with merchants - create indexed merchant df
    merchant_indexed = merchant_df.with_row_index("merchant_row_idx")

    # Group merchants by country and add index within country
    merchant_indexed = merchant_indexed.with_columns([
        (pl.col("merchant_id").cum_count().over("country") - 1).cast(pl.Int64).alias("country_merchant_idx")
    ])

    # Join transactions with merchants based on country and index
    txn_df = txn_df.join(
        merchant_indexed.select(["country", "country_merchant_idx", "merchant_id"]),
        left_on=["merchant_country", "merchant_idx"],
        right_on=["country", "country_merchant_idx"],
        how="left"
    )

    # For any nulls (countries not in merchant_df or index mismatch),
    # assign a random merchant from the SAME country
    # First, create a backup merchant selection per country
    country_to_merchants = {}
    for country in all_countries:
        country_merchants = merchant_df.filter(pl.col("country") == country)["merchant_id"].to_list()
        if country_merchants:
            country_to_merchants[country] = country_merchants

    # For each null merchant_id, pick a random merchant from the correct country
    fallback_merchants = []
    for row in txn_df.iter_rows(named=True):
        if row["merchant_id"] is None:
            country = row["merchant_country"]
            if country in country_to_merchants:
                fallback_merchants.append(rng.choice(country_to_merchants[country]))
            else:
                # Country has no merchants - use any random merchant
                fallback_merchants.append(rng.choice(merchant_df["merchant_id"].to_list()))
        else:
            fallback_merchants.append(row["merchant_id"])

    txn_df = txn_df.with_columns([
        pl.Series("merchant_id", fallback_merchants)
    ])

    print("  Generating amounts and other attributes...")

    # Step 9: Generate amounts (vectorized)
    amounts = np.round(rng.lognormal(mean=3.5, sigma=1.2, size=rows), 2)

    # Step 10: Generate card_present (vectorized)
    card_present = rng.random(size=rows) < 0.6

    # Step 11: Generate IP addresses with same-subnet logic for consecutive transactions
    print("  Generating IP addresses...")

    # Generate IPs considering previous transactions per card
    ip_addresses = []
    prev_ip_per_card = {}  # Track previous IP for each card
    prev_country_per_card = {}  # Track previous country for each card

    for row in txn_df.iter_rows(named=True):
        card_idx = row["card_idx"]
        country = row["merchant_country"]

        # Check if this card has a previous transaction
        if card_idx in prev_ip_per_card and card_idx in prev_country_per_card:
            prev_ip = prev_ip_per_card[card_idx]
            prev_country = prev_country_per_card[card_idx]

            # If same country, 60% chance to use same subnet
            if country == prev_country and rng.random() < 0.6:
                # Use same subnet
                ip = generate_ip_in_same_subnet(prev_ip, seed=seed + len(ip_addresses))
            else:
                # Different country or random variation - generate new IP
                ip = generate_ip_for_country(country, seed=seed + len(ip_addresses))
        else:
            # First transaction for this card
            ip = generate_ip_for_country(country, seed=seed + len(ip_addresses))

        # Update tracking
        prev_ip_per_card[card_idx] = ip
        prev_country_per_card[card_idx] = country
        ip_addresses.append(ip)

    # Add IP addresses to DataFrame
    txn_df = txn_df.with_columns([
        pl.Series("ip_address", ip_addresses)
    ])

    # Step 12: Assemble final DataFrame with correct column order
    result_df = txn_df.select([
        "t_id",
        "cc_num",
        "account_id",
        "merchant_id",
    ]).with_columns([
        pl.Series("amount", amounts),
        txn_df["ip_address"],
        pl.Series("card_present", card_present),
        txn_df["ts"]
    ])

    print(f"  Generated {result_df.height} transactions")
    return result_df


# ---------------------------
# Feature group helper - add account_id description
# ---------------------------

def create_feature_group_with_descriptions(fs, df, name, description, primary_key, event_time_col=None, topic_name=None, online_enabled=True, 
                                           features=None, time_travel_format="DELTA"):
    """Create feature group and add feature descriptions"""
    print(f"Creating feature group: {name}")
    if topic_name is None:
        try:
            topic_name = fs.name.removesuffix("_featurestore")
        except Exception:
            topic_name = None

    if features == None:
        fg = fs.create_feature_group(
            name=name,
            version=1,
            description=description,
            primary_key=primary_key,
            event_time=event_time_col,
            topic_name=topic_name,
            online_enabled=online_enabled,
            time_travel_format=time_travel_format
        )
    else:
        fg = fs.create_feature_group(
            name=name,
            version=1,
            description=description,
            primary_key=primary_key,
            event_time=event_time_col,
            topic_name=topic_name,
            online_enabled=online_enabled,
            time_travel_format=time_travel_format,
            features=features
        )

    fg.insert(df)

    feature_descriptions = {
        "merchant_details": {
            "merchant_id": "Unique sequential integer identifier for each merchant",
            "category": "Merchant category codes for goods or service purchased",
            "country": "Country where merchant resides",
            "cnt_chrgeback_prev_day": "Number of chargebacks for this merchant during the previous day",
            "cnt_chrgeback_prev_week": "Number of chargebacks for this merchant during the previous week",
            "cnt_chrgeback_prev_month": "Number of chargebacks for this merchant during the previous month",
            "last_modified": "Timestamp when the merchant details was last updated"
        },
        "bank_details": {
            "bank_id": "Unique sequential integer identifier for each bank",
            "country": "Country where bank resides",
            "credit_rating": "Bank credit rating on a scale of 1 to 10",
            "last_modified": "Timestamp when the bank details was last updated"
        },
        "account_details": {
            "account_id": "Unique sequential integer identifier for the card owner",
            "name": "Full name of the account owner",
            "address": "Address where account owner resides",
            "debt_end_prev_month": "Amount of debt/credit at end of previous month",
            "last_modified": "Timestamp when the account_details was last updated",
            "creation_date": "Timestamp when the account was created",
            "end_date": "Timestamp when the account was closed (null if still active)"
        },
        "card_details": {
            "card_id": "Unique sequential integer identifier for each card",
            "cc_num": "Credit card number in format XXXX-XXXX-XXXX-XXXX",
            "cc_expiry_date": "Card's expiration date (year and month only)",
            "account_id": "Foreign key reference to account_details table (bigint)",
            "bank_id": "Foreign key reference to bank_details table (bigint)",
            "issue_date": "Card's issue date (0 to 3 years before current date)",
            "card_type": "Type of card (Credit, Debit, Prepaid)",
            "status": "Current status of the card (Active, Blocked, Lost/Stolen)",
            "last_modified": "Timestamp when the card details was last updated"
        },
        "credit_card_transactions": {
            "t_id": "Unique identifier for this credit card transaction",
            "cc_num": "Foreign key reference to credit card (card_details.cc_num)",
            "account_id": "Foreign key reference to account_details (via card_details.account_id)",
            "merchant_id": "Foreign key reference to merchant table",
            "amount": "Credit card transaction amount in decimal format",
            "ip_address": "IP address of the physical or online merchant (format: XXX.XXX.XXX.XXX)",
            "card_present": "Whether credit card was used in a physical terminal (true) or online payment (false)",
            "ts": "Timestamp for this credit card transaction"
        },
        "cc_fraud": {
            "t_id": "Unique identifier for this credit card transaction",
            "cc_num": "Foreign key reference to credit card (card_details.cc_num)",
            "explanation": "Reasoning for why the Credit card transaction was marked as fraudulent (e.g., geographic ",
            "ts": "Timestamp for this credit card transaction"
        }
    }

    if name in feature_descriptions:
        for feature_name, feature_desc in feature_descriptions[name].items():
            if feature_name in df.columns:
                try:
                    fg.update_feature_description(feature_name=feature_name, description=feature_desc)
                    print(f"  Added description for: {feature_name}")
                except Exception as e:
                    print(f"  Warning: Could not add description for {feature_name}: {e}")

    return fg





def generate_fraud(
    transaction_df: pl.DataFrame,
    card_df: pl.DataFrame,
    merchant_df: pl.DataFrame,
    fraud_rate: float = 0.0001,  # 0.01%
    chain_attack_ratio: float = 0.9,  # 90% of fraud is chain attacks
    seed: int = 42
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Generate fraudulent transaction records and add them to the transaction DataFrame.
    IP addresses are generated to resolve in GeoIP database.
    
    Args:
        transaction_df: DataFrame with legitimate transactions (must have: t_id, cc_num, ts, amount, card_present)
        card_df: DataFrame with card details (must have: cc_num, account_id)
        merchant_df: DataFrame with merchant details (must have: merchant_id, country)
        fraud_rate: Percentage of transactions that should be fraudulent (default: 0.0001 = 0.01%)
        chain_attack_ratio: Ratio of chain attacks to total fraud (default: 0.9 = 90%)
        seed: Random seed for reproducibility
    
    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: 
            - Updated transaction_df with fraudulent transactions added
            - fraud_df with columns: t_id, cc_num, explanation, ts
    """
    print("Generating fraudulent transactions...")
    
    np.random.seed(seed)
    random.seed(seed)
    
    # Calculate number of fraud cases
    total_transactions = transaction_df.height
    total_fraud_transactions = max(int(total_transactions * fraud_rate), 1)
    
    # Split fraud between types
    chain_attack_transactions = int(total_fraud_transactions * chain_attack_ratio)
    geographic_fraud_transactions = total_fraud_transactions - chain_attack_transactions
    
    print(f"Total transactions: {total_transactions}")
    print(f"Generating {total_fraud_transactions} fraudulent transactions:")
    print(f"  - Chain attacks: {chain_attack_transactions} transactions")
    print(f"  - Geographic fraud: {geographic_fraud_transactions} transactions")
    
    fraud_records = []
    
    # Get the max t_id from original transactions to continue numbering
    max_t_id = transaction_df["t_id"].max()
    next_t_id = max_t_id + 1
    
    # ===========================
    # 1. CHAIN ATTACKS (90%)
    # ===========================
    if chain_attack_transactions > 0:
        # Get available cards with account_id
        available_cards = card_df.select(["cc_num", "account_id"]).unique()
        
        # Each chain attack will consist of 5-15 small transactions
        num_chain_attacks = max(chain_attack_transactions // 8, 1)  # Average 8 transactions per attack
        
        # Get time range from transaction_df
        min_ts = transaction_df["ts"].min()
        max_ts = transaction_df["ts"].max()
        total_seconds = int((max_ts - min_ts).total_seconds())
        
        # Sample merchants with countries
        available_merchants = merchant_df.select(["merchant_id", "country"])
        
        transactions_generated = 0
        
        for attack_idx in range(num_chain_attacks):
            if transactions_generated >= chain_attack_transactions:
                break
                
            # Select a random card for this attack
            card_sample = available_cards.sample(n=1, shuffle=True, seed=seed + attack_idx)
            cc_num = card_sample["cc_num"][0]
            account_id = card_sample["account_id"][0]
            
            # Number of transactions in this chain (5-15)
            num_txns_in_chain = random.randint(5, 15)
            num_txns_in_chain = min(num_txns_in_chain, chain_attack_transactions - transactions_generated)
            
            # Start time for this attack (random time within the transaction period)
            attack_start_seconds = random.randint(0, max(1, total_seconds - 7200))  # Leave room for 2 hours
            attack_start = min_ts + timedelta(seconds=attack_start_seconds)
            
            # Duration of attack: 10 minutes to 2 hours
            attack_duration_seconds = random.randint(600, 7200)
            
            # Generate IP address(es) for this attack - use a random country from GeoIP-valid ranges
            attack_country = random.choice(list(COUNTRY_IP_RANGES.keys()))
            primary_ip = generate_ip_for_country(attack_country, seed=seed + attack_idx)

            # 30% of attacks use multiple IPs from same country (more sophisticated)
            use_multiple_ips = attack_idx % 3 == 0
            secondary_ips = []
            if use_multiple_ips:
                # Generate 1-2 additional IPs from same country
                num_additional_ips = random.randint(1, 2)
                for i in range(num_additional_ips):
                    secondary_ips.append(
                        generate_ip_for_country(attack_country, seed=seed + attack_idx + i + 100)
                    )

            all_ips = [primary_ip] + secondary_ips

            # Track timing for burst patterns
            # Ensure monotonically increasing timestamps
            previous_txn_offset = 0

            # Generate transactions in this chain
            for txn_idx in range(num_txns_in_chain):
                # IP selection: rotate through available IPs
                ip_address = all_ips[txn_idx % len(all_ips)]

                # Transaction timing with burst patterns
                # 40% of transactions come in bursts (very close together)
                if txn_idx > 0 and random.random() < 0.4:
                    # Burst: 10 seconds to 2 minutes after previous
                    txn_offset = previous_txn_offset + random.randint(10, 120)
                else:
                    # Normal spread: pick random time but ensure it's after previous
                    remaining_time = attack_duration_seconds - previous_txn_offset
                    if remaining_time > 10:
                        gap = random.randint(10, min(remaining_time, 1800))  # 10 sec to 30 min gap
                        txn_offset = previous_txn_offset + gap
                    else:
                        # No time left, add small gap
                        txn_offset = previous_txn_offset + random.randint(5, 30)

                # Ensure we don't exceed attack duration
                txn_offset = min(txn_offset, attack_duration_seconds)
                previous_txn_offset = txn_offset
                txn_ts = attack_start + timedelta(seconds=txn_offset)

                # Varied amounts: test with small amounts first, then larger
                # Create dramatic progression to ensure test passes
                if txn_idx == 0:
                    # First transaction is always very small (testing if card works)
                    amount = round(random.uniform(1.0, 3.0), 2)
                elif txn_idx < 3:
                    # Next couple are small (building confidence)
                    amount = round(random.uniform(5.0, 15.0), 2)
                else:
                    # Later transactions are much larger (maximizing theft)
                    amount = round(random.uniform(35.0, 49.99), 2)
                
                # Merchant for this transaction
                merchant_sample = available_merchants.sample(n=1, shuffle=True, seed=seed + attack_idx + txn_idx)
                merchant_id = merchant_sample["merchant_id"][0]
                
                # Create fraud record
                fraud_records.append({
                    "t_id": next_t_id,
                    "cc_num": cc_num,
                    "account_id": account_id,
                    "merchant_id": merchant_id,
                    "amount": amount,
                    "ip_address": ip_address,
                    "card_present": False,  # Chain attacks are typically online
                    "explanation": f"Chain attack: Multiple small transactions (${amount}) in short time period",
                    "ts": txn_ts,
                    "fraud_type": "chain_attack"
                })
                
                next_t_id += 1
                transactions_generated += 1
    
    # ===========================
    # 2. GEOGRAPHIC FRAUD (10%)
    # ===========================
    if geographic_fraud_transactions > 0:
        # Get cards and merchants with countries
        available_cards = card_df.select(["cc_num", "account_id"]).unique()
        merchants_with_country = merchant_df.select(["merchant_id", "country"])
        
        # Define pairs of geographically distant countries for impossible travel scenarios
        # These are intentionally far apart to create clear fraud signals
        distant_country_pairs = [
            ("United States", "China"),      # ~11,000 km
            ("United States", "Japan"),      # ~10,000 km
            ("United States", "Australia"),  # ~15,000 km
            ("United Kingdom", "Australia"), # ~17,000 km
            ("United Kingdom", "Japan"),     # ~9,500 km
            ("Germany", "China"),            # ~7,500 km
            ("France", "India"),             # ~7,000 km
            ("Canada", "Russia"),            # ~7,000 km
            ("Brazil", "China"),             # ~17,000 km
            ("Spain", "Australia"),          # ~18,000 km
            ("Italy", "Japan"),              # ~9,700 km
            ("Mexico", "India"),             # ~14,500 km
        ]
        
        # Filter to only use country pairs where we have merchants for both countries
        available_countries = set(merchants_with_country["country"].unique().to_list())
        valid_country_pairs = [
            (c1, c2) for c1, c2 in distant_country_pairs 
            if c1 in available_countries and c2 in available_countries
        ]
        
        if not valid_country_pairs:
            print("Warning: No valid distant country pairs found for geographic fraud")
            # Fallback: use any two different countries
            countries_list = list(available_countries)
            if len(countries_list) >= 2:
                valid_country_pairs = [(countries_list[i], countries_list[j]) 
                                      for i in range(len(countries_list)) 
                                      for j in range(i+1, len(countries_list))][:10]
            else:
                valid_country_pairs = []
        
        if not valid_country_pairs:
            print("Warning: Cannot generate geographic fraud - insufficient countries")
        else:
            # Get time range from transaction_df
            min_ts = transaction_df["ts"].min()
            max_ts = transaction_df["ts"].max()
            total_seconds = int((max_ts - min_ts).total_seconds())
            
            # Each geographic fraud is a pair of transactions
            num_geographic_pairs = max(geographic_fraud_transactions // 2, 1)
            
            for pair_idx in range(num_geographic_pairs):
                # Select a random card
                card_sample = available_cards.sample(n=1, shuffle=True, seed=seed + 1000 + pair_idx)
                cc_num = card_sample["cc_num"][0]
                account_id = card_sample["account_id"][0]
                
                # Select two distant countries (cycle through the list)
                country1, country2 = valid_country_pairs[pair_idx % len(valid_country_pairs)]
                
                # Get merchants from each country
                merchants_c1 = merchants_with_country.filter(pl.col("country") == country1)["merchant_id"].to_list()
                merchants_c2 = merchants_with_country.filter(pl.col("country") == country2)["merchant_id"].to_list()
                
                if not merchants_c1 or not merchants_c2:
                    continue
                
                merchant1 = random.choice(merchants_c1)
                merchant2 = random.choice(merchants_c2)
                
                # First transaction time
                txn1_seconds = random.randint(0, max(1, total_seconds - 3600))
                txn1_ts = min_ts + timedelta(seconds=txn1_seconds)

                # Second transaction: VERY short time gap to ensure impossible travel
                # For distant countries (10,000+ km), even 30 minutes is clearly impossible
                # Vary the gap to create different fraud patterns
                if pair_idx % 3 == 0:
                    # Super short gap - 5-15 minutes (clearly impossible)
                    time_gap_seconds = random.randint(300, 900)
                elif pair_idx % 3 == 1:
                    # Short gap - 15-30 minutes (impossible for long distances)
                    time_gap_seconds = random.randint(900, 1800)
                else:
                    # Moderate gap - 30-60 minutes (still impossible for distant countries)
                    time_gap_seconds = random.randint(1800, 3600)

                txn2_ts = txn1_ts + timedelta(seconds=time_gap_seconds)
                
                # Amounts for both transactions
                amount1 = round(np.random.lognormal(mean=3.5, sigma=1.2), 2)
                amount2 = round(np.random.lognormal(mean=3.5, sigma=1.2), 2)
                
                # Generate IPs for each country - using GeoIP-valid IPs
                ip_address1 = generate_ip_for_country(country1, seed=seed + pair_idx * 2)
                ip_address2 = generate_ip_for_country(country2, seed=seed + pair_idx * 2 + 1)
                
                # Distance explanation
                time_gap_minutes = time_gap_seconds // 60
                
                # First transaction - card present (physical terminal)
                fraud_records.append({
                    "t_id": next_t_id,
                    "cc_num": cc_num,
                    "account_id": account_id,
                    "merchant_id": merchant1,
                    "amount": amount1,
                    "ip_address": ip_address1,
                    "explanation": f"Geographic fraud: Card present transaction in {country1}, followed by card present transaction in {country2} only {time_gap_minutes} minutes later (impossible travel)",
                    "ts": txn1_ts,
                    "fraud_type": "geographic",
                    "card_present": True  # Physical card used
                })
                
                next_t_id += 1
                
                # Second transaction - card present (physical terminal)
                fraud_records.append({
                    "t_id": next_t_id,
                    "cc_num": cc_num,
                    "account_id": account_id,
                    "merchant_id": merchant2,
                    "amount": amount2,
                    "ip_address": ip_address2,
                    "explanation": f"Geographic fraud: Card present transaction in {country2}, preceded by card present transaction in {country1} only {time_gap_minutes} minutes earlier (impossible travel)",
                    "ts": txn2_ts,
                    "fraud_type": "geographic",
                    "card_present": True  # Physical card used
                })
                
                next_t_id += 1
    
    # Convert to DataFrame
    if not fraud_records:
        print("Warning: No fraud records generated")
        empty_fraud_df = pl.DataFrame({
            "t_id": [],
            "cc_num": [],
            "explanation": [],
            "ts": []
        })
        return transaction_df, empty_fraud_df
    
    fraud_full_df = pl.DataFrame(fraud_records)
    
    # Create the fraud metadata DataFrame (for cc_fraud table)
    fraud_df = fraud_full_df.select(["t_id", "cc_num", "explanation", "ts"])
    
    # Create transaction records to add to transaction_df
    fraud_transactions = fraud_full_df.select([
        "t_id", "cc_num", "account_id", "merchant_id", 
        "amount", "ip_address", "card_present", "ts"
    ])
    
    # Combine with original transactions
    updated_transaction_df = pl.concat([transaction_df, fraud_transactions])
    
    print(f"Generated {fraud_df.height} fraudulent transaction records")
    print(f"  - Chain attacks: {fraud_full_df.filter(pl.col('fraud_type') == 'chain_attack').height}")
    print(f"  - Geographic fraud: {fraud_full_df.filter(pl.col('fraud_type') == 'geographic').height}")
    print(f"Updated transaction_df now has {updated_transaction_df.height} total transactions")
    
    return updated_transaction_df, fraud_df
