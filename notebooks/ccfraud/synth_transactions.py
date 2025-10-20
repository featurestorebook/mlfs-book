# import polars as pl
# import numpy as np
# from faker import Faker
# from datetime import datetime, timedelta
# from geopy.distance import geodesic
# import random
# import uuid
# import hsfs
# import hopsworks

# fake = Faker()
# Faker.seed(42)
# np.random.seed(42)
# random.seed(42)

# def generate_cc_fraud(transactions_df, current_date, start_date, end_date):
#     """Generate credit card fraud table"""
#     print("Generating credit card fraud...")
    
#     # 1% of transactions are fraudulent
#     n_fraud = len(transactions_df) // 100
#     selected_indices = random.sample(range(len(transactions_df)), n_fraud)
#     selected_transactions = transactions_df[selected_indices]
    
#     # Fraud type distribution
#     fraud_types = (['Card Not Present'] * (n_fraud // 2) + 
#                   ['Skimming'] * (n_fraud * 3 // 10) + 
#                   ['Lost/Stolen'] * (n_fraud // 5))
    
#     # Adjust for exact count
#     while len(fraud_types) < n_fraud:
#         fraud_types.append('Card Not Present')
#     fraud_types = fraud_types[:n_fraud]
#     random.shuffle(fraud_types)
    
#     explanations = [
#         "Suspicious transaction pattern detected",
#         "Card used in multiple locations simultaneously", 
#         "Transaction amount significantly higher than usual",
#         "Card used after being reported lost",
#         "Unusual merchant category for cardholder",
#         "Transaction from high-risk location",
#         "Card details compromised in data breach",
#         "Skimming device detected at ATM/POS",
#         "Online transaction with invalid security codes",
#         "Card used without PIN verification"
#     ]
    
#     # Generate fraud event times (1-12 months after transaction event_time)
#     fraud_event_times = []
#     transaction_times = selected_transactions['event_time'].to_list()
#     for transaction_time in transaction_times:
#         days_after = random.randint(30, 365)  # 1-12 months
#         fraud_time = transaction_time + timedelta(days=days_after)
#         fraud_event_times.append(fraud_time)
    
#     fraud_data = {
#         't_id': list(range(1, n_fraud + 1)),  # Auto-increment
#         'explanation': [random.choice(explanations) for _ in range(n_fraud)],
#         'event_time': fraud_event_times,
#         'fraud_type': fraud_types
#     }
    
#     df = pl.DataFrame(fraud_data)
#     return df


# def get_location_from_ip(ip_address):
#     """
#     Get approximate location (lat, lon) from IP address using a free IP geolocation service
#     This is a simplified version - in production you'd use a more reliable service
#     """
#     # For demonstration, we'll create a mapping of IP ranges to major cities
#     # In real implementation, you'd use an IP geolocation API
    
#     ip_parts = [int(x) for x in ip_address.split('.')]
#     first_octet = ip_parts[0]
    
#     # Simple mapping based on first octet (not geographically accurate, just for demo)
#     city_locations = {
#         range(1, 50): (40.7128, -74.0060),    # New York
#         range(50, 100): (34.0522, -118.2437), # Los Angeles  
#         range(100, 150): (51.5074, -0.1278),  # London
#         range(150, 200): (35.6762, 139.6503), # Tokyo
#         range(200, 255): (-33.8688, 151.2093) # Sydney
#     }
    
#     for ip_range, location in city_locations.items():
#         if first_octet in ip_range:
#             return location
    
#     # Default location if not found
#     return (40.7128, -74.0060)  # New York

# def generate_distant_ip_pairs():
#     """Generate pairs of IP addresses that are geographically distant"""
#     distant_pairs = []
    
#     # Major cities with their approximate coordinates
#     cities = [
#         ("1.1.1.1", 40.7128, -74.0060),     # New York
#         ("80.80.80.80", 34.0522, -118.2437), # Los Angeles
#         ("120.120.120.120", 51.5074, -0.1278), # London
#         ("160.160.160.160", 35.6762, 139.6503), # Tokyo
#         ("200.200.200.200", -33.8688, 151.2093), # Sydney
#         ("50.50.50.50", 41.8781, -87.6298),   # Chicago
#         ("90.90.90.90", 48.8566, 2.3522),    # Paris
#         ("130.130.130.130", 55.7558, 37.6173), # Moscow
#         ("170.170.170.170", -22.9068, -43.1729), # Rio de Janeiro
#         ("210.210.210.210", 31.2304, 121.4737)  # Shanghai
#     ]
    
#     # Generate pairs that are far apart (>1000 km)
#     for i in range(len(cities)):
#         for j in range(i+1, len(cities)):
#             city1 = cities[i]
#             city2 = cities[j]
#             distance = geodesic((city1[1], city1[2]), (city2[1], city2[2])).kilometers
#             if distance > 1000:  # At least 1000 km apart
#                 distant_pairs.append((city1[0], city2[0], distance))
    
#     return distant_pairs

# # def create_geographic_attacks(tid_offset, num_merchants, card_numbers, num_attacks=1000, start_date, end_date):
# #     """Create geographic attack transactions"""
# #     print(f"Creating {num_attacks} geographic attacks...")

# def create_geographic_attacks(
#     tid_offset: int,
#     num_merchants: int,
#     card_numbers: list[str],
#     start_date: datetime,
#     end_date: datetime,
#     num_attacks: int = 1000,
# ):

#     distant_pairs = generate_distant_ip_pairs()
#     geographic_transactions = []
#     fraud_records = []
    
#     # current_date = datetime(2024, 9, 11)
#     # start_date = current_date - timedelta(days=1*30)  # 1 month ago
    
#     for attack_id in range(num_attacks):
#         # Select random card
#         card_num = random.choice(card_numbers)
        
#         # Select random distant IP pair
#         ip1, ip2, distance = random.choice(distant_pairs)
        
#         # Generate first transaction timestamp
#         ts1 = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
        
#         # Generate second transaction 1-30 minutes later (impossible to travel that distance)
#         ts2 = ts1 + timedelta(minutes=random.randint(1, 30))
        
#         # Transaction amounts
#         amount1 = round(np.random.lognormal(3.5, 1.2), 2)
#         amount2 = round(np.random.lognormal(3.5, 1.2), 2)
        
#         # Merchant IDs
#         merchant1 = f"MERCH_{random.randint(0, num_merchants):05d}"
#         merchant2 = f"MERCH_{random.randint(0, num_merchants):05d}"
        
#         # Create transaction IDs
#         base_id = tid_offset + attack_id * 2  # Start after existing transactions
#         t_id1 = base_id
#         t_id2 = base_id+1
        
#         # First transaction
#         transaction1 = {
#             "t_id": t_id1,
#             "cc_num": card_num,
#             "merchant_id": merchant1,
#             "amount": amount1,
#             "ip_address": ip1,
#             "card_present": True,  # Both must be card present
#             "ts": ts1
#         }
        
#         # Second transaction  
#         transaction2 = {
#             "t_id": t_id2,
#             "cc_num": card_num,
#             "merchant_id": merchant2,
#             "amount": amount2,
#             "ip_address": ip2,
#             "card_present": True,  # Both must be card present
#             "ts": ts2
#         }
        
#         geographic_transactions.extend([transaction1, transaction2])
        
#         # Determine fraud type for geographic attack
#         fraud_types = ["Skimming", "Lost/Stolen"]
#         fraud_type = random.choice(fraud_types)
        
#         # Fraud detection time: 1-12 months after transaction
#         fraud_delay_days = random.randint(30, 365)
#         fraud_time1 = ts1 + timedelta(days=fraud_delay_days)
#         fraud_time2 = ts2 + timedelta(days=fraud_delay_days)
        
#         explanation = f"Geographic attack: Transactions {distance:.0f}km apart within {(ts2-ts1).total_seconds()/60:.1f} minutes"
        
#         fraud_records.extend([
#             {
#                 "t_id": base_id,
#                 "explanation": explanation,
#                 "event_time": fraud_time1,
#                 "fraud_type": fraud_type
#             },
#             {
#                 "t_id": base_id + 1,
#                 "explanation": explanation,
#                 "event_time": fraud_time2,
#                 "fraud_type": fraud_type
#             }
#         ])
    
#     return geographic_transactions, fraud_records

# # def create_chain_attacks(tid_offset, num_merchants, card_numbers, num_attacks=500, start_date, end_date):
# #     """Create chain attack transactions"""

# def create_chain_attacks(
#     tid_offset: int,
#     num_merchants: int,
#     card_numbers: list[str],
#     start_date: datetime,
#     end_date: datetime,
#     num_attacks: int = 500,
# ):
#     print(f"Creating {num_attacks} chain attacks...")
    
#     chain_transactions = []
#     fraud_records = []
        
#     transaction_counter = tid_offset  # Start after geographic attacks
    
#     for attack_id in range(num_attacks):
#         # Select random card
#         card_num = random.choice(card_numbers)
        
#         # Number of transactions in this chain (5-50)
#         num_transactions = random.randint(5, 50)
        
#         # Start time for the chain
#         chain_start = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
        
#         # Chain duration: 1-6 hours
#         chain_duration_hours = random.uniform(0.5, 6.0)
#         chain_end = chain_start + timedelta(hours=chain_duration_hours)
        
#         # Card present or not (consistent for the whole chain)
#         card_present = random.choice([True, False])
        
#         # Generate IP address for the chain (same for all transactions in online chains)
#         if card_present:
#             # Different locations for card present
#             base_ip = f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
#         else:
#             # Same IP for online transactions
#             ip_address = f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
        
#         chain_transaction_ids = []
        
#         for txn_idx in range(num_transactions):
#             # Transaction timestamp within chain duration
#             txn_time = chain_start + timedelta(seconds=random.randint(0, int(chain_duration_hours * 3600)))
            
#             # Small amounts for chain attacks (typically under $100)
#             amount = round(random.uniform(5.00, 99.99), 2)
            
#             # Merchant ID
#             merchant_id = f"MERCH_{random.randint(0, num_merchants):05d}"
                        
#             # IP address
#             if card_present:
#                 # Vary the last octet for different ATMs/terminals
#                 txn_ip = f"{base_ip}.{random.randint(1, 255)}"
#             else:
#                 txn_ip = ip_address
            
#             transaction = {
#                 "t_id": transaction_counter,
#                 "cc_num": card_num,
#                 "merchant_id": merchant_id,
#                 "amount": amount,
#                 "ip_address": txn_ip,
#                 "card_present": card_present,
#                 "ts": txn_time
#             }

#             transaction_counter += 1
            
#             chain_transactions.append(transaction)
#             chain_transaction_ids.append(transaction_counter - 1)
        
#         fraud_type = "Card Not Present"  # Chain attacks are typically card not present fraud
        
#         # Create fraud record for each transaction in the chain
#         for i, txn_id in enumerate(chain_transaction_ids):
#             # Fraud detection time: 1-12 months after transaction
#             fraud_delay_days = random.randint(30, 365)
#             fraud_time = chain_start + timedelta(days=fraud_delay_days)
            
#             explanation = f"Chain attack: {num_transactions} transactions within {chain_duration_hours:.1f} hours"
            
#             fraud_records.append({
#                 "t_id": txn_id,
#                 "explanation": explanation,
#                 "event_time": fraud_time,
#                 "fraud_type": fraud_type
#             })
    
#     return chain_transactions, fraud_records


# # def generate_merchant_details(rows=5000, start_date, end_date):
# #     """Generate merchant details DataFrame"""

# def generate_merchant_details(
#     rows: int = 5000,
#     start_date: datetime = None,
#     end_date: datetime = None,
# ) -> pl.DataFrame:
#     print("Generating merchant details...")
    
#     # Generate merchant categories
#     categories = [
#         "Groceries", "Travel", "Fashion", "Healthcare", "Restaurants", "Gas Stations",
#         "Electronics", "Home Improvement", "Entertainment", "Education", "Insurance",
#         "Automotive", "Books", "Sports", "Beauty", "Jewelry", "Pet Supplies", 
#         "Furniture", "Pharmacy", "Department Stores"
#     ] * 25  # 500 types total
    
#     # Get 160 largest countries (excluding North Korea)
#     countries = [
#         "United States", "China", "Japan", "Germany", "India", "United Kingdom", 
#         "France", "Italy", "Brazil", "Canada", "Russia", "South Korea", "Australia",
#         "Spain", "Mexico", "Indonesia", "Netherlands", "Saudi Arabia", "Turkey", 
#         "Taiwan", "Belgium", "Argentina", "Thailand", "Israel", "Poland", "Nigeria",
#         "Egypt", "Vietnam", "Bangladesh", "South Africa", "Philippines", "Chile",
#         "Finland", "Romania", "Czech Republic", "Portugal", "New Zealand", "Peru",
#         "Greece", "Qatar", "Algeria", "Kazakhstan", "Hungary", "Kuwait", "Ukraine",
#         "Morocco", "Slovakia", "Ecuador", "Dominican Republic", "Guatemala", "Oman",
#         "Kenya", "Myanmar", "Luxembourg", "Ghana", "Croatia", "Bolivia", "Uruguay",
#         "Costa Rica", "Lebanon", "Slovenia", "Lithuania", "Tunisia", "Azerbaijan",
#         "Sri Lanka", "Belarus", "Uzbekistan", "Panama", "Latvia", "Cameroon",
#         "Jordan", "Bosnia and Herzegovina", "Georgia", "Albania", "Mongolia",
#         "Armenia", "Jamaica", "Qatar", "Bahrain", "Estonia", "Trinidad and Tobago",
#         "Cyprus", "Mozambique", "Nepal", "Cambodia", "Honduras", "Madagascar",
#         "Senegal", "Zimbabwe", "Afghanistan", "Mali", "Burkina Faso", "Niger",
#         "Malawi", "Zambia", "Somalia", "Chad", "Guinea", "South Sudan", "Rwanda",
#         "Burundi", "Benin", "Tunisia", "Haiti", "Cuba", "Papua New Guinea",
#         "Nicaragua", "Liberia", "Sierra Leone", "Togo", "Libya", "Central African Republic",
#         "Mauritania", "Eritrea", "Gambia", "Botswana", "Gabon", "Lesotho",
#         "Guinea-Bissau", "Equatorial Guinea", "Mauritius", "Estonia", "Swaziland",
#         "Fiji", "Comoros", "Bhutan", "Solomon Islands", "Montenegro", "Luxembourg",
#         "Suriname", "Cape Verde", "Malta", "Brunei", "Belize", "Bahamas",
#         "Iceland", "Vanuatu", "Barbados", "Sao Tome and Principe", "Samoa",
#         "Saint Lucia", "Kiribati", "Micronesia", "Grenada", "Saint Vincent and the Grenadines",
#         "Tonga", "Seychelles", "Antigua and Barbuda", "Andorra", "Dominica",
#         "Marshall Islands", "Saint Kitts and Nevis", "Liechtenstein", "Monaco",
#         "Nauru", "Tuvalu", "San Marino", "Palau", "Vatican City"
#     ]
    
#     delta_days = (end_date - start_date).days
#     merchant_data = {
#         "merchant_id": [f"MERCH_{i:05d}" for i in range(rows)],
#         "category": [random.choice(categories) for _ in range(rows)],
#         "country": [random.choice(countries) for _ in range(rows)],
#         "cnt_chrgeback_prev_day": [round(np.random.exponential(2.5), 2) for _ in range(rows)],
#         "last_modified": [
#             start_date + timedelta(days=random.randint(0, delta_days))
#             for _ in range(rows)
#         ]
#     }
    
#     return pl.DataFrame(merchant_data)

# def generate_bank_details(rows=5000, start_date, end_date):
#     """Generate bank details DataFrame"""
#     print("Generating bank details...")
    
#     countries = [
#         "United States", "China", "Japan", "Germany", "India", "United Kingdom", 
#         "France", "Italy", "Brazil", "Canada", "Russia", "South Korea", "Australia",
#         "Spain", "Mexico", "Indonesia", "Netherlands", "Saudi Arabia", "Turkey", 
#         "Taiwan", "Belgium", "Argentina", "Thailand", "Ireland", "Poland", "Nigeria"
#     ] * 7  # Ensure we have enough
    
#     # Generate negatively skewed credit ratings (1-10, higher ratings more common)
#     credit_ratings = np.random.beta(5, 2, rows) * 9 + 1  # Beta distribution scaled to 1-10
#     credit_ratings = np.round(credit_ratings).astype(int)
#     credit_ratings = np.clip(credit_ratings, 1, 10)  # Ensure within bounds
    
#     delta_days = (end_date - start_date).days
#     bank_data = {
#         "bank_id": [f"BANK_{i:05d}" for i in range(rows)],
#         "country": [random.choice(countries) for _ in range(rows)],
#         "credit_rating": credit_ratings.tolist(),
#         "last_modified": [
#             end_date - timedelta(days=random.randint(0, delta_days))
#             for _ in range(rows)
#         ]
#     }
    
#     return pl.DataFrame(bank_data)


# def generate_account_details(
#     rows: int = 50_000,
#     account_creation_start_date: datetime = None,
#     current_date: datetime = None,
#     account_last_modified_start_date: datetime = None,
# ) -> pl.DataFrame:
#     print("Generating account details...")

#     # last_modified within [account_last_modified_start_date, current_date]
#     delta_days_lm = (current_date - account_last_modified_start_date).days
#     account_data = {
#         "account_id": [f"ACC_{i:06d}" for i in range(rows)],
#         "name": [fake.name() for _ in range(rows)],
#         "address": [fake.address().replace('\n', ', ') for _ in range(rows)],
#         "debt_end_prev_month": [round(np.random.normal(2500, 1500), 2) for _ in range(rows)],
#         "last_modified": [
#             current_date - timedelta(days=random.randint(0, delta_days_lm))
#             for _ in range(rows)
#         ],
#     }

#     # creation & end_date
#     creation_dates, end_dates = [], []
#     delta_days_creation = (current_date - account_creation_start_date).days
#     for _ in range(rows):
#         creation = current_date - timedelta(days=random.randint(0, delta_days_creation))
#         creation_dates.append(creation)
#         if random.random() < 0.9:
#             end_dates.append(None)
#         else:
#             end_dates.append(creation + timedelta(days=random.randint(1, max(1, delta_days_creation))))
#     account_data["creation_date"] = creation_dates
#     account_data["end_date"] = end_dates

#     return pl.DataFrame(account_data)


# def generate_account_details(rows=50000, account_creation_start_date, current_date, account_last_modified_start_date):
#     """Generate account details DataFrame"""
#     print("Generating account details...")
    
#     delta_days = (current_date - account_last_modified_start_date).days
#     account_data = {
#         "account_id": [f"ACC_{i:06d}" for i in range(rows)],
#         "name": [fake.name() for _ in range(rows)],
#         "address": [fake.address().replace('\n', ', ') for _ in range(rows)],
#         "debt_end_prev_month": [round(np.random.normal(2500, 1500), 2) for _ in range(rows)],
#         "last_modified": [
#             end_date - timedelta(days=random.randint(0, delta_days))
#             for _ in range(rows)
#         ]
#     }
    
#     # Generate creation and end dates
#     creation_dates = []
#     end_dates = []
    
#     delta_days = (current_date - account_creation_start_date).days
#     for i in range(rows):
#         creation = current_date - timedelta(days=random.randint(0, delta_days))
#         creation_dates.append(creation)
        
#         # 90% of accounts are still active (no end date)
#         if random.random() < 0.9:
#             end_dates.append(None)
#         else:
#             end_date = creation + timedelta(days=random.randint(0, delta_days))
#             end_dates.append(end_date)
    
#     account_data["creation_date"] = creation_dates
#     account_data["end_date"] = end_dates
    
#     return pl.DataFrame(account_data)

# def generate_card_details(
#     rows: int = 70_000,
#     num_accounts: int = 50_000,
#     num_banks: int = 5_000,
#     current_date: datetime = None,
#     issue_date: datetime = None,   # earliest possible issue date
#     expiry_date: datetime = None,  # latest possible expiry date
# ) -> pl.DataFrame:
#     print("Generating card details...")

#     account_ids = [f"ACC_{i:06d}" for i in range(num_accounts)]
#     bank_ids = [f"BANK_{i:05d}" for i in range(num_banks)]

#     card_data = {
#         "cc_num": [
#             f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-"
#             f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
#             for _ in range(rows)
#         ],
#         "account_id": [random.choice(account_ids) for _ in range(rows)],
#         "bank_id": [random.choice(bank_ids) for _ in range(rows)],
#     }

#     # Issue dates between [issue_date, current_date]
#     delta_issue_days = (current_date - issue_date).days
#     issue_dates = [
#         (current_date - timedelta(days=random.randint(0, delta_issue_days))).replace(day=1)
#         for _ in range(rows)
#     ]

#     # Expiry dates between [current_date, expiry_date]
#     delta_expiry_days = (expiry_date - current_date).days
#     expiry_dates = [
#         (current_date + timedelta(days=random.randint(0, delta_expiry_days))).replace(day=1)
#         for _ in range(rows)
#     ]

#     card_data["issue_date"] = issue_dates
#     card_data["cc_expiry_date"] = expiry_dates

#     card_types = ["Credit"] * 60 + ["Debit"] * 35 + ["Prepaid"] * 5
#     statuses = ["Active"] * 95 + ["Blocked"] * 4 + ["Lost/Stolen"] * 1
#     card_data["card_type"] = [random.choice(card_types) for _ in range(rows)]
#     card_data["status"] = [random.choice(statuses) for _ in range(rows)]

#     # last_modified within past 3 years
#     card_data["last_modified"] = [
#         current_date - timedelta(days=random.randint(0, delta_issue_days))
#         for _ in range(rows)
#     ]

#     return pl.DataFrame(card_data)


# def generate_card_details(rows=70000, num_accounts=50000, num_banks=5000, current_date, issue_date, expiry_date):
#     """Generate card details DataFrame"""
#     print("Generating card details...")
    
#     # Get account IDs for foreign key reference
#     account_ids = [f"ACC_{i:06d}" for i in range(num_accounts)]
#     bank_ids = [f"BANK_{i:05d}" for i in range(num_banks)]
    
#     card_data = {
#         "cc_num": [f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}" 
#                   for _ in range(rows)],
#         "account_id": [random.choice(account_ids) for _ in range(rows)],
#         "bank_id": [random.choice(bank_ids) for _ in range(rows)]
#     }
    
#     # Generate issue dates (0 to 3 years ago)
#     issue_dates = []
#     expiry_dates = []
    
#     delta_days = (current_date - start_date).days
#     expiry_days = expiry_date - current_date).days
#     for _ in range(rows):
#         issue_date = current_date - timedelta(days=random.randint(0, delta_days))
#         issue_dates.append(issue_date.replace(day=1))  # First day of month
        
#         expiry_date = current_date + timedelta(days=random.randint(0, expiry_days))
#         expiry_dates.append(expiry_date.replace(day=1))
    
#     card_data["issue_date"] = issue_dates
#     card_data["cc_expiry_date"] = expiry_dates
    
#     # Card types with specified distribution
#     card_types = ["Credit"] * 60 + ["Debit"] * 35 + ["Prepaid"] * 5
#     card_data["card_type"] = [random.choice(card_types) for _ in range(rows)]
    
#     # Status with specified distribution
#     statuses = ["Active"] * 95 + ["Blocked"] * 4 + ["Lost/Stolen"] * 1
#     card_data["status"] = [random.choice(statuses) for _ in range(rows)]
    
#     card_data["last_modified"] = [
#         current_date - timedelta(days=random.randint(0, delta_days))
#         for _ in range(rows)
#     ]
    
#     return pl.DataFrame(card_data)

# def generate_credit_card_transactions(tid_offset=0, rows=2000000, num_merchants=5000, num_cards=70000, start_date, end_date):
#     """Generate credit card transactions DataFrame"""
#     print("Generating credit card transactions...")
    
#     # Get card numbers and merchant IDs for foreign key references
#     cc_nums = [f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}" 
#                for _ in range(rows)]
#     merchant_ids = [f"MERCH_{i:05d}" for i in range(num_merchants)]
    
#     transaction_data = {
#        "t_id": [i for i in range(tid_offset, tid_offset + rows)], 
#         "cc_num": [random.choice(cc_nums) for _ in range(rows)],
#         "merchant_id": [random.choice(merchant_ids) for _ in range(rows)],
#         "amount": [round(np.random.lognormal(3.5, 1.2), 2) for _ in range(rows)],  # Log-normal for realistic amounts
#         "ip_address": [f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}" 
#                       for _ in range(rows)],
#         "card_present": [random.choice([True, False]) for _ in range(rows)]
#     }
    
#     delta_days = (end_date - start_date).days
#     transaction_data["ts"] = [
#         start_date + timedelta(seconds=random.randint(0, int((current_date - start_date).total_seconds())))
#         for _ in range(rows)
#     ]
    
#     return pl.DataFrame(transaction_data)


# # def generate_credit_card_transactions(
# #     start_date: datetime,
# #     end_date: datetime,
# #     rows: int = 2_000_000,
# #     tid_offset: int = 0,
# #     num_merchants: int = 5000,
# #     card_df: pl.DataFrame,
# # ) -> pl.DataFrame:
# #     print("Generating credit card transactions...")
# #     if card_df is None:
# #         raise ValueError("card_df is required to use existing cc_num/account_id")

# #     merchant_ids = [f"MERCH_{i:05d}" for i in range(num_merchants)]

# #     # sample from card_df
# #     cards_sample = card_df.select(["cc_num", "account_id"]).sample(
# #         n=rows, with_replacement=True, shuffle=True, seed=42
# #     )


# def generate_credit_card_transactions_from_existing(
#     card_df: pl.DataFrame,
#     merchant_df: pl.DataFrame,
#     start_date: datetime,
#     end_date: datetime,
#     rows: int = 2_000_000,
#     tid_offset: int = 0,
#     seed: int = 42,
# ) -> pl.DataFrame:
#     """
#     Generate credit card transactions using existing cc_num/account_id from card_details
#     and merchant_id from merchant_details. Ensures FK integrity.
#     """
#     rng = np.random.default_rng(seed)

#     # 1) Sample (cc_num, account_id) from card_details with replacement
#     #    (assumes card_df has columns: ["cc_num", "account_id"])
#     cards_sample = (
#         card_df.select(["cc_num", "account_id"])
#                .sample(n=rows, with_replacement=True, shuffle=True, seed=seed)
#     )

#     # 2) Sample merchant_id from merchant_details with replacement
#     merchants_sample = (
#         merchant_df.select(["merchant_id"])
#                    .sample(n=rows, with_replacement=True, shuffle=True, seed=seed + 1)
#     )

#     # 3) Vectorized synthetic fields
#     amounts = np.round(rng.lognormal(mean=3.5, sigma=1.2, size=rows), 2)
#     card_present = rng.integers(0, 2, size=rows).astype(bool)

#     # IPs
#     ip_parts = [rng.integers(1, 256, size=rows),
#                 rng.integers(0, 256, size=rows),
#                 rng.integers(0, 256, size=rows),
#                 rng.integers(0, 256, size=rows)]
#     ip_address = np.core.defchararray.add(
#         np.core.defchararray.add(ip_parts[0].astype(str), "."),
#         ""
#     )  # dummy to align add logic below
#     ip_address = (
#         ip_parts[0].astype(str) + "." +
#         ip_parts[1].astype(str) + "." +
#         ip_parts[2].astype(str) + "." +
#         ip_parts[3].astype(str)
#     )

#     # Timestamps uniform in range
#     total_seconds = int((end_date - start_date).total_seconds())
#     offsets = rng.integers(0, max(total_seconds, 1), size=rows)
#     ts = [start_date + timedelta(seconds=int(s)) for s in offsets]

#     # t_id
#     t_ids = np.arange(tid_offset, tid_offset + rows, dtype=np.int64)

#     # 4) Build DataFrame (includes account_id by request)
#     df = pl.DataFrame({
#         "t_id": t_ids,
#         "cc_num": cards_sample["cc_num"],
#         "account_id": cards_sample["account_id"],
#         "merchant_id": merchants_sample["merchant_id"],
#         "amount": amounts,
#         "ip_address": ip_address,
#         "card_present": card_present,
#         "ts": ts,
#     })

#     return df




# def create_feature_group_with_descriptions(fs, df, name, description, primary_key, event_time_col=None, topic_name=None):
#     """Create feature group and add feature descriptions"""
#     print(f"Creating feature group: {name}")
#     if topic_name is None:
#         topic_name = fs.name.removesuffix("_featurestore")
        
#     # Create feature group
#     fg = fs.create_feature_group(
#         name=name,
#         version=1,
#         description=description,
#         primary_key=primary_key,
#         event_time=event_time_col,
#         topic_name = topic_name,
#         online_enabled=False
#     )
    
#     # Insert data
#     fg.insert(df)
    
#     # Add feature descriptions based on table specifications
#     feature_descriptions = {
#         "merchant_details": {
#             "merchant_id": "Unique identifier for each merchant",
#             "category": "Merchant category codes for goods or service purchased (e.g., Groceries, Travel, Fashion, Healthcare)",
#             "country": "Country where merchant resides (160 largest countries in the world, excluding North Korea)",
#             "cnt_chrgeback_prev_day": "Number of chargebacks for this merchant during the previous day (Monday-Sunday)",
#             "last_modified": "Timestamp when the merchant details was last updated"
#         },
#         "bank_details": {
#             "bank_id": "Unique identifier for each bank",
#             "country": "Country where bank resides (160 largest countries in the world, excluding North Korea)",
#             "credit_rating": "Bank credit rating on a scale of 1 to 10 (negatively skewed distribution)",
#             "last_modified": "Timestamp when the bank details was last updated"
#         },
#         "account_details": {
#             "account_id": "Unique identifier for the card owner",
#             "name": "Full name of the account owner",
#             "address": "Address where account owner resides",
#             "debt_end_prev_month": "Amount of debt/credit at end of previous month",
#             "last_modified": "Timestamp when the account_details was last updated",
#             "creation_date": "Timestamp when the account was created",
#             "end_date": "Timestamp when the account was closed (null if still active)"
#         },
#         "card_details": {
#             "cc_num": "Credit card number in format XXXX-XXXX-XXXX-XXXX",
#             "cc_expiry_date": "Card's expiration date (3 to 5 years from current date, year and month only)",
#             "account_id": "Foreign key reference to account_details table",
#             "bank_id": "Foreign key reference to bank_details table", 
#             "issue_date": "Card's issue date (0 to 3 years before current date, year and month only)",
#             "card_type": "Type of card (Credit: 60%, Debit: 35%, Prepaid: 5%)",
#             "status": "Current status of the card (Active: 95%, Blocked: 4%, Lost/Stolen: 1%)",
#             "last_modified": "Timestamp when the card details was last updated"
#         },
#         "credit_card_transactions": {
#             "t_id": "Unique identifier for this credit card transaction",
#             "cc_num": "Foreign key reference to credit card",
#             "merchant_id": "Foreign key reference to merchant table",
#             "account_id": "Foreign key reference to account_details (via card_details)",
#             "amount": "Credit card transaction amount in decimal format",
#             "ip_address": "IP address of the physical or online merchant (format: XXX.XXX.XXX.XXX)",
#             "card_present": "Whether credit card was used in a physical terminal (true) or online payment (false)",
#             "ts": "Timestamp for this credit card transaction (uniform distribution over last 18 months)"
#         }
#     }
    
#     # Add descriptions for each feature
#     if name in feature_descriptions:
#         for feature_name, feature_desc in feature_descriptions[name].items():
#             if feature_name in df.columns:
#                 try:
#                     fg.update_feature_description(feature_name=feature_name, description=feature_desc)
#                     print(f"  Added description for: {feature_name}")
#                 except Exception as e:
#                     print(f"  Warning: Could not add description for {feature_name}: {e}")
    
#     return fg




import polars as pl
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
from geopy.distance import geodesic
import random
import uuid
import hsfs
import hopsworks

# seeds
fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)


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
    merchant_data = {
        "merchant_id": [f"MERCH_{i:05d}" for i in range(rows)],
        "category": [random.choice(categories) for _ in range(rows)],
        "country": [random.choice(countries) for _ in range(rows)],
        "cnt_chrgeback_prev_day": [round(np.random.exponential(2.5), 2) for _ in range(rows)],
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
    bank_data = {
        "bank_id": [f"BANK_{i:05d}" for i in range(rows)],
        "country": [random.choice(countries) for _ in range(rows)],
        "credit_rating": credit_ratings.tolist(),
        "last_modified": [end_date - timedelta(days=random.randint(0, delta_days)) for _ in range(rows)]
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
        "account_id": [f"ACC_{i:06d}" for i in range(rows)],
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


def generate_card_details(
    rows: int,
    num_accounts: int,
    num_banks: int,
    current_date: datetime,
    issue_date: datetime,
    expiry_date: datetime
) -> pl.DataFrame:
    """Generate card details DataFrame"""
    print("Generating card details...")
    if current_date is None or issue_date is None or expiry_date is None:
        raise ValueError("Provide current_date, issue_date and expiry_date")

    account_ids = [f"ACC_{i:06d}" for i in range(num_accounts)]
    bank_ids = [f"BANK_{i:05d}" for i in range(num_banks)]

    rows = int(rows)
    card_data = {
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
    """
    print("Generating credit card transactions from existing card + merchant tables...")
    rows = int(rows)
    rng = np.random.default_rng(seed)

    # Ensure required columns exist
    required_card_cols = {"cc_num", "account_id"}
    if not required_card_cols.issubset(set(card_df.columns)):
        raise ValueError(f"card_df must contain columns: {required_card_cols}")

    if "merchant_id" not in merchant_df.columns:
        raise ValueError("merchant_df must contain 'merchant_id' column")

    # 1) Sample (cc_num, account_id)
    # Polars sampling with replacement
    cards_sample = card_df.select(["cc_num", "account_id"]).sample(n=rows, with_replacement=True, shuffle=True, seed=seed)
    merchants_sample = merchant_df.select(["merchant_id"]).sample(n=rows, with_replacement=True, shuffle=True, seed=seed + 1)

    # 2) Amounts (log-normal) and card_present flag
    amounts = np.round(rng.lognormal(mean=3.5, sigma=1.2, size=rows), 2).tolist()
    card_present = rng.integers(0, 2, size=rows).astype(bool).tolist()

    # 3) IP addresses (random)
    a = rng.integers(1, 256, size=rows)
    b = rng.integers(0, 256, size=rows)
    c = rng.integers(0, 256, size=rows)
    d = rng.integers(0, 256, size=rows)
    ip_address = [f"{int(x)}.{int(y)}.{int(z)}.{int(w)}" for x, y, z, w in zip(a, b, c, d)]

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


# ---------------------------
# Feature group helper - add account_id description
# ---------------------------

def create_feature_group_with_descriptions(fs, df, name, description, primary_key, event_time_col=None, topic_name=None):
    """Create feature group and add feature descriptions"""
    print(f"Creating feature group: {name}")
    if topic_name is None:
        try:
            topic_name = fs.name.removesuffix("_featurestore")
        except Exception:
            topic_name = None

    fg = fs.create_feature_group(
        name=name,
        version=1,
        description=description,
        primary_key=primary_key,
        event_time=event_time_col,
        topic_name=topic_name,
        online_enabled=False
    )

    fg.insert(df)

    feature_descriptions = {
        "merchant_details": {
            "merchant_id": "Unique identifier for each merchant",
            "category": "Merchant category codes for goods or service purchased",
            "country": "Country where merchant resides",
            "cnt_chrgeback_prev_day": "Number of chargebacks for this merchant during the previous day",
            "last_modified": "Timestamp when the merchant details was last updated"
        },
        "bank_details": {
            "bank_id": "Unique identifier for each bank",
            "country": "Country where bank resides",
            "credit_rating": "Bank credit rating on a scale of 1 to 10",
            "last_modified": "Timestamp when the bank details was last updated"
        },
        "account_details": {
            "account_id": "Unique identifier for the card owner",
            "name": "Full name of the account owner",
            "address": "Address where account owner resides",
            "debt_end_prev_month": "Amount of debt/credit at end of previous month",
            "last_modified": "Timestamp when the account_details was last updated",
            "creation_date": "Timestamp when the account was created",
            "end_date": "Timestamp when the account was closed (null if still active)"
        },
        "card_details": {
            "cc_num": "Credit card number in format XXXX-XXXX-XXXX-XXXX",
            "cc_expiry_date": "Card's expiration date (year and month only)",
            "account_id": "Foreign key reference to account_details table",
            "bank_id": "Foreign key reference to bank_details table",
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
            "ts": "Timestamp for this credit card transaction (uniform distribution over the requested time range)"
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


