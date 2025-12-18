import polars as pl
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
from geopy.distance import geodesic
import random
import uuid
import hsfs
import hopsworks

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)
current_date = datetime(2024, 9, 11)  # One day after most recent data



# Helper function to generate country list (160 largest countries excluding North Korea)
def get_country_list():
    countries = [
        'China', 'India', 'United States', 'Indonesia', 'Pakistan', 'Brazil', 'Nigeria', 'Bangladesh',
        'Russia', 'Mexico', 'Japan', 'Philippines', 'Ethiopia', 'Vietnam', 'Egypt', 'Germany',
        'Turkey', 'Iran', 'Thailand', 'United Kingdom', 'France', 'Italy', 'Tanzania', 'South Africa',
        'Myanmar', 'Kenya', 'South Korea', 'Colombia', 'Spain', 'Uganda', 'Argentina', 'Algeria',
        'Sudan', 'Ukraine', 'Iraq', 'Afghanistan', 'Poland', 'Canada', 'Morocco', 'Saudi Arabia',
        'Uzbekistan', 'Peru', 'Angola', 'Malaysia', 'Mozambique', 'Ghana', 'Yemen', 'Nepal',
        'Venezuela', 'Madagascar', 'Cameroon', 'Ivory Coast', 'Niger', 'Australia', 'Sri Lanka',
        'Burkina Faso', 'Mali', 'Romania', 'Malawi', 'Chile', 'Kazakhstan', 'Zambia', 'Guatemala',
        'Ecuador', 'Syria', 'Netherlands', 'Senegal', 'Cambodia', 'Chad', 'Somalia', 'Zimbabwe',
        'Guinea', 'Rwanda', 'Benin', 'Burundi', 'Tunisia', 'Belgium', 'Bolivia', 'Cuba', 'Haiti',
        'Dominican Republic', 'Czech Republic', 'Greece', 'Jordan', 'Portugal', 'Azerbaijan',
        'Sweden', 'Honduras', 'United Arab Emirates', 'Hungary', 'Tajikistan', 'Belarus', 'Austria',
        'Papua New Guinea', 'Serbia', 'Israel', 'Switzerland', 'Togo', 'Sierra Leone', 'Hong Kong',
        'Laos', 'Paraguay', 'Bulgaria', 'Libya', 'Lebanon', 'Nicaragua', 'Kyrgyzstan', 'El Salvador',
        'Turkmenistan', 'Singapore', 'Denmark', 'Finland', 'Congo', 'Slovakia', 'Norway', 'Oman',
        'Palestine', 'Costa Rica', 'Liberia', 'Ireland', 'Central African Republic', 'New Zealand',
        'Mauritania', 'Panama', 'Kuwait', 'Croatia', 'Moldova', 'Georgia', 'Eritrea', 'Uruguay',
        'Bosnia and Herzegovina', 'Mongolia', 'Armenia', 'Jamaica', 'Qatar', 'Albania', 'Puerto Rico',
        'Lithuania', 'Namibia', 'Gambia', 'Botswana', 'Gabon', 'Lesotho', 'North Macedonia', 'Slovenia',
        'Guinea-Bissau', 'Latvia', 'Bahrain', 'Equatorial Guinea', 'Trinidad and Tobago', 'Estonia',
        'East Timor', 'Mauritius', 'Cyprus', 'Eswatini', 'Djibouti', 'Fiji', 'Reunion', 'Comoros',
        'Guyana', 'Bhutan', 'Solomon Islands', 'Macao', 'Montenegro', 'Luxembourg', 'Western Sahara',
        'Suriname', 'Cape Verde', 'Micronesia', 'Maldives', 'Malta', 'Brunei', 'Belize', 'Bahamas',
        'Iceland', 'Vanuatu', 'Barbados', 'Sao Tome and Principe', 'Samoa', 'Saint Lucia',
        'Kiribati', 'Grenada', 'Saint Vincent and the Grenadines', 'Tonga', 'Seychelles',
        'Antigua and Barbuda', 'Andorra', 'Dominica', 'Marshall Islands', 'Saint Kitts and Nevis',
        'Liechtenstein', 'Monaco', 'San Marino', 'Palau', 'Tuvalu', 'Nauru', 'Vatican City'
    ][:160]  # Take first 160 countries
    return countries

def generate_merchant_details():
    """Generate merchant details table"""
    print("Generating merchant details...")
    
    countries = get_country_list()
    current_date = datetime.now()
    
    # Generate data using Polars expressions where possible
    n_merchants = 5000
    
    merchant_data = {
        'merchant_id': [str(uuid.uuid4()) for _ in range(n_merchants)],
        'last_modified': [
            current_date - timedelta(days=random.randint(0, 3*365)) 
            for _ in range(n_merchants)
        ],
        'country': [random.choice(countries) for _ in range(n_merchants)],
        'cnt_chrgeback_prev_day': [
            round(np.random.exponential(2.5), 2) for _ in range(n_merchants)
        ]
    }
    
    df = pl.DataFrame(merchant_data)
    return df

def generate_bank_details():
    """Generate bank details table"""
    print("Generating bank details...")
    
    countries = get_country_list()
    current_date = datetime.now()
    n_banks = 5000
    
    # Generate negatively skewed credit ratings (higher ratings more common)
    credit_ratings = np.random.beta(5, 2, n_banks) * 9 + 1  # Beta distribution scaled to 1-10
    credit_ratings = np.round(credit_ratings).astype(int)
    credit_ratings = np.clip(credit_ratings, 1, 10)  # Ensure range 1-10
    
    bank_data = {
        'bank_id': [str(uuid.uuid4()) for _ in range(n_banks)],
        'last_modified': [
            current_date - timedelta(days=random.randint(0, 365)) 
            for _ in range(n_banks)
        ],
        'country': [random.choice(countries) for _ in range(n_banks)],
        'credit_rating': credit_ratings.tolist()
    }
    
    df = pl.DataFrame(bank_data)
    return df

def generate_account_details():
    """Generate account details table"""
    print("Generating account details...")
    
    current_date = datetime.now()
    n_accounts = 50000
    
    account_data = {
        'account_id': [str(uuid.uuid4()) for _ in range(n_accounts)],
        'last_modified': [
            current_date - timedelta(days=random.randint(0, 3*365)) 
            for _ in range(n_accounts)
        ],
        'name': [fake.name() for _ in range(n_accounts)],
        'address': [fake.address().replace('\n', ', ') for _ in range(n_accounts)],
        'debt_end_prev_month': [
            round(random.uniform(-5000, 15000), 2) for _ in range(n_accounts)
        ],
        'creation_date': [
            current_date - timedelta(days=random.randint(30, 5*365)) 
            for _ in range(n_accounts)
        ]
    }
    
    # Add end_date (some accounts might be closed)
    end_dates = []
    for i in range(n_accounts):
        if random.random() < 0.05:  # 5% of accounts are closed
            end_date = account_data['creation_date'][i] + timedelta(days=random.randint(30, 1000))
            end_dates.append(end_date)
        else:
            end_dates.append(None)
    
    account_data['end_date'] = end_dates
    
    df = pl.DataFrame(account_data)
    return df

def generate_card_details(account_ids, bank_ids):
    """Generate card details table"""
    print("Generating card details...")
    
    current_date = datetime.now()
    n_cards = 70000
    
    # Card type distribution
    card_types = ['Credit'] * 42000 + ['Debit'] * 24500 + ['Prepaid'] * 3500
    random.shuffle(card_types)
    
    # Status distribution
    statuses = ['Active'] * 66500 + ['Blocked'] * 2800 + ['Lost/Stolen'] * 700
    random.shuffle(statuses)
    
    card_data = {
        'cc_num': [fake.credit_card_number(card_type=None) for _ in range(n_cards)],
        'last_modified': [
            current_date - timedelta(days=random.randint(0, 3*365)) 
            for _ in range(n_cards)
        ],
        'cc_expiry_date': [
            (current_date + timedelta(days=random.randint(3*365, 5*365))).strftime('%Y-%m')
            for _ in range(n_cards)
        ],
        'account_id': [random.choice(account_ids) for _ in range(n_cards)],
        'bank_id': [random.choice(bank_ids) for _ in range(n_cards)],
        'issue_date': [
            (current_date - timedelta(days=random.randint(0, 3*365))).strftime('%Y-%m')
            for _ in range(n_cards)
        ],
        'card_type': card_types,
        'status': statuses
    }
    
    df = pl.DataFrame(card_data)
    return df

def generate_merchant_categories():
    """Generate 500 merchant category codes"""
    base_categories = [
        'Groceries', 'Travel', 'Fashion', 'Healthcare', 'Restaurants', 'Gas Stations',
        'Department Stores', 'Electronics', 'Home Improvement', 'Insurance', 'Utilities',
        'Entertainment', 'Books', 'Automotive', 'Sports', 'Beauty', 'Pet Care', 'Education',
        'Jewelry', 'Hotels', 'Airlines', 'Car Rental', 'Taxi', 'Parking', 'Tolls',
        'Medical Services', 'Dental', 'Veterinary', 'Pharmacy', 'Optical', 'Fitness',
        'Movies', 'Music', 'Gaming', 'Streaming', 'Subscriptions', 'Software', 'Hardware',
        'Internet Services', 'Phone Services', 'Cable TV', 'Furniture', 'Appliances',
        'Garden Centers', 'Florists', 'Gift Shops', 'Toy Stores', 'Baby Supplies',
        'Office Supplies', 'Professional Services', 'Legal Services', 'Accounting',
        'Real Estate', 'Banking', 'Investment', 'Charity', 'Government', 'Postal Services'
    ]
    
    # Extend to 500 categories by adding variations
    categories = base_categories.copy()
    for i, base in enumerate(base_categories):
        if len(categories) < 500:
            categories.extend([f"{base} Online", f"{base} Premium", f"{base} Discount"])
        if len(categories) < 500:
            categories.extend([f"{base} Chain", f"{base} Local", f"{base} Express"])
    
    # Add more generic numbered categories if needed
    while len(categories) < 500:
        categories.append(f"Category {len(categories) + 1}")
    
    return categories[:500]

def generate_credit_card_transactions(cc_nums, merchant_ids):
    """Generate credit card transactions table"""
    print("Generating credit card transactions...")
    
    current_date = datetime.now()
    n_transactions = 2000000
    categories = generate_merchant_categories()
    
    # Generate transaction data in batches for memory efficiency
    batch_size = 100000
    batches = []
    
    for batch_start in range(0, n_transactions, batch_size):
        batch_end = min(batch_start + batch_size, n_transactions)
        batch_size_actual = batch_end - batch_start
        
        print(f"Processing batch {batch_start//batch_size + 1}/{(n_transactions//batch_size) + 1}")
        
        # Generate amounts with realistic distribution
        amounts = np.random.lognormal(mean=3.5, sigma=1.2, size=batch_size_actual)
        amounts = np.round(amounts, 2)
        amounts = np.clip(amounts, 0.01, 50000)  # Reasonable transaction limits
        
        batch_data = {
            't_id': [str(uuid.uuid4()) for _ in range(batch_size_actual)],
            'cc_num': [random.choice(cc_nums) for _ in range(batch_size_actual)],
            'merchant_id': [random.choice(merchant_ids) for _ in range(batch_size_actual)],
            'amount': amounts.tolist(),
            'category': [random.choice(categories) for _ in range(batch_size_actual)],
            'ip_address': [fake.ipv4() for _ in range(batch_size_actual)],
            'event_time': [
                current_date - timedelta(days=random.randint(0, 18*30))
                for _ in range(batch_size_actual)
            ]
        }
        
        batch_df = pl.DataFrame(batch_data)
        batches.append(batch_df)
    
    # Concatenate all batches
    df = pl.concat(batches)
    return df

def generate_cc_fraud(transactions_df):
    """Generate credit card fraud table"""
    print("Generating credit card fraud...")
    
    # 1% of transactions are fraudulent
    n_fraud = len(transactions_df) // 100
    selected_indices = random.sample(range(len(transactions_df)), n_fraud)
    selected_transactions = transactions_df[selected_indices]
    
    # Fraud type distribution
    fraud_types = (['Card Not Present'] * (n_fraud // 2) + 
                  ['Skimming'] * (n_fraud * 3 // 10) + 
                  ['Lost/Stolen'] * (n_fraud // 5))
    
    # Adjust for exact count
    while len(fraud_types) < n_fraud:
        fraud_types.append('Card Not Present')
    fraud_types = fraud_types[:n_fraud]
    random.shuffle(fraud_types)
    
    explanations = [
        "Suspicious transaction pattern detected",
        "Card used in multiple locations simultaneously", 
        "Transaction amount significantly higher than usual",
        "Card used after being reported lost",
        "Unusual merchant category for cardholder",
        "Transaction from high-risk location",
        "Card details compromised in data breach",
        "Skimming device detected at ATM/POS",
        "Online transaction with invalid security codes",
        "Card used without PIN verification"
    ]
    
    # Generate fraud event times (1-12 months after transaction event_time)
    fraud_event_times = []
    transaction_times = selected_transactions['event_time'].to_list()
    for transaction_time in transaction_times:
        days_after = random.randint(30, 365)  # 1-12 months
        fraud_time = transaction_time + timedelta(days=days_after)
        fraud_event_times.append(fraud_time)
    
    fraud_data = {
        't_id': list(range(1, n_fraud + 1)),  # Auto-increment
        'explanation': [random.choice(explanations) for _ in range(n_fraud)],
        'event_time': fraud_event_times,
        'fraud_type': fraud_types
    }
    
    df = pl.DataFrame(fraud_data)
    return df

def create_feature_groups_and_save(fs, dataframes):
    """Create feature groups in Hopsworks and save DataFrames"""
    
    feature_group_configs = {
        'merchant_details': {
            'df': dataframes['merchant_details'],
            'primary_key': ['merchant_id'],
            'event_time': 'last_modified',
            'description': 'Details about merchants that execute transactions',
            'feature_descriptions': {
                'merchant_id': 'Unique identifier for each merchant',
                'last_modified': 'Timestamp when the merchant details was last updated (uniform 0 to 3 years before current date)',
                'country': 'Country where merchant resides (160 largest countries excluding North Korea)',
                'cnt_chrgeback_prev_day': 'Number of chargebacks for this merchant during the previous day (Monday-Sunday)'
            }
        },
        'bank_details': {
            'df': dataframes['bank_details'],
            'primary_key': ['bank_id'],
            'event_time': 'last_modified',
            'description': 'Details about banks that issue credit cards',
            'feature_descriptions': {
                'bank_id': 'Unique identifier for each bank',
                'last_modified': 'Timestamp when the bank details was last updated (uniform 0 to 1 year before current date)',
                'country': 'Country where bank resides (160 largest countries excluding North Korea)',
                'credit_rating': 'Bank credit rating on scale 1-10 (negatively skewed distribution, higher ratings more common)'
            }
        },
        'account_details': {
            'df': dataframes['account_details'],
            'primary_key': ['account_id'],
            'event_time': 'last_modified',
            'description': 'Information about the account and card',
            'feature_descriptions': {
                'account_id': 'Unique identifier for the card owner',
                'last_modified': 'Timestamp when the account_details was last updated',
                'name': 'Full name of the account owner',
                'address': 'Address where account owner resides',
                'debt_end_prev_month': 'Amount of debt/credit at end of previous month (decimal with 2 places)',
                'creation_date': 'Timestamp when the account was created',
                'end_date': 'Timestamp when the account was closed (null if still active)'
            }
        },
        'card_details': {
            'df': dataframes['card_details'],
            'primary_key': ['cc_num'],
            'event_time': 'last_modified',
            'description': 'Information about the account and card',
            'feature_descriptions': {
                'cc_num': 'Credit card number (primary key)',
                'last_modified': 'Timestamp when card details was last updated (uniform 0 to 3 years before current date)',
                'cc_expiry_date': 'Card expiration date in YYYY-MM format (3 to 5 years from current date)',
                'account_id': 'Foreign key reference to account_details table',
                'bank_id': 'Foreign key reference to bank_details table',
                'issue_date': 'Card issue date in YYYY-MM format (0 to 3 years before current date)',
                'card_type': 'Type of card: Credit (60%), Debit (35%), or Prepaid (5%)',
                'status': 'Current card status: Active (95%), Blocked (4%), or Lost/Stolen (1%)'
            }
        },
        'credit_card_transactions': {
            'df': dataframes['credit_card_transactions'],
            'primary_key': ['t_id'],
            'event_time': 'event_time',
            'description': 'Details about credit card transactions',
            'feature_descriptions': {
                't_id': 'Unique identifier for this credit card transaction',
                'cc_num': 'Foreign key reference to card_details table',
                'merchant_id': 'Foreign key reference to merchant_details table',
                'amount': 'Credit card transaction amount (decimal with 2 places, log-normal distribution)',
                'category': 'Merchant category code for goods or service purchased (500 different categories)',
                'ip_address': 'IP address of the physical or online merchant (format: XXX.XXX.XXX.XXX)',
                'event_time': 'Timestamp for this credit card transaction (uniform over last 18 months)'
            }
        },
        'cc_fraud': {
            'df': dataframes['cc_fraud'],
            'primary_key': ['t_id'],
            'event_time': 'event_time',
            'description': 'Reported credit card fraud',
            'feature_descriptions': {
                't_id': 'Auto-increment identifier (monotonically increasing primary key)',
                'explanation': 'Description explaining the reason for fraud detection',
                'event_time': 'Timestamp when this credit card transaction was marked as fraud (1-12 months after transaction)',
                'fraud_type': 'Type of fraud: Card Not Present (50%), Skimming (30%), or Lost/Stolen (20%)'
            }
        }
    }
    
    for fg_name, config in feature_group_configs.items():
        print(f"Creating feature group: {fg_name}")
        
        try:
            # Convert Polars DataFrame to Pandas for Hopsworks
            pandas_df = config['df'].to_pandas()
            
            # Create or get feature group with event_time
            create=False
            if fs.get_feature_group(fg_name, version=1) is None:
                create=True
                
            feature_group = fs.get_or_create_feature_group(
                name=fg_name,
                version=1,
                primary_key=config['primary_key'],
                event_time=config['event_time'],
                description=config['description']
            )

            # Insert data
            feature_group.insert(pandas_df)
            print(f"Successfully saved {len(pandas_df)} rows to {fg_name}")
            

            if create==True:
                # Add feature descriptions to each column
                for feature_name, feature_desc in config['feature_descriptions'].items():
                    if feature_name in pandas_df.columns:
                        try:
                            feature = feature_group.update_feature_description(feature_name=feature_name, description=feature_desc)
                            print(f"  Added description to feature: {feature_name}")
                        except Exception as e:
                            print(f"  Warning: Could not add description to {feature_name}: {e}")
            
            
        except Exception as e:
            print(f"Error creating feature group {fg_name}: {e}")



def get_location_from_ip(ip_address):
    """
    Get approximate location (lat, lon) from IP address using a free IP geolocation service
    This is a simplified version - in production you'd use a more reliable service
    """
    # For demonstration, we'll create a mapping of IP ranges to major cities
    # In real implementation, you'd use an IP geolocation API
    
    ip_parts = [int(x) for x in ip_address.split('.')]
    first_octet = ip_parts[0]
    
    # Simple mapping based on first octet (not geographically accurate, just for demo)
    city_locations = {
        range(1, 50): (40.7128, -74.0060),    # New York
        range(50, 100): (34.0522, -118.2437), # Los Angeles  
        range(100, 150): (51.5074, -0.1278),  # London
        range(150, 200): (35.6762, 139.6503), # Tokyo
        range(200, 255): (-33.8688, 151.2093) # Sydney
    }
    
    for ip_range, location in city_locations.items():
        if first_octet in ip_range:
            return location
    
    # Default location if not found
    return (40.7128, -74.0060)  # New York

def generate_distant_ip_pairs():
    """Generate pairs of IP addresses that are geographically distant"""
    distant_pairs = []
    
    # Major cities with their approximate coordinates
    cities = [
        ("1.1.1.1", 40.7128, -74.0060),     # New York
        ("80.80.80.80", 34.0522, -118.2437), # Los Angeles
        ("120.120.120.120", 51.5074, -0.1278), # London
        ("160.160.160.160", 35.6762, 139.6503), # Tokyo
        ("200.200.200.200", -33.8688, 151.2093), # Sydney
        ("50.50.50.50", 41.8781, -87.6298),   # Chicago
        ("90.90.90.90", 48.8566, 2.3522),    # Paris
        ("130.130.130.130", 55.7558, 37.6173), # Moscow
        ("170.170.170.170", -22.9068, -43.1729), # Rio de Janeiro
        ("210.210.210.210", 31.2304, 121.4737)  # Shanghai
    ]
    
    # Generate pairs that are far apart (>1000 km)
    for i in range(len(cities)):
        for j in range(i+1, len(cities)):
            city1 = cities[i]
            city2 = cities[j]
            distance = geodesic((city1[1], city1[2]), (city2[1], city2[2])).kilometers
            if distance > 1000:  # At least 1000 km apart
                distant_pairs.append((city1[0], city2[0], distance))
    
    return distant_pairs

def create_geographic_attacks(card_numbers, num_attacks=1000):
    """Create geographic attack transactions"""
    print(f"Creating {num_attacks} geographic attacks...")
    
    distant_pairs = generate_distant_ip_pairs()
    geographic_transactions = []
    fraud_records = []
    
    current_date = datetime(2024, 9, 11)
    start_date = current_date - timedelta(days=18*30)  # 18 months ago
    
    for attack_id in range(num_attacks):
        # Select random card
        card_num = random.choice(card_numbers)
        
        # Select random distant IP pair
        ip1, ip2, distance = random.choice(distant_pairs)
        
        # Generate first transaction timestamp
        ts1 = start_date + timedelta(seconds=random.randint(0, int((current_date - start_date).total_seconds())))
        
        # Generate second transaction 1-30 minutes later (impossible to travel that distance)
        ts2 = ts1 + timedelta(minutes=random.randint(1, 30))
        
        # Transaction amounts
        amount1 = round(np.random.lognormal(3.5, 1.2), 2)
        amount2 = round(np.random.lognormal(3.5, 1.2), 2)
        
        # Merchant IDs
        merchant1 = f"MERCH_{random.randint(0, 4999):05d}"
        merchant2 = f"MERCH_{random.randint(0, 4999):05d}"
        
        # Create transaction IDs
        base_id = 3000000 + attack_id * 2  # Start after existing transactions
        t_id1 = f"TXN_{base_id:08d}"
        t_id2 = f"TXN_{base_id+1:08d}"
        
        # First transaction
        transaction1 = {
            "t_id": t_id1,
            "cc_num": card_num,
            "merchant_id": merchant1,
            "amount": amount1,
            "ip_address": ip1,
            "card_present": True,  # Both must be card present
            "ts": ts1
        }
        
        # Second transaction  
        transaction2 = {
            "t_id": t_id2,
            "cc_num": card_num,
            "merchant_id": merchant2,
            "amount": amount2,
            "ip_address": ip2,
            "card_present": True,  # Both must be card present
            "ts": ts2
        }
        
        geographic_transactions.extend([transaction1, transaction2])
        
        # Create fraud records if enabled
        if ADD_TO_FRAUD_FG:
            # Determine fraud type for geographic attack
            fraud_types = ["Skimming", "Lost/Stolen"]
            fraud_type = random.choice(fraud_types)
            
            # Fraud detection time: 1-12 months after transaction
            fraud_delay_days = random.randint(30, 365)
            fraud_time1 = ts1 + timedelta(days=fraud_delay_days)
            fraud_time2 = ts2 + timedelta(days=fraud_delay_days)
            
            explanation = f"Geographic attack: Transactions {distance:.0f}km apart within {(ts2-ts1).total_seconds()/60:.1f} minutes"
            
            fraud_records.extend([
                {
                    "t_id": base_id,
                    "explanation": explanation,
                    "event_time": fraud_time1,
                    "fraud_type": fraud_type
                },
                {
                    "t_id": base_id + 1,
                    "explanation": explanation,
                    "event_time": fraud_time2,
                    "fraud_type": fraud_type
                }
            ])
    
    return geographic_transactions, fraud_records

def create_chain_attacks(card_numbers, num_attacks=500):
    """Create chain attack transactions"""
    print(f"Creating {num_attacks} chain attacks...")
    
    chain_transactions = []
    fraud_records = []
    
    current_date = datetime(2024, 9, 11)
    start_date = current_date - timedelta(days=18*30)  # 18 months ago
    
    transaction_counter = 5000000  # Start after geographic attacks
    
    for attack_id in range(num_attacks):
        # Select random card
        card_num = random.choice(card_numbers)
        
        # Number of transactions in this chain (5-50)
        num_transactions = random.randint(5, 50)
        
        # Start time for the chain
        chain_start = start_date + timedelta(seconds=random.randint(0, int((current_date - start_date).total_seconds())))
        
        # Chain duration: 1-6 hours
        chain_duration_hours = random.uniform(0.5, 6.0)
        chain_end = chain_start + timedelta(hours=chain_duration_hours)
        
        # Card present or not (consistent for the whole chain)
        card_present = random.choice([True, False])
        
        # Generate IP address for the chain (same for all transactions in online chains)
        if card_present:
            # Different locations for card present
            base_ip = f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
        else:
            # Same IP for online transactions
            ip_address = f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
        
        chain_transaction_ids = []
        
        for txn_idx in range(num_transactions):
            # Transaction timestamp within chain duration
            txn_time = chain_start + timedelta(seconds=random.randint(0, int(chain_duration_hours * 3600)))
            
            # Small amounts for chain attacks (typically under $100)
            amount = round(random.uniform(5.00, 99.99), 2)
            
            # Merchant ID
            merchant_id = f"MERCH_{random.randint(0, 4999):05d}"
            
            # Transaction ID
            t_id = f"TXN_{transaction_counter:08d}"
            transaction_counter += 1
            
            # IP address
            if card_present:
                # Vary the last octet for different ATMs/terminals
                txn_ip = f"{base_ip}.{random.randint(1, 255)}"
            else:
                txn_ip = ip_address
            
            transaction = {
                "t_id": t_id,
                "cc_num": card_num,
                "merchant_id": merchant_id,
                "amount": amount,
                "ip_address": txn_ip,
                "card_present": card_present,
                "ts": txn_time
            }
            
            chain_transactions.append(transaction)
            chain_transaction_ids.append(transaction_counter - 1)
        
        # Create fraud records if enabled
        if ADD_TO_FRAUD_FG:
            fraud_type = "Card Not Present"  # Chain attacks are typically card not present fraud
            
            # Create fraud record for each transaction in the chain
            for i, txn_id in enumerate(chain_transaction_ids):
                # Fraud detection time: 1-12 months after transaction
                fraud_delay_days = random.randint(30, 365)
                fraud_time = chain_start + timedelta(days=fraud_delay_days)
                
                explanation = f"Chain attack: {num_transactions} transactions within {chain_duration_hours:.1f} hours"
                
                fraud_records.append({
                    "t_id": txn_id,
                    "explanation": explanation,
                    "event_time": fraud_time,
                    "fraud_type": fraud_type
                })
    
    return chain_transactions, fraud_records

def create_fraud_feature_group(fraud_records):
    """Create the cc_fraud feature group"""
    if not fraud_records:
        print("No fraud records to create feature group")
        return None
        
    print("Creating cc_fraud feature group...")
    
    # Convert to DataFrame
    fraud_df = pl.DataFrame(fraud_records)
    
    create = False
    if fs.get_feature_group("cc_fraud", version=1) is None:
        create = True
    fraud_fg = fs.get_or_create_feature_group(
        name="cc_fraud",
        version=1,
        description="Reported credit card fraud",
        primary_key=["t_id"],
        event_time="event_time"
    )
    
    # Insert data
    fraud_fg.insert(fraud_df)
    
    # Add feature descriptions
    feature_descriptions = {
        "t_id": "Auto-increment identifier (monotonically increasing) for fraud record",
        "explanation": "Detailed explanation of the fraud pattern detected",
        "event_time": "Timestamp when this credit card transaction was marked as fraud (1-12 months after transaction)",
        "fraud_type": "Type of fraud detected (Card Not Present: 50%, Skimming: 30%, Lost/Stolen: 20%)"
    }
    
    if create==True:
        for feature_name, feature_desc in feature_descriptions.items():
            try:
                fraud_fg.update_feature_description(feature_name=feature_name, description=feature_desc)
                print(f"  Added description for: {feature_name}")
            except Exception as e:
                print(f"  Warning: Could not add description for {feature_name}: {e}")
    
    return fraud_fg



def generate_merchant_details():
    """Generate merchant details DataFrame"""
    print("Generating merchant details...")
    
    # Generate merchant categories
    categories = [
        "Groceries", "Travel", "Fashion", "Healthcare", "Restaurants", "Gas Stations",
        "Electronics", "Home Improvement", "Entertainment", "Education", "Insurance",
        "Automotive", "Books", "Sports", "Beauty", "Jewelry", "Pet Supplies", 
        "Furniture", "Pharmacy", "Department Stores"
    ] * 25  # 500 types total
    
    # Get 160 largest countries (excluding North Korea)
    countries = [
        "United States", "China", "Japan", "Germany", "India", "United Kingdom", 
        "France", "Italy", "Brazil", "Canada", "Russia", "South Korea", "Australia",
        "Spain", "Mexico", "Indonesia", "Netherlands", "Saudi Arabia", "Turkey", 
        "Taiwan", "Belgium", "Argentina", "Thailand", "Israel", "Poland", "Nigeria",
        "Egypt", "Vietnam", "Bangladesh", "South Africa", "Philippines", "Chile",
        "Finland", "Romania", "Czech Republic", "Portugal", "New Zealand", "Peru",
        "Greece", "Qatar", "Algeria", "Kazakhstan", "Hungary", "Kuwait", "Ukraine",
        "Morocco", "Slovakia", "Ecuador", "Dominican Republic", "Guatemala", "Oman",
        "Kenya", "Myanmar", "Luxembourg", "Ghana", "Croatia", "Bolivia", "Uruguay",
        "Costa Rica", "Lebanon", "Slovenia", "Lithuania", "Tunisia", "Azerbaijan",
        "Sri Lanka", "Belarus", "Uzbekistan", "Panama", "Latvia", "Cameroon",
        "Jordan", "Bosnia and Herzegovina", "Georgia", "Albania", "Mongolia",
        "Armenia", "Jamaica", "Qatar", "Bahrain", "Estonia", "Trinidad and Tobago",
        "Cyprus", "Mozambique", "Nepal", "Cambodia", "Honduras", "Madagascar",
        "Senegal", "Zimbabwe", "Afghanistan", "Mali", "Burkina Faso", "Niger",
        "Malawi", "Zambia", "Somalia", "Chad", "Guinea", "South Sudan", "Rwanda",
        "Burundi", "Benin", "Tunisia", "Haiti", "Cuba", "Papua New Guinea",
        "Nicaragua", "Liberia", "Sierra Leone", "Togo", "Libya", "Central African Republic",
        "Mauritania", "Eritrea", "Gambia", "Botswana", "Gabon", "Lesotho",
        "Guinea-Bissau", "Equatorial Guinea", "Mauritius", "Estonia", "Swaziland",
        "Fiji", "Comoros", "Bhutan", "Solomon Islands", "Montenegro", "Luxembourg",
        "Suriname", "Cape Verde", "Malta", "Brunei", "Belize", "Bahamas",
        "Iceland", "Vanuatu", "Barbados", "Sao Tome and Principe", "Samoa",
        "Saint Lucia", "Kiribati", "Micronesia", "Grenada", "Saint Vincent and the Grenadines",
        "Tonga", "Seychelles", "Antigua and Barbuda", "Andorra", "Dominica",
        "Marshall Islands", "Saint Kitts and Nevis", "Liechtenstein", "Monaco",
        "Nauru", "Tuvalu", "San Marino", "Palau", "Vatican City"
    ]
    
    merchant_data = {
        "merchant_id": [f"MERCH_{i:05d}" for i in range(5000)],
        "category": [random.choice(categories) for _ in range(5000)],
        "country": [random.choice(countries) for _ in range(5000)],
        "cnt_chrgeback_prev_day": [round(np.random.exponential(2.5), 2) for _ in range(5000)],
        "last_modified": [
            current_date - timedelta(days=random.randint(1, 3*365)) 
            for _ in range(5000)
        ]
    }
    
    return pl.DataFrame(merchant_data)

def generate_bank_details():
    """Generate bank details DataFrame"""
    print("Generating bank details...")
    
    countries = [
        "United States", "China", "Japan", "Germany", "India", "United Kingdom", 
        "France", "Italy", "Brazil", "Canada", "Russia", "South Korea", "Australia",
        "Spain", "Mexico", "Indonesia", "Netherlands", "Saudi Arabia", "Turkey", 
        "Taiwan", "Belgium", "Argentina", "Thailand", "Israel", "Poland", "Nigeria"
    ] * 7  # Ensure we have enough
    
    # Generate negatively skewed credit ratings (1-10, higher ratings more common)
    credit_ratings = np.random.beta(5, 2, 5000) * 9 + 1  # Beta distribution scaled to 1-10
    credit_ratings = np.round(credit_ratings).astype(int)
    credit_ratings = np.clip(credit_ratings, 1, 10)  # Ensure within bounds
    
    bank_data = {
        "bank_id": [f"BANK_{i:05d}" for i in range(5000)],
        "country": [random.choice(countries) for _ in range(5000)],
        "credit_rating": credit_ratings.tolist(),
        "last_modified": [
            current_date - timedelta(days=random.randint(1, 365)) 
            for _ in range(5000)
        ]
    }
    
    return pl.DataFrame(bank_data)

def generate_account_details():
    """Generate account details DataFrame"""
    print("Generating account details...")
    
    account_data = {
        "account_id": [f"ACC_{i:06d}" for i in range(50000)],
        "name": [fake.name() for _ in range(50000)],
        "address": [fake.address().replace('\n', ', ') for _ in range(50000)],
        "debt_end_prev_month": [round(np.random.normal(2500, 1500), 2) for _ in range(50000)],
        "last_modified": [
            current_date - timedelta(days=random.randint(1, 3*365)) 
            for _ in range(50000)
        ]
    }
    
    # Generate creation and end dates
    creation_dates = []
    end_dates = []
    
    for i in range(50000):
        creation = current_date - timedelta(days=random.randint(30, 5*365))
        creation_dates.append(creation)
        
        # 90% of accounts are still active (no end date)
        if random.random() < 0.9:
            end_dates.append(None)
        else:
            end_date = creation + timedelta(days=random.randint(30, 1000))
            end_dates.append(end_date)
    
    account_data["creation_date"] = creation_dates
    account_data["end_date"] = end_dates
    
    return pl.DataFrame(account_data)

def generate_card_details():
    """Generate card details DataFrame"""
    print("Generating card details...")
    
    # Get account IDs for foreign key reference
    account_ids = [f"ACC_{i:06d}" for i in range(50000)]
    bank_ids = [f"BANK_{i:05d}" for i in range(5000)]
    
    card_data = {
        "cc_num": [f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}" 
                  for _ in range(70000)],
        "account_id": [random.choice(account_ids) for _ in range(70000)],
        "bank_id": [random.choice(bank_ids) for _ in range(70000)]
    }
    
    # Generate issue dates (0 to 3 years ago)
    issue_dates = []
    expiry_dates = []
    
    for _ in range(70000):
        issue_date = current_date - timedelta(days=random.randint(1, 3*365))
        issue_dates.append(issue_date.replace(day=1))  # First day of month
        
        # Expiry is 3-5 years from current date
        expiry_years_ahead = random.randint(3, 5)
        expiry_date = current_date + timedelta(days=expiry_years_ahead*365)
        expiry_dates.append(expiry_date.replace(day=1))
    
    card_data["issue_date"] = issue_dates
    card_data["cc_expiry_date"] = expiry_dates
    
    # Card types with specified distribution
    card_types = ["Credit"] * 60 + ["Debit"] * 35 + ["Prepaid"] * 5
    card_data["card_type"] = [random.choice(card_types) for _ in range(70000)]
    
    # Status with specified distribution
    statuses = ["Active"] * 95 + ["Blocked"] * 4 + ["Lost/Stolen"] * 1
    card_data["status"] = [random.choice(statuses) for _ in range(70000)]
    
    card_data["last_modified"] = [
        current_date - timedelta(days=random.randint(1, 3*365)) 
        for _ in range(70000)
    ]
    
    return pl.DataFrame(card_data)

def generate_credit_card_transactions():
    """Generate credit card transactions DataFrame"""
    print("Generating credit card transactions...")
    
    # Get card numbers and merchant IDs for foreign key references
    cc_nums = [f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}" 
               for _ in range(70000)]
    merchant_ids = [f"MERCH_{i:05d}" for i in range(5000)]
    
    transaction_data = {
        "t_id": [f"TXN_{i:08d}" for i in range(2000000)],
        "cc_num": [random.choice(cc_nums) for _ in range(2000000)],
        "merchant_id": [random.choice(merchant_ids) for _ in range(2000000)],
        "amount": [round(np.random.lognormal(3.5, 1.2), 2) for _ in range(2000000)],  # Log-normal for realistic amounts
        "ip_address": [f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}" 
                      for _ in range(2000000)],
        "card_present": [random.choice([True, False]) for _ in range(2000000)]
    }
    
    # Generate timestamps uniformly over last 18 months
    start_date = current_date - timedelta(days=18*30)  # 18 months ago
    transaction_data["ts"] = [
        start_date + timedelta(seconds=random.randint(0, int((current_date - start_date).total_seconds())))
        for _ in range(2000000)
    ]
    
    return pl.DataFrame(transaction_data)

def create_feature_group_with_descriptions(df, name, description, primary_key, event_time_col=None):
    """Create feature group and add feature descriptions"""
    print(f"Creating feature group: {name}")
    
    # Create feature group
    fg = fs.create_feature_group(
        name=name,
        version=1,
        description=description,
        primary_key=primary_key,
        event_time=event_time_col,
        online_enabled=True
    )
    
    # Insert data
    fg.insert(df)
    
    # Add feature descriptions based on table specifications
    feature_descriptions = {
        "merchant_details": {
            "merchant_id": "Unique identifier for each merchant",
            "category": "Merchant category codes for goods or service purchased (e.g., Groceries, Travel, Fashion, Healthcare)",
            "country": "Country where merchant resides (160 largest countries in the world, excluding North Korea)",
            "cnt_chrgeback_prev_day": "Number of chargebacks for this merchant during the previous day (Monday-Sunday)",
            "last_modified": "Timestamp when the merchant details was last updated"
        },
        "bank_details": {
            "bank_id": "Unique identifier for each bank",
            "country": "Country where bank resides (160 largest countries in the world, excluding North Korea)",
            "credit_rating": "Bank credit rating on a scale of 1 to 10 (negatively skewed distribution)",
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
            "cc_expiry_date": "Card's expiration date (3 to 5 years from current date, year and month only)",
            "account_id": "Foreign key reference to account_details table",
            "bank_id": "Foreign key reference to bank_details table", 
            "issue_date": "Card's issue date (0 to 3 years before current date, year and month only)",
            "card_type": "Type of card (Credit: 60%, Debit: 35%, Prepaid: 5%)",
            "status": "Current status of the card (Active: 95%, Blocked: 4%, Lost/Stolen: 1%)",
            "last_modified": "Timestamp when the card details was last updated"
        },
        "credit_card_transactions": {
            "t_id": "Unique identifier for this credit card transaction",
            "cc_num": "Foreign key reference to credit card",
            "merchant_id": "Foreign key reference to merchant table",
            "amount": "Credit card transaction amount in decimal format",
            "ip_address": "IP address of the physical or online merchant (format: XXX.XXX.XXX.XXX)",
            "card_present": "Whether credit card was used in a physical terminal (true) or online payment (false)",
            "ts": "Timestamp for this credit card transaction (uniform distribution over last 18 months)"
        }
    }
    
    # Add descriptions for each feature
    if name in feature_descriptions:
        for feature_name, feature_desc in feature_descriptions[name].items():
            if feature_name in df.columns:
                try:
                    fg.update_feature_description(feature_name=feature_name, description=feature_desc)
                    print(f"  Added description for: {feature_name}")
                except Exception as e:
                    print(f"  Warning: Could not add description for {feature_name}: {e}")
    
    return fg

