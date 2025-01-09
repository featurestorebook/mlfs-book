import hopsworks
import pandas as pd
import common
import email-validator

def zipcode(address: pd.Series) -> pd.Series :
    # convert address to zipcode

def email(email):
    try:
        validation = validate_email(email, check_deliverability=True)
        extracted = tldextract.extract(validation.email)
        domain = f"{extracted.domain}.{extracted.suffix}"
        
        score = 50
        trusted_domains = ["gov", "edu"]
        if extracted.suffix in trusted_domains:
            score += 30
        elif extracted.suffix in ["com", "org"]:
            score += 20
        
        disposable_domains = ["mailinator.com", "guerrillamail.com", "10minutemail.com"]
        if domain in disposable_domains:
            score -= 40
        
        return min(max(score, 0), 100)
    except EmailNotValidError:
        return 0