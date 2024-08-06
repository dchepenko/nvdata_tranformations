import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# Load dataset
# Assuming the dataset is in a CSV file called 'startups.csv'
df = pd.read_csv('specter_food.csv')

# Weights
weights = {
    'category': 0.8,
    'geo': 0.8,
    'market': 0.8,
    'industry': 0.2,
    'foundation_year': 0.2,
    'funding_round': 0.5,
    'traffic_growth': 0.5,
    'employee_growth': 0.5,
    'third_party_ranking': 0.05,  # Additional weight for the third-party ranking
    'funding_amount_penalty': 0.05  # Additional weight for funding amount penalty
}

# Priority industries and countries
priority_industries = ['Finance', 'Mobility', 'Food']
priority_categories = ['Full-Stack Grocery Retail', 'On-demand Food Delivery', 'Grocery Aggregators and Delivery Platform', 'Dark Kitchens', 'Cloud Kitchens']
# priority_countries = ['Brazil', 'Colombia', 'Mexico', 'Egypt', 'Pakistan']
priority_countries = ['Egypt', 'Pakistan', 'Nepal', 'India', 'Iraq', 'Turkey', 'Qatar']
secondary_regions = ['Africa', 'South America', 'Asia']
# priority_markets = ['Brazil', 'Colombia', 'Mexico', 'Egypt', 'Pakistan']
priority_countries = ['Egypt', 'Pakistan', 'Nepal', 'India', 'Iraq', 'Turkey', 'Qatar']

# Normalize the year of foundation and MoM growths
scaler = MinMaxScaler()

df['normalized_foundation_year'] = scaler.fit_transform(df[['founded_date']])
# df['normalized_traffic_growth'] = df['Web Visits - Monthly Growth (%)']
# df['normalized_employee_growth'] = df['Employees - Monthly Growth (%)']

df['normalized_traffic_growth'] = scaler.fit_transform(df[['web_visits']])
df['normalized_employee_growth'] = scaler.fit_transform(df[['employees_monthly_growth']])
df['Rank'] = df['rank'].apply(lambda rank: float(str(rank).replace(',', '')))
df['normalized_third_party_ranking'] = scaler.fit_transform(df[['rank']])


# Scoring functions
def score_industry(industry):
    return 1 if industry in priority_industries else 0.3

def score_geo(geo):
    if geo in priority_countries:
        return 1
    elif geo in secondary_regions:
        return 0.75
    else:
        return 0.5
    
def score_market(geo):
    if geo in priority_countries:
        return 1
    else:
        return 0.5

def score_funding_round(last_round):
    # Assume the 'Last funding round' can be 'Seed', 'Series A', 'Series B', etc.
    funding_round_scores = {
        'Seed': 0.8,
        'Series A': 1.0,
        'Series B': 0.6,
        'Series C': 0.4,
        'Series D': 0.2,
        'Undisclosed': 0.1,
        'Private Equity': 0.1,
        'Non Equity Assistance': 0.1,
        'Convertible Note': 0.1,
        'Equity Crowdfunding': 0.1,
        'Grant': 0.1,
        'Angel': 0.1,
        'Series Unknown': 0.1,
        'Secondary Market': 0.1,
        'Debt Financing': 0.1,
        'Corporate Round': 0.1,
        'Series F': 0.1,
        'Product Crowdfunding': 0.1,
        'Initial Coin Offering': 0.1,
    }
    return funding_round_scores.get(last_round, 0.0)

def funding_amount_penalty(amount_raised):
    # Apply a penalty if the amount raised is greater than 54 million USD
    if float(amount_raised) > 5000000:
        return -1.0  # Penalty value, adjust as necessary
    else:
        return 0.0

# Calculate scores
df['industry_score'] = df['industry'].apply(score_industry)
df['category_score'] = df['category'].apply(score_industry)
df['geo_score'] = df['hq_location'].apply(score_geo)
df['market_score'] = df['top_country'].apply(score_market)
df['funding_round_score'] = df['last_funding_type'].apply(score_funding_round)
# df['funding_amount_penalty'] = df['Total Funding Amount (in USD)'].apply(funding_amount_penalty)

# Final score
df['final_score'] = (
    weights['industry'] * df['industry_score'] +
    weights['geo'] * df['geo_score'] +
    weights['market'] * df['market_score'] +
    weights['foundation_year'] * df['normalized_foundation_year'] +
    weights['funding_round'] * df['funding_round_score'] +
    weights['traffic_growth'] * df['normalized_traffic_growth'] +
    weights['employee_growth'] * df['normalized_employee_growth'] + 
    weights['third_party_ranking'] * df['normalized_third_party_ranking'] 
)

# Rank startups
df['rank'] = df['final_score'].rank(ascending=False)

# Sort by rank
df = df.sort_values(by='rank')

# Output the ranked dataframe
df.to_csv('ranked_startups_food.csv', index=False)
