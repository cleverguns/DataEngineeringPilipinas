import json
import pandas as pd

# Function to load JSON data from file
def load_json(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data

# Load JSON files into dictionaries
sales_data = load_json("sales_data.json")
customer_data = load_json("customer_data.json")
products_data = load_json("products_data.json")
shipping_data = load_json("shipping_data.json")

# Convert dictionaries to DataFrames
sales_df = pd.DataFrame(sales_data)
customer_df = pd.DataFrame(customer_data)
products_df = pd.DataFrame(products_data)
shipping_df = pd.DataFrame(shipping_data)

# Performing multi-step data integration process
merged_df = pd.merge(sales_df, customer_df, on='Customer_ID', how='left')

# Extracting year, month, and quarter from the date
merged_df['YearMonth'] = pd.to_datetime(merged_df['Date']).dt.to_period('M')
merged_df['Quarter'] = pd.to_datetime(merged_df['Date']).dt.quarter

# Merging with product data
merged_df = pd.merge(merged_df, products_df, on='Product', how='left')

# Merging with shipping data
merged_df = pd.merge(merged_df, shipping_df, on='Order_ID', how='left')

# Calculating discounted amount based on customer segment, loyalty level, and dynamic pricing
merged_df['Segment_Discount'] = merged_df.apply(
    lambda row: 0.1 if row['Segment'] == 'Gold' else (0.05 if row['Segment'] == 'Silver' else 0),
    axis=1
)

merged_df['Loyalty_Discount'] = merged_df.apply(
    lambda row: 0.15 if row['Loyalty_Level'] == 'Platinum' else (0.1 if row['Loyalty_Level'] == 'Gold' else 0),
    axis=1
)

merged_df['Dynamic_Pricing'] = merged_df.apply(
    lambda row: row['Base_Price'] * (0.1 * row['Popularity_score']),
    axis=1
)

merged_df['Discounted_Amount'] = merged_df['Amount'] * (1 - merged_df['Discount'] - merged_df['Segment_Discount'] - merged_df['Loyalty_Discount']) - merged_df['Dynamic_Pricing']

# Calculating total cost including shipping and discounted amount
merged_df['Total_Cost'] = merged_df['Discounted_Amount'] + merged_df['Shipping_Cost']

# Define SQL filename with full path
sql_filename = "C:\\Users\\jgimpaya.ENSLAP0542\\Desktop\\DE project\\data Integration-ingestion\\data_inserts.sql"

# Write SQL INSERT statements to the SQL file
with open(sql_filename, "w") as sql_file:
    for index, row in merged_df.iterrows():
        sql_values = [f"'{value}'" if isinstance(value, str) else str(value) for value in row.values]
        sql_statement = f"INSERT INTO your_table_name ({', '.join(merged_df.columns)}) VALUES ({', '.join(sql_values)});\n"
        sql_file.write(sql_statement)

print(f"Data has been saved to {sql_filename}")
