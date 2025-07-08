# Import libraries
from pymongo import MongoClient, ASCENDING
import pandas as pd
from sqlalchemy import create_engine, text
import time
import pprint

#########################################################################################################################################


####################################################### PHASE 1 #########################################################################


#########################################################################################################################################


########################################################## DATA PROCESSING ##############################################################

# Load the listings.csv into a DataFrame:
df_listings = pd.read_csv('listings.csv')
# Select specific columns:
listings_columns = [
    "id", "name", "host_id", "host_name", "host_since", "host_listings_count"
    , "neighbourhood_cleansed", "property_type", "room_type", "accommodates", "price",
    "minimum_nights", "number_of_reviews", "availability_30"
]
df_listings = df_listings[listings_columns]
# Clean and convert the price column:
df_listings['price'] = df_listings['price'].replace({'\$': '', ',': ''}, regex=True).astype(float)
df_listings['price'] = df_listings['price'].fillna(0)
# Convert host_since to datetime:
df_listings['host_since'] = pd.to_datetime(df_listings['host_since'], format='%Y-%m-%d')

# Load the calendar.csv into a DataFrame:
df_calendar = pd.read_csv('calendar.csv')
# Drop the adjusted_price column:
df_calendar = df_calendar.drop(columns=["adjusted_price"], errors="ignore")
# Clean and convert the price column:
df_calendar['price'] = df_calendar['price'].replace({'\$': '', ',': ''}, regex=True).astype(float)
# Convert the date column to datetime:
df_calendar['date'] = pd.to_datetime(df_calendar['date'], format='%Y-%m-%d')
# Map available to Boolean values:
df_calendar['available'] = df_calendar['available'].map({'t': True, 'f': False})

# Load the reviews.csv into a DataFrame:
df_reviews = pd.read_csv('reviews.csv')
# Handle missing values in the comments column:
df_reviews['comments'] = df_reviews['comments'].fillna('No comment')
# Convert the date column to datetime format:
df_reviews['date'] = pd.to_datetime(df_reviews['date'], format='%Y-%m-%d')

# Convert dataframes to dictionaries:
listings_dict = df_listings.to_dict(orient="records")
calendar_dict = df_calendar.to_dict(orient="records")
reviews_dict = df_reviews.to_dict(orient="records")

########################################################## MySQL ##############################################################

# Create Connection
username = 'root'
password = 'xddx'
host = 'localhost'
port = '3306'
database_name = 'BDAProject'
table_listings = 'listings'
table_calendar = 'calendar'
table_reviews = 'reviews'

# Create the connection URL for SQL Alchemy
db_url = f'mysql+mysqlconnector://{username}:{password}@{host}:{port}'
engine = create_engine(db_url)

# Create a relational database
with engine.connect() as connection:
    connection.execute(text(f"DROP DATABASE IF EXISTS {database_name}"))
    connection.execute(text(f"CREATE DATABASE {database_name}"))

db_url = f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database_name}'
engine = create_engine(db_url)

# Create tables
print("Creating tables in MySQL\n")
with engine.connect() as connection:
    connection.execute(text(f"""
    CREATE TABLE IF NOT EXISTS {table_listings} (
        id BIGINT PRIMARY KEY,
        name VARCHAR(100),
        host_id BIGINT,
        host_name VARCHAR(50),
        host_since DATE,
        host_listings_count INT,
        neighbourhood_cleansed VARCHAR(100),
        room_type VARCHAR(100),
        property_type VARCHAR(100),
        accommodates INT,
        price DECIMAL(10, 2),
        minimum_nights INT,
        number_of_reviews INT,
        availability_30 INT
    )
    """))

    connection.execute(text(f"""
    CREATE TABLE IF NOT EXISTS {table_calendar} (
        listing_id BIGINT,
        date DATE,
        available BOOLEAN,
        price DECIMAL(10, 2),
        minimum_nights INT,
        maximum_nights INT,
        PRIMARY KEY (listing_id, date),
        FOREIGN KEY (listing_id) REFERENCES {table_listings}(id) ON DELETE CASCADE
    )
    """))

    connection.execute(text(f"""
    CREATE TABLE IF NOT EXISTS {table_reviews} (
        listing_id BIGINT NOT NULL,
        id BIGINT PRIMARY KEY,
        date DATE,
        reviewer_id INT,
        reviewer_name VARCHAR(50),
        comments TEXT,
        FOREIGN KEY (listing_id) REFERENCES {table_listings}(id) ON DELETE CASCADE
    )
    """))

# Insert into tables
print("Inserting data into MySQL\n")
df_listings.to_sql(name=table_listings, con=engine, if_exists='append', index=False, method='multi')
df_calendar.to_sql(name=table_calendar, con=engine, if_exists='append', index=False, method='multi')
df_reviews.to_sql(name=table_reviews, con=engine, if_exists='append', index=False, method='multi')
print("Data inserted into MySQL\n")

########################################################## MONGODB ##############################################################

# Connect to MongoDB locally
client = MongoClient("mongodb://localhost:27017")

# Connect to MongoDB atlas
#client = MongoClient("mongodb+srv://dmvenes:xddx@clusterbda.ibdqi.mongodb.net/")

# Drop database for testing purposes
client.drop_database('BDAProject')
print("Database dropped")

# Create a NoSQL database
db = client['BDAProject']

# Define collections
collection_listings = db['listings']
collection_calendar = db['calendar']
collection_reviews = db['reviews']

# Insert data into collections with error handling
for collection, data_dict, name in [
    (collection_listings, listings_dict, "listings"),
    (collection_calendar, calendar_dict, "calendar"),
    (collection_reviews, reviews_dict, "reviews")
]:
    # Get initial document count
    initial_count = collection.count_documents({})
    print(f"# documents in {name} collection before insertion: {initial_count}")

    try:
        # Insert data
        result = collection.insert_many(data_dict, ordered=False)
        valid_count = len(result.inserted_ids)  # Successfully inserted documents
        print(f"{valid_count} documents inserted into {name} collection.")
    except Exception as e:
        # Handle insertion errors
        attempted_count = len(data_dict)
        valid_count = collection.count_documents({}) - initial_count
        print(f"{valid_count} documents inserted into {name} collection.")
        print(f"{attempted_count - valid_count} documents failed to insert.")
        print(f"Error: {e}")

    # Final document count
    final_count = collection.count_documents({})
    print(f"# documents in {name} collection after insertion: {final_count}\n")

########################################################## SIMPLE QUERIES ##############################################################

# Identify the simple data operations:
# Select all listings that are of type "Private room" and have at least 20 reviews
# Select all reviews from October 2019

# Define the MySQL simple queries:
mySQL_queries = [
    {"description": "Select all listings that are of type 'Private room' and have at least 20 reviews",
     "query": f"""
        SELECT * 
        FROM {table_listings} 
        WHERE room_type = 'Private room' AND number_of_reviews >= 20;
     """},

    {"description": "Select all reviews from October 2019",
     "query": f"""
        SELECT * 
        FROM {table_reviews}
        WHERE YEAR(date) = 2019 AND MONTH(date) = 10;
     """}
]

# Define the MongoDB simple queries:
mongoDB_queries = [
    {"description": "Select all listings that are of type 'Private room' and have at least 20 reviews",
     "collection": collection_listings,
     "query": {"room_type": "Private room", "number_of_reviews": {"$gte": 20}}},

    {"description": "Select all reviews from October 2019",
     "collection": collection_reviews,
     "query": {"date": {"$gte": pd.Timestamp('2019-10-01'), "$lt": pd.Timestamp('2019-11-01')}}}
]

# MySQL Query
print("MySQL simple query results:")
with engine.connect() as connection:
    for q in mySQL_queries:
        result = connection.execute(text(q["query"]))
        records = result.fetchall()  # Fetch all results
        print(f"{q['description']}: {len(records)} records found.")

# MongoDB Query
print("\nMongoDB simple query results:")
for q in mongoDB_queries:
    result = q["collection"].find(q["query"])
    list_result = list(result)
    print(f"{q['description']}: {len(list_result)} records found.")

########################################################## COMPLEX QUERIES ##############################################################

# Identify the complex data operations:
# Insert a new review to a listing whose host has been active since 2015.
# Delete the listings of the first 5 hosts with the highest number of ‘Entire rental unit’ (property_type)
# available on 2024-12-25.

# Define the MySQL complex queries:
mySQL_complex_queries = [
    {
        "description": "Insert a new review to a listing whose host has been active since 2015",
        "query": """
            INSERT INTO reviews (listing_id, id, date, reviewer_id, reviewer_name, comments)
            VALUES (
                (SELECT id
                 FROM listings
                 WHERE YEAR(host_since) = 2015
                 LIMIT 1),
                1234567,
                NOW(),
                1234567,
                'Placeholder name',
                'Placeholder comment.'
            );
        """
    },
    {
        "description": "Delete the listings of the first 5 hosts with the highest number of ‘Entire rental unit’ (property_type) available on 2024-12-25",
        "query": """
            WITH TopHosts AS (
                SELECT host_id, COUNT(*) AS num_listings
                FROM listings
                WHERE property_type = 'Entire rental unit'
                GROUP BY host_id
                ORDER BY num_listings DESC
                LIMIT 5
            ),
            AvailableOnXmas AS (
                SELECT c.listing_id
                FROM calendar c
                JOIN listings l ON c.listing_id = l.id
                WHERE c.date = '2024-12-25' AND c.available = TRUE
                  AND l.host_id IN (SELECT host_id FROM TopHosts)
            )
            DELETE FROM listings
            WHERE id IN (SELECT listing_id FROM AvailableOnXmas);
        """
    }
]

# Define the MongoDB complex queries:
mongoDB_complex_queries = [
    {"description": "Insert a new review to a listing whose host has been active since 2015",
     "collection": collection_reviews,
     "operation": "insert",  # Specify op type (to use below).
     "data": {
         # Find listing with a host active since 2015
         "listing_query": {"host_since": {"$gte": pd.Timestamp('2015-01-01'), "$lte": pd.Timestamp('2015-12-31')}},
         "review": {
             "date": pd.Timestamp.now().strftime("%Y-%m-%d"),  # Add the review now
             "reviewer_id": 1234567,
             "reviewer_name": "Real Person",
             "comments": "An okay experience."
         }}},
    {
        "description": "Delete the listings of the first 5 hosts with the highest number of 'Entire rental unit' (property_type) available on 2024-12-25",
        "collection": collection_listings,
        "operation": "delete",
        "data": {
            "top_hosts_pipeline": [
                {"$match": {"property_type": "Entire rental unit"}},  # Filter "Entire rental unit" property type
                {"$group": {
                    "_id": "$host_id",
                    "num_listings": {"$sum": 1}  # count listings per host
                }},
                {"$sort": {"num_listings": -1}},  # descending sort
                {"$limit": 5}  # Get top 5
            ]}}
]

# MySQL Query
print("\nMySQL complex query results:")
with engine.connect() as connection:
    for q in mySQL_complex_queries:
        try:
            start_time = time.time()
            result = connection.execute(text(q["query"]))
            connection.commit()
            end_time = time.time()
            print(f"{q['description']}: Affected {result.rowcount} rows.")
            print(f"Execution time: {end_time - start_time:.4f} seconds.")
        except Exception as e:
            print(f"Error executing query: {e}")

# MongoDB Query
print("\nMongoDB complex query results:")
for q in mongoDB_complex_queries:
    start_time = time.time()
    if q["operation"] == "insert":
        listing = collection_listings.find_one(q["data"]["listing_query"])
        # If there's a matching listing, we insert the review
        if listing:
            q["data"]["review"]["listing_id"] = listing["id"]  # Add the missing listing id
            result = q["collection"].insert_one(q["data"]["review"])
            print(f"{q['description']}: Inserted new review with ID {result.inserted_id}.")
        else:
            print(f"{q['description']}: No matching listing found.")

    if q["operation"] == "delete":
        # Identify top 5 hosts
        top_hosts_result = list(q["collection"].aggregate(q["data"]["top_hosts_pipeline"]))
        top_host_ids = [host["_id"] for host in top_hosts_result]

        # Find listings available on christmas day (from the top hosts)
        christmas_day = pd.Timestamp('2024-12-25')
        listings_available_pipeline = [
            {"$match": {"host_id": {"$in": top_host_ids}}},  # Filter listings by top hosts
            # Join with the calendar collection
            {"$lookup": {
                # Note: We can join documents on collections in MongoDB by using the $lookup (Aggregation) function
                "from": "calendar",
                "localField": "id",
                "foreignField": "listing_id",
                "as": "calendar_data"
            }},
            {"$unwind": "$calendar_data"},  # without this, we can't filter elements within the array
            # - https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
            {"$match": {"calendar_data.date": christmas_day, "calendar_data.available": True}}
        ]

        listings_to_delete = list(q["collection"].aggregate(listings_available_pipeline))
        # we just need the id's
        listing_ids_to_delete = [listing["_id"] for listing in listings_to_delete]

        if listing_ids_to_delete:
            delete_result = q["collection"].delete_many({"_id": {"$in": listing_ids_to_delete}})
            print(f"{q['description']}: Deleted {delete_result.deleted_count} listings.")
        else:
            print(f"{q['description']}: No matching listing found.")

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.4f} seconds.")

#########################################################################################################################################
