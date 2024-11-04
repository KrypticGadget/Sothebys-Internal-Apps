"""
MongoDB setup and test script for Sotheby's Address Validator
"""

import os
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, DESCENDING
import gridfs
from datetime import datetime

# Load environment variables
load_dotenv()

def setup_mongodb():
    """Set up MongoDB connection and initialize collections"""
    try:
        # Get MongoDB URI from environment variables
        uri = os.getenv('MONGODB_URI')
        if not uri:
            raise ValueError("MONGODB_URI not found in environment variables")

        # Create MongoDB client
        client = MongoClient(uri)
        
        # Test connection with ping
        client.admin.command('ping')
        print("✅ Successfully connected to MongoDB!")

        # Get database
        db = client.sothebys_validator
        
        # Initialize GridFS
        fs = gridfs.GridFS(db)
        
        # Set up collections with indexes
        setup_collections(db)
        
        return client, db, fs
    
    except Exception as e:
        print(f"❌ Error setting up MongoDB: {str(e)}")
        return None, None, None

def setup_collections(db):
    """Set up collections and their indexes"""
    try:
        # Users collection
        if 'users' not in db.list_collection_names():
            users = db.create_collection('users')
            users.create_index([('email', ASCENDING)], unique=True)
            print("✅ Created users collection with email index")

        # Processing logs collection
        if 'processing_logs' not in db.list_collection_names():
            logs = db.create_collection('processing_logs')
            logs.create_index([('timestamp', DESCENDING)])
            logs.create_index([('user_email', ASCENDING)])
            print("✅ Created processing_logs collection with indexes")

        # Test inserting a processing log
        test_log = {
            'filename': 'test.xlsx',
            'user_email': 'test@sothebysrealty.com',
            'timestamp': datetime.now(),
            'status': 'success',
            'records_processed': 10,
            'records_filtered': 5
        }
        db.processing_logs.insert_one(test_log)
        print("✅ Successfully inserted test processing log")

        # Test GridFS
        fs = gridfs.GridFS(db)
        test_file_id = fs.put(
            b"Test data",
            filename="test.txt",
            metadata={'test': True}
        )
        fs.delete(test_file_id)
        print("✅ Successfully tested GridFS operations")

    except Exception as e:
        print(f"❌ Error setting up collections: {str(e)}")

def display_database_info(db):
    """Display information about the database and its collections"""
    try:
        print("\n📊 Database Information:")
        print("-" * 50)
        
        # List collections
        collections = db.list_collection_names()
        print(f"\nCollections in database:")
        for collection in collections:
            count = db[collection].count_documents({})
            print(f"- {collection}: {count} documents")

        # Display indexes for each collection
        print("\nIndexes:")
        for collection in collections:
            print(f"\n{collection} indexes:")
            indexes = db[collection].list_indexes()
            for index in indexes:
                print(f"- {index['name']}: {index['key']}")

    except Exception as e:
        print(f"❌ Error displaying database info: {str(e)}")

def main():
    """Main setup function"""
    print("\n🚀 Setting up MongoDB for Sotheby's Address Validator...\n")
    
    # Set up MongoDB connection and collections
    client, db, fs = setup_mongodb()
    
    # Check if setup was successful
    if client is not None and db is not None and fs is not None:
        # Display database information
        display_database_info(db)
        
        print("\n✨ MongoDB setup complete!")
        
        # Clean up
        client.close()
    else:
        print("\n❌ MongoDB setup failed")

if __name__ == "__main__":
    main()