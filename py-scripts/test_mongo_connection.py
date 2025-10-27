from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

def test_mongo_connection():
    try:
        client = MongoClient("mongodb://localhost:27017/admin")
        # Test the connection
        client.admin.command('ping')
        print("MongoDB connection successful!")
    except ConnectionFailure as e:
        print(f"MongoDB connection failed: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    test_mongo_connection()
