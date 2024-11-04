# utils/database.py
import os
from pymongo import MongoClient
from datetime import datetime
import gridfs
import pandas as pd
import io

class DatabaseHandler:
    def __init__(self):
        # Get MongoDB connection string from environment variable
        mongo_uri = os.getenv('MONGODB_URI')
        if not mongo_uri:
            raise ValueError("MongoDB URI not found in environment variables")
        
        self.client = MongoClient(mongo_uri)
        self.db = self.client.sothebys_validator
        self.fs = gridfs.GridFS(self.db)
        
    def save_file(self, filename, file_data, file_type, metadata=None):
        """Save file to GridFS with metadata."""
        return self.fs.put(
            file_data,
            filename=filename,
            file_type=file_type,
            metadata=metadata,
            upload_date=datetime.now()
        )
    
    def get_file(self, file_id):
        """Retrieve file from GridFS."""
        return self.fs.get(file_id)
    
    def save_processing_log(self, log_entry):
        """Save processing log entry to MongoDB."""
        self.db.processing_logs.insert_one({
            **log_entry,
            'timestamp': datetime.now()
        })
    
    def get_processing_logs(self):
        """Retrieve all processing logs."""
        return list(self.db.processing_logs.find().sort('timestamp', -1))
    
    def save_processed_data(self, data_df, metadata):
        """Save processed pandas DataFrame to MongoDB."""
        # Convert DataFrame to CSV string
        csv_buffer = io.StringIO()
        data_df.to_csv(csv_buffer, index=False)
        
        # Save to GridFS
        file_id = self.fs.put(
            csv_buffer.getvalue().encode('utf-8'),
            filename=f"processed_{metadata['timestamp']}.csv",
            metadata=metadata
        )
        return file_id
    
    def get_processed_data(self, file_id):
        """Retrieve processed data as DataFrame."""
        file_data = self.fs.get(file_id)
        csv_data = io.StringIO(file_data.read().decode('utf-8'))
        return pd.read_csv(csv_data)