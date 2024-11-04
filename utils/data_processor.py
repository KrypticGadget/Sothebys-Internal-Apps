# utils/data_processor.py
import pandas as pd
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from utils.address_standardizer import AddressStandardizer

class DataProcessor:
    def __init__(self, valid_property_classes):
        """
        Initialize the DataProcessor with valid property classes.
        
        Property Class Codes:
        - CD: Residential Condominium
        - B9: Mixed Residential & Commercial Buildings
        - B2: Office Buildings
        - B3: Industrial & Manufacturing
        - C0/CO: Commercial Condominium (both formats accepted)
        - B1: Hotels & Apartments
        - C1: Walk-up Apartments
        - A9: Luxury / High-End Residential
        - C2: Elevator Apartments
        """
        self.valid_property_classes = valid_property_classes
        self.address_standardizer = AddressStandardizer()
        self.logger = logging.getLogger(__name__)
        
        # Property class descriptions for user feedback
        self.property_class_descriptions = {
            "CD": "Residential Condominium",
            "B9": "Mixed Residential & Commercial",
            "B2": "Office Buildings",
            "B3": "Industrial & Manufacturing",
            "C0": "Commercial Condominium",
            "CO": "Commercial Condominium",  # Alternative format
            "B1": "Hotels & Apartments",
            "C1": "Walk-up Apartments",
            "A9": "Luxury Residential",
            "C2": "Elevator Apartments"
        }
        
        # Mapping for standardizing property class formats
        self.property_class_standardization = {
            "CO": "C0",  # Map CO to C0 format
            "C0": "C0",  # Keep C0 as is
        }

    def standardize_property_class(self, property_class):
        """Standardize property class format."""
        if not property_class:
            return property_class
        return self.property_class_standardization.get(property_class, property_class)

    def load_data(self, file_path):
        """Load data with optimized settings."""
        try:
            return pd.read_excel(
                file_path,
                engine='openpyxl',
                dtype={
                    'Zipcode': str,
                    'Address': str,
                    'City': str,
                    'State': str,
                    'Property class': str
                }
            )
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise

    def analyze_property_classes(self, df):
        """
        Analyze property classes in the dataset and provide detailed feedback.
        
        Returns:
            tuple: (filtered_df, stats_dict) where stats_dict contains analysis results
        """
        try:
            # First standardize the property classes
            df = df.copy()
            df["Property class"] = df["Property class"].apply(self.standardize_property_class)
            
            # Count occurrences of each property class
            class_counts = df["Property class"].value_counts()
            
            # Separate valid and invalid classes
            valid_classes = {cls: count for cls, count in class_counts.items() 
                            if cls in self.valid_property_classes}
            invalid_classes = {cls: count for cls, count in class_counts.items() 
                             if cls not in self.valid_property_classes}
            
            # Filter the dataframe
            filtered_df = df[df["Property class"].isin(self.valid_property_classes)]
            
            # Calculate total CO/C0 records
            co_count = len(df[df["Property class"].isin(["CO", "C0"])])
            
            # Create statistics dictionary
            stats = {
                "total_records": len(df),
                "valid_records": len(filtered_df),
                "filtered_out": len(df) - len(filtered_df),
                "valid_classes": {
                    cls: {
                        "count": count,
                        "description": self.property_class_descriptions.get(cls, "Unknown"),
                        "percentage": (count / len(df) * 100) if len(df) > 0 else 0
                    }
                    for cls, count in valid_classes.items()
                },
                "invalid_classes": {
                    cls: count for cls, count in invalid_classes.items()
                },
                "co_records": co_count
            }
            
            return filtered_df, stats
            
        except Exception as e:
            self.logger.error(f"Error analyzing property classes: {str(e)}")
            raise

    def filter_data(self, df, status_callback=None):
        """Enhanced data filtering with detailed statistics."""
        try:
            if status_callback:
                status_callback("Analyzing property classes...")
            
            filtered_df, stats = self.analyze_property_classes(df)
            
            if status_callback:
                # Create detailed status message
                message = f"""
                📊 Property Class Analysis:
                Total Records: {stats['total_records']}
                Valid Records: {stats['valid_records']} ({(stats['valid_records']/stats['total_records']*100):.1f}%)
                Filtered Out: {stats['filtered_out']} ({(stats['filtered_out']/stats['total_records']*100):.1f}%)
                
                ℹ️ Note: Both 'C0' and 'CO' are treated as Commercial Condominium class
                
                ✅ Valid Property Classes:
                """
                
                # Add details for each valid class
                for cls, info in stats['valid_classes'].items():
                    description = info['description']
                    count = info['count']
                    percentage = info['percentage']
                    
                    # Special handling for C0/CO display
                    if cls in ["C0", "CO"]:
                        message += f"\n• Commercial Condominium (C0/CO): {count} ({percentage:.1f}%)"
                    else:
                        message += f"\n• {cls} - {description}: {count} ({percentage:.1f}%)"
                
                if stats['invalid_classes']:
                    message += "\n\n❌ Filtered Out Classes:"
                    for cls, count in stats['invalid_classes'].items():
                        message += f"\n• {cls}: {count}"
                
                status_callback(message)
            
            # Drop unnecessary columns
            filtered_df = filtered_df.drop(columns=["Block & Lot"], errors="ignore")
            
            return filtered_df, stats
            
        except Exception as e:
            self.logger.error(f"Error in filter_data: {str(e)}")
            raise

    def create_full_addresses(self, df):
        """Efficiently create full addresses for all rows."""
        try:
            # Ensure all address components are strings and handle NaN
            components = ['Address', 'City', 'State', 'Zipcode']
            for col in components:
                if col in df.columns:
                    df[col] = df[col].fillna('').astype(str).str.strip()

            # Vectorized operations for combining address components
            full_addresses = (
                df['Address'].str.strip() + ', ' +
                df['City'].str.strip() + ', ' +
                df['State'].str.strip() + ' ' +
                df['Zipcode'].str.strip()
            )
            
            return full_addresses.str.strip(', ')
            
        except Exception as e:
            self.logger.error(f"Error creating full addresses: {str(e)}")
            raise

    def standardize_addresses(self, df, status_callback=None):
        """
        Batch process address standardization with progress updates and component handling.
        """
        try:
            if status_callback:
                status_callback("Creating full addresses...")
            
            # First create all full addresses
            df['Full Address'] = self.create_full_addresses(df)
            
            # Get unique addresses to avoid processing duplicates
            unique_addresses = df['Full Address'].unique()
            
            if status_callback:
                status_callback(f"Standardizing {len(unique_addresses)} unique addresses...")
            
            # Batch process all unique addresses
            standardized_results = self.address_standardizer.standardize_batch(unique_addresses)
            
            # Create a mapping for full addresses
            address_mapping = {
                addr: result['full_address']
                for addr, result in standardized_results.items()
            }
            
            # Update the DataFrame with standardized full addresses
            df['Full Address'] = df['Full Address'].map(address_mapping)
            
            if status_callback:
                status_callback("Updating address components...")
            
            # Update individual components
            for idx, row in df.iterrows():
                original_address = row['Full Address']
                if original_address in standardized_results:
                    result = standardized_results[original_address]
                    if result.get('components'):
                        components = result['components']
                        for field, value in components.items():
                            if field in df.columns:
                                df.at[idx, field] = value
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error in standardize_addresses: {str(e)}")
            return df

    def remove_duplicates(self, df):
        """Optimized duplicate removal."""
        try:
            # Convert sale date once for all rows
            if 'Sale date' in df.columns:
                df['Sale date'] = pd.to_datetime(df['Sale date'], errors='coerce')
                df = df.sort_values('Sale date', ascending=False)

            return df.drop_duplicates(subset=['Full Address'], keep='first').reset_index(drop=True)
            
        except Exception as e:
            self.logger.error(f"Error removing duplicates: {str(e)}")
            return df

    def process_file(self, file_path, status_callback=None):
        """
        Process file with enhanced property class filtering and status updates.
        """
        try:
            if status_callback:
                status_callback("Loading data...")
            
            df = self.load_data(file_path)
            initial_count = len(df)
            
            if status_callback:
                status_callback(f"Analyzing {initial_count} records...")
            
            filtered_df, filter_stats = self.filter_data(df, status_callback)
            filtered_count = len(filtered_df)
            
            if filtered_count == 0:
                if status_callback:
                    status_callback("❌ No valid property classes found in the data.")
                return None
            
            if status_callback:
                status_callback(f"Standardizing {filtered_count} addresses...")
            
            standardized_df = self.standardize_addresses(filtered_df, status_callback)
            
            if status_callback:
                status_callback("Removing duplicates...")
            
            deduped_df = self.remove_duplicates(standardized_df)
            final_count = len(deduped_df)
            
            # Add processing timestamp and filter statistics
            deduped_df["Processed Date"] = datetime.now().strftime("%B %d, %Y at %I:%M %p")
            
            # Add property class descriptions
            deduped_df['Property Class Description'] = deduped_df['Property class'].map(self.property_class_descriptions)
            
            # Reorder columns to put important information first
            columns = [
                'Full Address', 
                'Address',
                'City', 
                'State',
                'Zipcode',
                'Property class', 
                'Property Class Description'
            ] + [
                col for col in deduped_df.columns if col not in [
                    'Full Address', 'Address', 'City', 'State', 'Zipcode',
                    'Property class', 'Property Class Description'
                ]
            ]
            deduped_df = deduped_df[columns]
            
            if status_callback:
                status_callback(f"""
                ✅ Processing complete!
                
                📈 Final Statistics:
                • Initial records: {initial_count}
                • Valid property classes: {filtered_count}
                • Final records after deduplication: {final_count}
                • Removed duplicates: {filtered_count - final_count}
                
                Most common property types in final dataset:
                {deduped_df['Property class'].value_counts().head(3).to_string()}
                """)
            
            return deduped_df
            
        except Exception as e:
            self.logger.error(f"Error processing file: {str(e)}")
            if status_callback:
                status_callback(f"Error: {str(e)}")
            return None