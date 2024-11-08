# utils/data_processor.py
import re
import pandas as pd
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from utils.address_standardizer import AddressStandardizer

class DataProcessor:
    def __init__(self, valid_property_classes):
        """Initialize DataProcessor with enhanced address handling."""
        self.valid_property_classes = valid_property_classes
        self.address_standardizer = AddressStandardizer()
        self.logger = logging.getLogger(__name__)
        
        # Property class descriptions
        self.property_class_descriptions = {
            "CD": "Residential Condominium",
            "B9": "Mixed Residential & Commercial",
            "B2": "Office Buildings",
            "B3": "Industrial & Manufacturing",
            "C0": "Commercial Condominium",
            "CO": "Commercial Condominium",
            "B1": "Hotels & Apartments",
            "C1": "Walk-up Apartments",
            "A9": "Luxury Residential",
            "C2": "Elevator Apartments"
        }
        
        # Property class standardization mapping
        self.property_class_standardization = {
            "CO": "C0",
            "C0": "C0",
            "B1": "B1"
        }
        
        # Address component columns
        self.address_components = ['Address', 'City', 'State', 'Zipcode']

    def standardize_property_class(self, property_class):
        """Standardize property class format."""
        if not property_class:
            return property_class
        return self.property_class_standardization.get(property_class, property_class)

    def load_data(self, file_path):
        """Load data with enhanced type handling."""
        try:
            return pd.read_excel(
                file_path,
                engine='openpyxl',
                dtype={
                    'Zipcode': str,
                    'Address': str,
                    'City': str,
                    'State': str,
                    'Property class': str,
                    'Full Address': str
                }
            )
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise

    def analyze_property_classes(self, df):
        """Analyze property classes with enhanced statistics."""
        try:
            df = df.copy()
            df["Property class"] = df["Property class"].apply(self.standardize_property_class)
            
            class_counts = df["Property class"].value_counts()
            valid_classes = {cls: count for cls, count in class_counts.items() 
                           if cls in self.valid_property_classes}
            invalid_classes = {cls: count for cls, count in class_counts.items() 
                             if cls not in self.valid_property_classes}
            
            filtered_df = df[df["Property class"].isin(self.valid_property_classes)]
            co_count = len(df[df["Property class"].isin(["CO", "C0"])])
            
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
                "invalid_classes": invalid_classes,
                "co_records": co_count
            }
            
            return filtered_df, stats
            
        except Exception as e:
            self.logger.error(f"Error analyzing property classes: {str(e)}")
            raise

    def standardize_addresses(self, df, status_callback=None):
        """Enhanced address standardization with component splitting."""
        try:
            if status_callback:
                status_callback("Processing addresses...")
            
            # Create or update full addresses
            if 'Full Address' not in df.columns:
                df['Full Address'] = self.create_full_addresses(df)
            
            # Get unique addresses
            unique_addresses = df['Full Address'].unique()
            total_addresses = len(unique_addresses)
            
            if status_callback:
                status_callback(f"Standardizing {total_addresses} unique addresses...")
            
            # Process addresses in batches
            standardized_results = self.address_standardizer.standardize_batch(unique_addresses)
            
            # Initialize address component columns
            for component in self.address_components:
                if component not in df.columns:
                    df[component] = ''
            
            # Validation function for addresses
            def validate_address(row):
                """Validate that no common abbreviations remain in the address."""
                common_abbrevs = ['St', 'Ave', 'Rd', 'Blvd', 'Ln', 'Dr', 'Ct', 'Pl', 'Ft']
                address = row['Address']
                remaining_abbrevs = [abbr for abbr in common_abbrevs if f" {abbr}" in address]
                if remaining_abbrevs:
                    self.logger.warning(f"Found unexpanded abbreviations in address: {address}")
                    return False
                return True
            
            # Update DataFrame with standardized components
            for idx, row in df.iterrows():
                original_address = row['Full Address']
                if original_address in standardized_results:
                    result = standardized_results[original_address]
                    if result.get('components'):
                        components = result['components']
                        
                        # Update individual components with expanded forms
                        if components.get('Address'):
                            # Expand street abbreviations
                            address = components['Address']
                            address = self._expand_street_abbreviations(address)
                            df.at[idx, 'Address'] = address
                        
                        if components.get('City'):
                            df.at[idx, 'City'] = components['City']
                        
                        if components.get('State'):
                            # Expand state abbreviations
                            state = self._expand_state_abbreviation(components['State'])
                            df.at[idx, 'State'] = state
                        
                        if components.get('Zipcode'):
                            df.at[idx, 'Zipcode'] = components['Zipcode']
                        
                        # Update full address with expanded components
                        full_address = f"{df.at[idx, 'Address']}, {df.at[idx, 'City']}, {df.at[idx, 'State']} {df.at[idx, 'Zipcode']}"
                        df.at[idx, 'Full Address'] = full_address
            
            # Check for unexpanded abbreviations
            unexpanded_addresses = df[~df.apply(validate_address, axis=1)]
            if not unexpanded_addresses.empty:
                if status_callback:
                    status_callback("\nWarning: Found addresses with unexpanded abbreviations:")
                    for _, row in unexpanded_addresses.iterrows():
                        status_callback(f"- {row['Full Address']}")
            
            # Ensure consistent column order
            columns = ['Full Address'] + self.address_components + [
                col for col in df.columns 
                if col not in ['Full Address'] + self.address_components
            ]
            df = df[columns]
            
            if status_callback:
                status_callback("Address standardization complete!")
                
                # Add component statistics
                filled_components = {
                    component: (df[component].notna() & (df[component] != '')).sum()
                    for component in self.address_components
                }
                stats_message = "\nAddress Component Statistics:"
                for component, count in filled_components.items():
                    percentage = (count / len(df)) * 100
                    stats_message += f"\n• {component}: {count} ({percentage:.1f}%)"
                status_callback(stats_message)
                
                # Add validation statistics
                if not unexpanded_addresses.empty:
                    stats_message += f"\n\n⚠️ Found {len(unexpanded_addresses)} addresses with unexpanded abbreviations"
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error in standardize_addresses: {str(e)}")
            return df

    def _expand_street_abbreviations(self, address):
        """Expand common street abbreviations."""
        street_abbrev = {
            r'\bSt\b': 'Street',
            r'\bAve\b': 'Avenue',
            r'\bRd\b': 'Road',
            r'\bBlvd\b': 'Boulevard',
            r'\bLn\b': 'Lane',
            r'\bDr\b': 'Drive',
            r'\bCt\b': 'Court',
            r'\bPl\b': 'Place',
            r'\bTer\b': 'Terrace',
            r'\bCir\b': 'Circle',
            r'\bHwy\b': 'Highway',
            r'\bPkwy\b': 'Parkway',
            r'\bSq\b': 'Square',
            r'\bFt\b': 'Fort',  # Added Fort abbreviation
            # Directionals
            r'\bN\b': 'North',
            r'\bS\b': 'South',
            r'\bE\b': 'East',
            r'\bW\b': 'West',
            r'\bNE\b': 'Northeast',
            r'\bNW\b': 'Northwest',
            r'\bSE\b': 'Southeast',
            r'\bSW\b': 'Southwest'
        }
        
        # First pass: expand standard abbreviations
        for abbr, full in street_abbrev.items():
            address = re.sub(abbr, full, address)
        
        # Second pass: catch remaining specific cases
        specific_cases = {
            r'Fort Greene Pl\b': 'Fort Greene Place',
            r'Ft Greene Pl\b': 'Fort Greene Place',
            r'Ft. Greene Pl\b': 'Fort Greene Place',
            r'Fort Greene Pl.\b': 'Fort Greene Place',
            # Add more specific cases as needed
        }
        
        for case, replacement in specific_cases.items():
            address = re.sub(case, replacement, address)
        
        return address

    def _expand_state_abbreviation(self, state):
        """Expand state abbreviations to full names."""
        state_mapping = {
            'NY': 'New York',
            'NJ': 'New Jersey',
            'CT': 'Connecticut',
            'PA': 'Pennsylvania',
            'MA': 'Massachusetts',
            'FL': 'Florida',
            'CA': 'California',
            # Add more states as needed
        }
        return state_mapping.get(state, state)

    def create_full_addresses(self, df):
        """Create full addresses with enhanced component handling."""
        try:
            # Ensure all components are strings
            for col in self.address_components:
                if col in df.columns:
                    df[col] = df[col].fillna('').astype(str).str.strip()
            
            # Combine components with smart handling
            def combine_components(row):
                components = []
                if row.get('Address'):
                    components.append(row['Address'])
                if row.get('City'):
                    components.append(row['City'])
                if row.get('State'):
                    state_zip = row['State']
                    if row.get('Zipcode'):
                        state_zip += f" {row['Zipcode']}"
                    components.append(state_zip)
                return ', '.join(filter(None, components))
            
            return df.apply(combine_components, axis=1)
            
        except Exception as e:
            self.logger.error(f"Error creating full addresses: {str(e)}")
            raise

    def filter_data(self, df, status_callback=None):
        """Enhanced data filtering with detailed statistics."""
        try:
            if status_callback:
                status_callback("Analyzing property classes...")
            
            filtered_df, stats = self.analyze_property_classes(df)
            
            if status_callback:
                self._create_filter_status_message(stats, status_callback)
            
            # Drop unnecessary columns
            filtered_df = filtered_df.drop(columns=["Block & Lot"], errors="ignore")
            
            return filtered_df, stats
            
        except Exception as e:
            self.logger.error(f"Error in filter_data: {str(e)}")
            raise

    def _create_filter_status_message(self, stats, status_callback):
        """Create detailed filter status message."""
        message = f"""
        📊 Property Class Analysis:
        Total Records: {stats['total_records']}
        Valid Records: {stats['valid_records']} ({(stats['valid_records']/stats['total_records']*100):.1f}%)
        Filtered Out: {stats['filtered_out']} ({(stats['filtered_out']/stats['total_records']*100):.1f}%)
        
        ℹ️ Note: Both 'C0' and 'CO' are treated as Commercial Condominium class
        
        ✅ Valid Property Classes:
        """
        
        for cls, info in stats['valid_classes'].items():
            description = info['description']
            count = info['count']
            percentage = info['percentage']
            
            if cls in ["C0", "CO"]:
                message += f"\n• Commercial Condominium (C0/CO): {count} ({percentage:.1f}%)"
            else:
                message += f"\n• {cls} - {description}: {count} ({percentage:.1f}%)"
        
        if stats['invalid_classes']:
            message += "\n\n❌ Filtered Out Classes:"
            for cls, count in stats['invalid_classes'].items():
                message += f"\n• {cls}: {count}"
        
        status_callback(message)

    def remove_duplicates(self, df):
        """Enhanced duplicate removal with address component consideration."""
        try:
            # Sort by sale date if available
            if 'Sale date' in df.columns:
                df['Sale date'] = pd.to_datetime(df['Sale date'], errors='coerce')
                df = df.sort_values('Sale date', ascending=False)
            
            # Remove duplicates based on standardized address
            deduped_df = df.drop_duplicates(
                subset=['Full Address'],
                keep='first'
            ).reset_index(drop=True)
            
            # Log duplicate removal statistics
            removed_count = len(df) - len(deduped_df)
            self.logger.info(f"Removed {removed_count} duplicate addresses")
            
            return deduped_df
            
        except Exception as e:
            self.logger.error(f"Error removing duplicates: {str(e)}")
            return df

    def process_file(self, file_path, status_callback=None):
        """Process file with enhanced address handling and validation."""
        try:
            if status_callback:
                status_callback("Loading data...")
            
            df = self.load_data(file_path)
            initial_count = len(df)
            
            if status_callback:
                status_callback(f"Analyzing {initial_count} records...")
            
            # Filter and standardize
            filtered_df, filter_stats = self.filter_data(df, status_callback)
            if len(filtered_df) == 0:
                status_callback("❌ No valid property classes found.")
                return None
            
            # Standardize addresses
            standardized_df = self.standardize_addresses(filtered_df, status_callback)
            
            # Remove duplicates
            deduped_df = self.remove_duplicates(standardized_df)
            
            # Add metadata
            deduped_df["Processed Date"] = datetime.now().strftime("%B %d, %Y at %I:%M %p")
            deduped_df['Property Class Description'] = deduped_df['Property class'].map(
                self.property_class_descriptions
            )
            
            # Final column ordering
            columns = [
                'Full Address', 'Address', 'City', 'State', 'Zipcode',
                'Property class', 'Property Class Description'
            ] + [
                col for col in deduped_df.columns 
                if col not in [
                    'Full Address', 'Address', 'City', 'State', 'Zipcode',
                    'Property class', 'Property Class Description'
                ]
            ]
            deduped_df = deduped_df[columns]
            
            # Final statistics
            if status_callback:
                self._create_final_status_message(
                    initial_count, 
                    len(filtered_df), 
                    len(deduped_df), 
                    deduped_df,
                    status_callback
                )
            
            return deduped_df
            
        except Exception as e:
            self.logger.error(f"Error processing file: {str(e)}")
            if status_callback:
                status_callback(f"Error: {str(e)}")
            return None

    def _create_final_status_message(self, initial_count, filtered_count, 
                                   final_count, df, status_callback):
        """Create detailed final status message."""
        message = f"""
        ✅ Processing complete!
        
        📈 Final Statistics:
        • Initial records: {initial_count}
        • Valid property classes: {filtered_count}
        • Final records after deduplication: {final_count}
        • Removed duplicates: {filtered_count - final_count}
        
        Most common property types in final dataset:
        {df['Property class'].value_counts().head(3).to_string()}
        
        Address Component Completeness:
        """
        
        for component in self.address_components:
            filled = (df[component].notna() & (df[component] != '')).sum()
            percentage = (filled / len(df)) * 100
            message += f"\n• {component}: {filled}/{len(df)} ({percentage:.1f}%)"
        
        status_callback(message)
