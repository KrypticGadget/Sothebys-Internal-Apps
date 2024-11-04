# utils/address_standardizer.py
import os
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderQuotaExceeded
from geopy.extra.rate_limiter import RateLimiter
import logging
from dotenv import load_dotenv
import time
import re
from functools import lru_cache
import concurrent.futures
import threading
import random
import json
import hashlib

class AddressStandardizer:
    """Optimized address standardizer with enhanced parsing and caching."""
    
    def __init__(self):
        load_dotenv()
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Get user agent from environment variables
        self.user_agent = os.getenv('NOMINATIM_USER_AGENT')
        if not self.user_agent:
            raise ValueError("NOMINATIM_USER_AGENT not found in environment variables")
        
        # Initialize thread-local storage for geocoders
        self._thread_local = threading.local()
        
        # Cache for standardized addresses
        self._address_cache = {}
        self._load_cache()
        
        # Rate limiting settings
        self.min_delay = 1.1
        self.max_delay = 2.0
        self.max_retries = 5
        self.error_wait = 5.0
        self.batch_size = 10
        
        # Address parsing patterns
        self.address_patterns = [
            # Pattern 1: Standard format with zip
            r'^(.*?),\s*([^,]+),\s*([A-Z]{2})\s*(\d{5})(?:-\d{4})?$',
            # Pattern 2: Format with county
            r'^(.*?),\s*([^,]+),\s*(?:[^,]+\s*County,\s*)?([A-Z]{2})\s*(\d{5})(?:-\d{4})?$',
            # Pattern 3: Basic format
            r'^(.*?),\s*([^,]+),\s*([^,]+)\s*(\d{5})(?:-\d{4})?$'
        ]
        
        self._init_rate_limiter()

    def _init_rate_limiter(self):
        """Initialize rate limiter with conservative settings."""
        self.geocode = RateLimiter(
            self.geolocator.geocode,
            min_delay_seconds=self.min_delay,
            max_retries=self.max_retries,
            error_wait_seconds=self.error_wait,
            swallow_exceptions=False
        )

    @property
    def geolocator(self):
        """Thread-safe geolocator instance with rotating user agents."""
        if not hasattr(self._thread_local, 'geolocator'):
            thread_id = threading.get_ident()
            user_agent = f"{self.user_agent}_{thread_id}_{random.randint(1000, 9999)}"
            
            self._thread_local.geolocator = Nominatim(
                user_agent=user_agent,
                timeout=10
            )
        return self._thread_local.geolocator

    def _get_cache_file(self):
        """Get cache file path."""
        cache_dir = "data/cache"
        os.makedirs(cache_dir, exist_ok=True)
        return os.path.join(cache_dir, "address_cache.json")

    def _load_cache(self):
        """Load address cache from file."""
        try:
            cache_file = self._get_cache_file()
            if os.path.exists(cache_file):
                with open(cache_file, 'r') as f:
                    self._address_cache = json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading cache: {str(e)}")
            self._address_cache = {}

    def _save_cache(self):
        """Save address cache to file."""
        try:
            cache_file = self._get_cache_file()
            with open(cache_file, 'w') as f:
                json.dump(self._address_cache, f)
        except Exception as e:
            self.logger.error(f"Error saving cache: {str(e)}")

    def _get_cache_key(self, address):
        """Generate cache key for address."""
        if not address:
            return None
        return hashlib.md5(address.lower().encode()).hexdigest()

    @lru_cache(maxsize=1000)
    def _clean_address(self, address):
        """Clean and normalize address string."""
        if not address:
            return address
        
        # Convert to string and clean
        address = str(address)
        
        # Remove extra spaces
        address = re.sub(r'\s+', ' ', address.strip())
        
        # Remove special characters except essential ones
        address = re.sub(r'[^a-zA-Z0-9\s,\.#\-]', '', address)
        
        # Standardize state abbreviations
        state_pattern = r',\s*([A-Za-z]+)\s+(\d{5})'
        address = re.sub(state_pattern, lambda m: f", {m.group(1).upper()} {m.group(2)}", address)
        
        return address

    def parse_normalized_address(self, full_address):
        """Parse address into components with multiple parsing strategies."""
        try:
            if not full_address:
                return None
            
            # Check cache first
            cache_key = self._get_cache_key(full_address)
            if cache_key in self._address_cache:
                return self._address_cache[cache_key].get('components')
            
            # Clean the address
            cleaned_address = self._clean_address(full_address)
            
            # Try regex patterns first
            components = self._try_regex_patterns(cleaned_address)
            if components:
                self._cache_result(cache_key, cleaned_address, components)
                return components
            
            # Try geocoding
            components = self._geocode_address(cleaned_address)
            if components:
                self._cache_result(cache_key, cleaned_address, components)
                return components
            
            # Final fallback to manual parsing
            components = self._manual_parse(cleaned_address)
            if components:
                self._cache_result(cache_key, cleaned_address, components)
            
            return components
            
        except Exception as e:
            self.logger.error(f"Error parsing address: {str(e)}")
            return None

    def _try_regex_patterns(self, address):
        """Try multiple regex patterns to parse address."""
        for pattern in self.address_patterns:
            match = re.match(pattern, address)
            if match:
                return {
                    'Address': match.group(1).strip(),
                    'City': match.group(2).strip(),
                    'State': match.group(3).strip(),
                    'Zipcode': match.group(4).strip()
                }
        return None

    def _geocode_address(self, address):
        """Geocode address using Nominatim with retries."""
        for attempt in range(self.max_retries):
            try:
                time.sleep(random.uniform(self.min_delay, self.max_delay))
                
                location = self.geolocator.geocode(
                    address,
                    addressdetails=True,
                    language='en',
                    exactly_one=True
                )
                
                if location and location.raw.get('address'):
                    return self._extract_components(location.raw['address'])
                    
            except (GeocoderTimedOut, GeocoderQuotaExceeded):
                if attempt < self.max_retries - 1:
                    time.sleep(self.error_wait * (attempt + 1))
                continue
                
            except Exception as e:
                self.logger.error(f"Geocoding error: {str(e)}")
                break
        
        return None

    def _extract_components(self, address_parts):
        """Extract address components from geocoded result."""
        # Extract house number and street
        house_number = address_parts.get('house_number', '')
        street = address_parts.get('road', '') or address_parts.get('street', '')
        
        if house_number and street:
            street_address = f"{house_number} {street}"
        else:
            street_address = street or house_number
        
        # Get city with multiple fallbacks
        city = (address_parts.get('city') or 
               address_parts.get('town') or 
               address_parts.get('village') or 
               address_parts.get('suburb') or
               address_parts.get('neighbourhood'))
        
        # Get state and postal code
        state = address_parts.get('state', '')
        postal_code = address_parts.get('postcode', '')
        
        # Clean and standardize components
        components = {
            'Address': street_address.strip(),
            'City': city.strip() if city else '',
            'State': state.strip() if state else '',
            'Zipcode': postal_code.strip() if postal_code else ''
        }
        
        # Validate components
        if all(components.values()):
            return components
        return None

    def _manual_parse(self, full_address):
        """Enhanced manual address parsing as final fallback."""
        try:
            parts = full_address.split(',')
            if len(parts) >= 3:
                # Handle state and zip
                last_part = parts[-1].strip()
                state_zip_match = re.search(r'([A-Z]{2})\s*(\d{5}(?:-\d{4})?)', last_part)
                
                if state_zip_match:
                    state = state_zip_match.group(1)
                    zipcode = state_zip_match.group(2)
                else:
                    state_zip = last_part.split()
                    state = state_zip[0] if len(state_zip) > 0 else ''
                    zipcode = state_zip[-1] if len(state_zip) > 1 else ''
                
                # Get city
                city = parts[-2].strip()
                
                # Combine remaining parts for street address
                address = ','.join(parts[:-2]).strip()
                
                components = {
                    'Address': address,
                    'City': city,
                    'State': state,
                    'Zipcode': zipcode
                }
                
                # Validate components
                if all(components.values()):
                    return components
            
            return None
            
        except Exception as e:
            self.logger.error(f"Manual parsing error: {str(e)}")
            return None

    def _cache_result(self, cache_key, address, components):
        """Cache address parsing result."""
        if cache_key and components:
            self._address_cache[cache_key] = {
                'full_address': address,
                'components': components
            }
            self._save_cache()

    def standardize_batch(self, addresses, max_workers=3):
        """Standardize addresses in batches with enhanced error handling."""
        results = {}
        total_addresses = len(addresses)
        
        for i in range(0, total_addresses, self.batch_size):
            batch = addresses[i:i + self.batch_size]
            
            for address in batch:
                try:
                    # Check cache first
                    cache_key = self._get_cache_key(address)
                    if cache_key in self._address_cache:
                        results[address] = self._address_cache[cache_key]
                        continue
                    
                    # Process new address
                    components = self.parse_normalized_address(address)
                    results[address] = {
                        'full_address': address,
                        'components': components
                    }
                    
                except Exception as e:
                    self.logger.error(f"Batch processing error for {address}: {str(e)}")
                    results[address] = {
                        'full_address': address,
                        'components': None
                    }
            
            # Delay between batches
            time.sleep(random.uniform(self.min_delay, self.max_delay))
        
        return results

    def standardize(self, address):
        """Standardize single address with enhanced validation."""
        if not address:
            return {'full_address': address, 'components': None}
        
        try:
            cache_key = self._get_cache_key(address)
            if cache_key in self._address_cache:
                return self._address_cache[cache_key]
            
            components = self.parse_normalized_address(address)
            result = {
                'full_address': address,
                'components': components
            }
            
            if components:
                self._cache_result(cache_key, address, components)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Standardization error for {address}: {str(e)}")
            return {
                'full_address': address,
                'components': None
            }