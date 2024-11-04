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
    """Optimized address standardizer with caching and rate limiting."""
    
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
        self.min_delay = 1.1  # Minimum delay between requests
        self.max_delay = 2.0  # Maximum delay for randomization
        self.max_retries = 5  # Maximum number of retries
        self.error_wait = 5.0  # Wait time after error
        self.batch_size = 10   # Process in smaller batches
        
        # Initialize rate limiter with more conservative settings
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
            # Create unique user agent for this thread
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
        """Clean address string."""
        if not address:
            return address
        
        address = str(address)
        address = re.sub(r'\s+', ' ', address.strip())
        address = re.sub(r'[^a-zA-Z0-9\s,\.#-]', '', address)
        
        return address

    def parse_normalized_address(self, full_address):
        """Parse address into components with fallback."""
        try:
            if not full_address:
                return None
            
            # Check cache first
            cache_key = self._get_cache_key(full_address)
            if cache_key in self._address_cache:
                return self._address_cache[cache_key].get('components')
            
            # Add random delay to help with rate limiting
            time.sleep(random.uniform(self.min_delay, self.max_delay))
            
            # Try geocoding with retries
            for attempt in range(self.max_retries):
                try:
                    location = self.geolocator.geocode(
                        full_address,
                        addressdetails=True,
                        language='en',
                        exactly_one=True
                    )
                    
                    if location and location.raw.get('address'):
                        components = self._extract_components(location.raw['address'])
                        
                        # Cache the result
                        self._address_cache[cache_key] = {
                            'full_address': location.address,
                            'components': components
                        }
                        self._save_cache()
                        
                        return components
                    
                except (GeocoderTimedOut, GeocoderQuotaExceeded):
                    if attempt < self.max_retries - 1:
                        time.sleep(self.error_wait * (attempt + 1))
                    continue
                    
                except Exception as e:
                    self.logger.error(f"Error in geocoding: {str(e)}")
                    break
            
            # Fallback: Parse address manually
            components = self._manual_parse(full_address)
            if components:
                self._address_cache[cache_key] = {
                    'full_address': full_address,
                    'components': components
                }
                self._save_cache()
            
            return components
            
        except Exception as e:
            self.logger.error(f"Error parsing address: {str(e)}")
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
        
        # Get city (with fallbacks)
        city = (address_parts.get('city') or 
               address_parts.get('town') or 
               address_parts.get('village') or 
               address_parts.get('suburb') or
               address_parts.get('neighbourhood'))
        
        # Get state and postal code
        state = address_parts.get('state', '')
        postal_code = address_parts.get('postcode', '')
        
        return {
            'Address': street_address.strip(),
            'City': city.strip() if city else '',
            'State': state.strip() if state else '',
            'Zipcode': postal_code.strip() if postal_code else ''
        }

    def _manual_parse(self, full_address):
        """Manual address parsing as fallback."""
        try:
            parts = full_address.split(',')
            if len(parts) >= 3:
                # Get state and zip from last part
                state_zip = parts[-1].strip().split()
                state = state_zip[0] if len(state_zip) > 0 else ''
                zipcode = state_zip[-1] if len(state_zip) > 1 else ''
                
                # Get city from second to last part
                city = parts[-2].strip()
                
                # Get street address from remaining parts
                address = ','.join(parts[:-2]).strip()
                
                return {
                    'Address': address,
                    'City': city,
                    'State': state,
                    'Zipcode': zipcode
                }
            
            return None
            
        except Exception:
            return None

    def standardize_batch(self, addresses, max_workers=3):
        """
        Standardize addresses in batches with rate limiting.
        """
        results = {}
        total_addresses = len(addresses)
        
        # Process in smaller batches
        for i in range(0, total_addresses, self.batch_size):
            batch = addresses[i:i + self.batch_size]
            
            for address in batch:
                try:
                    # Check cache first
                    cache_key = self._get_cache_key(address)
                    if cache_key in self._address_cache:
                        cached_result = self._address_cache[cache_key]
                        results[address] = cached_result
                        continue
                    
                    # Add random delay
                    time.sleep(random.uniform(self.min_delay, self.max_delay))
                    
                    components = self.parse_normalized_address(address)
                    if components:
                        results[address] = {
                            'full_address': address,
                            'components': components
                        }
                    else:
                        results[address] = {
                            'full_address': address,
                            'components': None
                        }
                        
                except Exception as e:
                    self.logger.error(f"Error processing address {address}: {str(e)}")
                    results[address] = {
                        'full_address': address,
                        'components': None
                    }
            
            # Small delay between batches
            time.sleep(random.uniform(self.min_delay, self.max_delay))
        
        return results

    def standardize(self, address):
        """
        Standardize single address with caching.
        """
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
            
            self._address_cache[cache_key] = result
            self._save_cache()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error standardizing address {address}: {str(e)}")
            return {
                'full_address': address,
                'components': None
            }