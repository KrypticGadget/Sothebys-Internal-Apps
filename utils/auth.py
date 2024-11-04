# utils/auth.py
import os
from dotenv import load_dotenv
import bcrypt
import jwt
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AuthHandler:
    def __init__(self):
        load_dotenv()
        self.secret_key = os.getenv('JWT_SECRET_KEY')
        if not self.secret_key:
            raise ValueError("JWT_SECRET_KEY not found in environment variables")
            
        # Initialize with default admin user (case insensitive key)
        admin_email = 'Matt.Sadik@sothebys.realty'
        admin_password = "Chance72$$"
        salt = bcrypt.gensalt()
        hashed_password = bcrypt.hashpw(admin_password.encode('utf-8'), salt)
        
        self.users = {
            admin_email: {
                'password': hashed_password,
                'name': 'Matt Sadik',
                'role': 'admin',
                'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'created_by': 'system'
            }
        }
        logger.info("AuthHandler initialized with admin user")

    def verify_email_domain(self, email):
        """Verify email belongs to Sotheby's domain."""
        email = email.lower()
        valid = email.endswith(('sothebys.realty', 'sothebysrealty.com'))
        logger.info(f"Email domain verification for {email}: {'valid' if valid else 'invalid'}")
        return valid

    def add_user(self, email, password, name, added_by):
        """Add a new user to the system."""
        try:
            if not self.verify_email_domain(email):
                raise ValueError("Only Sotheby's email addresses are allowed")
            
            # Case-insensitive check for existing user
            if any(existing.lower() == email.lower() for existing in self.users.keys()):
                raise ValueError("User already exists")
            
            # Hash password
            salt = bcrypt.gensalt()
            hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
            
            self.users[email] = {
                'password': hashed,
                'name': name,
                'role': 'user',
                'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'created_by': added_by
            }
            logger.info(f"Successfully added new user: {email}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding user {email}: {str(e)}")
            raise

    def is_admin(self, email):
        """Check if user is an admin."""
        # Case-insensitive check for admin role
        matching_email = next(
            (stored_email for stored_email in self.users.keys() 
             if stored_email.lower() == email.lower()),
            None
        )
        is_admin = matching_email and self.users[matching_email].get('role') == 'admin'
        logger.info(f"Admin check for {email}: {is_admin}")
        return is_admin

    def get_all_users(self):
        """Get list of all users (excluding passwords)."""
        try:
            users = {
                email: {
                    'name': info['name'],
                    'role': info.get('role', 'user'),
                    'created_at': info.get('created_at', 'N/A'),
                    'created_by': info.get('created_by', 'N/A')
                }
                for email, info in self.users.items()
            }
            logger.info(f"Retrieved {len(users)} users")
            return users
        except Exception as e:
            logger.error(f"Error getting users: {str(e)}")
            return {}

    def login(self, email, password):
        """Authenticate user and return JWT token."""
        try:
            # Case-insensitive email check
            matching_email = next(
                (stored_email for stored_email in self.users.keys() 
                 if stored_email.lower() == email.lower()),
                None
            )
            
            if not matching_email:
                logger.warning(f"Login failed: User {email} not found")
                return None
            
            stored_password = self.users[matching_email]['password']
            if isinstance(stored_password, str):
                stored_password = stored_password.encode('utf-8')
            
            if bcrypt.checkpw(password.encode('utf-8'), stored_password):
                token = jwt.encode({
                    'email': matching_email,  # Use the correctly-cased email
                    'name': self.users[matching_email]['name'],
                    'role': self.users[matching_email].get('role', 'user'),
                    'exp': datetime.utcnow() + timedelta(days=1)
                }, self.secret_key, algorithm='HS256')
                
                logger.info(f"Login successful for user: {matching_email}")
                return token
            
            logger.warning(f"Login failed: Invalid password for {email}")
            return None
            
        except Exception as e:
            logger.error(f"Login error for {email}: {str(e)}")
            return None

    def verify_token(self, token):
        """Verify JWT token and return user info."""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=['HS256'])
            # Case-insensitive email check
            matching_email = next(
                (stored_email for stored_email in self.users.keys() 
                 if stored_email.lower() == payload['email'].lower()),
                None
            )
            
            if matching_email:
                logger.info(f"Token verified for user: {matching_email}")
                return {
                    'email': matching_email,
                    'name': self.users[matching_email]['name'],
                    'role': self.users[matching_email].get('role', 'user')
                }
            
            logger.warning("Token verification failed: User not found")
            return None
            
        except jwt.ExpiredSignatureError:
            logger.warning("Token verification failed: Token expired")
            return None
        except jwt.InvalidTokenError:
            logger.warning("Token verification failed: Invalid token")
            return None
        except Exception as e:
            logger.error(f"Token verification error: {str(e)}")
            return None

    def delete_user(self, email, admin_email):
        """Delete a user (admin only)."""
        try:
            if not self.is_admin(admin_email):
                raise ValueError("Only administrators can delete users")
                
            # Case-insensitive checks
            admin_match = next(
                (stored_email for stored_email in self.users.keys() 
                 if stored_email.lower() == admin_email.lower()),
                None
            )
            user_match = next(
                (stored_email for stored_email in self.users.keys() 
                 if stored_email.lower() == email.lower()),
                None
            )
            
            if email.lower() == admin_email.lower():
                raise ValueError("Cannot delete your own admin account")
            if not user_match:
                raise ValueError("User not found")
            
            del self.users[user_match]
            logger.info(f"User {email} successfully deleted by admin {admin_email}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting user {email}: {str(e)}")
            raise