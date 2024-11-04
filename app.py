# app.py
import streamlit as st
import pandas as pd
import os
from datetime import datetime
from utils.data_processor import DataProcessor
from utils.auth import AuthHandler
import base64

def load_css(css_file):
    """Load CSS file"""
    try:
        with open(css_file) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Error loading CSS file: {str(e)}")

# Initialize handlers
auth_handler = AuthHandler()

# Configure page
st.set_page_config(
    page_title="Sotheby's Address Validator",
    page_icon="./assets/sothebys-favicon.ico",
    layout="wide"
)

# Load CSS
load_css('static/styles.css')

# Helper functions
def safe_read_excel(file):
    """Safely read Excel file, handling empty files and other errors."""
    try:
        return pd.read_excel(file)
    except (pd.errors.EmptyDataError, ValueError):
        st.error("The uploaded file appears to be empty or invalid.")
        return None
    except Exception as e:
        st.error(f"Error reading file: {str(e)}")
        return None

def get_readable_timestamp():
    """Get current timestamp in readable format."""
    return datetime.now().strftime("%B %d, %Y at %I:%M %p")

def get_file_timestamp():
    """Get timestamp for file naming."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def load_image(image_path):
    try:
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except Exception:
        return None

def display_logo(context='main'):
    """Display logo with context-specific styling"""
    logo_data = load_image('./assets/sothebys-logo.png')
    if logo_data:
        container_class = 'logo-container-login' if context == 'login' else 'logo-container-main'
        st.markdown(f"""
            <div class="{container_class}">
                <img src="data:image/png;base64,{logo_data}" />
            </div>
        """, unsafe_allow_html=True)

# Configure valid property classes
VALID_PROPERTY_CLASSES = ["CD", "B9", "B2", "B3", "CO", "C0" "B1", "C1", "A9", "C2"]
data_processor = DataProcessor(VALID_PROPERTY_CLASSES)

# Authentication check
if 'user_token' not in st.session_state:
    st.session_state.user_token = None

# Login Section Update
if not st.session_state.user_token:
    display_logo('login')
    st.markdown('<h1 class="standard-text">Login</h1>', unsafe_allow_html=True)
    
    # Center the login form
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        with st.form("login_form"):
            st.markdown("""
                <div class="login-container">
                    <h3 style='text-align: center; color: #002A5C; margin-bottom: 20px; font-weight: 500;'>
                        Use your @sothebys.realty email to login
                    </h3>
            """, unsafe_allow_html=True)
            
            email = st.text_input("Email", placeholder="example@sothebys.realty")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Login")
            
            if submitted:
                if not email or not password:
                    st.error("Please enter both email and password")
                else:
                    if auth_handler.verify_email_domain(email):
                        st.info(f"Attempting login with email: {email}")
                        token = auth_handler.login(email, password)
                        if token:
                            st.session_state.user_token = token
                            st.rerun()
                        else:
                            st.error(f"Invalid credentials for {email}")
                    else:
                        st.error("Please use your Sotheby's International Realty email (@sothebys.realty)")
            st.markdown("</div>", unsafe_allow_html=True)
    st.stop()

# Main application (only shown to authenticated users)
user = auth_handler.verify_token(st.session_state.user_token)
if not user:
    st.session_state.user_token = None
    st.rerun()

display_logo('main')

# Sidebar with user info and logout
with st.sidebar:
    st.markdown(f'<div class="user-welcome">Welcome, {user["name"]}</div>', unsafe_allow_html=True)
    
    if st.button("Logout"):
        st.session_state.user_token = None
        st.rerun()

# Navigation
if user.get('role') == 'admin':
    page = st.sidebar.radio("", ['Process New Data', 'View History', 'User Management'])
else:
    page = st.sidebar.radio("", ['Process New Data', 'View History'])

if page == 'Process New Data':
   
    # First, show the title and subtitle
    st.markdown('<h1 class="standard-text">Property Address Validator</h1>', unsafe_allow_html=True)
    st.markdown('<h2 class="standard-text">Process Property Data</h2>', unsafe_allow_html=True)

    # Add the info box with properly escaped HTML
    st.markdown("""
        This tool helps you standardize and validate property addresses from your Property Shark export.

        **Step 1: Tax Class Filtering**

        The tool will first filter for only these specific tax classes:
        - CD - Residential Condominium
        - B9 - Mixed Residential & Commercial Buildings
        - B2 - Office Buildings
        - B3 - Industrial & Manufacturing
        - C0/CO - Commercial Condominium (both formats accepted)
        - B1 - Hotels & Apartments
        - C1 - Walk-up Apartments
        - A9 - Luxury Residential
        - C2 - Elevator Apartments

        *Note:* Any records with tax classes not listed above will be automatically filtered out.

        **Step 2: Address Processing**

        For all properties with valid tax classes, the tool will:
        - Professionally format and standardize addresses
        - Verify each address against official records
        - Split addresses into proper components (street, city, state, zip)
        - Remove any duplicate entries

        **Final Output**

        You'll receive:
        - A clean, verified list of properties with valid tax classes
        - Standardized and verified addresses
        - Detailed statistics about the filtering and verification process
        - A downloadable file with all processed data
    """, unsafe_allow_html=True)




    
    uploaded_file = st.file_uploader("Upload Property Shark Data Export (Excel)", type="xlsx", key="property")
    
    
    if uploaded_file:
        # Create a progress message container
        progress_message = st.empty()
        progress_message.info("Starting to process your file... 🚀")
        
        input_data = safe_read_excel(uploaded_file)
        if input_data is None or input_data.empty:
            st.error("Unable to process the uploaded file. Please ensure it contains valid data. 📋")
        else:
            try:
                file_timestamp = get_file_timestamp()
                readable_timestamp = get_readable_timestamp()
                original_filename = uploaded_file.name
                
                base_name = os.path.splitext(original_filename)[0]
                processed_filename = f"{base_name}_processed_{file_timestamp}.csv"
                
                # Create a status container to show real-time updates
                status_container = st.empty()
                
                # Define the callback function
                def status_callback(message):
                    status_container.markdown(f"""
                        <div style="color: #FFFFFF;">
                            {message}
                        </div>
                    """, unsafe_allow_html=True)
                
                # Process the data with status updates
                processed_data = data_processor.process_file(uploaded_file, status_callback)
                
                if processed_data is None or processed_data.empty:
                    progress_message.error("No valid data was found after processing. Please check your input file. 🚫")
                else:
                    # Calculate statistics
                    processed_records = len(processed_data)
                    total_records = len(input_data)
                    removed_records = total_records - processed_records
                    
                    # Update final status
                    status_container.markdown(f"""
                        <div style="color: #FFFFFF;">
                            ✅ Processing complete!<br>
                            📊 Original records: {total_records}<br>
                            🎯 Valid addresses: {processed_records}<br>
                            🗑️ Removed duplicates/invalid: {removed_records}<br>
                        </div>
                    """, unsafe_allow_html=True)
                    
# Replace the section after saving the processed data and before the download button
                    # Save the processed data
                    os.makedirs("data/outputs", exist_ok=True)
                    output_path = os.path.join("data/outputs", processed_filename)
                    processed_data.to_csv(output_path, index=False)
                    
                    # Clear the progress message
                    progress_message.success("✨ Processing complete! Your addresses have been verified and standardized.")
                    
                    # Show the results
                    st.markdown('<div class="standard-text">✅ Verified Address List</div>', unsafe_allow_html=True)
                    st.markdown('<div class="standard-text-dark">', unsafe_allow_html=True)
                    
                    # Add filter options
                    col1, col2 = st.columns(2)
                    with col1:
                        search_term = st.text_input("🔍 Search addresses", "")
                    with col2:
                        property_class = st.selectbox(
                            "📋 Filter by Property Class",
                            options=["All"] + list(data_processor.property_class_descriptions.keys()),
                            format_func=lambda x: f"{x} - {data_processor.property_class_descriptions.get(x, '')}" if x != "All" else "All Classes"
                        )
                    
                    # Filter the dataframe based on search term and property class
                    filtered_df = processed_data.copy()
                    if search_term:
                        filtered_df = filtered_df[filtered_df["Full Address"].str.contains(search_term, case=False, na=False)]
                    if property_class != "All":
                        filtered_df = filtered_df[filtered_df["Property class"] == property_class]
                    
                    # Show property class distribution
                    st.markdown("""
                        <div style="background-color: rgba(255, 255, 255, 0.1); padding: 15px; border-radius: 5px; margin: 10px 0;">
                            <h4 style="color: #FFFFFF; margin-bottom: 10px;">Property Class Distribution</h4>
                    """, unsafe_allow_html=True)
                    
                    # Create property class summary
                    class_summary = processed_data["Property class"].value_counts()
                    total_records = len(processed_data)
                    
                    for cls, count in class_summary.items():
                        percentage = (count / total_records) * 100
                        description = data_processor.property_class_descriptions.get(cls, "Unknown")
                        st.markdown(f"""
                            <div style="color: #FFFFFF; margin-bottom: 5px;">
                                {cls} - {description}: {count} records ({percentage:.1f}%)
                            </div>
                        """, unsafe_allow_html=True)
                    
                    st.markdown("</div>", unsafe_allow_html=True)
                    
                    # Display the dataframe with a note about sorting
                    st.markdown("""
                        <div style="color: #FFFFFF; font-size: 0.8em; margin-bottom: 10px;">
                            💡 Tip: Click on any column header to sort the data
                        </div>
                    """, unsafe_allow_html=True)
                    


                    # Show the dataframe with all relevant columns
                    display_columns = [
                        "Full Address",
                        "Address",
                        "City",
                        "State",
                        "Zipcode",
                        "Property class",
                        "Property Class Description",
                        "Processed Date"
                    ]

                    st.dataframe(
                        filtered_df[display_columns],
                        height=400
                    )

                    st.markdown('</div>', unsafe_allow_html=True)
                    
                    # Add download button with clear instructions
                    st.markdown("""
                        <div style="background-color: rgba(255, 255, 255, 0.1); padding: 15px; border-radius: 5px; margin-top: 20px;">
                            <p style="color: #FFFFFF; margin-bottom: 10px;">
                                ⬇️ Your processed file is ready for download! 
                                Click the button below to save it to your computer.
                            </p>
                        </div>
                    """, unsafe_allow_html=True)
                    
                    st.download_button(
                        label="📥 Download Processed Address List",
                        data=processed_data.to_csv(index=False),
                        file_name=processed_filename,
                        mime="text/csv",
                        help="Click to download your processed and verified address list"
                    )
                    
            except Exception as e:
                progress_message.error(f"Oops! Something went wrong while processing your file. Please try again or contact support if the problem persists. ❌")
                st.exception(e)

elif page == 'View History':
    st.markdown('<h1 class="standard-text">Processing History</h1>', unsafe_allow_html=True)
    
    output_dir = "data/outputs"
    if os.path.exists(output_dir):
        processed_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
        
        if not processed_files:
            st.info("No processing history available yet.")
        else:
            processed_files.sort(key=lambda x: os.path.getmtime(os.path.join(output_dir, x)), reverse=True)
            
            st.markdown('<h2 class="standard-text">Processed Files</h2>', unsafe_allow_html=True)
            for filename in processed_files:
                file_path = os.path.join(output_dir, filename)
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                
                with st.expander(f"{filename} (Processed on {mod_time.strftime('%B %d, %Y at %I:%M %p')})"):
                    try:
                        df = pd.read_csv(file_path)
                        st.markdown('<div class="standard-text-dark">', unsafe_allow_html=True)
                        st.dataframe(df)
                        st.markdown('</div>', unsafe_allow_html=True)
                        
                        with open(file_path, 'rb') as f:
                            st.download_button(
                                label=f"Download {filename}",
                                data=f,
                                file_name=filename,
                                mime="text/csv"
                            )
                    except Exception as e:
                        st.error(f"Error loading file: {str(e)}")
    else:
        st.info("No processing history available yet.")

elif page == 'User Management' and user.get('role') == 'admin':
    st.markdown('<h1 class="standard-text">User Management</h1>', unsafe_allow_html=True)
    
    with st.form("add_user_form", clear_on_submit=True):
        st.markdown('<h2 class="standard-text">Add New User</h2>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        with col1:
            new_email = st.text_input("Email (@sothebysrealty.com)")
            new_name = st.text_input("Full Name")
        with col2:
            new_password = st.text_input("Password", type="password")
            confirm_password = st.text_input("Confirm Password", type="password")
        
        submitted = st.form_submit_button("Add User")
        
        if submitted:
            if new_password != confirm_password:
                st.error("Passwords do not match!")
            elif not new_email or not new_name or not new_password:
                st.error("All fields are required!")
            else:
                try:
                    auth_handler.add_user(new_email, new_password, new_name, user['email'])
                    st.success(f"Successfully added user: {new_email}")
                except ValueError as e:
                    st.error(str(e))
                except Exception as e:
                    st.error(f"An error occurred: {str(e)}")
    
    # Display current users
    st.markdown('<h2 class="standard-text">Current Users</h2>', unsafe_allow_html=True)
    users = auth_handler.get_all_users()
    
    if users:
        # Prepare user data for display
        user_data = []
        for email, info in users.items():
            user_data.append({
                'Email': email,
                'Name': info['name'],
                'Role': info['role'].capitalize(),
                'Created On': info.get('created_at', 'N/A'),
                'Created By': info.get('created_by', 'N/A')
            })
        
        # Display user table
        st.markdown('<div class="standard-text-dark">', unsafe_allow_html=True)
        user_df = pd.DataFrame(user_data)
        st.dataframe(user_df, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Delete user section
        st.markdown('<h2 class="standard-text">Delete User</h2>', unsafe_allow_html=True)
        
        # Create selectbox for user deletion
        delete_email = st.selectbox(
            "Select user to delete",
            options=[email for email in users.keys() if email != user['email']],
            format_func=lambda x: f"{x} ({users[x]['name']})"
        )
        
        # Delete user button and functionality
        if st.button("Delete Selected User", type="secondary"):
            if delete_email:
                try:
                    auth_handler.delete_user(delete_email, user['email'])
                    st.success(f"Successfully deleted user: {delete_email}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error deleting user: {str(e)}")
    else:
        st.info("No users found in the system.")