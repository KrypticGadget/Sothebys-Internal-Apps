# SIR Prospect Address Validator

## TLDR - Critical Information
- **Live App URL**: [Streamlit-URL]
- **Admin Login**: 
  - Email: ****.*****@********.******
  - Password: **********
- **Key Features**: Upload Property Shark exports, standardize addresses, remove duplicates
- **Access**: Only @********.****** email domains
- **Support**: REDACTED
- **Security**: JWT authentication, role-based access, domain-restricted
- **Data**: All processed files stored in MongoDB with backup
- **Deployment**: Hosted on Vercel, MongoDB Atlas backend

## Quick Start
1. Login with Sotheby's email
2. Upload Property Shark Excel export
3. Get standardized address list
4. Download processed file
5. View processing history

A secure, enterprise-grade Streamlit application for processing and validating real estate property addresses, specifically designed for SIR.

## Features

### Core Functionality
- Upload and process Property Shark data exports
- Standardize property addresses
- Remove duplicate entries
- Track processing history
- View and download processed files

### Enterprise Features
- Secure authentication system
- Domain-restricted access (@********.******)
- User management interface
- Processing history tracking
- Role-based access control
- Professional branding


## Authentication

### Initial Admin Access
- Email: REDACTED
- Password: Contact administrator for credentials

### User Management
- Only administrators can add new users
- Domain restricted to @********.******
- Secure password policies enforced
- User activity tracking

## Usage

1. Login with Sotheby's credentials
2. Navigate using the sidebar menu:
   - Process New Data
   - View History
   - User Management (Admin only)

### Processing Data
1. Upload Property Shark Excel export
2. System will automatically:
   - Standardize addresses
   - Remove duplicates
   - Generate downloadable processed file

### Viewing History
- Access all previously processed files
- Download original or processed files
- View processing statistics

### User Management (Admin)
- Add new users
- View user list
- Manage user access
- Track user activity

## Development

### Technology Stack
- Python 3.9+
- Streamlit for web interface
- Pandas for data processing
- MongoDB for data storage
- JWT for authentication
- bcrypt for password hashing

### Styling
- Custom Sotheby's branding
- Professional UI/UX
- Responsive design
- Consistent color scheme (#002A5C)

## Security Features
- Domain-restricted access
- Secure password handling
- JWT token authentication
- Session management
- Role-based access control
- Secure file handling

## Deployment

The application is deployed on Vercel and can be accessed at:
[Your-Streamlit-URL]

### Production Configuration
1. Set up MongoDB Atlas cluster
2. Configure Vercel environment variables
3. Add Vercel's IP addresses to MongoDB network access

## Support

For support, please contact:
- Technical Issues: [Your Technical Contact]
- User Access: REDACTED

## License

Internal use only - SIR

## Setting up Nominatim User Agent

To use the Nominatim service for address normalization, you need to set up a user agent. This can be done by adding the following line to your environment variables:

```bash
NOMINATIM_USER_AGENT=your_unique_user_agent
```

Replace `your_unique_user_agent` with a unique identifier for your application. This is required by Nominatim to identify your requests.
