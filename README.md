# FindMyMail Callback Server

This project is a simple web server built using `aiohttp` to handle POST requests containing contact information, extract relevant details, and save them to a CSV file and Google BigQuery.

## File Definitions

### `findmymail_callback_server.py`
This is the main server file that sets up an `aiohttp` web server to handle POST requests. It extracts contact information from the requests and saves the data to a CSV file and Google BigQuery.

### `findmail.py`
This file contains the logic for extracting email addresses and other contact information from the BigQuery. It may include functions for parsing and validating email addresses.

### `website_status_code.py`
This file includes functionality to check the status codes of websites. Send HTTP requests to websites and interpret the responses, which used for monitoring website availability and health.

### `requirements.txt`
This file lists all the Python dependencies required to run the server. It ensures that all necessary packages are installed in the virtual environment.
