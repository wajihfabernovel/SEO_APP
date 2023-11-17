import streamlit as st
import subprocess
import os
from datetime import datetime

# Function to run Lighthouse and save the report
def run_lighthouse(preset, urls):
    getdate = datetime.now().strftime("%Y%m%d")
    relative_path = 'lighthouse_reports/'  # Ensure this directory exists or change to a desired path

    # Create the directory if it does not exist
    if not os.path.exists(relative_path):
        os.makedirs(relative_path)

    for url in urls:
        # Sanitize the URL to create a valid filename
        sanitized_url = url.replace("://", "_").replace("/", "_").replace("?", "_").replace("&", "_").replace(".", "_")
        json_filename = f"{sanitized_url}_{getdate}.json"
        json_filepath = os.path.join(relative_path, json_filename)

        # Command to run Lighthouse
        cmd = [
            "lighthouse",
            url,
            f"--preset={preset}",
            "--output=json",
            f"--output-path={json_filepath}",
            '--chrome-flags="--headless --no-sandbox --disable-gpu"'
        ]
        

        # Run the command
        subprocess.run(cmd)

        
        list_ = []
        list_.append(json_filepath)
    

# Streamlit app layout
st.title('Lighthouse Report Generator')

# Input fields for the preset and URLs
preset = st.selectbox('Choose your preset', ['desktop', 'mobile'])
urls_input = st.text_area('Enter the URLs (one per line)', height=100)
generate_reports = st.button('Generate Reports')

# When the button is clicked
if generate_reports and urls_input:
    urls = urls_input.split('\n')
    # Remove any empty strings in case of blank lines
    urls = [url.strip() for url in urls if url.strip()]
    run_lighthouse(preset, urls)


import shutil

# Function to zip all JSON files in the specified directory
def zip_json_files(directory, zip_name):
    shutil.make_archive(zip_name, 'zip', directory)

# Streamlit code to add a button and handle the download
# This code should be placed after the loop that processes all URLs
zip_json_files('lighthouse_reports', 'lighthouse_reports_zip')
if st.button('Download All JSON Reports'):
    with open('lighthouse_reports_zip.zip', 'rb') as file:
        st.download_button(label='Download ZIP',
                           data=file,
                           file_name='lighthouse_reports.zip',
                           mime='application/zip')
