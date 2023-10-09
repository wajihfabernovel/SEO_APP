from st_pages import Page, add_page_title, show_pages
import streamlit as st
import requests
import polars as pl
import io
import pandas as pd
import streamlit_authenticator as stauth
from google.ads.googleads.client import GoogleAdsClient


pl.Config.set_tbl_hide_column_data_types(True)

#Import the YAML file into your script
import yaml
from yaml.loader import SafeLoader
with open('./config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)
    
    
   #Create the authenticator object
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
) 
    
name, authentication_status, username = authenticator.login('Login', 'main')    

    
# Streamlit UI
if authentication_status:
    authenticator.logout('Logout', 'main')
    if __name__ == "__main__":    
        st.markdown("""
        <style>
        .logo {
            max-width: 10%;
            position: absolute;
            top: 15px;
            left: 15px;
            z-index: 999;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Display the logo
        st.image("./logo.png", use_column_width=True)  # Using OpenAI's favicon as an example logo
        
        show_pages(
            [
                Page("./main.py", "Home", "🏠"),
                Page("./combined.py", "SEO Tool", "📈")
                # The pages appear in the order you pass them
                ]
        )
        "## Welcome to EY Fabernovel SEO Dashboard !"
        st.balloons()
elif authentication_status == False:
    st.error('Username/password is incorrect')
elif authentication_status == None:
    st.warning('Please enter your username and password')    
        # Optional method to add title and icon to current page
