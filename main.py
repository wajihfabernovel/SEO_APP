import streamlit as st
import requests
import polars as pl
import io
import pandas as pd
import streamlit_authenticator as stauth
from google.ads.googleads.client import GoogleAdsClient

pl.Config.set_tbl_hide_column_data_types(True)

# Import the YAML file into your script
import yaml
from yaml.loader import SafeLoader
with open('./config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

# Create the authenticator object
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
        st.image("./logo.png", use_column_width=True)

        # Sidebar for navigation
        page = st.sidebar.radio(
            "Navigation",
            ["Home", "SEO Tool", "Prediction Tool", "Audit Tool 1", "Audit Tool 2"]
        )

        # Page logic based on selected page
        if page == "Home":
            st.title("Home")
            st.balloons()
            st.write("## Welcome to EY Fabernovel SEO Dashboard!")
        
        elif page == "SEO Tool":
            st.title("SEO Tool")
            # You can run the content of combined.py here
            exec(open("./combined.py").read())
        
        elif page == "Prediction Tool":
            st.title("Prediction Tool")
            # You can run the content of projection_SEO.py here
            exec(open("./projection_SEO.py").read())
        
        elif page == "Audit Tool 1":
            st.title("Audit Tool 1")
            # You can run the content of lighthouse_2.py here
            exec(open("./lighthouse_2.py").read())
        
        elif page == "Audit Tool 2":
            st.title("Audit Tool 2")
            # You can run the content of lighthouse.py here
            exec(open("./lighthouse.py").read())

elif authentication_status == False:
    st.error('Username/password is incorrect')

elif authentication_status == None:
    st.warning('Please enter your username and password')
