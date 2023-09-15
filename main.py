import streamlit as st
import requests
import polars as pl
import io
import pandas as pd
import streamlit_authenticator as stauth

pl.Config.set_tbl_hide_column_data_types(True)

#Import the YAML file into your script
import yaml
from yaml.loader import SafeLoader
with open('./config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

API_KEY = 'e31f38c36540a234e23b614a7ffb4fc4'


#Create the authenticator object
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)

name, authentication_status, username = authenticator.login('Login', 'main')

def brand_ranking (keywords,DB,your_brand_domain): 
    
    dfs_r = pl.DataFrame([])  # List to store dataframes for each keyword
    your_brand_position = None
    competitors = pl.DataFrame([])
    t = pl.DataFrame([])
    b = pl.DataFrame([])
    rank = pl.DataFrame([])
    for keyword in keywords:
        url = f"https://api.semrush.com/?type=phrase_organic&key={API_KEY}&phrase={keyword}&export_columns=Kd,Dn,Po,&database={DB}"
        response = requests.get(url)
        # Make sure the request was successful before processing
        if response.status_code == 200:
            df = pl.read_csv(io.StringIO(response.text), separator=';', eol_char='\n').with_columns(Key=pl.lit(keyword))
            dfs_r = dfs_r.vstack(df)
            
            for i in range(len(df)):
                domain = df['Domain'][i]
                position = df['Position'][i]
                Keys = df['Key'][i]
    
                if (domain in your_brand_domain) or (your_brand_domain in domain):
                    your_brand_position = position
    
                    b = b.with_columns(keyword = pl.lit(Keys),brand_domain = pl.lit(your_brand_domain),brand_ranking= pl.lit(your_brand_position))
                    rank = rank.vstack(b)
    
                else:
                    t = t.with_columns(keyword = pl.lit(Keys),brand_domain = pl.lit(domain), brand_ranking= pl.lit(position))
                    competitors = competitors.vstack(t)            
        else:
            print(f"Failed to fetch data for keyword: {keyword}. Status Code: {response.status_code}")
                
    return rank, competitors



def seo(keywords, DB):

    
    dfs = pl.DataFrame([])  # List to store dataframes for each keyword

    for keyword in keywords:
        url = f"https://api.semrush.com/?type=phrase_all&key={API_KEY}&phrase={keyword}&export_columns=Dt,Db,Ph,Nq,Cp,Co,Nr&database={DB}"
        response = requests.get(url)

        # Make sure the request was successful before processing
        if response.status_code == 200:
            df = pl.read_csv(io.StringIO(response.text), separator=';', eol_char='\n')
            df = df.with_columns(pl.col("Competition").cast(pl.Float32))
            dfs = dfs.vstack(df)
            
        else:
            print(f"Failed to fetch data for keyword: {keyword}. Status Code: {response.status_code}")

    return dfs

# Function to download the DataFrame as an Excel file
def download_excel(df):
    # Convert Polars DataFrame to Pandas DataFrame for Excel export
    df_pd = df.to_pandas()
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_pd.to_excel(writer, index=False, sheet_name='Sheet1')
    output.seek(0)
    return output.getvalue()


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

        # Add a spacer after the logo
        st.write("\n\n\n")
        st.title("SEO Dashboard")
        st.write("Enter a keyword and select a country to fetch SEO data.")

        uploaded_file = st.file_uploader("Upload an Excel file containing keywords", type=["xlsx"])
            
        # Allow user to manually enter keywords
        keywords_input = st.text_area("Or enter keywords manually (one keyword per line)")

        your_brand_domain = st.text_input("Enter your brand domain")
        
        DB = st.selectbox("Select a country:", ["us", "uk", "ca", "au", "de", "fr", "es", "it", "br", "mx", "in"])  # Add more countries as needed
        if st.button("Fetch Data"):
            
            if uploaded_file is not None:
                data = pl.read_excel(uploaded_file,read_csv_options={"has_header": False})
                keywords = data['column_1'].to_list()
                
                # Fetch and display SEO data
                dataframes = seo(keywords, DB)
                rankings, competition = brand_ranking(keywords,DB,your_brand_domain)
                st.write(dataframes)
                st.write(rankings)
                #st.write(competition)
                    
            elif keywords_input:
                keywords = keywords_input.split(',')

                # Fetch and display SEO data
                dataframes = seo(keywords, DB)
                rankings, competition = brand_ranking(keywords,DB,your_brand_domain)
                st.write(dataframes)
                st.write(rankings)
                #st.write(competition)
                
            # Download 
            st.write("\n\n\n")
            st.download_button(
                label="Download data as Excel",
                data= download_excel(dataframes),
                file_name='volume.xlsx',
                mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
elif authentication_status == False:
    st.error('Username/password is incorrect')
elif authentication_status == None:
    st.warning('Please enter your username and password')            
