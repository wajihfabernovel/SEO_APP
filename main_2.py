import streamlit as st
import requests
import polars as pl
import io
import pandas as pd
import streamlit_authenticator as stauth
from google.ads.googleads.client import GoogleAdsClient
import xlsxwriter
from streamlit_extras.dataframe_explorer import dataframe_explorer
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)

pl.Config.set_tbl_hide_column_data_types(True)



API_KEY = 'e31f38c36540a234e23b614a7ffb4fc4'

creds = {
    'developer_token' : "q2Om6GmAhWjE2z_p8Da_Fw",
    'client_id' : "899223584116-m7n92thr3co9gr0otu7g64o85r6i46ko.apps.googleusercontent.com",
    'client_secret' : "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",
    'refresh_token' : "1//03aBH-xmgK1j6CgYIARAAGAMSNwF-L9IrHTMIAOSWIcjj147tjSBl5Z83teClPGIF2S-wcGCrCtO83BlRy5VaT4PoPA06_EVx5hQ",
    'use_proto_plus' : "False"} 

@st.cache_data
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
                for j in range (len(your_brand_domain)):
                    if (domain in your_brand_domain[j]) or (your_brand_domain[j] in domain):
                        your_brand_position = position
    
                        b = b.with_columns(keyword = pl.lit(Keys),brand_domain = pl.lit(your_brand_domain[j]),brand_ranking= pl.lit(your_brand_position))
                        rank = rank.vstack(b)
    
                    else:
                        t = t.with_columns(keyword = pl.lit(Keys),brand_domain = pl.lit(domain), brand_ranking= pl.lit(position))
                        competitors = competitors.vstack(t)            
        else:
            print(f"Failed to fetch data for keyword: {keyword}. Status Code: {response.status_code}")
                
    return rank, competitors.unique(maintain_order=True)


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

def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a UI on top of a dataframe to let viewers filter columns

    Args:
        df (pd.DataFrame): Original dataframe

    Returns:
        pd.DataFrame: Filtered dataframe
    """
    # modify = st.checkbox("Add filters")

    # if not modify:
    #     return df

    df = df.copy()

    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

        if is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    modification_container = st.container()

    with modification_container:
        to_filter_columns = st.multiselect("Filter dataframe on", df.columns)
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            # Treat columns with < 10 unique values as categorical
            if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                user_cat_input = right.multiselect(
                    f"Values for {column}",
                    df[column].unique(),
                    default=list(df[column].unique()),
                )
                df = df[df[column].isin(user_cat_input)]
            elif is_numeric_dtype(df[column]):
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100
                user_num_input = right.slider(
                    f"Values for {column}",
                    min_value=_min,
                    max_value=_max,
                    value=(_min, _max),
                    step=step,
                )
                df = df[df[column].between(*user_num_input)]
            elif is_datetime64_any_dtype(df[column]):
                user_date_input = right.date_input(
                    f"Values for {column}",
                    value=(
                        df[column].min(),
                        df[column].max(),
                    ),
                )
                if len(user_date_input) == 2:
                    user_date_input = tuple(map(pd.to_datetime, user_date_input))
                    start_date, end_date = user_date_input
                    df = df.loc[df[column].between(start_date, end_date)]
            else:
                user_text_input = right.text_input(
                    f"Substring or regex in {column}",
                )
                if user_text_input:
                    df = df[df[column].astype(str).str.contains(user_text_input)]
    return df
# Function to download the DataFrame as an Excel file

def to_excel(dfs, sheet_names):
    """
    Convert multiple dataframes to one Excel file with multiple sheets
    """
    output = io.BytesIO()
    with xlsxwriter.Workbook(output) as writer:
        for df, sheet_name in zip(dfs, sheet_names):
            df.write_excel(writer, worksheet=sheet_name,has_header=True,autofit=True)
    output.seek(0)
    return output



# Streamlit UI

if __name__ == "__main__":
    
    if "load_state" not in st.session_state:
        st.session_state.load_state = False
        
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
    st.title("SemRush")
    st.write("Enter a keyword and select a country to fetch SEO data.")

    if 'user_input' not in st.session_state or st.session_state.user_input is None:
        uploaded_file = st.file_uploader('Upload a file', type=['xlsx'])
        if uploaded_file is not None:
            st.session_state.user_input = uploaded_file
    else:
        uploaded_file = st.session_state.user_input
        
    # Allow user to manually enter keywords
    keywords_input = st.text_area("Or enter keywords manually (seperated by a , )")
    
    your_brand_domain = st.text_input("Enter your brand domain")
    
    DB = st.selectbox("Select a country:", ["AF",
"AL",
"DZ",
"AO",
"AR",
"AM",
"AU",
"AT",
"AZ",
"BS",
"BH",
"BD",
"BY",
"BE",
"BZ",
"BO",
"BA",
"BW",
"BR",
"BN",
"BG",
"CV",
"KH",
"CM",
"CA",
"CL",
"CO",
"CG",
"CR",
"HR",
"CY",
"CZ",
"DK",
"DO",
"EC",
"EG",
"SV",
"EE",
"ET",
"FI",
"FR",
"GE",
"DE",
"GH",
"GR",
"GT",
"GY",
"HT",
"HN",
"HK",
"HU",
"IS",
"IN",
"ID",
"IE",
"IL",
"IT",
"JM",
"JP",
"JO",
"KZ",
"KW",
"LV",
"LB",
"LY",
"LT",
"LU",
"MG",
"MY",
"MT",
"MU",
"MX",
"MD",
"MN",
"ME",
"MA",
"MZ",
"NA",
"NP",
"NL",
"NZ",
"NI",
"NG",
"NO",
"OM",
"PK",
"PY",
"PE",
"PH",
"PL",
"PT",
"RO",
"RU",
"SA",
"SN",
"RS",
"SG",
"LK",
"SK",
"SI",
"ZA",
"KR",
"ES",
"SE",
"TH",
"TT",
"TN",
"TR",
"UA",
"AE",
"UK",
"US",
"UY",
"VE",
"VN",
"ZM",
"ZW"]) 
    
    if st.button("Submit") or st.session_state.load_state:    
        st.session_state.load_state = True
        
        if uploaded_file is not None:
            data = pl.read_excel(uploaded_file,read_csv_options={"has_header": False})
            keywords = data['column_1'].to_list()
            
            # Fetch and display SEO data
            rankings, competition = brand_ranking(keywords,DB.lower(),your_brand_domain)
            
            #api_client = GoogleAdsClient.load_from_storage("cred.yaml")
            st.write("SemRush Keyword's ranking ")
            filtered_rankings = dataframe_explorer(rankings, case=False)
            st.dataframe(filtered_rankings,hide_index =True,use_container_width=True)
            filtered_competition = dataframe_explorer(competition, case=False)
            st.dataframe(filtered_competition,hide_index =True,use_container_width=True)
                
        elif keywords_input:
            keywords = keywords_input.split(',')
            your_brand_domain_input = your_brand_domain.split(',')
            # Fetch and display SEO data
            rankings, competition = brand_ranking(keywords,DB.lower(),your_brand_domain_input)
            # Initialize the GoogleAdsClient with the credentials and developer token
            #api_client = GoogleAdsClient.load_from_storage("cred.yaml")
            
            st.write("SemRush Keyword's ranking ")
            st.write(rankings)
            filtered_rankings = dataframe_explorer(rankings.to_pandas(), case=False)
            st.dataframe(filtered_rankings,hide_index =True,use_container_width=True)
            filtered_competition = dataframe_explorer(competition.to_pandas(), case=False)
            st.dataframe(filtered_competition,hide_index =True,use_container_width=True)
            
            
        st.write("\n\n\n")
        excel_file = to_excel([rankings], ["SemRush_Keyword", "SemRush_Ranking"])
        st.download_button(
        label="Download Excel file",
        data=excel_file,
        file_name="dataframes.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    
