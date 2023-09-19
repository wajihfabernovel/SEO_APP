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

API_KEY = 'e31f38c36540a234e23b614a7ffb4fc4'

creds = {
    'developer_token' : "q2Om6GmAhWjE2z_p8Da_Fw",
    'client_id' : "899223584116-m7n92thr3co9gr0otu7g64o85r6i46ko.apps.googleusercontent.com",
    'client_secret' : "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",
    'refresh_token' : "1//03aBH-xmgK1j6CgYIARAAGAMSNwF-L9IrHTMIAOSWIcjj147tjSBl5Z83teClPGIF2S-wcGCrCtO83BlRy5VaT4PoPA06_EVx5hQ",
    'use_proto_plus' : "False"} 

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
def search_for_language_constants(client, customer_id, language_name):
    """Searches for language constants where the name includes a given string.

    Args:
        client: An initialized Google Ads API client.
        customer_id: The Google Ads customer ID.
        language_name: String included in the language name to search for.
    """
    # Get the GoogleAdsService client.
    googleads_service = client.get_service("GoogleAdsService")

    # Create a query that retrieves the language constants where the name
    # includes a given string.
    query = f"""
        SELECT
        language_constant.resource_name
        FROM language_constant
        WHERE language_constant.name LIKE '%{language_name}%'"""

    # Issue a search request and process the stream response to print the
    # requested field values for the carrier constant in each row.
    stream = googleads_service.search_stream(
        customer_id=customer_id, query=query
    )
    batches = [batch.results for batch in stream]
    return batches[0][0].language_constant.resource_name


def map_locations_ids_to_resource_names(api_client, customer_id, location_name):
    """Converts a list of location IDs to resource names.

    Args:
        client: an initialized GoogleAdsClient instance.
        location_ids: a list of location ID strings.

    Returns:
        a list of resource name strings using the given location IDs.
    """
    # Get the GoogleAdsService client.
    googleads_service = api_client.get_service("GoogleAdsService")

    # Create a query that retrieves the language constants where the name
    # includes a given string.
    query = f"""
        SELECT
        geo_target_constant.resource_name
        FROM geo_target_constant
        WHERE geo_target_constant.name LIKE '%{location_name}%'"""

    # Issue a search request and process the stream response to print the
    # requested field values for the carrier constant in each row.
    stream = googleads_service.search_stream(
        customer_id=customer_id, query=query
    )
    batches = [batch.results for batch in stream]
    return batches[0][0].geo_target_constant.resource_name


def generate_historical_metrics(api_client, customer_id,keywords):
    overview = pl.DataFrame([])
    final_overview = pl.DataFrame([])
    monthly_results = pl.DataFrame([])
    final_monthly_results = pl.DataFrame([])
    
    keyword = api_client.get_service("KeywordPlanIdeaService")
    request = api_client.get_type("GenerateKeywordHistoricalMetricsRequest")
    keyword_plan_network = api_client.get_type(
        "KeywordPlanNetworkEnum"
    ).KeywordPlanNetwork.GOOGLE_SEARCH_AND_PARTNERS
    
    request.customer_id = customer_id
    request.language = search_for_language_constants(api_client, "3117864871", "English")
    request.geo_target_constants.extend([map_locations_ids_to_resource_names(api_client, "3117864871", "France")])

    request.keyword_plan_network = keyword_plan_network
    request.keywords.extend(keywords)

    keyword_historical_metrics_response = keyword.generate_keyword_historical_metrics(
        request=request
    )
    for result in keyword_historical_metrics_response.results:
        metric = result.keyword_metrics
        overview = overview.with_columns(search_query=pl.lit(result.text), appro_monthly_search = pl.lit(metric.avg_monthly_searches),competition_level = pl.lit(metric.competition))
        # These metrics include those for both the search query and any
        # variants included in the response.
        # If the metric is undefined, print (None) as a placeholder.
        
        final_overview = final_overview.vstack(overview)
        
        # Approximate number of searches on this query for the past twelve months.
        for month in metric.monthly_search_volumes:
            if month.month == 13:
                monthly_results = monthly_results.with_columns(search_query=pl.lit(result.text),Appro_monthly=month.monthly_searches, month = 1,Year = month.year+1)
                final_monthly_results = final_monthly_results.vstack(monthly_results)
            else:   
                monthly_results = monthly_results.with_columns(search_query=pl.lit(result.text),Appro_monthly=month.monthly_searches, month =month.month,Year = month.year) 
                final_monthly_results = final_monthly_results.vstack(monthly_results)
    return final_overview,final_monthly_results
    
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
        name_to_api_key = {
            "Leclerc": {
                "client_id": "3117864871",
                "credentials": creds
            },
            "BLW": {
                "client_id": "3117864871",
                "credentials": creds         
            }
        }
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
        keywords_input = st.text_area("Or enter keywords manually (seperated by a , )")
        col1, col2 = st.columns(2)
        
        your_brand_domain = st.text_input("Enter your brand domain")
        client_credentials = st.selectbox("Select a Google Ads account:", ["Leclerc", "BLW"])  # Add more countries as needed
        with col1:
            DB = st.selectbox("Select a country:", ["us", "uk", "ca", "au", "de", "fr", "es", "it", "br", "mx", "in"]) 
        with col2:
            lang = st.selectbox("Select a language:", ["French", "English"])  # Add more countries as needed
        client_ = name_to_api_key[client_credentials]["client_id"]
        if st.button("Fetch Data"):
            
            if uploaded_file is not None:
                data = pl.read_excel(uploaded_file,read_csv_options={"has_header": False})
                keywords = data['column_1'].to_list()
                
                # Fetch and display SEO data
                dataframes = seo(keywords, DB)
                rankings, competition = brand_ranking(keywords,DB,your_brand_domain)
                api_client = GoogleAdsClient.load_from_dict(
                    name_to_api_key[client_credentials]["credentials"]
                )
                #api_client = GoogleAdsClient.load_from_storage("cred.yaml")
                overview, monthly_results = generate_historical_metrics(api_client,client_,keywords)
                st.write("SemRush Keyword Volume data")
                st.write(dataframes)
                st.write("Google Keyword Planner Volume data")
                st.write(overview)
                st.write("Google Keyword Palnner App Monthly Volume data")
                st.write(monthly_results)
                st.write("SemRush Keyword's ranking ")
                st.write(rankings)
                #st.write(competition)
                    
            elif keywords_input:
                keywords = keywords_input.split(',')

                # Fetch and display SEO data
                dataframes = seo(keywords, DB)
                rankings, competition = brand_ranking(keywords,DB,your_brand_domain)
                # Initialize the GoogleAdsClient with the credentials and developer token
                api_client = GoogleAdsClient.load_from_dict(
                    name_to_api_key[client_credentials]["credentials"]
                )
                #api_client = GoogleAdsClient.load_from_storage("cred.yaml")
                overview, monthly_results = generate_historical_metrics(api_client,client_,keywords)
                
                st.write("SemRush Keyword Volume data")
                st.write(dataframes)
                st.write("Google Keyword Planner Volume data")
                st.write(overview)
                st.write("Google Keyword Palnner App Monthly Volume data")
                st.write(monthly_results)
                st.write("SemRush Keyword's ranking ")
                st.write(rankings)
                #st.write(competition)
                
            # Download 
            dataframes = {
                'Sheet1': dataframes,
                'Sheet2': overview,
                'Sheet3': monthly_results,
                'Sheet4': rankings
            }
            file_name='volume.xlsx',
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
