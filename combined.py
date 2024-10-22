import streamlit as st
import requests
import polars as pl
import io
import pandas as pd
import datetime
import calendar
import plotly.express as px
import logging
import streamlit_authenticator as stauth
import yaml

# Set up logging
logging.basicConfig(
    filename="app.log",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# Load authentication config (Optional, if you're using authentication)
with open('config.yaml') as file:
    config = yaml.safe_load(file)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized'],
    key='authenticator_key'  # Ensure this key is unique
)

name, authentication_status, username = authenticator.login('Login', 'main')

if authentication_status:
    st.success(f'Welcome {name}')
    authenticator.logout('Logout', 'main')

elif authentication_status == False:
    st.error('Username/password is incorrect')

elif authentication_status == None:
    st.warning('Please enter your username and password')

if authentication_status:
    # Main app code starts here
    
    # Date handling logic for start and end dates
    today = datetime.datetime.now()
    min = datetime.date(today.year-2, today.month, 1)
    today = datetime.date.today()
    year = today.year if today.month > 1 else today.year - 1
    month = today.month - 1 if today.month > 1 else 12
    last_day = calendar.monthrange(year, month)[1]
    max = datetime.date(year, month, last_day)

    start_d = st.date_input("Choose the start date", value=max, format="YYYY/MM/DD", max_value=max, min_value=min)
    end_d = st.date_input("Choose the end date", value=max, format="YYYY/MM/DD", max_value=max, min_value=min)

    # Ensure valid month and year transitions
    if start_d.month == 1:    
        start_month = 12
        start_year = start_d.year - 1
    else:
        start_month = start_d.month
        start_year = start_d.year

    if end_d.month == 1: 
        end_month = 12
        end_year = end_d.year - 1
    else: 
        end_month = end_d.month
        end_year = end_d.year

    # Ensure that all variables are initialized
    if 'start_year' not in locals():
        start_year = start_d.year
    if 'end_year' not in locals():
        end_year = end_d.year

    st.write("\n\n\n")
    
    # Input for keywords or file upload
    uploaded_file = st.file_uploader("Upload a file containing keywords", type=["xlsx"])
    keywords_input = st.text_area("Or enter keywords manually (separated by a line)")
    
    # Define your API credentials
    creds = {
        'developer_token' : "q2Om6GmAhWjE2z_p8Da_Fw",
        'client_id' : "899223584116-m7n92thr3co9gr0otu7g64o85r6i46ko.apps.googleusercontent.com",
        'client_secret' : "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",
        'refresh_token' : "1//03aBH-xmgK1j6CgYIARAAGAMSNwF-L9IrHTMIAOSWIcjj147tjSBl5Z83teClPGIF2S-wcGCrCtO83BlRy5VaT4PoPA06_EVx5hQ",
        'use_proto_plus' : "False"
    } 

    name_to_api_key = {
        "Leclerc": {
            "client_id": "3117864871",
            "credentials": creds
        }
    }

    # Initialize GoogleAdsClient with credentials
    client_credentials = "Leclerc"
    api_client = GoogleAdsClient.load_from_dict(name_to_api_key[client_credentials]["credentials"])
    client_ = name_to_api_key[client_credentials]["client_id"]

    # Handle file upload or manual keyword input
    if st.checkbox("Submit"):
        if uploaded_file is not None:
            data = pl.read_excel(uploaded_file, read_csv_options={"has_header": False})
            keywords = data['column_1'].to_list()
            
            # Fetch and display Google Keyword Planner data
            overview, monthly_results, graph = generate_historical_metrics(api_client, client_, keywords, lang, DB, start_month, start_year, end_month, end_year)
            st.write("Google Keyword Planner Volume data")
            st.dataframe(overview, hide_index=True, use_container_width=True)
            st.write("Google Keyword Planner App Monthly Volume data")
            st.dataframe(monthly_results, hide_index=True, use_container_width=True)
            st.write("\n\n\n\n\n")
        
            # Plotly graph 
            fig = px.line(graph, x=graph.index, y=graph.columns, title='Keywords volume over time')
            fig.update_xaxes(dtick="M1", tickformat="%b\n%Y")
            st.plotly_chart(fig, use_container_width=True)
            st.write("\n\n\n\n\n")

            # Option to download data as Excel file
            excel_file = convert_to_excel([overview, monthly_results], sheet_names=["search_volume_overview", "monthly_search_volume"])
            st.download_button(
                label="Download Excel file",
                data=excel_file,
                file_name="dataframes.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        
        elif keywords_input:
            keywords = keywords_input.split('\n')

            # Fetch and display Google Keyword Planner data
            overview, monthly_results, graph = generate_historical_metrics(api_client, client_, keywords, lang, DB, start_month, start_year, end_month, end_year)
            st.write("Google Keyword Planner Volume data")
            st.dataframe(overview, hide_index=True, use_container_width=True)
            st.write("Google Keyword Planner Monthly Volume data")
            st.dataframe(monthly_results, hide_index=True, use_container_width=True)
            st.write("\n\n\n\n\n")

            # Plotly graph
            fig = px.line(graph, x=graph.index, y=graph.columns, title='Keywords volume over time')
            fig.update_xaxes(dtick="M1", tickformat="%b\n%Y")
            st.plotly_chart(fig, use_container_width=True)
            st.write("\n\n\n\n\n")
            
            # Option to download data as Excel file
            excel_file = convert_to_excel([overview, monthly_results], sheet_names=["search_volume_overview", "monthly_search_volume"])
            st.download_button(
                label="Download Excel file",
                data=excel_file,
                file_name="dataframes.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

# Utility function for downloading as Excel
def convert_to_excel(dfs, sheet_names):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for df, sheet_name in zip(dfs, sheet_names):
            df.to_excel(writer, sheet_name, index=False)
    output.seek(0)
    return output

# Function to fetch historical metrics (adjust based on your existing logic)
def generate_historical_metrics(api_client, customer_id, keywords, language, location, start_month, start_year, end_month, end_year):
    keyword_service = api_client.get_service("KeywordPlanIdeaService")
    request = api_client.get_type("GenerateKeywordHistoricalMetricsRequest")
    keyword_plan_network = api_client.get_type("KeywordPlanNetworkEnum").KeywordPlanNetwork.GOOGLE_SEARCH_AND_PARTNERS

    request.customer_id = customer_id
    request.language = search_for_language_constants(api_client, customer_id, language)
    
    if location:
        location_resource = map_locations_ids_to_resource_names(api_client, customer_id, location)
        if location_resource:
            request.geo_target_constants.extend([location_resource])
        else:
            st.error(f"Location '{location}' not found.")
            return None, None, None

    request.keyword_plan_network = keyword_plan_network
    request.keywords.extend(keywords)
    request.historical_metrics_options.year_month_range.start.year = start_year
    request.historical_metrics_options.year_month_range.start.month = start_month
    request.historical_metrics_options.year_month_range.end.year = end_year
    request.historical_metrics_options.year_month_range.end.month = end_month

    try:
        response = keyword_service.generate_keyword_historical_metrics(request=request)
    except Exception as e:
        st.error(f"Error fetching historical metrics: {e}")
        logging.error(f"Error fetching historical metrics for {keywords}: {e}")
        return None, None, None

    overview = []
    monthly_results = []
    for result in response.results:
        metric = result.keyword_metrics
        overview.append({
            "search_query": result.text,
            "avg_monthly_searches": metric.avg_monthly_searches,
            "competition": metric.competition
        })
        for month in metric.monthly_search_volumes:
            monthly_results.append({
                "search_query": result.text,
                "year": month.year,
                "month": month.month,
                "monthly_searches": month.monthly_searches
            })

    overview_df = pd.DataFrame(overview)
    monthly_results_df = pd.DataFrame(monthly_results)
    monthly_results_df['Date'] = monthly_results_df.apply(lambda row: datetime.date(row['year'], row['month'], 1), axis=1)

    # Pivot the data for plotting
    graph_df = monthly_results_df.pivot(index='Date', columns='search_query', values='monthly_searches')

    return overview_df, monthly_results_df.pivot(index='search_query', columns='Date', values='monthly_searches'), graph_df

# Helper function to search for language constants
def search_for_language_constants(client, customer_id, language_name):
    googleads_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT language_constant.resource_name
        FROM language_constant
        WHERE language_constant.name LIKE '%{language_name}%'"""
    stream = googleads_service.search_stream(
        customer_id=customer_id, query=query
    )
    batches = [batch.results for batch in stream]
    return batches[0][0].language_constant.resource_name

# Helper function to map location names to resource names
def map_locations_ids_to_resource_names(api_client, customer_id, location_name):
    googleads_service = api_client.get_service("GoogleAdsService")
    query = f"""
        SELECT geo_target_constant.resource_name
        FROM geo_target_constant
        WHERE geo_target_constant.name LIKE '%{location_name}%'"""
    stream = googleads_service.search_stream(
        customer_id=customer_id, query=query
    )
    batches = [batch.results for batch in stream]
    return batches[0][0].geo_target_constant.resource_name

# Function to load language list
def language_full_list(client, customer_id):
    googleads_service = client.get_service("GoogleAdsService")
    query = "SELECT language_constant.name FROM language_constant"
    stream = googleads_service.search_stream(customer_id=customer_id, query=query)
    return [row.language_constant.name for batch in stream for row in batch.results]

# Function to load location list
def location_full_list(client, customer_id):
    googleads_service = client.get_service("GoogleAdsService")
    query = "SELECT geo_target_constant.name FROM geo_target_constant"
    stream = googleads_service.search_stream(customer_id=customer_id, query=query)
    return [row.geo_target_constant.name for batch in stream for row in batch.results]

# Streamlit UI for displaying the keyword data
# Streamlit UI for displaying the keyword data
if __name__ == "__main__":
    if "load_state" not in st.session_state:
        st.session_state.load_state = False

    # Load available languages and locations
    client_credentials = "Leclerc"
    api_client = GoogleAdsClient.load_from_dict(
        name_to_api_key[client_credentials]["credentials"]
    )
    client_ = name_to_api_key[client_credentials]["client_id"]
    
    list_language = language_full_list(api_client, client_)
    list_location = location_full_list(api_client, client_)

    st.write("\n\n\n")
    
    st.write("Enter a keyword and select a country to fetch SEO data.")
    uploaded_file = st.file_uploader("Upload an Excel file containing keywords", type=["xlsx"], key='file_uploader_key')
        
    # Allow user to manually enter keywords
    keywords_input = st.text_area("Or enter keywords manually (separated by a line)", key='keywords_input_key')
    st.title("Google Ads")

    col1, col2 = st.columns(2)
    
    with col1:
        DB = st.selectbox("Select a country:", [""] + list_location, key='location_select_key')
    with col2:
        default_ix_2 = list_language.index("French")
        lang = st.selectbox("Select a language:", list_language, index=default_ix_2, key='language_select_key')
    
    col3, col4 = st.columns(2)   
    with col3:
        today = datetime.datetime.now()
        min = datetime.date(today.year-2, today.month, 1)
        today = datetime.date.today()
        year = today.year if today.month > 1 else today.year - 1
        month = today.month - 1 if today.month > 1 else 12
        last_day = calendar.monthrange(year, month)[1]
        max = datetime.date(year, month, last_day)
        max_2 = datetime.date(year, month, last_day)
        start_d = st.date_input("Choose the start date", value=max, format="YYYY/MM/DD", max_value=max, min_value=min, key='start_date_key')
    with col4:
        end_d = st.date_input("Choose the end date", value=max, format="YYYY/MM/DD", max_value=max, min_value=min, key='end_date_key')
    
    # Ensure valid month transitions
    if start_d.month == 1:    
        start_month = 12
        start_year = start_d.year - 1
    else:
        start_month = start_d.month
        start_year = start_d.year

    if end_d.month == 1: 
        end_month = 12
        end_year = end_d.year - 1
    else: 
        end_month = end_d.month
        end_year = end_d.year

    # Submit form to fetch data
    if st.checkbox("Submit", key='submit_key'):
        if uploaded_file is not None:
            data = pl.read_excel(uploaded_file, read_csv_options={"has_header": False})
            keywords = data['column_1'].to_list()
            
            # Fetch and display Google Keyword Planner data
            overview, monthly_results, graph = generate_historical_metrics(api_client, client_, keywords, lang, DB, start_month, start_year, end_month, end_year)
            st.write("Google Keyword Planner Volume data")
            st.dataframe(overview, hide_index=True, use_container_width=True)
            st.write("Google Keyword Planner App Monthly Volume data")
            st.dataframe(monthly_results, hide_index=True, use_container_width=True)
            st.write("\n\n\n\n\n")
        
            # Plotly graph 
            fig = px.line(graph, x=graph.index, y=graph.columns, title='Keywords volume over time')
            fig.update_xaxes(dtick="M1", tickformat="%b\n%Y")
            st.plotly_chart(fig, use_container_width=True)
            st.write("\n\n\n\n\n")

            # Option to download data as Excel file
            excel_file = convert_to_excel([overview, monthly_results], sheet_names=["search_volume_overview", "monthly_search_volume"])
            st.download_button(
                label="Download Excel file",
                data=excel_file,
                file_name="dataframes.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key='download_button_key'
            )
        
        elif keywords_input:
            keywords = keywords_input.split('\n')

            # Fetch and display Google Keyword Planner data
            overview, monthly_results, graph = generate_historical_metrics(api_client, client_, keywords, lang, DB, start_month, start_year, end_month, end_year)
            st.write("Google Keyword Planner Volume data")
            st.dataframe(overview, hide_index=True, use_container_width=True)
            st.write("Google Keyword Planner Monthly Volume data")
            st.dataframe(monthly_results, hide_index=True, use_container_width=True)
            st.write("\n\n\n\n\n")

            # Plotly graph
            fig = px.line(graph, x=graph.index, y=graph.columns, title='Keywords volume over time')
            fig.update_xaxes(dtick="M1", tickformat="%b\n%Y")
            st.plotly_chart(fig, use_container_width=True)
            st.write("\n\n\n\n\n")
            
            # Option to download data as Excel file
            excel_file = convert_to_excel([overview, monthly_results], sheet_names=["search_volume_overview", "monthly_search_volume"])
            st.download_button(
                label="Download Excel file",
                data=excel_file,
                file_name="dataframes.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key='download_button_manual_key'
            )


