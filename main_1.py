import streamlit as st
import requests
import polars as pl
import io
import pandas as pd
import streamlit_authenticator as stauth
from google.ads.googleads.client import GoogleAdsClient
import datetime 
import xlsxwriter
import plotly.express as px
pl.Config.set_tbl_hide_column_data_types(True)



API_KEY = 'e31f38c36540a234e23b614a7ffb4fc4'

creds = {
    'developer_token' : "q2Om6GmAhWjE2z_p8Da_Fw",
    'client_id' : "899223584116-m7n92thr3co9gr0otu7g64o85r6i46ko.apps.googleusercontent.com",
    'client_secret' : "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",
    'refresh_token' : "1//03aBH-xmgK1j6CgYIARAAGAMSNwF-L9IrHTMIAOSWIcjj147tjSBl5Z83teClPGIF2S-wcGCrCtO83BlRy5VaT4PoPA06_EVx5hQ",
    'use_proto_plus' : "False"} 

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

def language_full_list(client, customer_id):
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
        language_constant.name
        FROM language_constant"""

    # Issue a search request and process the stream response to print the
    # requested field values for the carrier constant in each row.
    stream = googleads_service.search_stream(
        customer_id=customer_id, query=query
    )
    t = []
    for batch in stream:
        for row in batch.results:
            language_name = row.language_constant.name
            t.append(language_name)
    return t

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

def location_full_list(client, customer_id):
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
        geo_target_constant.name
        FROM geo_target_constant"""

    # Issue a search request and process the stream response to print the
    # requested field values for the carrier constant in each row.
    stream = googleads_service.search_stream(
        customer_id=customer_id, query=query
    )
    t = []
    for batch in stream:
        for row in batch.results:
            location_name = row.geo_target_constant.name
            t.append(location_name)
    return t

def generate_historical_metrics(api_client, customer_id,keywords,language,location,start_month,start_year,end_month,end_year):
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
    request.language = search_for_language_constants(api_client, customer_id, language)
    request.geo_target_constants.extend([map_locations_ids_to_resource_names(api_client, customer_id,location)])

    request.keyword_plan_network = keyword_plan_network
    request.keywords.extend(keywords)
    request.historical_metrics_options.year_month_range.start.year = start_year
    request.historical_metrics_options.year_month_range.start.month=start_month
    request.historical_metrics_options.year_month_range.end.year = end_year
    request.historical_metrics_options.year_month_range.end.month=end_month
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
                monthly_results = monthly_results.with_columns(search_query=pl.lit(result.text),Appro_monthly=month.monthly_searches,day= 1, month = 1,Year = month.year+1)
                final_monthly_results = final_monthly_results.vstack(monthly_results)
            else:   
                monthly_results = monthly_results.with_columns(search_query=pl.lit(result.text),Appro_monthly=month.monthly_searches,day= 1, month =month.month,Year = month.year) 
                final_monthly_results = final_monthly_results.vstack(monthly_results)
                
        final_monthly_results_final = final_monthly_results.with_columns(pl.col('month').apply(lambda x:datetime.date(1900, x, 1).strftime('%B')))
        final_monthly_results_final = final_monthly_results_final.with_columns(pl.concat_str([pl.col('day'),pl.col("month"),pl.col("Year")],separator=" ").alias('Date')).select(pl.col('search_query'),pl.col('Date'),pl.col('Appro_monthly'))
        final_monthly_results_final = final_monthly_results_final.with_columns(
               pl.col("Date").str.to_date("%d %B %Y")
            )
    return final_overview,final_monthly_results_final.pivot(values ="Appro_monthly", index = "search_query", columns = "Date"),final_monthly_results_final.pivot(values ="Appro_monthly", index = "Date", columns = "search_query")
    
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
    st.title("Google Ads")
    st.write("Enter a keyword and select a country to fetch SEO data.")

    uploaded_file = st.file_uploader("Upload an Excel file containing keywords", type=["xlsx"])
        
    # Allow user to manually enter keywords
    keywords_input = st.text_area("Or enter keywords manually (seperated by a , )")
    col1, col2 = st.columns(2)

    client_credentials = st.selectbox("Select a Google Ads account:", ["Leclerc", "BLW"])  # Add more countries as needed
    
    # Initialize the GoogleAdsClient with the credentials and developer token
    api_client = GoogleAdsClient.load_from_dict(
                name_to_api_key[client_credentials]["credentials"]
            )
    client_ = name_to_api_key[client_credentials]["client_id"]
    list_language = language_full_list(api_client,client_)
    list_location=location_full_list(api_client,client_)
    with col1:
        DB = st.selectbox("Select a country:", list_location) 
    with col2:
        lang = st.selectbox("Select a language:", list_language)  # Add more countries as needed
    col3, col4 = st.columns(2)   
    with col3:
        today = datetime.datetime.now()
        min = datetime.date(today.year-2,today.month, 1)
        max = datetime.date(today.year,today.month - 1, 30)
        start_d = st.date_input("Choose the start date",value = max,format="YYYY/MM/DD",max_value =max,min_value =min)
    with col4:
        end_d = st.date_input("Choose the end date",value =max, format="YYYY/MM/DD",max_value =max,min_value =min)
        
    # Transform the month 13 to 1 of the next year 
    if datetime.datetime.strptime(str(start_d), "%Y-%m-%d").month == 1 :    
        start_month = datetime.datetime.strptime(str(12), "%m").month +1
        start_year = datetime.datetime.strptime(str(start_d), "%Y-%m-%d").year - 1
        end_month = datetime.datetime.strptime(str(end_d), "%Y-%m-%d").month
        end_year = datetime.datetime.strptime(str(end_d), "%Y-%m-%d").year
    elif datetime.datetime.strptime(str(end_d), "%Y-%m-%d").month == 1 : 
        start_month = datetime.datetime.strptime(str(start_d), "%Y-%m-%d").month
        start_year = datetime.datetime.strptime(str(start_d), "%Y-%m-%d").year
        end_month = datetime.datetime.strptime(str(12), "%m").month +1
        end_year = datetime.datetime.strptime(str(end_d), "%Y-%m-%d").year - 1
    else: 
        start_month = datetime.datetime.strptime(str(start_d), "%Y-%m-%d").month
        start_year = datetime.datetime.strptime(str(start_d), "%Y-%m-%d").year
        end_month = datetime.datetime.strptime(str(end_d), "%Y-%m-%d").month
        end_year = datetime.datetime.strptime(str(end_d), "%Y-%m-%d").year
        
    if st.button("Submit"):
        
        if uploaded_file is not None:
            data = pl.read_excel(uploaded_file,read_csv_options={"has_header": False})
            keywords = data['column_1'].to_list()
            
            # Fetch and display SEO data
            
            #api_client = GoogleAdsClient.load_from_storage("cred.yaml")
            
            overview, monthly_results,graph = generate_historical_metrics(api_client,client_,keywords,lang,DB,start_month,start_year,end_month,end_year)
            st.write("Google Keyword Planner Volume data")
            st.dataframe(overview,hide_index =True,use_container_width=True)
            st.write("Google Keyword Palnner App Monthly Volume data")
            st.dataframe(monthly_results,hide_index =True,use_container_width=True)
            st.write("\n\n\n\n\n")
        
            # Plotly graph 
            fig = px.line(graph, x="Date", y=graph.columns,
                          hover_data={"Date": "|%B %Y"},
                          title='Keywords volume overtime')
            fig.update_xaxes(
                dtick="M1",
                tickformat="%b\n%Y")
            st.plotly_chart(fig, use_container_width=True)

            #st.write(competition)
                
        elif keywords_input:
            keywords = keywords_input.split(',')

            #api_client = GoogleAdsClient.load_from_storage("cred.yaml")
            overview, monthly_results, graph = generate_historical_metrics(api_client,client_,keywords,lang,DB,start_month,start_year,end_month,end_year)

            st.write("Google Keyword Planner Volume data")
            st.dataframe(overview,hide_index =True,use_container_width=True)
            st.write("Google Keyword Palnner App Monthly Volume data")
            st.dataframe(monthly_results,hide_index =True,use_container_width=True)
            st.write("\n\n\n\n\n")
            
            # Plotly graph 
            fig = px.line(graph, x="Date", y=graph.columns,
                          hover_data={"Date": "|%B %Y"},
                          title='Keywords volume overtime')
            fig.update_xaxes(
                dtick="M1",
                tickformat="%b\n%Y")
            st.plotly_chart(fig, use_container_width=True)
            
            #st.write(competition)
            
        st.write("\n\n\n")
        excel_file = to_excel([overview, monthly_results], ["search_volume_overview", "monthly_search_volume"])
        st.download_button(
        label="Download Excel file",
        data=excel_file,
        file_name="dataframes.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
        
