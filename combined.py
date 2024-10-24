import requests
import polars as pl
import io
import pandas as pd
import streamlit_authenticator as stauth
from google.ads.googleads.client import GoogleAdsClient
import datetime 
import calendar
import xlsxwriter
import plotly.express as px
pl.Config.set_tbl_hide_column_data_types(True)
from streamlit_extras.dataframe_explorer import dataframe_explorer
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)
API_KEY = 'e31f38c36540a234e23b614a7ffb4fc4'
creds = {
    'developer_token' : "q2Om6GmAhWjE2z_p8Da_Fw",
    'client_id' : "899223584116-m7n92thr3co9gr0otu7g64o85r6i46ko.apps.googleusercontent.com",
    'client_secret' : "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",
    'refresh_token' : "1//03aBH-xmgK1j6CgYIARAAGAMSNwF-L9IrHTMIAOSWIcjj147tjSBl5Z83teClPGIF2S-wcGCrCtO83BlRy5VaT4PoPA06_EVx5hQ",
    'use_proto_plus' : "False"} 
#@st.cache_data
def set_day_to_15(date_str, date_format="%Y-%m-%d"):
    """
    Set the day in the given date string to 15.
    Parameters:
    - date_str (str): The input date string.
    - date_format (str): The format of the input date string. Default is "%Y-%m-%d" (e.g., "2023-10-06").
    Returns:
    - str: The modified date string with the day set to 15.
    """
    date_obj = datetime.datetime.strptime(date_str, date_format)
    modified_date_obj = date_obj.replace(day=15)
    return modified_date_obj.strftime(date_format)
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
    googleads_service = client.get_service("GoogleAdsService")
    query = "SELECT geo_target_constant.name FROM geo_target_constant"
    stream = googleads_service.search_stream(customer_id=customer_id, query=query)
    locations = [row.geo_target_constant.name for batch in stream for row in batch.results]
    return locations
    

def generate_historical_metrics(api_client, customer_id, keywords, language, location, start_d, end_d):
    keyword_service = api_client.get_service("KeywordPlanIdeaService")
    request = api_client.get_type("GenerateKeywordHistoricalMetricsRequest")
    keyword_plan_network = api_client.get_type("KeywordPlanNetworkEnum").KeywordPlanNetwork.GOOGLE_SEARCH_AND_PARTNERS

    # Extract year and month from the start and end dates
    start_month = datetime.datetime.strptime(str(start_d), "%Y-%m-%d").month +1
    start_year = datetime.datetime.strptime(str(start_d), "%Y-%m-%d").year
    end_month = datetime.datetime.strptime(str(end_d), "%Y-%m-%d").month+1
    end_year = datetime.datetime.strptime(str(end_d), "%Y-%m-%d").year


    # Ensure the start date is before the end date
    if start_d > end_d:
        st.error("Start date cannot be later than end date.")
        return None, None, None

    # Set the customer ID and network type
    request.customer_id = customer_id
    request.keyword_plan_network = keyword_plan_network

    # Set the language
    request.language = search_for_language_constants(api_client, customer_id, language)

    # Set the location if provided
    if location:
        location_resource = map_locations_ids_to_resource_names(api_client, customer_id, location)
        if location_resource:
            request.geo_target_constants.extend([location_resource])
        else:
            st.error(f"Location '{location}' not found.")
            return None, None, None

    # Add keywords
    request.keywords.extend(keywords)

    # Create and set the YearMonthRange for historical metrics options
    year_month_range = api_client.get_type("YearMonthRange")
    year_month_range.start.year = start_year
    year_month_range.start.month = start_month
    year_month_range.end.year = end_year
    year_month_range.end.month = end_month

    # Assign the YearMonthRange object to the historical metrics options
    request.historical_metrics_options.year_month_range.CopyFrom(year_month_range)

    try:
        # Fetch historical metrics from the API
        response = keyword_service.generate_keyword_historical_metrics(request=request)
    except Exception as e:
        st.error(f"Error fetching historical metrics: {e}")
        return None, None, None

    # Parse the response to generate dataframes
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
                "month": month.month-1,
                "monthly_searches": month.monthly_searches
            })

    # Convert to pandas DataFrames for further analysis
    overview_df = pd.DataFrame(overview)
    monthly_results_df = pd.DataFrame(monthly_results)
    monthly_results_df['Date'] = monthly_results_df.apply(lambda row: datetime.date(row['year'], row['month'], 1), axis=1)

    # Pivot the monthly results dataframe for easier plotting
    graph_df = monthly_results_df.pivot(index='Date', columns='search_query', values='monthly_searches')

    return overview_df, monthly_results_df.pivot(index='search_query', columns='Date', values='monthly_searches'), graph_df

    
# Function to download the DataFrame as an Excel file
def convert_to_excel(dfs, sheet_names):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for df, sheet_name in zip(dfs, sheet_names):
            df.to_excel(writer, sheet_name, index=False)
    output.seek(0)
    return output
########################################@
API_KEY_SEM = 'e31f38c36540a234e23b614a7ffb4fc4'
creds = {
    'developer_token' : "q2Om6GmAhWjE2z_p8Da_Fw",
    'client_id' : "899223584116-m7n92thr3co9gr0otu7g64o85r6i46ko.apps.googleusercontent.com",
    'client_secret' : "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",
    'refresh_token' : "1//03aBH-xmgK1j6CgYIARAAGAMSNwF-L9IrHTMIAOSWIcjj147tjSBl5Z83teClPGIF2S-wcGCrCtO83BlRy5VaT4PoPA06_EVx5hQ",
    'use_proto_plus' : "False"} 
def brand_ranking (keywords,DB,your_brand_domain): 
    data = {"Domain": [0], "Position": [0],"Key": [0] }
    dfs_r = pl.DataFrame([])  # List to store dataframes for each keyword
    your_brand_position = None
    competitors = pl.DataFrame([])
    final_compet = pl.DataFrame([])
    t = pl.DataFrame([])
    b = pl.DataFrame([])
    rank = pl.DataFrame([])
    API_KEY_SEM = 'e31f38c36540a234e23b614a7ffb4fc4'
    for keyword in keywords:
        url = f"https://api.semrush.com/?type=phrase_organic&key={API_KEY_SEM}&phrase={keyword}&export_columns=Kd,Dn,Po,&database={DB}"
        response = requests.get(url)
        # Make sure the request was successful before processing
        if response.status_code == 200:
            df = pl.read_csv(io.StringIO(response.text), separator=';', eol_char='\n').with_columns(Key=pl.lit(keyword))
            if df.shape[1] == 3:
                dfs_r = dfs_r.vstack(df)
                for i in range(len(df)):
                    domain = df['Domain'][i]
                    position = df['Position'][i]
                    Keys = df['Key'][i]
                    for j in range (len(your_brand_domain)):
                        if (domain in your_brand_domain[j]) or (your_brand_domain[j] in domain):
                            your_brand_position = position
                            b = b.with_columns(keyword = pl.lit(Keys),brand_domain = pl.lit(domain),brand_ranking= pl.lit(your_brand_position))
                            rank = rank.vstack(b)
        
                        else:
                            
                            t = t.with_columns(keyword = pl.lit(Keys),brand_domain = pl.lit(domain), brand_ranking= pl.lit(position))
                            competitors = competitors.vstack(t)  
                final_compet = final_compet.vstack(competitors.head(30))
                competitors = competitors.clear()
        else:
            print("semrush does not work")
            st.write(f"Failed to fetch data for keyword: {keyword}. Status Code: {response.status_code}")  
    if rank.is_empty(): 
        return rank, rank, final_compet.unique(maintain_order=True)
    else : 
        st.write('it is not empty')
        rank = rank.group_by(["keyword","brand_domain"]).agg(pl.col("brand_ranking").min())
        return rank,rank.pivot(values="brand_ranking",index="keyword",columns="brand_domain"), final_compet.unique(maintain_order=True)
def seo(keywords, DB):
    
    dfs = pl.DataFrame([])  # List to store dataframes for each keyword
    for keyword in keywords:
        url = f"https://api.semrush.com/?type=phrase_all&key={API_KEY_SEM}&phrase={keyword}&export_columns=Dt,Db,Ph,Nq,Cp,Co,Nr&database={DB}"
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
# Streamlit UI
if __name__ == "__main__":
    if "load_state" not in st.session_state:
            st.session_state.load_state = False
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
    
    # Add a spacer after the logo
    st.write("\n\n\n")
    
    st.write("Enter a keyword and select a country to fetch SEO data.")
    uploaded_file = st.file_uploader("Upload an Excel file containing keywords", type=["xlsx"])
        
    # Allow user to manually enter keywords
    keywords_input = st.text_area("Or enter keywords manually (seperated by a line)")
    st.title("Google Ads")
    col1, col2 = st.columns(2)
    client_credentials = "Leclerc"  # Add more countries as needed
    
    
    # Initialize the GoogleAdsClient with the credentials and developer token
    api_client = GoogleAdsClient.load_from_dict(
                name_to_api_key[client_credentials]["credentials"]
            )
    client_ = name_to_api_key[client_credentials]["client_id"]
    list_language = language_full_list(api_client,client_)
    list_location=location_full_list(api_client,client_)
    
    with col1:
        DB = st.selectbox("Select a country:", [""] + list_location)
    with col2:
        default_ix_2 = list_language.index("French")
        lang = st.selectbox("Select a language:", list_language, index= default_ix_2)  # Add more countries as needed
    col3, col4 = st.columns(2)   
    with col3:
        today = datetime.datetime.now()
        min = datetime.date(today.year-2,today.month, 1)
        today = datetime.date.today()
        year = today.year if today.month > 1 else today.year - 1
        month = today.month - 1 if today.month > 1 else 12
        last_day = calendar.monthrange(year, month)[1]
        max = datetime.date(year, month, last_day)
        max_2 = datetime.date(year,month, last_day)
        start_d = st.date_input("Choose the start date",value = max,format="YYYY/MM/DD",max_value =max,min_value =min)
    with col4:
        end_d = st.date_input("Choose the end date",value =max, format="YYYY/MM/DD",max_value =max,min_value =min)
        
        
    st.write("\n\n\n")
        
    st.write("\n\n\n")
    #st.title("SemRush")
   
    # Allow user to manually enter keywords
    
    #your_brand_domain = st.text_input("Enter your brand domain")
    sem_lang = ["AF",
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
    "ZW"]
    #default_ix_3 = sem_lang.index("FR")
    #DB_sem = st.selectbox("Select a country:", sem_lang, index=default_ix_3) 
    #searche_type = ["allSearches","branded","search-intent","long-tail","categories"]
    #devices = ["allDevices","desktop","mobile","tablet"]
    #audience = ["international","us","uk","aus"]
    #value = ["exact","average"]
    #st.write("\n\n\n")
    #st.title("Advanced Web Ranking")
    #col1, col2,col3 = st.columns(3)
    #with col1:
        #web_date = st.date_input("Choose a month",value = max_2,format="YYYY-MM-DD",max_value =max_2,min_value =min)
        #web_date = web_date.strftime("%Y-%m-%d")
        #web_date_final = set_day_to_15(web_date)
        #web_date_final = datetime.datetime.strptime(web_date_final, "%Y-%m-%d")
    #with col2:
        #web_search = st.selectbox("Select search type:", searche_type) 
    #with col3:
        #web_device = st.selectbox("Select search type:", devices)# Add more countries as needed
    #col4, col5 = st.columns(2)   
    #with col4:
        #web_aud = st.selectbox("Select an audience:", audience)  # Add more countries as needed    
    #with col5:
        #web_val = st.selectbox("Select value type:", value)    
    
    
    if st.checkbox("Submit") :
    #or st.session_state.load_state:    
    #    st.session_state.load_state = True
        
        if uploaded_file is not None:
            data = pl.read_excel(uploaded_file,read_csv_options={"has_header": False})
            keywords = data['column_1'].to_list()
            
            # Fetch and display SEO data
            
            #api_client = GoogleAdsClient.load_from_storage("cred.yaml")
            
            overview, monthly_results,graph = generate_historical_metrics(api_client,client_,keywords,lang,DB,start_d, end_d)
            st.write("Google Keyword Planner Volume data")
            st.dataframe(overview,hide_index =True,use_container_width=True)
            st.write("Google Keyword Palnner App Monthly Volume data")
            st.dataframe(monthly_results,hide_index =True,use_container_width=True)
            st.write("\n\n\n\n\n")
        
            # Plotly graph 
            fig = px.line(graph, x=graph.index, y=graph.columns, title='Keywords volume overtime')
            fig.update_xaxes(dtick="M1", tickformat="%b\n%Y")
            st.plotly_chart(fig, use_container_width=True)
            st.write("\n\n\n\n\n")
            #your_brand_domain_input = your_brand_domain.split(',')
            # Fetch and display SEO data
            #rank_,rankings, competition = brand_ranking(keywords,DB_sem.lower(),your_brand_domain_input)
            
            #api_client = GoogleAdsClient.load_from_storage("cred.yaml")
            #st.write("SemRush Keyword's ranking ")
            #filtered_rankings = dataframe_explorer(rankings.to_pandas(), case=False)
            #st.dataframe(filtered_rankings,hide_index =True,use_container_width=True)
            #filtered_competition = dataframe_explorer(competition.to_pandas(), case=False)
            #st.dataframe(filtered_competition,hide_index =True,use_container_width=True)            
        
            #myAPIToken = 'c186250c0f3ba9502c38caa53efc7edb'
            #params = {
                #"action": "export_ctr",
                #"token": myAPIToken,  # Get token from environment variable
                #"inputs": f'{{"date":"{web_date_final}", "searches-type":"{web_search}", "value":"{web_val}", "device":"{web_device}", "audience":"{web_aud}", "format":"json"}}'
            #}
            
            #url = f"https://api.awrcloud.com/v2/get.php"
            #response = requests.get(url, params=params)
            # Make sure the request was successful before processing
            
            #data = response.json()
            #web_ranking = pl.DataFrame(data["details"]).with_columns(pl.col("position").cast(pl.Int32).alias("position"))
            #final_table = competition.vstack(rank_).join(web_ranking,left_on="brand_ranking", right_on="position").join(overview,left_on="keyword",right_on="search_query").select(["brand_domain","keyword","brand_ranking","web_ctr","appro_monthly_search"]).sort(["brand_domain","brand_ranking"], descending=[False, False]).with_columns(pl.col("appro_monthly_search").sum().over("brand_domain").alias("sum")).with_columns((((pl.col("appro_monthly_search")*(pl.col("web_ctr")/100))/pl.col("sum"))*100).alias("Part_des_voix_%")).select(["brand_domain","keyword","Part_des_voix_%"]).to_pandas()
            #st.dataframe(dataframe_explorer(final_table, case=False),hide_index =True,use_container_width=True) 
            #bar_chart = final_table.groupby(['brand_domain'])["Part_des_voix_%"].sum().reset_index().sort_values(by="Part_des_voix_%", ascending=False).head(10)
            #st.dataframe(dataframe_explorer(bar_chart, case=False),hide_index =True,use_container_width=True) 
                
        elif keywords_input:
            
            keywords = keywords_input.split('\n')
            
            #api_client = GoogleAdsClient.load_from_storage("cred.yaml")
            overview, monthly_results, graph = generate_historical_metrics(api_client,client_,keywords,lang,DB,start_d, end_d)
            st.write("Google Keyword Planner Volume data")
            st.dataframe(overview,hide_index =True,use_container_width=True)
            st.write("Google Keyword Palnner App Monthly Volume data")
            st.dataframe(monthly_results,hide_index =True,use_container_width=True)
            st.write("\n\n\n\n\n")
            
            # Plotly graph 
            fig = px.line(graph, x=graph.index, y=graph.columns, title='Keywords volume overtime')
            fig.update_xaxes(dtick="M1", tickformat="%b\n%Y")
            st.plotly_chart(fig, use_container_width=True)
            st.write("\n\n\n\n\n")
            #your_brand_domain_input = your_brand_domain.split(',')
            # Fetch and display SEO data
            
            #rank_,rankings, competition = brand_ranking(keywords,DB_sem.lower(),your_brand_domain_input)
            #st.write("SemRush Keyword's ranking ")
            #if not rankings.is_empty():
                #filtered_rankings = dataframe_explorer(rankings.to_pandas(), case=False)
                #st.dataframe(filtered_rankings,hide_index =True,use_container_width=True)
                #filtered_competition = dataframe_explorer(competition.to_pandas(), case=False)
                #st.dataframe(filtered_competition,hide_index =True,use_container_width=True)
            #else: 
                #filtered_competition = dataframe_explorer(competition.to_pandas(), case=False)
                #st.dataframe(filtered_competition,hide_index =True,use_container_width=True)
            #try:     
                #myAPIToken = 'c186250c0f3ba9502c38caa53efc7edb'
                #params = {
                    #"action": "export_ctr",
                    #"token": myAPIToken,  # Get token from environment variable
                    #"inputs": f'{{"date":"{web_date_final}", "searches-type":"{web_search}", "value":"{web_val}", "device":"{web_device}", "audience":"{web_aud}", "format":"json"}}'
                #}
                
                #url = f"https://api.awrcloud.com/v2/get.php"
    
                #response = requests.get(url, params=params)
                # Make sure the request was successful before processing
                #data = response.json()
                #web_ranking = pl.DataFrame(data["details"]).with_columns(pl.col("position").cast(pl.Int32).alias("position"))
                #final_table = competition.vstack(rank_).join(web_ranking,left_on="brand_ranking", right_on="position").join(overview,left_on="keyword",right_on="search_query").select(["brand_domain","keyword","brand_ranking","web_ctr","appro_monthly_search"]).sort(["brand_domain","brand_ranking"], descending=[False, False]).with_columns(pl.col("appro_monthly_search").sum().over("brand_domain").alias("sum")).with_columns((((pl.col("appro_monthly_search")*(pl.col("web_ctr")/100))/pl.col("sum"))*100).alias("Part_des_voix_%")).select(["brand_domain","keyword","Part_des_voix_%"]).to_pandas()
                #st.dataframe(dataframe_explorer(final_table, case=False),hide_index =True,use_container_width=True) 
                #bar_chart = final_table.groupby(['brand_domain'])["Part_des_voix_%"].sum().reset_index().sort_values(by="Part_des_voix_%", ascending=False).head(10)
                #st.dataframe(dataframe_explorer(bar_chart, case=False),hide_index =True,use_container_width=True)    
                    
                #fig_1 = px.bar(bar_chart, y='Part_des_voix_%', x='brand_domain', text_auto='.2s')
                #st.plotly_chart(fig_1, use_container_width=True)
                #st.write("\n\n\n")
                
            #except: 
                #st.error('The domain you chose is not part of the top 100 domains for any of the keyword(s) ! Please choose another domain or change the keyword(s)', icon="🚨")
            #excel_file = to_excel([overview, monthly_results,rankings,competition], ["search_volume_overview", "monthly_search_volume","SemRush_Keyword", "SemRush_Ranking"])
            excel_file = convert_to_excel([overview, monthly_results],sheet_names=["search_volume_overview", "monthly_search_volume"])
            st.download_button(
                    label="Download Excel file",
                    data=excel_file,
                    file_name="dataframes.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            # Initialize the GoogleAdsClient with the credentials and developer token
            #api_client = GoogleAdsClient.load_from_storage("cred.yaml")
