import streamlit as st
import requests
import polars as pl
import io
import pandas as pd
import streamlit_authenticator as stauth
from google.ads.googleads.client import GoogleAdsClient
from streamlit_extras.dataframe_explorer import dataframe_explorer
from dateutil.relativedelta import relativedelta
import xlsxwriter
import plotly.express as px
from statistics import mean
import datetime
from datetime import  timedelta
pl.Config.set_tbl_hide_column_data_types(True)
import polars.selectors as cs



API_KEY = 'e31f38c36540a234e23b614a7ffb4fc4'

creds = {
    'developer_token' : "q2Om6GmAhWjE2z_p8Da_Fw",
    'client_id' : "899223584116-m7n92thr3co9gr0otu7g64o85r6i46ko.apps.googleusercontent.com",
    'client_secret' : "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",
    'refresh_token' : "1//03aBH-xmgK1j6CgYIARAAGAMSNwF-L9IrHTMIAOSWIcjj147tjSBl5Z83teClPGIF2S-wcGCrCtO83BlRy5VaT4PoPA06_EVx5hQ",
    'use_proto_plus' : "False"} 

@st.cache_data

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
    #final_overview = pl.DataFrame([])
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
        #overview = overview.with_columns(search_query=pl.lit(result.text), appro_monthly_search = pl.lit(metric.avg_monthly_searches),competition_level = pl.lit(metric.competition))
        # These metrics include those for both the search query and any
        # variants included in the response.
        # If the metric is undefined, print (None) as a placeholder.
        
        #final_overview = final_overview.vstack(overview)
        
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
    return final_monthly_results_final.pivot(values ="Appro_monthly", index = "search_query", columns = "Date")
    

def brand_ranking (keywords,DB,your_brand_domain): 
    
    your_brand_position = None

    b = pl.DataFrame([])
    non_rank = pl.DataFrame([])
    rank = pl.DataFrame([])
    rest_ = []
    API_KEY_SEM = 'e31f38c36540a234e23b614a7ffb4fc4'
    for keyword in keywords:
        url = f"https://api.semrush.com/?type=phrase_organic&key={API_KEY_SEM}&phrase={keyword}&export_columns=Kd,Dn,Po,&database={DB}"
        response = requests.get(url)
        # Make sure the request was successful before processing
        
        if response.status_code == 200:
            df = pl.read_csv(io.StringIO(response.text), separator=';', eol_char='\n').with_columns(Key=pl.lit(keyword))
            if df.shape[1] == 3:
                #dfs_r = dfs_r.vstack(df)
                for i in range(len(df)):
                    domain = df['Domain'][i]
                    position = df['Position'][i]
                    Keys = df['Key'][i]
                    for j in range (len(your_brand_domain)):
                        if (domain in your_brand_domain[j]) or (your_brand_domain[j] in domain):
                            your_brand_position = position

                            b = b.with_columns(keyword = pl.lit(Keys),brand_domain = pl.lit(domain),brand_ranking= pl.lit(your_brand_position))
                            rank = rank.vstack(b)
                        elif your_brand_domain[j] not in (df["Domain"]):
                            rest_.append(keyword)
            else: 
                print(keyword)
                non_rank = non_rank.vstack(non_rank.with_columns(keyword = pl.lit(keyword),brand_domain = pl.lit(your_brand_domain),brand_ranking= pl.lit(0)))
        else:
            st.error(response.text)
            st.error(f"Failed to fetch data for keyword: {keyword}. Status Code: {response.status_code}. Check your API")
    final_rest = pl.Series(rest_).unique().to_list()
    for k in range(len(final_rest)) : 
        b = b.vstack(b.with_columns(keyword = pl.lit(final_rest[k]),brand_domain = pl.lit(your_brand_domain),brand_ranking= pl.lit(0)))
         
    if (not non_rank.is_empty()) and (not rank.is_empty()) and (not b.is_empty()) :
      
        rank = rank.vstack(non_rank).vstack(b)  
    elif rank.is_empty() and (not non_rank.is_empty()) and (not b.is_empty()): 
 
        rank = non_rank.vstack(non_rank).vstack(b)
    elif non_rank.is_empty() and (not rank.is_empty()) and (not b.is_empty()): 

        rank = rank.vstack(b)
    elif b.is_empty() and (not rank.is_empty()) and (not non_rank.is_empty()):

        rank = rank.vstack(non_rank)
    elif (not rank.is_empty()) and non_rank.is_empty() and b.is_empty():

        rank = rank
    elif rank.is_empty() and (not non_rank.is_empty()) and b.is_empty():

        rank = non_rank
    elif rank.is_empty() and non_rank.is_empty() and (not b.is_empty()):

        rank = b

    new_rank = rank.group_by(["keyword","brand_domain"]).agg(pl.col("brand_ranking").min())
    return new_rank.pivot(values="brand_ranking",index="keyword",columns="brand_domain")

def ctr(web_date_final,web_search,web_val,web_device,web_aud): 
    myAPIToken = 'c186250c0f3ba9502c38caa53efc7edb'
    params = {
        "action": "export_ctr",
        "token": myAPIToken,  # Get token from environment variable
        "inputs": f'{{"date":"{web_date_final}", "searches-type":"{web_search}", "value":"{web_val}", "device":"{web_device}", "audience":"{web_aud}", "format":"json"}}'
    }
    
    url = f"https://api.awrcloud.com/v2/get.php"

    response = requests.get(url, params=params)

    # Make sure the request was successful before processing
    data = response.json()
    web_ranking = pl.DataFrame(data["details"]).with_columns(pl.col("position").cast(pl.Int64).alias("position")).select(["position","web_ctr"])
    return web_ranking

def rename_columns_with_year_incremented(df):
    new_column_names = []
    for col in df.columns[1:]:
        # Parse the date
        date = datetime.datetime.strptime(col, '%Y-%m-%d')
        # Increment the year by 1
        incremented_date = date.replace(year=date.year + 1)
        # Format the date to "Month Year" with incremented year
        new_col_name = incremented_date.strftime('%B %Y')
        new_column_names.append(new_col_name)
    return df.rename(dict(zip(df.columns[1:], new_column_names)))

def prod(impressions_df,ctr_df):
    impressions_df = rename_columns_with_year_incremented(impressions_df)
    
    impressions_melted = impressions_df.melt(id_vars=["search_query"], variable_name="Month", value_name="Impr")
    
    # Join on Keyword and Month
    joined_df = impressions_melted.join(ctr_df, left_on=["search_query", "Month"],right_on=["Keyword", "Month"])
    
    # Calculate the Product
    joined_df = joined_df.with_columns(((pl.col("Impr") * pl.col("web_ctr"))/100).alias("Clicks"))
    result = joined_df.pivot(index='search_query', columns='Month', values='Clicks')
    return result

def total_sum_volume(df):

    s = pl.Series("search_query", ["Total Volume"])

    # Calculate the sum for each month column
    total_sums = df.select(pl.exclude('search_query')).sum(axis=0).insert_at_idx(0, s)

    return df.vstack(total_sums)


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

    # Allow user to manually enter keywords
    
    

    client_credentials = "Leclerc"  # Add more countries as needed
    
    # Initialize the GoogleAdsClient with the credentials and developer token
    api_client = GoogleAdsClient.load_from_dict(
                name_to_api_key[client_credentials]["credentials"]
            )
    print(api_client)
    client_ = name_to_api_key[client_credentials]["client_id"]
    list_language = language_full_list(api_client,client_)
    list_location=location_full_list(api_client,client_)
    # Create two columns for the keyword input fields
    col1, col2 = st.columns(2)
    col3, col4 = st.columns(2)

    # Text area for brand keywords in column 1
    with col1:
        brand_keywords = st.text_area("Enter Brand Keywords")

    # Text area for non-brand keywords in column 2
    with col2:
        non_brand_keywords = st.text_area("Enter Non-Brand Keywords")
    
    with col3:
        default_ix = list_location.index("France")
        DB = st.selectbox("Select a country:", list_location, index = default_ix) 
    with col4:
        default_ix_2 = list_language.index("French")
        lang = st.selectbox("Select a language:", list_language, index= default_ix_2)  # Add more countries as needed
    
    col5, col6 = st.columns(2)   
    with col5:
        today = datetime.datetime.now()
        min_ = datetime.date(today.year-2,today.month, 1)
        max_ = datetime.date(today.year-1,12, 30)
        max_2 = datetime.date(today.year-1,11, 30)
        max_start = datetime.date(today.year-1,today.month, 30)
        start_d = st.date_input("Choose the start date",value = max_start,format="YYYY/MM/DD",max_value =max_,min_value =min_)
    with col6:
        end_d = st.date_input("Choose the end date",value =max_, format="YYYY/MM/DD",max_value =max_,min_value =min_)
                
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
    st.write("\n\n\n")
    st.title("Coverage Percentage")    
    
    # Create two columns for the percentage input fields
    col7, col8 = st.columns(2)

    # Input field for brand percentage in column 3
    with col7:
        brand_percentage = st.number_input("Enter Percentage for Brand Keywords", min_value=0.0, max_value=1.0, step=0.01, format="%.2f")

    # Input field for non-brand percentage in column 4
    with col8:
        non_brand_percentage = st.number_input("Enter Percentage for Non-Brand Keywords", min_value=0.0, max_value=1.0, step=0.01, format="%.2f")
    
    # Allow user to manually enter keywords
    st.write("\n\n\n")
    st.title("SemRush") 
    your_brand_domain = st.text_input("Enter your brand domain")
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
    default_ix_3 = sem_lang.index("FR")
    DB_sem = st.selectbox("Select a country:", sem_lang, index=default_ix_3) 
    
    searche_type = ["allSearches","branded","search-intent","long-tail","categories"]
    devices = ["allDevices","desktop","mobile","tablet"]
    audience = ["international","us","uk","aus"]
    value = ["exact","average"]
    st.write("\n\n\n")
    
    st.title("Advanced Web Ranking")
    col1, col2,col3 = st.columns(3)
    with col1:
        web_date = st.date_input("Choose a month",value = max_2,format="YYYY-MM-DD",max_value =max_2,min_value =min_)
        web_date = web_date.strftime("%Y-%m-%d")
        web_date_final = set_day_to_15(web_date)
        #web_date_final = datetime.datetime.strptime(web_date_final, "%Y-%m-%d")
    with col2:
        web_search = st.selectbox("Select search type:", searche_type) 
    with col3:
        web_device = st.selectbox("Select search type:", devices)# Add more countries as needed
    col4, col5 = st.columns(2)   
    with col4:
        web_aud = st.selectbox("Select an audience:", audience)  # Add more countries as needed    
    with col5:
        web_val = st.selectbox("Select value type:", value)
            
    if st.checkbox("Submit"):
        
        if  brand_keywords or non_brand_keywords:
            
            keywords_brand = brand_keywords.split('\n')
            keywords_non_brand = non_brand_keywords.split('\n')
            if (keywords_brand ==[""]) and (keywords_non_brand ==[""]):
                st.error('You need to submit at least one keyword', icon="🚨")
                pass 
            elif keywords_brand ==[""] :
                keywords = keywords_non_brand
            elif keywords_non_brand ==[""]:
                keywords = keywords_brand
            else:
                keywords = keywords_brand + keywords_non_brand

            #api_client = GoogleAdsClient.load_from_storage("cred.yaml")
            monthly_results= generate_historical_metrics(api_client,client_,keywords,lang,DB,start_month,start_year,end_month,end_year)
            st.subheader("Search Volume for the last 12 months")
            monthly_results_total = total_sum_volume(monthly_results)
            st.dataframe(monthly_results_total,hide_index =True,use_container_width=True)
            st.write("\n\n\n\n\n")
            your_brand_domain_input = your_brand_domain.split(',')
            # Fetch and display SEO data
            rankings = brand_ranking(keywords,DB_sem.lower(),your_brand_domain_input)
            st.subheader("Ranking")
            if not rankings.is_empty():
                filtered_rankings = dataframe_explorer(rankings.to_pandas(), case=False)
                st.dataframe(filtered_rankings,hide_index =True,use_container_width=True)
            
            else: 
                st.error('This is no ranking', icon="🚨")
            
            month_columns = monthly_results.columns[1:]  # Assuming month data starts from the 2nd column

            df = monthly_results.with_columns([
                (pl.when(pl.col("search_query").is_in(keywords_brand)).then(pl.col(month)*brand_percentage).otherwise(pl.col(month)*non_brand_percentage)).alias(month)
                for i,month in enumerate(month_columns)
            ])
            st.subheader("Impressions for the last 12 months")
            df_total = total_sum_volume(df)
            st.dataframe(df_total,hide_index =True,use_container_width=True)
            # Generate a list of the next 12 months
            current_month = datetime.datetime.now()
            months = [(current_month + relativedelta(months=i+1)).strftime("%B %Y") for i in range(12)]
            # Initialize session state for rankings if not already done
            if 'rankings_data' not in st.session_state:
                st.session_state['rankings_data'] = []
            for keyword in keywords:
                st.write(f"Projection for {keyword}")
                with st.expander("Enter Rankings"):
                    for month in months:
                        input_key = f"{keyword}_{month}"
                        print(input_key)

                        # Assuming 'value_' is correctly set as per your logic
                        value_ = rankings.filter(pl.col("keyword") == keyword).select(cs.last()).item()

                        # Use the value from the existing rankings data as the default, if it exists
                        existing_rank = next((item['Ranking'] for item in st.session_state['rankings_data'] if item['Keyword'] == keyword and item['Month'] == month), value_)

                        # Create the input and update session state on change
                        if value_ == 0: 
                            rank = st.number_input(f"Ranking for {month}", key=input_key, max_value=101, min_value=1, value=101, step=1)
                        elif value_ == 1:
                            rank = st.number_input(f"Ranking for {month}", key=input_key, max_value=value_, min_value=1, value=1, step=1)
                        else : 
                            rank = st.number_input(f"Ranking for {month}", key=input_key, max_value=value_, min_value=1, value=value_, step=1)
                        # Update the rankings data in the session state
                        st.session_state['rankings_data'] = [item for item in st.session_state['rankings_data'] if not (item['Keyword'] == keyword and item['Month'] == month)]
                        st.session_state['rankings_data'].append({'Keyword': keyword, 'Month': month, 'Ranking': rank})

            # Convert the session state data to DataFrame
            st.write("Projected Rankings")
            ranking_data_df = pl.DataFrame(st.session_state['rankings_data'])

            # Pivot the DataFrame
            pivot_df = ranking_data_df.pivot(index='Keyword', columns='Month', values='Ranking')

            # Display the pivot table
            st.dataframe(pivot_df,hide_index =True,use_container_width=True)
            ctr_ = ctr(web_date_final,web_search,web_val,web_device,web_aud)
            # Join with the CTR DataFrame using a left join
            joined_df = ranking_data_df.join(ctr_,left_on="Ranking",right_on="position", how="left")

            # Apply 'where' and 'otherwise' to handle rankings above 20
            joined_df = joined_df.with_columns(pl.col("web_ctr").fill_null(strategy="zero"))

            # Pivot the table back to its original format (if needed)
            result_df = joined_df.pivot(index="Keyword", columns="Month", values="web_ctr")
            st.subheader("CTR")
            st.dataframe(result_df,hide_index =True,use_container_width=True)
            
            clics = prod(df,joined_df)
            st.subheader("Clicks")
            clics_total = total_sum_volume(clics)
            st.dataframe(clics_total.to_pandas().round(),hide_index =True,use_container_width=True)
            total = clics_total.to_pandas().tail(1)
            st.write("\n\n\n")
            
            # Melt the DataFrame using the numeric columns names
            clics_chart_bar = total.melt(id_vars='search_query', 
                               value_vars=[col for col in total.columns if col != 'search_query'],
                               var_name='Month', value_name='Volume')
            st.metric(label="Total Clicks", value=round(clics_chart_bar.sum()["Volume"],0))  
      
            fig_1 = px.bar(clics_chart_bar, x='Month', y='Volume', title='Total Clicks per Month')
            st.plotly_chart(fig_1, use_container_width=True)
            st.write("\n\n\n")
            
            clics_chart_line = clics_total.to_pandas().iloc[:-1].melt(id_vars='search_query', 
                               value_vars=[col for col in total.columns if col != 'search_query'],
                               var_name='Month', value_name='Volume')
            fig_2 = px.line(clics_chart_line, x="Month", y="Volume", color='search_query',title='Total Clicks per Keyword overtime')
            st.plotly_chart(fig_2, use_container_width=True)
            st.write("\n\n\n")
            
        st.write("\n\n\n")
        excel_file = to_excel([monthly_results_total,rankings,df_total,pivot_df,result_df,clics_total,pl.from_pandas(clics_chart_bar),pl.from_pandas(clics_chart_line)], ["Volume", "Ranking","Impressions","Projected Ranking","CTR","Clicks","Bar chart data","Line chart data"])
        st.download_button(
        label="Download Excel file",
        data=excel_file,
        file_name="SEO_Projection.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
        
