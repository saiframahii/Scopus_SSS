import streamlit as st
import time
from elsapy.elsclient import ElsClient
from elsapy.elssearch import ElsSearch
import json
import pandas as pd
from itertools import product
from concurrent.futures import ThreadPoolExecutor, as_completed
from streamlit.logger import get_logger


# Function to perform search
def perform_search(query, index="scopus"):
    print(f"Executing search for query: {query}")
    els_search = ElsSearch(query, index)
    els_search.execute(client, count=config['papers_per_search'])

    results_df = els_search.results_df

    # check if results are empty
    if 'error' in results_df.columns and 'Result set was empty' in results_df['error'].values:
        print(f"No results found for query: {query}")

    else:
        # Select needed columns and rename them
        required_columns  = ['dc:identifier', 'prism:coverDate', 'dc:creator', 'prism:publicationName', 'dc:title', 'prism:doi']

        # Ensure all required columns are present in the DataFrame
        for col in required_columns:
            if col not in results_df.columns:
                results_df[col] = None

        filtered_df = results_df[required_columns ].copy()
        # filtered_df = filtered_df.rename(columns={'prism:doi': 'Original DOI'})
        filtered_df['DOI'] = filtered_df['prism:doi'].apply(lambda doi: f"https://doi.org/{doi}" if pd.notna(doi) else None)
        filtered_df = filtered_df.drop(columns=['prism:doi'])
        filtered_df['Publication Year'] = pd.to_datetime(filtered_df['prism:coverDate'], errors='coerce').dt.year
        filtered_df = filtered_df.rename(columns={
            'dc:identifier': 'Identifier',
            'dc:creator': 'Authors',
            'prism:publicationName': 'Journal',
            'dc:title': 'Title',
            'prism:coverDate': 'Publication Date'
        })

        # Add the query to the DataFrame
        filtered_df['Query'] = query

        return filtered_df

# Function to handle a single search combination
def search_combination(combination):
    query = f'TITLE-ABS-KEY({combination[0]} AND {combination[1]} AND {combination[2]})'

    # filter with document type
    doctype_query = " OR ".join([f'LIMIT-TO(DOCTYPE, "{doctype}")' for doctype in config['doctype']])
    if config['doctype']:
        query += f' AND ({doctype_query})'

    # filter with publication year
    if config['publication_year']:
        pubyear = config['publication_year']
        if '-' in pubyear:
            start_year, end_year = pubyear.split('-')
            query += f' AND PUBYEAR > {start_year} AND PUBYEAR < {end_year}'
        else:
            query += f' AND (LIMIT-TO(PUBYEAR, {pubyear}))'

    # filter with author name
    if config['author_name']:
        query += f' AND AUTHOR-NAME({config["author_name"]})'

    print(f"Executing search for query: {query}")
    results = perform_search(query)
    return query, results

# Initialize session state for results
if "all_results" not in st.session_state:
    st.session_state.all_results = pd.DataFrame()

if "log_container" not in st.session_state:
    st.session_state.log_container = st.sidebar.container()
    st.session_state.log_container.header("Search Logs")

# Streamlit app
st.title("Scopus Sub-Keyword Synonym Search")

# Load configuration from user input
papers_per_search = st.number_input("Papers per Search", min_value=1, value=10)
publication_year = st.text_input("Publication Year (e.g., 2020 or 2015-2024)", "")
author_name = st.text_input("Author Name", "")
doctype = st.multiselect("Document Types", ["ar", "ab", "bk", "ch", "cp", "cr", "dp", "ed", "er", "le", "mm", "no", "rp", "tb", "re", "sh"])

first_subkeywords = st.text_area("First Sub-keywords (comma separated)", "lighting control,shading control,daylighting").split(",")
second_subkeywords = st.text_area("Second Sub-keywords (comma separated)", "reinforcement learning,machine learning,artificial intelligence").split(",")
third_subkeywords = st.text_area("Third Sub-keywords (comma separated)", "buildings,office buildings,commercial buildings").split(",")

if st.button("Run Search"):
    config = {
        "apikey": "fbfe4e46c250211aed09a652acf325f6",
        "papers_per_search": papers_per_search,
        "author_name": author_name,
        "publication_year": publication_year,
        "doctype": doctype,
        "first_subkeywords": first_subkeywords,
        "second_subkeywords": second_subkeywords,
        "third_subkeywords": third_subkeywords
    }

    client = ElsClient(config['apikey'])

    search_combinations = list(product(first_subkeywords, second_subkeywords, third_subkeywords))

    total_queries = len(search_combinations)
    st.info(f"Total number of queries: {total_queries}")

    start_time = time.time()

    query_logs = []

    with st.session_state.log_container:

        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_combination = {executor.submit(search_combination, combination): combination for combination in search_combinations}
            for future in as_completed(future_to_combination):
                combination = future_to_combination[future]
                try:
                    query, results = future.result()
                    st.write(f"Executing search for query: {query}")
                    if results is not None and not results.empty:
                        st.session_state.all_results = pd.concat([st.session_state.all_results, results], ignore_index=True)
                    else:
                        st.write(f"No results found for query: {query}")
                except Exception as exc:
                    st.write(f'Combination {combination} generated an exception: {exc}')

    end_time = time.time()
    elapsed_time = end_time - start_time

    st.info(f"Total time taken for all queries: {elapsed_time:.2f} seconds")
    total_papers = len(st.session_state.all_results)
    st.info(f"Total count of papers: {total_papers}")

    st.dataframe(st.session_state.all_results)

# Show save button and text input outside the search button if condition
if not st.session_state.all_results.empty:
    csv_name = st.text_input("Enter CSV file name (with .csv extension):", "search_results.csv")
    if csv_name:
        csv_data = st.session_state.all_results.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download CSV",
            data=csv_data,
            file_name=csv_name,
            mime='text/csv'
        )

   