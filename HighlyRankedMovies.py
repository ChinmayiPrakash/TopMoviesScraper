from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import timedelta

#define default args 
default_args = {
    'owner' : 'chinmayi',
    'depends_on_past':False,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),

}
#define dag
dag = DAG(
    'web_scraping_to_sql',
    default_args=default_args,
    description='A DAG to scrape data from a webpage and store it in SQLite',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

url = "{{var.value['Highly-Ranked-Films']}}"
csv_path = 'top_50_films.csv'
df = pd.DataFrame(columns=["Average Rank","Film","Year"])
count = 0
db_name = 'Movies.db'
table_name = 'Top_50'

# Function to scrape data and save it to CSV
def scrape_data(**kwargs):
    global count , df , csv_path , url
    # Load the webpage by get Request
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    # Scraping Required info
    # Fetches all the tables in the webpage
    tables = data.find_all('tbody')
    # Fetches all the rows from table
    rows = tables[0].find_all('tr')

    for row in rows:
        if count < 50:
            col = row.find_all('td')
            if len(col) != 0:
                data_dict = {'Average Rank': col[0].contents[0], 'Film': col[1].contents[0], 'Year': col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
                count += 1

    # Save to CSV
    df.to_csv(csv_path, index=False)

# Function to save CSV data to SQLite
def csv_to_sqlite(**kwargs):
    #csv_path = 'C:\\Users\\chinm\\Documents\\Python_Programs\\BeautifulSoups\\top_50_films.csv'
    #db_name = 'Movies.db'
    #table_name = 'Top_50'
    global csv_path , db_name , table_name
    # Load CSV into DataFrame
    df = pd.read_csv(csv_path)

    # Save DataFrame to SQLite
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

# Define the tasks
scrape_task = PythonOperator(
    task_id='scrape_data',
    python_callable=scrape_data,
    dag=dag,
)

csv_to_sqlite_task = PythonOperator(
    task_id='csv_to_sqlite',
    python_callable=csv_to_sqlite,
    dag=dag,
)

# Set task dependencies
scrape_task >> csv_to_sqlite_task