# TopMoviesScraper
# **Web Scraping to SQLite DAG**

## **Overview**
This DAG performs web scraping of data from a webpage, processes it, and stores it in an SQLite database. The DAG is designed to scrape the top 50 films from a webpage, save the data into a CSV file, and then load the data from the CSV file into a SQLite database for further processing.

The DAG is executed daily, and it includes two main tasks:
1. **Scrape Data**: Scrapes the required data from the webpage and saves it into a CSV file.
2. **CSV to SQLite**: Reads the CSV file and stores its contents into a SQLite database.

---

## **Key Components**
### **Airflow DAG**
The DAG is defined with specific parameters to control execution, including retry settings, start date, and email notifications on failure. 

- **Owner**: Specifies the owner of the DAG.
- **Retries**: The DAG will retry failed tasks up to once, with a 5-minute delay between retries.
- **Schedule Interval**: The DAG is scheduled to run daily, starting from one day ago.

### **Data Scraping**
The web scraping functionality is designed to extract data from the webpage specified in the `url`. The target webpage contains a table with the top 50 films, including their ranking, name, and release year.

- The webpage is fetched using an HTTP GET request.
- BeautifulSoup is used to parse the HTML page and extract the data from the table.
- The data is processed into a structured format and saved as a CSV file (`top_50_films.csv`).

### **Data Storage in SQLite**
Once the data is scraped and saved as a CSV, the next task is to load it into an SQLite database.

- The CSV file is read into a Pandas DataFrame.
- A connection to the SQLite database (`Movies.db`) is established.
- The data is inserted into a table called `Top_50` in the database, with the option to replace the table if it already exists.

---

## **Steps**
### **Scraping Data**
The scraping task does the following:
- Sends an HTTP GET request to the provided URL to fetch the HTML content of the webpage.
- Parses the HTML using BeautifulSoup to extract the relevant data (rank, film name, and year) from the table.
- Collects the data into a Pandas DataFrame and saves it as a CSV file (`top_50_films.csv`).

### **Saving Data to SQLite**
The SQLite task performs the following:
- Reads the CSV file containing the scraped data into a Pandas DataFrame.
- Establishes a connection to the SQLite database (`Movies.db`).
- Inserts the data into a table (`Top_50`) in the SQLite database. If the table already exists, it is replaced.

---

## **Task Dependencies**
- **scrape_task**: The first task in the DAG, which scrapes data from the webpage and saves it to CSV.
- **csv_to_sqlite_task**: The second task, which loads the data from the CSV file into the SQLite database.

The tasks are executed in sequence, where `scrape_task` runs first, followed by `csv_to_sqlite_task`.

---
