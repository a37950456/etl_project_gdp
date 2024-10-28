# ETL Pipeline Project: Countries by GDP

## Introduction

In this practice project, you will put the skills acquired through the course to use and create a complete ETL pipeline for accessing data from a website and processing it to meet the requirements.

### Project Scenario

An international firm that is looking to expand its business in different countries across the world has recruited you. You have been hired as a junior Data Engineer and are tasked with creating an automated script that can extract the list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), as logged by the International Monetary Fund (IMF). Since IMF releases this evaluation twice a year, this code will be used by the organization to extract the information as it is updated.

The required data seems to be available on the URL mentioned below:
- [IMF GDP Data](https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29)

The required information needs to be made accessible as a CSV file `Countries_by_GDP.csv` as well as a table `Countries_by_GDP` in a database file `World_Economies.db` with attributes `Country` and `GDP_USD_billion`.

Your boss wants you to demonstrate the success of this code by running a query on the database table to display only the entries with more than a 100 billion USD economy. Also, you should log the entire process of execution in a file named `etl_project_log.txt`.

You must create a Python code `etl_project_gdp.py` that performs all the required tasks.

## Objectives

You have to complete the following tasks for this project:

1. Write a data extraction function to retrieve the relevant information from the required URL.
2. Transform the available GDP information into 'Billion USD' from 'Million USD'.
3. Load the transformed information to the required CSV file and as a database file.
4. Run the required query on the database.
5. Log the progress of the code with appropriate timestamps.

## Getting Started

Clone the repository and run the script:
```bash
git clone [repository-url]
cd [repository-folder]
python etl_project_gdp.py