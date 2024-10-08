# July Project 1 - San Francisco Crime

## Project Context and Goals

The goal of this project was to create an end-to-end pipeline that will extract data from a constantly-updating (live) dataset and successfully load it to a relational database, imitating a real-life data warehouse. Transformations on the data would be done within the pipeline prior to loading (ETL), and also after loading (ELT), to simulate potential data manipulation requirements of real-time applications.

The data should be loaded in an easy-to-use format so as to allow the data analysts(DA)/data scientists(DS) to immediately begin working with it without having to commit a sizeable amount of time to data manipulations. This means that the data should be loaded onto several distinct tables that can easily be merged with each other and that contains intuitive column names and data types for each distinct record.

The DA/DS person should not be concerned with the accuracy of each data record and the process by which each data record is inserted or updated within the database. The pipeline should abstract and automate all of these processes as much as possible.

## Business Objective

The objective of our project is to provide analytical datasets from our San Francisco crime API and supporting police stations and holiday databases. The datasets are clean, regularly updated, and supported with several custom table views generated from these datasets.

## Consumers

The users of our data are San Francisco city budget and policy analysts and officials. Our data is useful for getting up-to-date information on crimes that occur throughout San Francisco. Our users would want to access the data primarily through sql queries and the custom sql views from the database. This data can also be used to create dashboards with maps and metrics.

## Questions

There are three general question categories our data addresses: broad questions about crime in the city of San Francisco, questions about crime incidents during holidays, and crime incidents within police districts.

Examples of these questions include but are not limited to:

- What day of the week seems to have the highest number of crimes?
- What month of the year has the most crimes?
- What time of the day (morning, afternoon, evening, night) seems to have the highest number of crimes?
- Which holidays have the highest incidences of crime and may require additional staffing or resources?
- Which police districts may need additional resources, specialized training, etc. based on the type and quantity of crime?
- Which intersection have the most violent crimes?

Our data will allow our users to make resourcing and policy decisions based on historical and newly generated data.

## Source datasets

The chosen live dataset for the project was the [San Francisco Crimes 2018 to Present](https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-2018-to-Present/wg3w-h783/about_data) which stores all reported crime incidents in the city of San Francisco, California. This public dataset provides the following advantages:

- The San Francisco Data Portal provides an API endpoint that allows unlimited HTTP requests to extract the dataset.
- The dataset contains a significant amount of distinct columns (fields) that can then be joined separately with a different dataset to facilitate data aggregations.
- The dataset also contains system fields that hold metadata information for each record (such as when the row was created, last updated, and a unique identifier for the row).
- The dataset can be queried prior to extraction based on a SQL-like query language in order to specify which exact data you need (e.g. data created after a specific date).

In addition to the San Francisco Crimes data, we used static datasets for [2023 and 2024 holidays](https://www.sf.gov/information/city-and-county-san-francisco-holiday-schedule) and [San Francisco police stations](https://data.sfgov.org/Public-Safety/Police-Stations-2011-/rwdu-9wb2/about_data). 

## Solution Architecture Diagram

Below is the solution architecture description and diagram, illustrating the key components and their interactions in our project.

<img width="1000" alt="Screenshot 2024-07-26 at 1 20 14 PM" src="https://github.com/user-attachments/assets/7a43b72d-f821-4cb5-8db7-5e70a21eb74a">

- **Python** was used for:
  - Extracting the crime data, via an API, from the city of chicago website.
  - Reading .csv files into pandas dataframes. This is for Holidays, police stations, and wards data.
  - Transforming data -> drop unnecessary columns, rename columns, and create a calendar (dates) dataframe that merges all the dates in 2023 and 2024 with the holidays information from the csv file.
  - Load data to our postgres database.

- **PostgreSQL DBMS** was used for:
  - Storing all our data: crime data, dates, police stations, and ward offices.

- **AWS RDS** was used for:
  - Hosting and managing our postgres database.

- **SQL** was used for:
  - Creating views off of the data that is loaded

- Other programs used:
  - **Docker** was used to containerize our pipeline
  - **ECR** was used to host our docker container
  - **ECS** was used to run the docker container
  - **S3** was used to store the `.env` file.
  
## ELT/ETL Techniques Applied

I employed Extract, Transform, Load (ETL) and Extract, Load, Transform (ELT) techniques to efficiently process and transform raw data into a format suitable for analysis.

### Extraction Pattern

I am using a live dataset that updates periodically (daily). My pipeline first checks if the database exists. If the database doesn't exist, for the first time the code runs, the pipeline extracts a thousand records at a time, and moves the offset until the end of the current day's data. Then the pipeline completes the ETL process and loads the data into the database, which is hosted and managed on AWS RDS. This serves as a backfill of the database. 

### Data Transformation Patterns

#### ETL

There are two places I use data transformation patterns. The first is after the extraction of the crime data and import of the .csv data. I used Pandas to drop columns, change column names, and generate the holiday dataframe.

#### ELT

The second set of transformations happens after the data has been loaded into the database. I use sql templates to generate views in the database. These transformations include CTEs, joining, grouping, sorting, and aggregation functions. The SQL transformations result in several table views in the database. The below ERD diagram for the tables and views can be seen below:

<img width="823" alt="Screenshot 2024-07-26 at 7 35 50 PM" src="https://github.com/user-attachments/assets/c0a9d86d-716a-4482-89bb-f27212411a0c">

### Data Loading Patterns

Our pipeline extracts, transforms and loads one weeks worth of data at a time until the database has been completely backfilled.

## Limitations and Lessons Learned

Throughout the course of the project, I encountered various challenges and gained valuable insights. Here are some key lessons learnt during the project:

- It was very important for me to be able to practice each step of the process. This was extremely valuable in contributing to our learning but did slow us down and may have added additional work or stress toward the end.
- We had hoped to eliminate some of the technical debt in the code by making it more modular or object oriented. This is something that can be a follow on task.
- We started by using Trello to manage tasks but found it worked better for our group to orangize tasks in the issues section of github. We were able to add code, comments and have discussion for each issue.
- Include a `.dockerignore` file in the directory when building a docker image. If using a secrets file or an `.env` file, make sure to add it to the .dockerignore file so that it isn't included in the image.
- When running the docker container on ECS, the cluster needs to be set up using a VPC with private subnets and a NAT Gateway. This can be done by searching for VPC on AWS, and then creating a VPC. Without this, I was getting the following error:

   ```md
   Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x7f02624f9e20>: Failed to establish a new connection: [Errno 110] Connection timed out')
   ```

## Installation and Running Instructions

### 1. Install PostgreSQL and pgAdmin

Download PostgreSQL and pgAdmin by going to <https://www.postgresql.org/download/> and selecting the installer for your Operating System. Download the latest version of PostgreSQL.

After downloading the installer, run the installer on your computer.

Select all options for when promoted for the components you wish to install.

<img width="687" alt="Screenshot 2024-07-26 at 7 43 00 PM" src="https://github.com/user-attachments/assets/92970851-9696-49b9-986a-7a64d4f38d32">

Enter a password:

<img width="692" alt="Screenshot 2024-07-26 at 7 43 57 PM" src="https://github.com/user-attachments/assets/421c1637-6e96-4e76-8bf7-ad68c99a6b21">

Enter a port number. Note: The default port number `5432` is usually used when using PostgreSQL.

When installed successfully, you should be able to find `pgAdmin 4` in your applications.

<img width="481" alt="Screenshot 2024-07-26 at 7 44 33 PM" src="https://github.com/user-attachments/assets/de1bb61b-ded6-47cc-8395-ede9e8307348">

### 2. Clone Repository

Clone the github repository using the options from the `< > Code` drop down menu here:

<img width="400" alt="Screenshot 2024-07-26 at 8 05 35 PM" src="https://github.com/user-attachments/assets/dada8b63-9e9f-48bb-b4cf-b0f72d364471">

### 3. Get App Token

In order to execute the pipeline, an App Token is needed to send requests through the API. To obtain the App Token, please follow the link [here](https://data.cityofchicago.org/profile/edit/developer_settings).

### 4. Create .env file

Create an `.env` file in the main directory with the following format:

```py
# City of San Francisco App Token
APP_TOKEN=<App Token>

# db
DB_USERNAME=<postgresql db username>
DB_PASSWORD=<postgresql db password>
SERVER_NAME=<servername>  # usually `localhost`
DATABASE_NAME=<name of db>
PORT=<port>  # usually 5432
```

### 5. Run pipeline

Once the .env file is created with the relevant App Token and database information, the pipeline can be ran using python. Assuming python is installed on your system, type the following command into the terminal from the directory:

```bash
python -m etl_project.pipeline
```

Depending on the system it will take at least 5 mins to run. On the first run the pipeline will incrementally backfill the database with all the available crime records from the prior year. After that, on subsequent runs, it will upsert data based on the currently stored max `updated_at` date.

---

## AWS Screenshots

Docker was used to containerize the pipeline, which was then loaded into AWS ECR, and ran on AWS ECS. Below are screenshots of this:

### AWS ECS

Screenshot of task running:

<img width="891" alt="Screenshot 2024-07-26 at 8 10 10 PM" src="https://github.com/user-attachments/assets/cce81d20-c8f3-42a3-a561-d07526f8db36">

Screenshot of logs:

<img width="875" alt="Screenshot 2024-07-26 at 8 11 06 PM" src="https://github.com/user-attachments/assets/46664db5-58c5-443c-8c4e-156427fc4bd6">

### AWS ECR

Screenshot of image in ECR repository:

<img width="926" alt="Screenshot 2024-07-26 at 8 11 43 PM" src="https://github.com/user-attachments/assets/dbaa6630-a2be-4f4b-8892-510fece7bf6a">

### AWS RDS

Screenshot of database in RDS:

<img width="895" alt="Screenshot 2024-07-26 at 8 12 25 PM" src="https://github.com/user-attachments/assets/80eebbcf-320c-4dea-b281-cb94164a81ef">

### ECS Task

<img width="889" alt="Screenshot 2024-07-26 at 12 00 27 PM" src="https://github.com/user-attachments/assets/ee0d3b94-b515-49bf-bb7d-86022235cea6">

<img width="866" alt="Screenshot 2024-07-26 at 11 59 09 AM" src="https://github.com/user-attachments/assets/bf686da4-62b5-45da-894e-88ede993ff53">

### ETL flow Logger

<img width="867" alt="Screenshot 2024-07-26 at 11 54 21 AM" src="https://github.com/user-attachments/assets/226277d2-dddb-4277-a444-b698d09d66ba">

### Scheduled using cloud watch

<img width="909" alt="Screenshot 2024-07-26 at 11 49 25 AM" src="https://github.com/user-attachments/assets/57e21ee8-14e3-48ce-a52f-dccd9c16c763">

