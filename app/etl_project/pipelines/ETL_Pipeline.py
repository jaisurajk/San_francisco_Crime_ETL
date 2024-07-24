import requests
import pandas as pd 
from dotenv import load_dotenv
from pathlib import Path
import yaml
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, Table, Column, String, Integer, Boolean, Float, JSON, DateTime, Date, MetaData, inspect
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine.base import Engine
import os
import schedule
import logging
import time

def __init__(self, pipeline_name: str, log_folder_path: str):
    self.pipeline_name = pipeline_name
    self.log_folder_path = log_folder_path
    logger = logging.getLogger(pipeline_name)
    logger.setLevel(logging.INFO)
    self.file_path = (
        f"{self.log_folder_path}/{time.time()}.log" 
    )
    file_handler = logging.FileHandler(self.file_path)
    file_handler.setLevel(logging.INFO)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    self.logger = logger

def get_logs(self) -> str:
    """
    Returns contents of log file as str object.
    """
    with open(self.file_path, "r") as file:
        return "".join(file.readlines())

def extract_crime_api(APP_TOKEN:str, column_name:str, start_time:str, end_time:str, limit:int) -> pd.DataFrame:
    # start_time='2023-11-01T00:00:00.000', 
    # end_time='2023-12-31T23:59:59.999', 
    #soql_date = f"where=incident_datetime between '{start_time}' and '{end_time}'" 
    soql_date = f"where={column_name} between '{start_time}' and '{end_time}'" 
    ### data set - crime incidents reported in SFO ##########
    response_data = []
    i = 0
    # APP_TOKEN = "ef0oV4r2jOuH9KGAEwWRQfrKl"
    # limit=20000
    offset = 0
    while True:
        print(i)
        offset = i * limit # if limit = 1000 -> offset = 0, 1000, 2000, etc.
        # print("offset", offset)
        # print("limit", limit)
        response = requests.get(f"https://data.sfgov.org/resource/wg3w-h783.json?"
                                f"$$app_token={APP_TOKEN}"
                                f"&${soql_date}"
                                f"&$limit={limit}"
                                f"&$offset={offset}"
                                f"&$select=:*,*") # include metadata field info
        # print(response.request)
        print(response.status_code)
        if not response.status_code==200:
            raise Exception

        if i >= 1000:
            raise Exception

        if response.json() == []:
            break

        response_data.extend(response.json())
        i += 1
        crime_df = pd.json_normalize(data=response_data)
        # print(crime_df.size)

    crime_df = pd.json_normalize(data=response_data)
    # print(crime_df.size)
    # print(crime_df.columns)
    # print(crime_df["cnn"])
    return crime_df

def transform_crime_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform data transformations on the input DataFrame.

    Parameters:
    - df (pd.DataFrame): Input DataFrame to be transformed.

    Returns:
    - pd.DataFrame: Transformed DataFrame.

    Transformations:
    1. Drop specified columns:
        - ':@computed_region_jwn9_ihcz'
        - ':@computed_region_h4ep_8xdi'
        - ':@computed_region_n4xg_c4py'
        - ':@computed_region_nqbw_i6c3'
        - ':@computed_region_26cr_cadq'
        - ':@computed_region_qgnn_b9vv'
        - 'supervisor_district_2012'
        - 'latitude'
        - 'longitude'
        - 'point.type'
        - 'point.coordinates'
        - ':@computed_region_jg9y_a9du'

    2. Rename columns based on the specified mapping:
        - ':id' -> 'id'
        - ':created_at' -> 'created_at'
        - ':updated_at' -> 'updated_at'
        - ':version' -> 'version'
    """

    # print("Original columns")
    # print(df.columns)
    # Transformation 1: Drop columns
    cols_to_drop = [
        ':@computed_region_jwn9_ihcz', 
        ':@computed_region_h4ep_8xdi',
        ':@computed_region_n4xg_c4py', 
        ':@computed_region_nqbw_i6c3',
        ':@computed_region_26cr_cadq', 
        ':@computed_region_qgnn_b9vv',
        ':@computed_region_jg9y_a9du',
        'latitude',
        'longitude',
        'point.type',
        'point.coordinates',
        'supervisor_district_2012',
        'incident_datetime',
        'report_datetime',
        'row_id',
        'incident_id',
        'incident_number',
        'report_type_code',
        'report_type_description',
        'filed_online',
        'incident_code',
        'supervisor_district',
        'cad_number',
    ]
    # print("Dropped columns")
    # print(cols_to_drop)
    df = df.drop(columns=cols_to_drop)
    # print(df.columns)
    # Transformation 2: Rename columns
    col_mapping = {
        ':id':'crime_id',
        ':created_at':'created_at',
        ':updated_at':'updated_at',
        ':version':'version',
    }
    df = df.rename(columns=col_mapping)
    #df.delete row where police_district is NULL or empty

    # print("current columns")
    # print(df.columns)
    # new_df = df['crime_id']
    new_df = df[["crime_id", 
                 "incident_date", 
                 "incident_time", 
                 "incident_year", 
                 "incident_day_of_week",
                 "incident_category", 
                 "incident_subcategory", 
                 "incident_description",
                 "resolution",
                 "police_district",
                 "intersection",
                 "cnn",
                 "analysis_neighborhood"
                ]]
    # print(df.columns)
    #new_df.dropna(how="any",inplace=True)
    # new_df2 = df.,
    # print(type(new_df))
    # print(new_df)
    # print(df.columns)
    return new_df

def create_crime_table(engine:Engine) -> Table:
    """
    Create table for crimes data with applicable column names. 
    """
    meta = MetaData()
    table = Table(
        "crime_data", meta, 
        Column("crime_id", String, primary_key=True),
        Column("incident_date", String),
        Column("incident_time", String),
        Column("incident_year", String),
        Column("incident_day_of_week", String),
        Column("incident_category", String),
        Column("incident_subcategory", String),
        Column("incident_description", String),
        Column("resolution", String),
        Column("police_district", String),
        Column("intersection", String),
        Column("cnn", String),
        Column("analysis_neighborhood", String),
    )
    meta.create_all(bind=engine, checkfirst=True) # does not re-create table if it already exists
    return table

def extract_police_station(csv_file_path):
    """
    Return a dataframe object with transformed columns based on a given CSV file path.
    """
    df = pd.read_csv(csv_file_path)
    df.columns = [column.lower().replace(" ","_") for column in df.columns]
    return df

def transform_police_station_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform data transformations on the input DataFrame.

    Parameters:
    - df (pd.DataFrame): Input DataFrame to be transformed.

    Returns:
    - pd.DataFrame: Transformed DataFrame.

    Transformations:
    1. Drop specified columns:
        - ':@computed_region_jwn9_ihcz'
        - ':@computed_region_h4ep_8xdi'
        - ':@computed_region_n4xg_c4py'
        - ':@computed_region_nqbw_i6c3'
        - ':@computed_region_26cr_cadq'
        - ':@computed_region_qgnn_b9vv'
        - 'supervisor_district_2012'
        - 'latitude'
        - 'longitude'
        - 'point.type'
        - 'point.coordinates'
        - ':@computed_region_jg9y_a9du'

    2. Rename columns based on the specified mapping:
        - ':id' -> 'id'
        - ':created_at' -> 'created_at'
        - ':updated_at' -> 'updated_at'
        - ':version' -> 'version'
    """
    # Transformation 1: Drop columns
    cols_to_drop = [
        'location'
    ]
    df = df.drop(columns=cols_to_drop)
    # print(df.columns)
    return df

def extract_holidays(csv_file_path):
    """
    Return a dataframe object with transformed columns based on a given CSV file path.
    """
    df = pd.read_csv(csv_file_path)
    df.columns = [column.lower().replace(" ","_") for column in df.columns]
    return df

def transform_holidays(date_df: pd.DataFrame) -> pd.DataFrame:
    date_df['date'] = pd.to_datetime(date_df['date'])
    date_df['day'] = date_df['date'].dt.day
    date_df['month'] = date_df['date'].dt.month
    date_df['month_name'] = date_df['date'].dt.month_name()
    date_df['year'] = date_df['date'].dt.year
    date_df['day_of_week'] = date_df['date'].dt.dayofweek
    date_df['day_of_week_name'] = date_df['date'].dt.day_name()
    return date_df

def create_postgres_connection(username:str, password:str, host:str, port:int, database:str) -> Engine:
    """
    Connect to postgres server using provided pgAdmin credentials.
    """
    connection_url = URL.create(
        drivername="postgresql+pg8000", 
        username=username,
        password=password,
        host=host, 
        port=port,
        database=database)

    return create_engine(connection_url)

# def create_crime_table(engine:Engine) -> Table:
#     """
#     Create table for crimes data with applicable column names. 
#     """
#     meta = MetaData()
#     table = Table(
#         "crime_data", meta, 
#         Column("crime_id", String, primary_key=True),
#         Column("created_at", String),
#         Column("updated_at", String),
#         Column("version", String),
#         Column("incident_datetime", String),
#         Column("incident_date", String),
#         Column("incident_time", String),
#         Column("incident_year", String),
#         Column("incident_day_of_week", String),
#         Column("report_datetime", String),
#         Column("row_id", String),
#         Column("incident_id", String),
#         Column("incident_number", String),
#         Column("incident_code", String),
#         Column("report_type_description", String),
#     )
#     meta.create_all(bind=engine, checkfirst=True) # does not re-create table if it already exists
#     return table

def create_date_table(engine:Engine) -> Table:
    """
    Create table for 2023 and 2024 dates and holiday data. 
    """
    meta = MetaData()
    table = Table(
        "holiday", meta, 
        Column('date',Date,primary_key=True),
        Column('name', String),
        Column('day',Integer),
        Column('month',Integer),
        Column('month_name',String),
        Column('year',Integer),
        Column('day_of_week',Integer),
        Column('day_of_week_name',String),
    )
    meta.create_all(bind=engine)
    return table

def create_police_table(engine:Engine) -> Table:
    """
    Create table for Chicago police district data. 
    """
    meta = MetaData()
    table = Table(
        "police_stations", meta,
        Column('company_name',String,primary_key=True),
        Column('district_name',String),
        Column('address',String),
        Column('telephone_number',String)
    )
    meta.create_all(bind=engine)
    return table

def get_schema_metadata(engine: Engine) -> Table:
    metadata = MetaData(bind=engine)
    metadata.reflect()  # get target table schemas into metadata object
    return metadata


def _create_table(table_name: str, metadata: MetaData, engine: Engine):
    # testdict = {}
    # print(metadata.tables)
    # print(f"Table name = {table_name}")
    # table_name = "police_stations"
    mytable_name = table_name
    # print(f"Table name = {table_name}")
    existing_table = metadata.tables[f'{table_name}']
    new_metadata = MetaData()
    columns = [
        Column(column.name, column.type, primary_key=column.primary_key)
        for column in existing_table.columns
    ]
    new_table = Table(table_name, new_metadata, *columns)
    new_metadata.create_all(bind=engine)
    return new_metadata


def load_data_to_postgres(chunksize:int, data:list[dict], table_name:Table, engine:Engine) -> None:
    """
    Upsert data incrementally (chunking) into specific postgres table. 
    """
    source_metadata = get_schema_metadata(engine=engine)
    target_metadata = _create_table(
        table_name=table_name, metadata=source_metadata, engine=engine
    )
    # print("Table")
    # print(target_metadata.tables)
    table = target_metadata.tables[table_name]
    key_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]
    insert_statement = postgresql.insert(table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=key_columns,
        set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns},
    )
    engine.execute(upsert_statement)
    
def create_logs_table(engine:Engine) -> Table:
    """
    Create table for pipeline metadata logs. 
    """
    meta = MetaData()
    table = Table(
        "logs", meta, 
        Column('run_id',Integer,primary_key=True),
        Column('status',String,primary_key=True),
        Column('pipeline_name',String),
        Column('timestamp',DateTime(timezone=True)),
        Column('config',JSON),
        Column('logs',String)
    )
    meta.create_all(bind=engine, checkfirst=True) # does not re-create table if it already exists
    return table

def create_logs_data(run_id:int, status:str, pipeline_name:str, config:dict, logs:str) -> list[dict]:
    """
    Returns a list[dict] object to be used as data argument in load_data_to_postgres function for inserting logs data to database.
    """
    return [{
        "run_id":run_id, 
        "status":status, 
        "pipeline_name": pipeline_name, 
        "timestamp":datetime.now(), 
        "config": config, 
        "logs":logs}]

def get_logs_table_run_id(logs_table_name:str, engine: Engine) -> int:
    """
    Returns next run_id as int from logs table (current max of run_id column + 1).
    """
    select_max_run_id_query = f"select max(run_id) from {logs_table_name}"
    max_run_id = [dict(row) for row in engine.execute(select_max_run_id_query).all()][0].get("max")
    if max_run_id == None:
        return 1
    return max_run_id+1

def run_pipeline_schedule(pipeline_config:dict):
    
    #     pipeline_end_time = time.time()
            print("Pipeline start")
            print("Helloe world")
            print("Pipeline start")
            # pipeline_start_time = time.time()

            # pipeline_run_time = pipeline_end_time - pipeline_start_time
            # pipeline_logging.logger.info(f"Pipeline finished in {pipeline_run_time} seconds")
            # pipeline_logging.logger.info("Successful pipeline run")
            
            #
    
if __name__=="__main__":
    load_dotenv()
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
    start_time='2023-12-25T00:00:00.000' 
    end_time='2023-12-31T23:59:59.999'
    APP_TOKEN = "ef0oV4r2jOuH9KGAEwWRQfrKl"
    limit = 20000
    column_name = "incident_datetime"
    df = extract_crime_api(APP_TOKEN, column_name, start_time, end_time, limit)
    # print(df)
    df_transform_crime = transform_crime_data(df)
    # print(df_transform)
    # print(df_transform.columns)
    # print(df_transform.dtypes)
    config = pipeline_config.get("config")
    police_station_data_path = config.get("police_station_data_path")
    df_police_extract = extract_police_station(police_station_data_path)
    # print(df_police_extract)
    # print(df_police_extract.columns)
    df_police_transform = transform_police_station_data(df_police_extract)
    # print(df_police_transform)
    config_holidays = pipeline_config.get("config")
    holidays_data_path = config_holidays.get("holidays_data_path")
    df_holidays_extract = extract_holidays(holidays_data_path)
    # print(df_holidays_extract)
    df_holidays_transform = transform_holidays(df_holidays_extract)
    # print(df_holidays_transform)
    # print("df_holidays_transform.columns")
    # print(df_holidays_transform.columns)

    APP_TOKEN = os.environ.get("APP_TOKEN")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    # Connecting to postgres
    engine = create_postgres_connection(
        username=DB_USERNAME, 
        password=DB_PASSWORD, 
        host=SERVER_NAME, 
        port=PORT, 
        database=DATABASE_NAME)
    # print(engine)
    crime_table=create_crime_table(engine=engine)
    # print(crime_table)
    date_table = create_date_table(engine=engine)
    # print(date_table)
    police_table = create_police_table(engine=engine)
    # print(police_table)
    chunksize=config.get("chunksize")
    # Creating table in database for pipeline metadata logs (does not re-create table if it already exists)
    logs_table = create_logs_table(engine=engine)
    logs_table_name=config.get("logs_table_name")
    # # Extracting next run_id value to be used for writing new records to metadata logs table
    # run_id = get_logs_table_run_id(logs_table_name=logs_table_name, engine=engine)
    # Log pipeline start to logs table in postgres
    pipeline_name=pipeline_config.get("name")
    # logs_data = create_logs_data(run_id=run_id, status="start", pipeline_name=pipeline_name, config=config, logs=None)
    data = df_transform_crime.to_dict(orient='records')
    load_data_to_postgres(chunksize=chunksize, data=data, table_name=crime_table, engine=engine)

    #load police stations data from dataframe to database table 'police_stations'
    data = df_police_transform.to_dict(orient='records')
    load_data_to_postgres(chunksize=chunksize, data=data, table_name=police_table, engine=engine)

    #load holidays data from dataframe to database table 'holiday'
    # print(df_holidays_transform['date'])
    data = df_holidays_transform.to_dict(orient='records')
    load_data_to_postgres(chunksize=chunksize, data=data, table_name=date_table, engine=engine)

    print(type(df_transform_crime))
    data = df_transform_crime.to_dict(orient='records')
    # data = df_transform_crime
    load_data_to_postgres(chunksize=chunksize, data=data, table_name=crime_table, engine=engine)

    # schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(
    #     run_pipeline_schedule,
    #     pipeline_config=pipeline_config
    # )
    # while True:
    #     schedule.run_pending()
    #     time.sleep(pipeline_config.get("schedule").get("poll_seconds"))
    