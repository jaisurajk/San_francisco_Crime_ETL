import requests
import pandas as pd 

# response = requests.get(f"https://data.sfgov.org/resource/rwdu-9wb2.json")
# print(response.status_code)
# print(response.request)
# response_data = response.json()
# print(response_data)
# crime_df = pd.json_normalize(data=response_data)
# print(crime_df)

# response_2 = requests.get(f"https://data.sfgov.org/resource/pu5n-qu5c.json?$offset=10000")
# print(response_2.status_code)
# print(response_2.request)
# response_2_data = response_2.json()
# # print(response_2_data)
# crime_df2 = pd.json_normalize(data=response_2_data)
# print(crime_df2.size)

# response_3 = requests.get(f"https://data.sfgov.org/resource/wg3w-h783.json?$offset=10000")
# print(response_3.status_code)
# print(response_3.request)
# response_3_data = response_3.json()
# # print(response_2_data)
# crime_df3 = pd.json_normalize(data=response_3_data)
# print(crime_df3.size)


    # response = requests.get(f"https://data.sfgov.org/resource/pu5n-qu5c.json?"
    #                         f"$$app_token={APP_TOKEN}"
    #                         f"&$limit={limit}"
    #                         f"&$offset={offset}"
    #                         f"&$select=:*,*") # include metadata field info
#start_time="2023-11-14T00:00:00.000", 
#end_time="2023-11-19T23:59:59.999", 
#soql_date = f"where={column_name} between '{start_time}' and '{end_time}'" 

# start_time="2023-11-01T00:00:00.000", 
# end_time="2023-11-30T23:59:59.999", 
# soql_date = f"where=incident_datetime between '{start_time}' and '{end_time}'" 
# *********************Good one**************************
# response_data = []
# i = 0
# APP_TOKEN = "IKyQtWycRXvCBskD8wNLiKulp"
# limit=1000
# offset = 0
# while True:
#     print(i)
#     offset = i * limit # if limit = 1000 -> offset = 0, 1000, 2000, etc.
#     print("offset", offset)
#     print("limit", limit)
#     response = requests.get(f"https://data.sfgov.org/resource/pu5n-qu5c.json?"
#                             f"$$app_token={APP_TOKEN}"
#                             f"&$limit={limit}"
#                             f"&$offset={offset}"
#                             f"&$select=:*,*") # include metadata field info
#     print(response.request)
#     if not response.status_code==200:
#         raise Exception

#     if i >= 1000:
#         raise Exception

#     if response.json() == []:
#         break

#     response_data.extend(response.json())
#     i += 1
#     crime_df = pd.json_normalize(data=response_data)
#     print(crime_df.size)

# crime_df = pd.json_normalize(data=response_data)
# print(crime_df.size)
# print(crime_df["cnn"])



# response = requests.get(f"https://data.sfgov.org/resource/pu5n-qu5c.json?"
#                             f"$$app_token={APP_TOKEN}"
#                             f"&$limit={limit}"
#                             f"&$offset={offset}"
#                             f"&$select=:*,*") # include metadata field info

# print(response.status_code)
# print(response.request)
# data = response.json()
# crime_df = pd.json_normalize(data=data)
# print(crime_df.size)
# print(crime_df.columns)
# print(crime_df["cnn"])



#f"&${soql_date}"
start_time='2023-11-01T00:00:00.000', 
end_time='2023-12-31T23:59:59.999', 
#soql_date = f"where=incident_datetime between '{start_time}' and '{end_time}'" 
soql_date = f"where=incident_datetime between '2023-11-01T00:00:00.000' and '2023-12-31T23:59:59.999'" 
### data set - crime incidents reported in SFO ##########
response_data = []
i = 0
APP_TOKEN = "ef0oV4r2jOuH9KGAEwWRQfrKl"
limit=20000
offset = 0
while True:
    print(i)
    offset = i * limit # if limit = 1000 -> offset = 0, 1000, 2000, etc.
    print("offset", offset)
    print("limit", limit)
    response = requests.get(f"https://data.sfgov.org/resource/wg3w-h783.json?"
                            f"$$app_token={APP_TOKEN}"
                            f"&${soql_date}"
                            f"&$limit={limit}"
                            f"&$offset={offset}"
                            f"&$select=:*,*") # include metadata field info
    print(response.request)
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
    print(crime_df.size)

crime_df = pd.json_normalize(data=response_data)
print(crime_df.size)
print(crime_df.columns)
print(crime_df["cnn"])

# ':id', ':created_at', ':updated_at', ':version',
#        ':@computed_region_jwn9_ihcz', ':@computed_region_h4ep_8xdi',
#        ':@computed_region_n4xg_c4py', ':@computed_region_nqbw_i6c3',
#        ':@computed_region_26cr_cadq', ':@computed_region_qgnn_b9vv',
#        'incident_datetime', 'incident_date', 'incident_time', 'incident_year',
#        'incident_day_of_week', 'report_datetime', 'row_id', 'incident_id',
#        'incident_number', 'cad_number', 'report_type_code',
#        'report_type_description', 'incident_code', 'incident_category',
#        'incident_subcategory', 'incident_description', 'resolution',
#        'intersection', 'cnn', 'police_district', 'analysis_neighborhood',
#        'supervisor_district', 'supervisor_district_2012', 'latitude',
#        'longitude', 'point.type', 'point.coordinates', 'filed_online',
#        ':@computed_region_jg9y_a9du']

':id',':created_at', ':version',':updated_at','incident_datetime', 'incident_date', 'incident_time', 'incident_year',
       'incident_day_of_week', 'report_datetime', 'row_id', 'incident_id',
       'incident_number', 'cad_number', 'report_type_code',
       'report_type_description', 'incident_code', 'incident_category',
       'incident_subcategory', 'incident_description', 'resolution',
       'intersection', 'cnn', 'police_district', 'analysis_neighborhood',
        'supervisor_district',

#        ':@computed_region_jwn9_ihcz', ':@computed_region_h4ep_8xdi',
#        ':@computed_region_n4xg_c4py', ':@computed_region_nqbw_i6c3',
#        ':@computed_region_26cr_cadq', ':@computed_region_qgnn_b9vv',
# 'supervisor_district_2012', 'latitude',
#        'longitude', 'point.type', 'point.coordinates',':@computed_region_jg9y_a9du'