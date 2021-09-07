# Importing required module to perform the task
import requests
import pandas as pd 
import boto3
import json
from datetime import datetime

# To display the complete rows in the dataframe
pd.set_option('display.max_rows', None)

# Reading countries data to know the countries details
json_data=pd.read_json('https://api.covid19api.com/countries')
json_data

# Storing the file in s3 bucket
def upload_file_to_bucket(bucket_name, file_path):
    try:
        s3=boto3.client('s3')
        s3.upload_file(file_path,bucket_name,file_path)
        # Print the message CSV files data get successfully uploaded to s3 bucket
        print(file_path+' successfully uploaded to s3')
    except: 
        print('Error');
    
# Creating CSV file to save all the records of the different task
def saveToCSV(name, data):
    try:
        df = pd.DataFrame(data)
        df.to_csv(name,index=False)
        # Print the message that CSV files data get successfully to local device in assignm
        print('data succesfully save to assignment/'+name)
    except:
        print('I/O ERROR')

# This function helps to sort the data and get the top three countries with highest number of cases
def sortingFunction(list):
    sorted_data = sorted(list, key=lambda x: x['deaths'], reverse=True)
    sortedItem = [];
    count = 0;
    
# To get top 3 counties which have highest covid confirmed cases
    for data in sorted_data:
        if(count <= 2):
            sortedItem.append(data)
        count+= 1
    return sortedItem

# This function helps to get the details of corona report which is mentioned in the given task
def coronaReportAnalyser():
    # Variables
    list_of_countries_url = 'https://api.covid19api.com/countries';
    covid_report_url = 'https://api.covid19api.com/country/';
    covid_data_by_country = []
    india_covid_cases_filtered_by_month_count = 0;
    india_covid_cases_filtered_by_month = []
    covid_death_list = []
    countries = []
    top_most_effected_countries_with_covid = [];
    
    # Get request
    list_of_countries = requests.get(list_of_countries_url).json()
    
    for nation in list_of_countries:
        countries.append(nation.get('Slug'))
    
    # Did this because the client is blocking our request because we are doing many request at a time - 248 api request.
    countries = ['INDIA','burundi','ala-aland-islands','nepal','australia','singapore','maldives']
    
    for country in countries:
        # Temporary variable for counting the confirmed and covid cases
        confirmed_cases_count = 0
        covid_death_count = 0
        covid_data = requests.get(covid_report_url+country).json()
        try :
            for data in covid_data:
                # Spliting the date into year and months
                date = data.get('Date').split('-')
                year = int(date[0])
                month = int(date[1])
                confirmed_cases_count+= data.get('Confirmed')
                covid_death_count+= data.get('Deaths')
                # To get the data of india covid cases from april to may in year 2020 
                if(year == 2020 and (month >= 4 and month <= 5) and country == 'INDIA'):
                    india_covid_cases_filtered_by_month_count+= confirmed_cases_count
            temp_object = {};
            temp_object['country'] = country
            temp_object['confirmed'] = confirmed_cases_count
            covid_data_by_country.append(temp_object)
            
            temp_object1 = {};
            temp_object1['country'] = country
            temp_object1['deaths'] = covid_death_count
            covid_death_list.append(temp_object1)
            
        except(Exception):
            print('Error',Exception)
            
    temp_object2={};
    temp_object2['country']='INDIA'
    temp_object2['confirmed']=india_covid_cases_filtered_by_month_count
    india_covid_cases_filtered_by_month.append(temp_object2)
    top_most_effected_countries_with_covid = sortingFunction(covid_death_list)
    
    # Calling this function to store the data in the assignment folder
    saveToCSV(datetime.now().strftime('covid_data_by_country-%Y-%m-%d-%H-%M.csv'),covid_data_by_country)
    saveToCSV(datetime.now().strftime('india_covid_cases_filtered_by_month-%Y-%m-%d-%H-%M.csv'),india_covid_cases_filtered_by_month)
    saveToCSV(datetime.now().strftime('top_3_most_effected_countries_with_covid-%Y-%m-%d-%H-%M.csv'),top_most_effected_countries_with_covid)
    
    # Calling this function will help to copy the data in s3
    upload_file_to_bucket('coda-covid-data-s3-abcd',datetime.now().strftime('covid_data_by_country-%Y-%m-%d-%H-%M.csv'))
    upload_file_to_bucket('coda-covid-data-s3-abcd',datetime.now().strftime('india_covid_cases_filtered_by_month-%Y-%m-%d-%H-%M.csv'))
    upload_file_to_bucket('coda-covid-data-s3-abcd',datetime.now().strftime('top_3_most_effected_countries_with_covid-%Y-%m-%d-%H-%M.csv'))
    
coronaReportAnalyser()

