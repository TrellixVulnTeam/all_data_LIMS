import requests
import pandas as pd
import numpy as np 
import pandas_gbq
import keyring
import zipfile
import io
import os

GCP_PROJECT_ID = keyring.get_password('gcp', 'project_id')
PROGRAM_URL = 'https://data.ed.gov/dataset/9dc70e6b-8426-4d71-b9d5-70ce6094a3f4/'\
               'resource/ff68afc4-6d23-459d-9f60-4006e4f85583/download/most-recent-'\
               'cohorts-field-of-study_01192021.zip'
COLLEGE_URL = 'https://data.ed.gov/dataset/9dc70e6b-8426-4d71-b9d5-70ce6094a3f4/'\
               'resource/823ac095-bdfc-41b0-b508-4e8fc3110082/download/most-recent-'\
               'cohorts-all-data-elements_01192021.zip'


def college_programs_extract_data():
    program_data = requests.get(PROGRAM_URL)
    zip_contents = zipfile.ZipFile(io.BytesIO(program_data.content))
    zip_contents.extractall("/home/chris/all_data/")
    df = pd.read_csv('Most-Recent-Cohorts-Field-of-Study.csv').replace('PrivacySuppressed', 0)
    os.remove('Most-Recent-Cohorts-Field-of-Study.csv')
    
    return df
    
    
def colleges_extract_data():
    college_data = requests.get(COLLEGE_URL)
    zip_contents = zipfile.ZipFile(io.BytesIO(college_data.content))
    zip_contents.extractall("/home/chris/all_data/")
    df = pd.read_csv('Most-Recent-Cohorts-All-Data-Elements.csv').replace('PrivacySuppressed', 0)
    os.remove('Most-Recent-Cohorts-All-Data-Elements.csv')
    
    return df
    

def load_to_gbq(program_data, college_data):
    pandas_gbq.to_gbq(program_data, 'college_scorecard.programs', project_id=GCP_PROJECT_ID, 
                      if_exists='replace')

    pandas_gbq.to_gbq(college_data, 'college_scorecard.schools', project_id=GCP_PROJECT_ID, 
                      if_exists='replace')



if __name__ == "__main__":
    program_data = college_programs_extract_data()
    college_data = colleges_extract_data()

    load_to_gbq(program_data, college_data)