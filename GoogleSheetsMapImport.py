#Google Sheets import


#Libraries
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.cloud import bigquery
import logging
import pandas as pd

#Path variable
try:
    script_path = os.path.dirname(os.path.abspath(__file__)) + "/"
except:
    script_path = ""

#SETUP LOGGING
logging.basicConfig(filename=script_path + 'GoogleSheetsMapImport.log',level=logging.INFO)


#Bigquery Settings
dataset = "GoogleApiData"
filename = "GoogleSheetsData.csv"
table = "GA_map"
projectName = "nomadic-coast-234608"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Auth/TOPO-Bigquery-Service-Acount.json"


class GoogleSheets:
    def __init__(self):
        self.data = None
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        # The ID and range of a sample spreadsheet.
        self.SPREADSHEET_ID = '1OVFYZ1zqzVhK9fLo4N8QKJrFn6jQC0Kt9pyAFy9867c'
        self.RANGE_NAME = 'Map!A:C'

    def RefreshData(self):
        logging.info('Refressing google sheets data...')

        """Shows basic usage of the Sheets API.
        Prints values from a sample spreadsheet.
        """

        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(script_path + 'Auth/' + 'token.pickle'):
            with open(script_path + 'Auth/' + 'token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    script_path + "Auth/GoogleSheetsCredentials.json", self.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(script_path + 'Auth/' + 'token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=self.SPREADSHEET_ID,
                                    range=self.RANGE_NAME).execute()
        values = result.get('values', [])

        self.data = pd.DataFrame(values[1:], columns = values[0])

        logging.info('Refressing google sheets data... Done')



#Bigquery Database        
class Database:
    def __init__(self, table = table, dataset=dataset, projectName = projectName):
        
        self.settings = {}
        self.settings['projectName'] = projectName
        self.settings['table'] = table
        self.settings['dataset'] = dataset
        self.settings['bigquery'] = bigquery.Client(project=self.settings['projectName'])
        

    def RemoveAllData(self):
        
        print("Removing old map data... ")
        logging.info("Removing old map data... ")
        
        queryRD = """
        DELETE FROM `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""` 
        WHERE Google_Analytics_Title IS NOT Null"""

        print(queryRD)
        
        query_job = self.settings['bigquery'].query(queryRD)
        
        results = query_job.result()  # Waits for job to complete.  
  
        print([x for x in results])
        print("Removing old map data... Done")

        
        
    def load_data_from_file(self, DataFrameObject):
        
        
        dataset_id = self.settings['dataset']
        table_id = self.settings['table']
        
        print("Loading file to bigquery...")
        logging.info("Loading file to bigquery...")
        bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        try:
            os.remove(script_path + "Load/" + filename)
            logging.info("Old file removed")
        except:
            logging.info("Old file not found")

        DataFrameObject.to_csv(script_path + "Load/" + filename, index=False, header=False)
        logging.info('File saved to ' + script_path + "Load/" + filename)

        with open(script_path + "Load/" + filename, 'rb') as source_file:
            # This example uses CSV, but you can use other formats.
            # See https://cloud.google.com/bigquery/loading-data
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = 'text/csv'
            job = bigquery_client.load_table_from_file(
                source_file, table_ref, job_config=job_config)

        job.result()  # Waits for job to complete

        logging.info('Loaded {} rows into {}:{}.'.format(
            job.output_rows, dataset_id, table_id))
        
        print("Loading file to bigquery...Done")
        logging.info("Loading file to bigquery...Done")



if __name__ == '__main__':

    GSheets = GoogleSheets()
    GSheets.RefreshData()

    Bg = Database()
    Bg.RemoveAllData()

    assert len(GSheets.data) > 0, "Gooogle Sheets returned no data"

    Bg.load_data_from_file(GSheets.data)
    logging.info("Operation completed")
