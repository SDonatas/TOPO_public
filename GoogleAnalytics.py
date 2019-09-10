#!/venv/bin python3
import logging
from google.cloud import bigquery
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import csv
import datetime
import pandas as pd
import os
import pickle


#Path variable
try:
    script_path = os.path.dirname(os.path.abspath(__file__)) + "/"
except:
    script_path = ""


#SETUP LOGGING
logging.basicConfig(filename=script_path + 'GoogleAnalytics.log',level=logging.INFO)

Timestamp = datetime.datetime.now()

#Bigquery Settings
dataset = "GoogleApiData"
table = "GA_data"
projectName = "nomadic-coast-234608"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Auth/TOPO-Bigquery-Service-Acount.json"

#"""Hello Analytics Reporting API V4."""
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = script_path + "Auth/" + "TOPO-Bigquery-Service-Acount.json"
VIEW_ID = '45217667'
filename = 'GoogleAnalytics.csv'


#Report Definition
Report = {
              'viewId': VIEW_ID,
              'pageSize': 100000,
              'dateRanges': [{'startDate': (Timestamp - datetime.timedelta(days=1)).strftime("%Y-%m-%d"), 'endDate': 'today'}],
              'metrics': [{'expression': 'ga:sessions'},
                            {'expression': 'ga:users'},
                            {'expression': 'ga:newUsers'},
                            {'expression': 'ga:bounces'},
                            {'expression': 'ga:transactionsPerSession'}
                            ],

              'dimensions': [{'name': 'ga:date'},
                             {'name': 'ga:pageTitle'},
                             #{'name': 'ga:landingPagePath'},
                            {'name': 'ga:channelGrouping'}
                            ]
            }



#schema
schema = {table:{}}
schema[table] = {**schema[table], **{x['name'].replace(":", "_"):['DATE', 'NULLABLE'] for x in Report['dimensions'] if x['name'] in ['ga:date']}}
schema[table] = {**schema[table], **{x['name'].replace(":", "_"):['STRING', 'NULLABLE'] for x in Report['dimensions'] if x['name'] not in ['ga:date']}}
schema[table] = {**schema[table], **{x['expression'].replace(":", "_"):['FLOAT', 'NULLABLE'] for x in Report['metrics']}}
schema[table] = {**schema[table], **{'UploadTimestamp': ['TIMESTAMP', 'REQUIRED'], 'key': ['STRING', 'REQUIRED'], 'UniqueKey': ['STRING', 'REQUIRED']}}

print([x for x in schema[table].keys()])



#Bigquery Database        
class Database:
    def __init__(self, table, dataset=dataset, projectName = projectName):
        
        self.settings = {}
        self.settings['projectName'] = projectName
        self.settings['table'] = table
        self.settings['dataset'] = dataset
        self.settings['bigquery'] = bigquery.Client(project=self.settings['projectName'])
        

    def RemoveDuplicates(self):

        fromdate = Timestamp - datetime.timedelta(days=365)
        fromdate = fromdate.strftime("%Y-%m-%d")
        
        print("Removing duplicates... ")
        logging.info("Removing duplicates... ")
        
        queryRD = """
        DELETE FROM `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""` 
        WHERE UniqueKey in (
        SELECT UniqueKey from (


        SELECT main.key, secondary.key2, main.UploadTimestamp, main.UniqueKey, secondary.maxtimestamp, secondary.count from `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""` main
        LEFT JOIN (SELECT key key2, count(key) count, max(UploadTimestamp) maxtimestamp FROM `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""`
                    WHERE ga_date >= DATE('"""+ fromdate +"""')
                    GROUP BY key2
                    HAVING count > 1) secondary
                 
        ON main.key = secondary.key2
        
        WHERE main.UploadTimestamp <> secondary.maxtimestamp 
        AND secondary.key2 is not Null
        AND ga_date >= DATE('"""+ fromdate +"""')))

        """

        print(queryRD)
        
        query_job = self.settings['bigquery'].query(queryRD)
        
        results = query_job.result()  # Waits for job to complete.  
  
        print([x for x in results])
        print("Removing duplicates... Done")


    def AddTable(self):
        
        schema_bq = []
        for field, datatype in schema[self.settings['table']].items():
            schema_bq.append(bigquery.SchemaField(field, datatype[0], mode=datatype[1]))
        
        table_ref = self.settings['bigquery'].dataset(self.settings['dataset']).table(self.settings['table'])
        table = bigquery.Table(table_ref, schema=schema_bq)
        table = self.settings['bigquery'].create_table(table)  # API request

        assert table.table_id == self.settings['table']
        logging.info("Table has been created {}".format(self.settings['table']))

        
        
    def load_data_from_file(self, NumberOfReports):
        
        
        dataset_id = self.settings['dataset']
        table_id = self.settings['table']
        
        print("Loading file to bigquery...")
        logging.info("Loading file to bigquery...")
        bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        for report_number in range(0, NumberOfReports):
            print("Loading report no: {}".format(str(report_number)))

            with open(script_path + "Load/GA_Report_"+str(report_number)+"_"+filename, 'rb') as source_file:
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




class GoogleAnalytics:

    def __init__ (self, ReportDic):
        assert type(ReportDic) == dict
        self.ReportDic = ReportDic
        self.analytics = build('analyticsreporting', 'v4', credentials=ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES))
        self.response = None
        self.timestamp = Timestamp
        self.reports = None

    def get_report(self):
      self.response = self.analytics.reports().batchGet(body={'reportRequests': [self.ReportDic]}).execute()


    def print_response(self):

      assert self.response != None
     
      for report in self.response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

        for row in report.get('data', {}).get('rows', []):
          dimensions = row.get('dimensions', [])
          dateRangeValues = row.get('metrics', [])

          for header, dimension in zip(dimensionHeaders, dimensions):
            print (header + ': ' + dimension)

          for i, values in enumerate(dateRangeValues):
            print ('Date range: ' + str(i))
            for metricHeader, value in zip(metricHeaders, values.get('values')):
              print (metricHeader.get('name') + ': ' + value)


    def get_report_structure (self):

        #response_data
        logging.info("Total Google Analytics Reports: " + str(len(self.response['reports'])))

        self.reports = len(self.response['reports'])
        
        for report_number in range(0, self.reports):

            rows = []
            header = []
            print("Report number: " + str(report_number))

            def replace_item(some_string):
                new_string = some_string.replace(":", "_")
                return new_string


            #Build header for the list
            # Dimensions
            for item in self.response['reports'][int(report_number)]['columnHeader']['dimensions']:
                header.append(replace_item("dimension_"+item))

            #Metrics
            for item in self.response['reports'][int(report_number)]['columnHeader']['metricHeader']['metricHeaderEntries']:
                header.append( replace_item("metric_"+item['name']+"_"+item['type']) )

            logging.info("Header: " + str(len(header)))
            
            #Bring Data into a coherent list
            for item in self.response['reports'][int(report_number)]['data']['rows']:
                rows.append(item['dimensions'] + item['metrics'][0]['values'])

            #Check integrity of data rows list
            for item in rows:
                assert len(item) == len(header), "Header columns did not match data columns!"

            print(len(rows))
            print(int(self.response['reports'][int(report_number)]['data']['rowCount']))

            assert len(rows) == int(self.response['reports'][int(report_number)]['data']['rowCount']), "Actual row count did not mach metadata count!"
            assert len(rows) < 100000

            try:
                os.remove(script_path + "Load/GA_Report_"+str(report_number)+"_"+filename,'w')
            except:
                logging.info("File not found: " + script_path + "Load/GA_Report_"+str(report_number)+"_"+filename)
            

            #Split landing page column and keep only product name
            for key, x in enumerate(rows):
                rows[key][0] = datetime.datetime.strptime(rows[key][0], "%Y%m%d").strftime("%Y-%m-%d")
                rows[key][1] = rows[key][1].replace(" - TOPO CENTRAS", "").lower().strip()
                #rows[key][2] = rows[key][2].split("/")[-1]
                #rows[key][2] = rows[key][2].split(".html")[0]
                #rows[key][2] = rows[key][2].split("?")[0]

            #Add dimension keys & Timestamp
            for key, x in enumerate(rows):
                rows[key].append(self.timestamp)
                rows[key].append(x[0] + x[1] + x[2] + x[3])
                rows[key].append(x[0] + x[1] + x[2] + x[3] + str(self.timestamp))


            with open(script_path + "Load/GA_Report_"+str(report_number)+"_"+filename,'w') as resultFile:
                writer = csv.writer(resultFile)
                writer.writerows(rows)

            logging.info("GA File for report " + str(report_number) + " written to "+"../Load/GA_Report_"+str(report_number)+"_"+filename)
            logging.info("Header for the report " + str(report_number) +": " + ", ".join(header))
 


if __name__ == '__main__':

    Analytics = GoogleAnalytics(Report)
    Analytics.get_report()
    Analytics.get_report_structure()
    Db = Database(table)

    try:
        Db.AddTable()
        logging.info("Added table")
        print("Added table")
    except:
        logging.info("Table already exist...")
        print("Table already exist...")

    Db.RemoveDuplicates()
    Db.load_data_from_file(Analytics.reports)


    #Db.load_data_from_file(1)