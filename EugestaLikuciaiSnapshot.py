import pandas as pd
import os
import datetime
from unicodecsv import csv
from google.cloud import bigquery
import logging
import pymssql
import numpy as np


#script_path = "/mnt/Tiekeju_kainininku_importavimas/"
try:
    script_path = os.path.dirname(os.path.abspath(__file__)) + "/"
except:
    script_path = ""
    
#SETUP LOGGING
logging.basicConfig(format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s', filename=script_path + 'EugestaLikuciaiSnapshot.log',level=logging.INFO)


#Bigquery Settings
dataset = "OperationsFileParsing"
projectName = "nomadic-coast-234608"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Auth/TOPO-Bigquery-Service-Acount.json"

schema = {'Eugesta_Snapshot': {'Timestamp': ['DATETIME', 'NULLABLE'],
                                              'N37_BAR_KODAS': ['STRING', 'NULLABLE'],
                                              'I17_KIEKIS': ['STRING', 'NULLABLE'],
                                              'N37_PAV': ['STRING', 'NULLABLE'],
                                              'N37_TRUM_PAV': ['STRING', 'NULLABLE']
         }}

def printlog(text):
    print(text)
    logging.info(text)



#Bigquery Database object    
class Database:
    def __init__(self, table, dataset=dataset, projectName = projectName):
        
        self.settings = {}
        self.settings['projectName'] = projectName
        self.settings['table'] = table
        self.settings['dataset'] = dataset
        self.settings['bigquery'] = bigquery.Client(project=self.settings['projectName'])
        
    def AddTable(self):
        
        schema_bq = []
        for field, datatype in schema[self.settings['table']].items():
            schema_bq.append(bigquery.SchemaField(field, datatype[0], mode=datatype[1]))
        
        table_ref = self.settings['bigquery'].dataset(self.settings['dataset']).table(self.settings['table'])
        table = bigquery.Table(table_ref, schema=schema_bq)
        table = self.settings['bigquery'].create_table(table)  # API request

        assert table.table_id == self.settings['table']
        logging.debug("Table has been created {}".format(self.settings['table']))
        

    def RemoveOldData(self):
        
        printlog("Removing Old Data... ")
        
        queryRD = """
        DELETE FROM `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""` 
        WHERE """ + [x for x in schema[self.settings['table']].keys()][0] + """ IS NOT NULL"""
        
        printlog(queryRD)
        
        query_job = self.settings['bigquery'].query(queryRD)
        
        results = query_job.result()  # Waits for job to complete.  
  
        printlog([x for x in results])
        printlog("Removing Old Data... Done")
        
    def GetMaxDate(self):
        
        printlog("Getting Max data... ")
        
        queryRD = """
        SELECT max(Timestamp) FROM `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""`"""
        
        query_job = self.settings['bigquery'].query(queryRD)
        
        results = query_job.result()  # Waits for job to complete.  
  
        return list([list(x) for x in results])[0][0]
        printlog("Getting Max data... Done")
        
        
    def load_data_from_file(self, DataFrameObject):
        
        dataset_id = self.settings['dataset']
        table_id = self.settings['table']
            
        source_file_name = script_path + 'Transfer/TableUpload_' + dataset_id + '_' + table_id + '.csv'
        
        try:
            os.remove(source_file_name)
            printlog("File {} successfully removed".format(source_file_name))
        except:
            printlog("File {} not found not removed".format(source_file_name))
    
        DataFrameObject.to_csv(source_file_name, index = False, header=False)
        
        printlog("Loading file to bigquery...")
        bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        with open(source_file_name, 'rb') as source_file:
            # This example uses CSV, but you can use other formats.
            # See https://cloud.google.com/bigquery/loading-data
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = 'text/csv'
            job = bigquery_client.load_table_from_file(
                source_file, table_ref, job_config=job_config)

        job.result()  # Waits for job to complete

        printlog('Loaded {} rows into {}:{}.'.format(
            job.output_rows, dataset_id, table_id))
        
        
        printlog("Loading file to bigquery...Done")


class SQLDatabase:
    def __init__(self):
        
        #Get credentials
        with open (script_path + 'Auth/' + 'credentials.txt', 'r', encoding='utf-8') as openfile:
            rd = csv.reader(openfile, delimiter=',')
            credentials = list(rd)
        
        self.Timestamp = datetime.datetime.now().strftime('%Y-%m-%d')
        self.settings = {}
        self.settings['user'] = credentials[1][0]
        self.settings['password'] = credentials[1][1]
        self.settings['server'] = '192.168.0.220'
        
        self.getAllQueries()
    
    def getquery(self, database, query):
    
        conn = pymssql.connect(server=self.settings['server'], user=self.settings['user'], password=self.settings['password'], database=database)
        cursor = conn.cursor()

        #cursor.execute("select name FROM sys.databases")
        #cursor.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES")
        #cursor.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA")
        #print(query + (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%Y%m%d") + """'""")
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [x[0] for x in cursor.description]
        conn.close()

        #select name FROM sys.databases
        return pd.DataFrame(rows, columns=columns)
    
        
    def stripbarcodes(self, data):
        data['N37_BAR_KODAS'] = data['N37_BAR_KODAS'].map(lambda x: ((12 - len(str(int(x)))) * '0') + str(x))
        data['N37_BAR_KODAS'] = data['N37_BAR_KODAS'].map(lambda x: str(x).strip()[:12])
        return data
    
    def getAllQueries(self):
        #Query_EugestosSandelio = """select 
        #                        N37_BAR_KODAS, 
        #                        sum(I17_KIEKIS) as I17_KIEKIS,
        #                        N37_PAV,
        #                        N37_TRUM_PAV
        #                    from 
        #                         [whTC].[dbo].[i17_vpro] WITH (NOLOCK)
        #                       left join [whTC].[dbo].[n37_pmat] WITH (NOLOCK) on N37_KODAS_PS = I17_KODAS_PS 
        #                    where 
        #                        I17_KODAS_IS IN ('101', '1013', '1014', '1015')
        #                        and I17_SERIJA = 'A' 
        #                     /*   and I17_KIEKIS > 0 */
        #                     
        #                     GROUP BY N37_BAR_KODAS, N37_PAV, N37_TRUM_PAV
        #                    """

        Query_EugestosSandelioRivile = """select n37.N37_BAR_KODAS as N37_BAR_KODAS , sum(rvl.I17_KIEKIS + rvl.I17_REZERVAS) I17_KIEKIS, n37.N37_PAV as N37_PAV, n37.N37_TRUM_PAV as N37_TRUM_PAV from openquery(RIVILE, 'select * from i17_vpro WHERE I17_KODAS_IS IN (''101'', ''1013'', ''1014'', ''1015'') and I17_SERIJA = ''A''') rvl
                                          left join [whTC].[dbo].[n37_pmat] n37 WITH (NOLOCK) on n37.N37_KODAS_PS COLLATE SQL_Lithuanian_CP1257_CI_AS = rvl.I17_KODAS_PS COLLATE SQL_Lithuanian_CP1257_CI_AS
                                          group by N37_BAR_KODAS, N37_PAV, N37_TRUM_PAV"""

    
        #self.data_MusuSandelio = self.stripbarcodes(self.getquery("whTC", Query_MusuSandelio))
        self.data_EugestosSandelio = self.stripbarcodes(self.getquery("whTC", Query_EugestosSandelioRivile))
        self.data_EugestosSandelio['Timestamp'] = self.Timestamp
        self.data_EugestosSandelio = self.data_EugestosSandelio[[x for x in schema['Eugesta_Snapshot'].keys()]]
        
        
if __name__ == "__main__":
    SQLsync = SQLDatabase()
    SQLsync.getAllQueries()
    Bigquery = Database('Eugesta_Snapshot')
    DatabaseDate = Bigquery.GetMaxDate()
    if DatabaseDate != None:
        if pd.to_datetime(DatabaseDate).date() < pd.to_datetime(SQLsync.data_EugestosSandelio['Timestamp'].max()).date():
            Bigquery.load_data_from_file(SQLsync.data_EugestosSandelio)
        else:
            printlog('Datafor given timestamp is already available')
    