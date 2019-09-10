#Import libraries
from google.cloud import bigquery
import os
import pymssql
import csv
import datetime
import pandas as pd
import logging

#Servers SQL config map
Servers = [{'ServerName': 'MSSS', 'Address': '192.168.0.220'},
           {'ServerName': 'RGASNEW02', 'Address': '192.168.0.38'}]


#Path variable
try:
    script_path = os.path.dirname(os.path.abspath(__file__)) + "/"
except:
    script_path = ""
    
    
#Credentials open
with open (script_path + 'Auth/credentials.txt', 'r', encoding='utf-8') as openfile:
    rd = csv.reader(openfile, delimiter=',')
    credentials = list(rd)
    
user=credentials[1][0]
password=credentials[1][1]


#SETUP LOGGING
logging.basicConfig(filename=script_path + 'OnlinePardavimai.log',level=logging.INFO)


#Timestamp
Timestamp = datetime.datetime.now()

#Bigquery Settings
dataset = "Vadybos_Ataskaitos"
projectName = "nomadic-coast-234608"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Auth/TOPO-Bigquery-Service-Acount.json"


schema = {'PardavimaiOnline': {'queryFields': {
                                        'DateOfPurchase': ['TIMESTAMP', 'REQUIRED'],
                                         'Descr': ['STRING', 'NULLABLE'],
                                         'Model': ['STRING', 'NULLABLE'],
                                         'Brand': ['STRING', 'NULLABLE'],


                                         'sTotal': ['FLOAT', 'REQUIRED']
                                         
                                         },

                                 'calculatedFields': {
                                
                                         'UploadTimestamp': ['TIMESTAMP', 'REQUIRED'],
                                         'key': ['STRING', 'REQUIRED'],
                                         'UniqueKey': ['STRING', 'REQUIRED']}
                                 



                                 }}
         


#toexclude = ['Year0_sTotal', 'Year0_minus_1_sTotal', 'Year0_minus_2_sTotal', 'Year0_PeopleIN', 'Year0_minus_1_PeopleIN', 'Year0_minus_2_PeopleIN', 'Year0_Checkout', 'Year0_minus_1_Checkout', 'Year0_minus_2_Checkout']
#Translation dictionaries from Bigquery to SQL notation
#SQLDatatypes = {'INTEGER':'INT', 'FLOAT': 'FLOAT', 'DATE': 'DATE', 'STRING': 'VARCHAR(100)'}
#SQLValidation = {'REQUIRED': 'NOT NULL', 'NULLABLE': None}



#query = """SELECT t1.DateOfPurchase, Descr, sum(sTotal) sTotal FROM [wh].[dbo].[factAll] as t1
#LEFT JOIN [wh].[dbo].[Product] as t2
#    ON t1.ProductId = t2.Id
#LEFT JOIN [wh].[dbo].[FACDep] AS t3
#    ON t1.BusinessUnitId = t3.code

#/* Will look 30 days rolling perdiod, in case if some backward changes */
#    
#WHERE t1.DateOfPurchase >= Cast('{}' as datetime) and
#        t3.Region = 'E-prekyba'
#group by t1.DateOfPurchase, Descr
#ORDER BY t1.DateOfPurchase, Descr""".format((Timestamp - datetime.timedelta(days=30)).strftime("%Y/%m/%d")) 



query = """SELECT t1.DateOfPurchase, t2.Descr, t2.Model, t4.Descr as Brand, sum(sQnt) sTotal FROM [wh].[dbo].[factAll] as t1
LEFT JOIN [wh].[dbo].[Product] as t2
    ON t1.ProductId = t2.Id
LEFT JOIN [wh].[dbo].[FACDep] AS t3
    ON t1.BusinessUnitId = t3.code
LEFT JOIN [wh].[dbo].[Brand] AS t4
    ON t2.BrandId = t4.Id

/* Will look 30 days rolling perdiod, in case if some backward changes */
    
WHERE t1.DateOfPurchase >= Cast('{}' as datetime) and
        t3.Region = 'E-prekyba'
group by t1.DateOfPurchase, t2.Descr, t2.Model, t4.Descr
ORDER BY t1.DateOfPurchase, Descr""".format((Timestamp - datetime.timedelta(days=30)).strftime("%Y/%m/%d")) 




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
        for field, datatype in {**schema[self.settings['table']]['queryFields'], **schema[self.settings['table']]['calculatedFields']}.items():
            schema_bq.append(bigquery.SchemaField(field, datatype[0], mode=datatype[1]))
        
        table_ref = self.settings['bigquery'].dataset(self.settings['dataset']).table(self.settings['table'])
        table = bigquery.Table(table_ref, schema=schema_bq)
        table = self.settings['bigquery'].create_table(table)  # API request

        assert table.table_id == self.settings['table']
        logging.debug("Table has been created {}".format(self.settings['table']))
        

    def RemoveDuplicates(self):

        fromdate = str(Timestamp - datetime.timedelta(days=365))
        
        print("Removing duplicates... ")
        logging.info("Removing duplicates... ")
        
        queryRD = """
        DELETE FROM `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""` 
        WHERE UniqueKey in (
        SELECT UniqueKey from (


        SELECT main.key, secondary.key2, main.UploadTimestamp, main.UniqueKey, secondary.maxtimestamp, secondary.count from `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""` main
        LEFT JOIN (SELECT key key2, count(key) count, max(UploadTimestamp) maxtimestamp FROM `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""`
                    WHERE UploadTimestamp >= TIMESTAMP('"""+ fromdate +"""')
                    GROUP BY key2
                    HAVING count > 1) secondary
                 
        ON main.key = secondary.key2
        
        WHERE main.UploadTimestamp <> secondary.maxtimestamp 
        AND secondary.key2 is not Null
        AND DateOfPurchase > TIMESTAMP('"""+ fromdate +"""')))

        """

        print(queryRD)
        
        query_job = self.settings['bigquery'].query(queryRD)
        
        results = query_job.result()  # Waits for job to complete.  
  
        print([x for x in results])
        print("Removing duplicates... Done")


        
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


#Query SQL Server
class SQLObject:
    def __init__(self, server):
        self.server = server
        self.settings = {}
    
    def getquery(self, database, query):

        conn = pymssql.connect(server=self.server, user=user, password=password, database=database)
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
        self.data = pd.DataFrame(rows, columns=columns)


class DataProcess:
    def AddKeys(self, data):
        print(data.columns)
        #del data['Descr']
        #data['Descr'] = data.apply(lambda x: x['Brand'].lower() + " " + x['Model'].lower(), axis=1)
        data['Descr'] = data['Descr'].map(lambda x: x.lower().strip())
        data['UploadTimestamp'] = Timestamp
        data['key'] = data.apply(lambda x: str(x['DateOfPurchase']) + x['Descr'], axis=1)
        data['UniqueKey'] = data.apply(lambda x: str(x['DateOfPurchase']) + x['Descr'] + str(Timestamp), axis=1)
        return data
  
 

#DateOfPurchase, Descr, sTotal


Db = Database('PardavimaiOnline')


#Db.AddTable()
#print("Added table...")
#logging.info("Added table...")
#except:
#    print("Table already exists")
#    logging.info("Table already exists")

SQLData = SQLObject(Servers[0]['Address'])
SQLData.getquery('wh', query)
SQLData.data = DataProcess().AddKeys(SQLData.data)
Db.load_data_from_file(SQLData.data)
Db.RemoveDuplicates()



