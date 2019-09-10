#Libraries
import pandas as pd
import os
import datetime
from time import sleep
from google.cloud import bigquery
import requests
import json
import logging
import pymssql
import csv



#Path variable
try:
    script_path = os.path.dirname(os.path.abspath(__file__)) + "/"
except:
    script_path = ""

#SETUP LOGGING
logging.basicConfig(filename=script_path + 'MarguardETL.log',level=logging.INFO)


#Bigquery Settings
dataset = "RAW_Kainos"
projectName = "nomadic-coast-234608"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Auth/TOPO-Bigquery-Service-Acount.json"


#SQL credentials
with open (script_path + 'Auth/credentials.txt', 'r', encoding='utf-8') as openfile:
    rd = csv.reader(openfile, delimiter=',')
    credentials = list(rd)
    
user=credentials[1][0]
password=credentials[1][1]


#SQL Queries
QueryProducts = """
SELECT Br.Descr as 'Brand'
      ,Pr.Model
      ,Pr.Code as 'Code'
     
  FROM [wh].[dbo].[Product] Pr
  
  LEFT JOIN [wh].[dbo].[Brand] Br
  on Pr.BrandId = Br.Id
  WHERE Br.Descr is not Null AND Pr.Model is not Null and LEN(Pr.Code) >= 11
  GROUP BY Br.Descr, Pr.Model, Pr.Code"""


def printlog(text):
    logging.info(text)
    print(str(text))


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





#Bigquery Database        
class Database:
    def __init__(self, table, dataset=dataset, projectName = projectName):
        
        self.settings = {}
        self.settings['projectName'] = projectName
        self.settings['table'] = table
        self.settings['dataset'] = dataset
        self.settings['bigquery'] = bigquery.Client(project=self.settings['projectName'])
        

    def RemoveDuplicates(self, fromdate):
        
        printlog("Removing duplicates... ")
        
        queryRD = """
        DELETE FROM `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""` 
        WHERE UniqueKey in (
        SELECT UniqueKey from (


        SELECT main.key, secondary.key2, main.UploadTimestamp, main.UniqueKey, secondary.maxtimestamp, secondary.count from `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""` main
        LEFT JOIN (SELECT key key2, count(key) count, max(UploadTimestamp) maxtimestamp FROM `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""`
                    WHERE date >= DATE('"""+ fromdate +"""')
                    GROUP BY key2
                    HAVING count > 1) secondary
                 
        ON main.key = secondary.key2
        
        WHERE main.UploadTimestamp <> secondary.maxtimestamp 
        AND secondary.key2 is not Null
        AND date >= DATE('"""+ fromdate +"""')))

        """

        printlog(queryRD)
        
        query_job = self.settings['bigquery'].query(queryRD)
        
        results = query_job.result()  # Waits for job to complete.  
  
        printlog([x for x in results])
        printlog("Removing duplicates... Done")


    def DeleteAllRows(self, column):
        printlog("Removing all rows... ")
        queryDelete = """
        DELETE FROM `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""` 
        WHERE """ + column + """ is not Null"""

        printlog(queryDelete)

        query_job = self.settings['bigquery'].query(queryDelete)
        
        results = query_job.result()  # Waits for job to complete.  
  
        printlog([x for x in results])


        printlog("Removing all rows... Done")
        
        
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
                 
    
class MarguardAPI:
    def __init__(self):
        self.campaigns = None
        self.products = None
        self.prices = None
        self.data = None
    
    def getCampaigns(self):
        request = requests.get("http://client.pricewisely.com/api/keyhere/json/campaigs")
        self.campaigns = json.loads(request.text)


    def getProducts(self, campaign):
        request = requests.get("http://client.pricewisely.com/api/keyhere/json/products?campaign_id="+str(campaign))
        if self.products == None:
            self.products = {campaign: json.loads(request.text)}
        else:
            self.products[campaign] = json.loads(request.text)
            
    def getHistoricPrice(self, date, campaign):
        #date YYYY-MM-DD
        request = requests.get("http://client.pricewisely.com/api/keyhere/json/report/daily-prices?date="+str(date)+"&campaign_id="+str(campaign))
        if self.prices == None:
            self.prices = {campaign: {date: json.loads(request.text)['data']}}
        else:
            if campaign not in self.prices.keys():
                self.prices[campaign] = {date: json.loads(request.text)['data']}
            else:
                self.prices[campaign][date] = json.loads(request.text)['data']
      
            
            
    def GetPricesforDates(self, fromdate, todate):
        #fromdate and todate must be YYYY-MM-DD format
        printlog('Getting prices data...')
        fromdate = datetime.datetime.strptime(fromdate, "%Y-%m-%d")
        todate = datetime.datetime.strptime(todate, "%Y-%m-%d")
        difference = (todate - fromdate).days
        
        self.getCampaigns()
        
        for campaign in [x['id'] for x in self.campaigns['data']]:
            for date_incrementor in range(0, difference+1):
                print((fromdate + datetime.timedelta(days=date_incrementor)).date().strftime("%Y-%m-%d"))
                self.getHistoricPrice((fromdate + datetime.timedelta(days=date_incrementor)).date().strftime("%Y-%m-%d"), campaign)
        
        printlog('Getting prices data... Done')
        
        
    def ProcessPriceData(self):
        
        printlog('Starting to process data...')
        self.data = []
        PriceColumns = ['campaign', 'product_id', 'external_id', 'brand_name', 'product_name', 'category_name', 'date', 'merchant_id', 'merchant_name', 'is_owner_shop', 'price', 'scan_time', 'out_of_stock', 'our_price_difference', 'our_price_difference_percentage']
        for campaign in [x['id'] for x in self.campaigns['data']]:
            for date in self.prices[campaign].keys():
                for row in self.prices[campaign][date]:
                    for price in row['prices']:
                        self.data.append([campaign, row['product_id'], row['external_id'], row['brand_name'], row['product_name'], row['category_name'], price['date'], price['merchant_id'], price['merchant_name'], price['is_owner_shop'], price['price'], price['scan_time'], price['out_of_stock'], price['our_price_difference'], price['our_price_difference_percentage']])


        self.data = pd.DataFrame(self.data, columns=PriceColumns)
        
        #Clean data
        self.data['product_name'] = self.data['product_name'].astype(str)
        self.data['product_name'] = self.data['product_name'].str.replace("\n", "")
        self.data['product_name'] = self.data['product_name'].str.replace("\t", "")
        
        
        self.data['scan_time'] = self.data['scan_time'].astype(str).map(lambda x: None if x == '' or x=='nan' or x=='None' else x[:10])
        self.data['date'] = self.data['date'].astype(str).map(lambda x: None if x == '' or x=='nan' or x=='None' else x[:10])
        
        #self.data['scan_time'] = self.data.apply(lambda x: pd.to_datetime(x['scan_time']).strftime("%Y-%m-%d") if x['scan_time']!=None else None, axis=1)
        
        #self.data['date'] = self.data.apply(lambda x: pd.to_datetime(x['date']).strftime("%Y-%m-%d") if x['date'] != None else None, axis=1)
            
        #Create a key for removing duplicates  
        self.data['key'] = self.data.apply(lambda x: str(x['campaign']) + "-" + str(x['product_id']) + "-" + str(x['merchant_id']) + "-" + str(x['date']), axis=1)
        self.data['UploadTimestamp'] = datetime.datetime.now()
        self.data['UniqueKey'] = self.data.apply(lambda x: x['key'] + '-' + str(x['UploadTimestamp'])[:19], axis=1)
        
        printlog('Starting to process data... Done')

        
if __name__ == "__main__":
       
    FromDate = (datetime.datetime.now() - datetime.timedelta(days=10)).date().strftime("%Y-%m-%d")
    ToDate = (datetime.datetime.now() - datetime.timedelta(days=0)).date().strftime("%Y-%m-%d")

    #FromDate = "2017-08-01"
    #ToDate = "2017-08-02"

    #Get Prices    
    Marguard = MarguardAPI()
    Marguard.GetPricesforDates(FromDate, ToDate)
    Marguard.ProcessPriceData()


    #Database load
    Db = Database('Marguard')
    Db.load_data_from_file(Marguard.data)
    Db.RemoveDuplicates(FromDate)

    #Upload product data
    SQLData = SQLObject('192.168.0.220')
    SQLData.getquery('wh', QueryProducts)

    if 'Db' in locals() or 'Db' in globals():
        del Db

    Db = Database('BrandModel')
    Db.DeleteAllRows('Code')
    Db.load_data_from_file(SQLData.data)
    