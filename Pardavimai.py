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
logging.basicConfig(filename=script_path + 'ParadvimuAtaskaitaETL.log',level=logging.INFO)

#Bigquery Settings
dataset = "Vadybos_Ataskaitos"
projectName = "nomadic-coast-234608"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Auth/TOPO-Bigquery-Service-Acount.json"


schema = {'Pardavimai_factAll': {'queryFields': {
                                        'FullDate': ['DATE', 'REQUIRED'],
                                         'Year': ['INTEGER', 'REQUIRED'],
                                         'Month': ['INTEGER', 'REQUIRED'],
                                         'Day': ['INTEGER', 'REQUIRED'],
                                         'Week': ['INTEGER', 'REQUIRED'],
                                         'Day7': ['INTEGER', 'REQUIRED'],
                                         'CurrencyId': ['STRING', 'REQUIRED'],
                                         'FACCompany': ['STRING', 'REQUIRED'],
                                         'FACDepartment': ['STRING', 'REQUIRED'],
                                         'FACName': ['STRING', 'REQUIRED'],
                                         'FACGName': ['STRING', 'REQUIRED'],
                                         'FACRegion': ['STRING', 'REQUIRED'],
                                         'Type': ['STRING', 'REQUIRED'],
                                         'ClickNCollect': ['STRING', 'REQUIRED'],
                                         'GroupResolveTo': ['STRING', 'REQUIRED'],
                                         'pQnt': ['INTEGER', 'REQUIRED'],
                                         'bQnt': ['INTEGER', 'REQUIRED'],
                                         'sQnt': ['INTEGER', 'REQUIRED'],
                                         'pCost': ['INTEGER', 'REQUIRED'],
                                         'pTotal': ['INTEGER', 'REQUIRED'],
                                         'bCost': ['FLOAT', 'REQUIRED'],
                                         'sCost': ['FLOAT', 'REQUIRED'],
                                         'sTotal': ['FLOAT', 'REQUIRED'],
                                         'sDiscount': ['FLOAT', 'REQUIRED'],
                                         'PriceIVAT': ['FLOAT', 'REQUIRED'],
                                         'PeopleIn': ['INTEGER', 'REQUIRED'],
                                         'CheckCount': ['INTEGER', 'REQUIRED'],
                                         'LoyaltyCheckCount': ['INTEGER', 'REQUIRED'],
                                         'Total': ['FLOAT', 'REQUIRED'],
                                         'AccessorySum': ['FLOAT', 'REQUIRED']},

                                 'calculatedFields': {
                                
                                         'Year0_sTotal': ['FLOAT', 'REQUIRED'],
                                         'Year0_minus_1_sTotal': ['FLOAT', 'REQUIRED'],
                                         'Year0_minus_2_sTotal': ['FLOAT', 'REQUIRED'],

                                         'Year0_PeopleIN': ['FLOAT', 'NULLABLE'],
                                         'Year0_minus_1_PeopleIN': ['FLOAT', 'REQUIRED'],
                                         'Year0_minus_2_PeopleIN': ['FLOAT', 'REQUIRED'],

                                         'Year0_Checkout': ['FLOAT', 'NULLABLE'],
                                         'Year0_minus_1_Checkout': ['FLOAT', 'REQUIRED'],
                                         'Year0_minus_2_Checkout': ['FLOAT', 'REQUIRED']}
                                 



                                 }}
         



toexclude = ['Year0_sTotal', 'Year0_minus_1_sTotal', 'Year0_minus_2_sTotal', 'Year0_PeopleIN', 'Year0_minus_1_PeopleIN', 'Year0_minus_2_PeopleIN', 'Year0_Checkout', 'Year0_minus_1_Checkout', 'Year0_minus_2_Checkout']

#Translation dictionaries from Bigquery to SQL notation
SQLDatatypes = {'INTEGER':'INT', 'FLOAT': 'FLOAT', 'DATE': 'DATE', 'STRING': 'VARCHAR(100)'}
SQLValidation = {'REQUIRED': 'NOT NULL', 'NULLABLE': None}



query = """SELECT d.Date as 'FullDate',
       d.yy as 'Year',
       d.mm as 'Month',
       d.dd as 'Day',
       d.week as 'Week',
       d.day7 as 'Day7',
       f.CurrencyId as 'CurrencyId',
       RTRIM(LTRIM(FAC.Company)) as 'FACCompany',
       RTRIM(LTRIM(FAC.Departament)) as 'FACDepartment',
       RTRIM(LTRIM(FAC.FACName)) as 'FACName',
       RTRIM(LTRIM(FAC.FACGName)) as 'FACGName',
       RTRIM(LTRIM(FAC.Region)) as 'FACRegion',
       RTRIM(LTRIM(Serv.Type)) as 'Type',
       RTRIM(LTRIM(CnC.Name)) as 'ClickNCollect',
       
       /* Grupės duomenys */
       CASE WHEN TCGroupTable.ResolveTo IS NULL THEN 'Grupė nerasta' ELSE TCGroupTable.ResolveTo END as 'GroupResolveTo',
       
       /*
       TCGroupTable.TCGroupId as 'TCGroupTable_TCGroupId',
       TCGroupTable.TCGroupName as 'TCGroupTable_TCGroupName',
       CAST(TCGroupTable.ParentId AS VARCHAR(12)) as 'TCGroupTable_ParentId',
       TCGroupTable.level as 'TCGroupTable_level', */
       
       /* Metrics */
       sum(f.pQnt) as 'pQnt',
       sum(f.bQnt) as 'bQnt',
       sum(f.sQnt) as 'sQnt', 
       sum(f.pCost) as 'pCost',
       sum(f.pTotal) as 'pTotal',
       sum(f.bCost) as 'bCost',
       sum(f.sCost) as 'sCost',
       sum(f.sTotal) as 'sTotal',
       sum(f.sDiscount) as 'sDiscount',
       sum(f.PriceIVAT) as 'PriceIVAT',
       sum(f.PeopleIn) as 'PeopleIn',
       sum(f.CheckCount) as 'CheckCount',
       sum(f.LoyaltyCheckCount) as 'LoyaltyCheckCount',
       sum(f.Total) as 'Total',
       sum(f.AccessorySum) as 'AccessorySum'
       
       
       
FROM            [factAll] AS f
LEFT JOIN [dimDate] as d
ON f.DateId = d.DateId

/* Table join was in the original so will not keep */
LEFT OUTER JOIN
factProductInfo AS p ON f.BusinessUnitId = p.BUCode AND f.ProductId = p.ProductId

LEFT JOIN [FACDep] AS FAC
ON f.BusinessUnitId = FAC.code

LEFT JOIN [vServiceAll] as Serv
ON f.ServiceId = Serv.ServiceId

LEFT JOIN [vdimClickNCollect] as CnC
ON f.ClickNCollect = CnC.Id

LEFT JOIN [dimProduct] Prod
ON f.ProductID = Prod.ProductId


/* Joining Group table already fixed to resolve quickly to parent id */

LEFT JOIN (

    SELECT lv7.TCGroupId, lv7.TCGroupName, lv7.ParentId, lv7.[level], lv2.TCGroupName as 'ResolveTo'  FROM (

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 7 AND TCGroupId is not Null) lv7

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 6 AND TCGroupId is not Null) lv6

    ON lv7.ParentId = lv6.TCGroupId

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 5 AND TCGroupId is not Null) lv5

    ON lv6.ParentId = lv5.TCGroupId

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 4 AND TCGroupId is not Null) lv4

    ON lv5.ParentId = lv4.TCGroupId

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 3 AND TCGroupId is not Null) lv3

    ON lv4.ParentId = lv3.TCGroupId

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 2 AND TCGroupId is not Null) lv2

    ON lv3.ParentId = lv2.TCGroupId
    )





    UNION ALL





    SELECT lv6.TCGroupId, lv6.TCGroupName, lv6.ParentId, lv6.[level], lv2.TCGroupName as 'ResolveTo'  FROM (

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 6 AND TCGroupId is not Null) lv6

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 5 AND TCGroupId is not Null) lv5

    ON lv6.ParentId = lv5.TCGroupId

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 4 AND TCGroupId is not Null) lv4

    ON lv5.ParentId = lv4.TCGroupId

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 3 AND TCGroupId is not Null) lv3

    ON lv4.ParentId = lv3.TCGroupId

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 2 AND TCGroupId is not Null) lv2

    ON lv3.ParentId = lv2.TCGroupId
    )



    UNION ALL





    SELECT lv5.TCGroupId, lv5.TCGroupName, lv5.ParentId, lv5.[level], lv2.TCGroupName as 'ResolveTo'  FROM (

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 5 AND TCGroupId is not Null) lv5

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 4 AND TCGroupId is not Null) lv4

    ON lv5.ParentId = lv4.TCGroupId

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 3 AND TCGroupId is not Null) lv3

    ON lv4.ParentId = lv3.TCGroupId

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 2 AND TCGroupId is not Null) lv2

    ON lv3.ParentId = lv2.TCGroupId
    )



    UNION ALL





    SELECT lv4.TCGroupId, lv4.TCGroupName, lv4.ParentId, lv4.[level], lv2.TCGroupName as 'ResolveTo'  FROM (
    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 4 AND TCGroupId is not Null) lv4

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 3 AND TCGroupId is not Null) lv3

    ON lv4.ParentId = lv3.TCGroupId

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 2 AND TCGroupId is not Null) lv2

    ON lv3.ParentId = lv2.TCGroupId
    )

    UNION ALL


    SELECT lv3.TCGroupId, lv3.TCGroupName, lv3.ParentId, lv3.[level], lv2.TCGroupName as 'ResolveTo'  FROM (
    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 3 AND TCGroupId is not Null) lv3

    LEFT JOIN

    (SELECT TCGroupId, TCGroupName, CAST(ParentId AS VARCHAR(12)) AS ParentId, [level]
    FROM dbo.vdimTCGroup
    WHERE [level] = 2 AND TCGroupId is not Null) lv2

    ON lv3.ParentId = lv2.TCGroupId
    )
    
    ) TCGroupTable

ON Prod.TCGroupId = TCGroupTable.TCGroupId




/* Service id -1 is for preke */
WHERE d.yy >= {} AND f.ServiceId = '-1' AND d.Date < convert(varchar, getdate(), 23)

GROUP BY d.Date, d.yy, d.mm, d.dd, d.week, d.day7, f.CurrencyId, FAC.Company, FAC.Departament, FAC.FACName, FAC.FACGName, FAC.Region, Serv.Type, CnC.Name, TCGroupTable.ResolveTo""".format((datetime.datetime.now() - datetime.timedelta(days=365.242 * 2)).strftime("%Y"))



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
        

    def RemoveOldData(self):
        
        printlog("Removing Old Data... ")
        
        queryRD = """
        DELETE FROM `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""` 
        WHERE """ + [x for x in schema[self.settings['table']]['queryFields'].keys()][0] + """ IS NOT NULL"""
        
        printlog(queryRD)
        
        query_job = self.settings['bigquery'].query(queryRD)
        
        results = query_job.result()  # Waits for job to complete.  
  
        printlog([x for x in results])
        printlog("Removing Old Data... Done")
        
        
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


    def DeleteAndCreateTableWithinSQL(self, database, table):
        printlog('Deleting and Recreating table to load to SQL...')
        conn = pymssql.connect(server=self.server, user=user, password=password, database=database)
        cursor = conn.cursor()
        SQLSchema = []


        for k, v in {**schema[table]['queryFields'], **schema[table]['calculatedFields']}.items():
            SQLSchema.append(k + " " + SQLDatatypes[v[0]] + (" " + SQLValidation[v[1]] if SQLValidation[v[1]] != None else "" ))

        print("""IF OBJECT_ID('{}', 'U') IS NOT NULL\nDROP TABLE {}\n""".format(table, table) + """CREATE TABLE {} ({})""".format(table, ", ".join(SQLSchema)))
        cursor.execute("""IF OBJECT_ID('{}', 'U') IS NOT NULL\nDROP TABLE {}\n""".format(table, table) + """CREATE TABLE {} ({})""".format(table, ", ".join(SQLSchema)))
        conn.commit()
        conn.close()
        printlog('Deleting and Recreating table to load to SQL... Done')


    def UploadTransformedSQLDataToAnotherTable(self, database, table, data):
        printlog('Uploading data to SQL ...')
        conn = pymssql.connect(server=self.server, user=user, password=password, database=database)
        cursor = conn.cursor()
        sql = "INSERT INTO {}({}) VALUES ({})".format(table, ", ".join([x for x in {**schema[table]['queryFields'], **schema[table]['calculatedFields']}.keys()]), ", ".join(['%s' for x in {**schema[table]['queryFields'], **schema[table]['calculatedFields']}.keys()]))
        #print(sql)

        cursor.executemany(sql, [tuple(x) for x in data.values.tolist()])

        conn.commit()
        conn.close()

        printlog('Uploading data to SQL ... Done')


    def cleandata(self, table):
        
        self.settings['table'] = table
        
        datatypes = {'INTEGER': int,
             'FLOAT': float,
             'STRING': str}

        for key, value in {x:y for x, y in schema[self.settings['table']]['queryFields'].items() if y[0] != 'DATE'}.items():
            self.data[key] = self.data[key].astype(datatypes[value[0]])
            #Fill na values
            if value[0] in ['INTEGER', 'FLOAT']:
                self.data[key].fillna(0, inplace=True)

        for key, value in {x:y for x, y in schema[self.settings['table']]['queryFields'].items() if y[0] == 'DATE'}.items():
            #printlog(key, value)
            self.data[key] = pd.to_datetime(self.data[key])
            #self.data[key] = self.data[key].map(lambda x: x.strftime("%Y-%m-%d"))
            
   #Transform all years so it shows equivalent YTD time period for previous years         
    def TransformYTD(self):
        #maxmonth = self.data[self.data['Year'] == self.data['Year'].max()]['Month'].max()
        #maxday = self.data[(self.data['Year'] == self.data['Year'].max()) & (self.data['Month'] == maxmonth)]['Day'].max()
        
        if self.data['FullDate'].max() <= datetime.datetime.now():
            maxday = self.data['FullDate'].max().day
            maxmonth = self.data['FullDate'].max().month
        else:
            maxday = datetime.datetime.now().day
            maxmonth = datetime.datetime.now().month
        
        #Filter out month
        self.data = self.data[(self.data['Month'] <= maxmonth)]
        
        #Filter only for max day
        for field in ['pQnt', 'bQnt', 'sQnt', 'pCost', 'pTotal', 'bCost', 'sCost', 'sTotal',
                      'sDiscount', 'PriceIVAT', 'PeopleIn', 'CheckCount', 'LoyaltyCheckCount', 'Total', 'AccessorySum']:

            self.data[field] = self.data.apply(lambda x: 0 if (x['Day'] > maxday and x['Month'] == maxmonth) else x[field], axis=1)

        #self.data[(self.data['Month'] == maxmonth)] = self.data[(self.data['Month'] == maxmonth) & (self.data['Day'] <= maxday)]



        #Filter out day
        #self.data = self.data[(self.data['Month'] == maxmonth) & (self.data['Year'] == self.data['Year'].max())] ['Day'].max()



        for key, value in {x:y for x, y in schema[self.settings['table']]['queryFields'].items() if y[0] == 'DATE'}.items():
            self.data[key] = self.data[key].map(lambda x: x.strftime("%Y-%m-%d"))


    def AddYearSalesColumns(self):
        #Add 3x year totals  as columns, i.e highest year to lowers year. So that google data studio could be used with tables to use change functionality
        years = [x for x in self.data.Year.unique()]
        print(years)
        max_year = max(years)
        #years = sorted(years, reverse=True)

        #Sales by ear transpose to columns
        for year in range(max_year, max_year-3, -1):
            self.data[str(year) + "_sTotal"] = self.data.apply(lambda x: x['sTotal'] if x['Year'] == year else 0, axis=1)

        #PeopleIn by ear transpose to columns
        for year in range(max_year, max_year-3, -1):
            self.data[str(year) + "PeopleIn"] = self.data.apply(lambda x: x['PeopleIn'] if x['Year'] == year else 0, axis=1)

        #Checkout by ear transpose to columns
        for year in range(max_year, max_year-3, -1):
            self.data[str(year) + "CheckCount"] = self.data.apply(lambda x: x['CheckCount'] if x['Year'] == year else 0, axis=1)







#Biguery object instance
Db = Database('Pardavimai_factAll')

#Delete old data from Bigquery
Db.RemoveOldData()

#Query local database for report data
SQLData = SQLObject(Servers[0]['Address'])
SQLData.getquery('wh', query)

#Perform data clean and manipulations
SQLData.cleandata('Pardavimai_factAll')
SQLData.TransformYTD()
SQLData.AddYearSalesColumns()

#Load data to bigquery
Db.load_data_from_file(SQLData.data)


#Load same data to another SQL Table locally
SQLReportUpload = SQLObject(Servers[1]['Address'])
SQLReportUpload.DeleteAndCreateTableWithinSQL('PreparedReports', 'Pardavimai_factAll')
SQLReportUpload.UploadTransformedSQLDataToAnotherTable('PreparedReports', 'Pardavimai_factAll', SQLData.data)

