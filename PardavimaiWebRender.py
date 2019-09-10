#!/venv/bin python3
from flask import Flask, request, redirect, url_for, flash, render_template, send_from_directory
import logging
import os
from google.cloud import bigquery
import csv
import datetime
import pymssql
import pandas as pd
import os
import pickle
from werkzeug.utils import secure_filename
import hashlib
import numpy as np
from xhtml2pdf import pisa
from io import StringIO


#Duomenu nuostatos exceptions (pakeicia barkodus ekselio puseje kurie nesutampa su musu barkodais, bet pavadinimas 100 proc sutampa)
Nuostatos = {'EugestosBarkodaiMap': {'007117194551': '711719455165',
									'007117194105': '711719410577',
									'007117197024': '711719702412',
									'001929400532': '192940053267',
									'001925639972': '192563997207',
									'004391700023':'43917000237'
									}



			}


#Path variable
try:
	script_path = os.path.dirname(os.path.abspath(__file__)) + "/"
except:
	script_path = ""


#Flask file upload settings
UPLOAD_FOLDER = script_path + 'Upload'
ALLOWED_EXTENSIONS = set(['xlsx'])


#SETUP LOGGING
logging.basicConfig(filename=script_path + 'Paradvimuweb.log',level=logging.INFO)


#Database objects
#Bigquery Settings
dataset = "Vadybos_Ataskaitos"
projectName = "nomadic-coast-234608"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Auth/TOPO-Bigquery-Service-Acount.json"


#Bigquery schema definition
#Bigquery schema definition
schema = {'Eshop': {'Savaite': ['INTEGER', 'NULLABLE'],
			'Savaites_xx_xx': ['STRING', 'NULLABLE'],
			'Eshop': ['STRING', 'NULLABLE'],
			'Uzsakymo_busena': ['STRING', 'NULLABLE'],
			'Miestas': ['STRING', 'NULLABLE'],
			'Atsiemimas': ['STRING', 'NULLABLE'],
			'Preke': ['STRING', 'NULLABLE'],
			'PriceBucket': ['STRING', 'NULLABLE'],
			'QuantityBucket': ['STRING', 'NULLABLE'],
			'Galutine_kaina': ['FLOAT', 'NULLABLE'],
			'Kiekis_uzsakymu': ['FLOAT', 'NULLABLE'],
			'Apsipirkimu_kiekis': ['FLOAT', 'NULLABLE'],
			'Bendra_suma': ['FLOAT', 'NULLABLE']
}}


#Servers SQL config map
Servers = [{'ServerName': 'MSSS', 'Address': '192.168.0.220'},
		   {'ServerName': 'RGASNEW02', 'Address': '192.168.0.38'}]

#Credentials open
with open (script_path + 'Auth/credentials.txt', 'r', encoding='utf-8') as openfile:
	rd = csv.reader(openfile, delimiter=',')
	credentials = list(rd)
	
user=credentials[1][0]
password=credentials[1][1]


#Bigquery Database object
class Picklecheck:
	def __init__(self, filename, exp = 1):
		self.timestamp = datetime.datetime.now()
		self.filename = filename
		self.exp = exp

	def Check(self):
		if os.path.isfile(script_path + self.filename) == True:
			with open(script_path + self.filename, 'rb') as handle:
				self.pickle = pickle.load(handle)

			if self.exp != None:
				if (self.timestamp - self.pickle['timestamp']).days >= self.exp:
					return None
				else:
					return self.pickle
			else:
				return self.pickle

		else:
			return None
	def SavePickle(self, dataobject):
		#Save to pickle
		with open(script_path + self.filename, 'wb') as handle:
			pickle.dump(dataobject, handle, protocol=pickle.HIGHEST_PROTOCOL)




class Database:
	def __init__(self, table, dataset=dataset, projectName = projectName):
		
		self.settings = {}
		self.settings['projectName'] = projectName
		self.settings['table'] = table
		self.settings['dataset'] = dataset
		self.settings['bigquery'] = bigquery.Client(project=self.settings['projectName'])
		
	def getMaxDate(self):

		logging.info("Quering getMaxDate... ")

		queryRD = """
		SELECT max(FullDate) Atnaujinta_iki FROM `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""` """

		logging.info(queryRD)

		query_job = self.settings['bigquery'].query(queryRD)

		results = query_job.result()  # Waits for job to complete.  

		logging.info("Quering getMaxDate... Done")

		results = ([x[0] for x in results])
		results = results[0].strftime("%Y-%m-%d")
		return results

	def DeleteFromTable(self, keyfield):
		logging.info("Deleting data from table {}".format(self.settings['table']))

		queryDelete = """DELETE FROM {} WHERE {} IS NOT NULL""".format("""`"""+self.settings['dataset'] + """.""" + self.settings['table']+"""` """, keyfield)

		logging.info(queryDelete)

		query_job = self.settings['bigquery'].query(queryDelete)

		results = query_job.result()  # Waits for job to complete.

		logging.info("Deleting data from table {}. Done.".format(self.settings['table']))

		print("Deleting data from table {}. Done.".format(self.settings['table']))

		return results


	
	def RefreshData(self, columns, WhereStatement = None):

		logging.info("Querying All... ")
		query_job = self.settings['bigquery'].query("""SELECT * FROM """ + self.settings['dataset'] + """.""" + self.settings['table'] + (""" WHERE """ + WhereStatement) if WhereStatement != None else "")

		try:
			results = query_job.result()  # Waits for job to complete.
		except:
			logging.info('Adding table...')
			self.AddTable()
			results = []

		results = list([list(x) for x in results])
		results = pd.DataFrame(results, columns = columns)
		logging.info("Querying... Done")
		self.LastResults = results
		self.LastRefresh = datetime.datetime.now()
		logging.info('Refreshing data (bigquery)... Done. Saved to Last Results')


	def AddTable(self):
		
		schema_bq = []
		for field, datatype in schema[self.settings['table']].items():
			schema_bq.append(bigquery.SchemaField(field, datatype[0], mode=datatype[1]))
		
		table_ref = self.settings['bigquery'].dataset(self.settings['dataset']).table(self.settings['table'])
		table = bigquery.Table(table_ref, schema=schema_bq)
		table = self.settings['bigquery'].create_table(table)  # API request

		assert table.table_id == self.settings['table']
		logging.info("Table has been created {}".format(self.settings['table']))

	def load_data_from_file(self, DataFrameObject):
		
		dataset_id = self.settings['dataset']
		table_id = self.settings['table']
			
		source_file_name = script_path + 'Transfer/TableUpload_' + dataset_id + '_' + table_id + '.csv'
		
		try:
			os.remove(source_file_name)
			logging.info("File {} successfully removed".format(source_file_name))
		except:
			logging.info("File {} not found not removed".format(source_file_name))
	
		DataFrameObject.to_csv(source_file_name, index = False, header=False)
		
		logging.info("Loading file to bigquery...")
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

		logging.info('Loaded {} rows into {}:{}.'.format(
			job.output_rows, dataset_id, table_id))
		
		
		logging.info("Loading file to bigquery...Done")

		


class SQLObject:
	def __init__(self, server):
		self.server = server
		self.settings = {}
		self.timestamp = datetime.datetime.now()
	
	def getquery(self, database, query, backup = True):

		conn = pymssql.connect(server=self.server, user=user, password=password, database=database)
		cursor = conn.cursor()

		cursor.execute(query)
		rows = cursor.fetchall()
		columns = [x[0] for x in cursor.description]
		conn.close()

		#select name FROM sys.databases
		self.data = pd.DataFrame(rows, columns=columns)
		
		if backup == True:
			self.dataBackup = pd.DataFrame(rows, columns=columns)
		else:
			self.dataBackup = None


class EugestaParse:
	def __init__ (self):
		self.data = {}
		self.RunDatetime = None
		self.SheetName = 'finansiniai likučiai'.lower().strip()
		self.ResultData = None
		self.pickle = Picklecheck("EugestaParse.pickle", None)

	def run(self):
		self.RunDatetime = datetime.datetime.now()
		pickle_check = self.pickle.Check()
		if pickle_check != None and 'EugestaUpload' not in GlobalCache.keys():
			return pickle_check
		else:
			Inputdocuments = self.parseFiles()

			if Inputdocuments == False:
				#raise ValueError("No Eugesta data in excel files")
				flash('No Eugesta data in excel files. Please upload new file')
				return None
			elif Inputdocuments == None:
				flash('Unknown error. Please check files try again.')
				return None
			else:
				EugestaBigqueryData = Database('Eugesta_Snapshot', 'OperationsFileParsing')
				EugestaBigqueryData.RefreshData(['Timestamp', 'N37_BAR_KODAS', 'I17_KIEKIS', 'N37_PAV', 'N37_TRUM_PAV'], str("""Timestamp = '""" + self.RunDatetime.strftime("%Y-%m-%d") + """'"""))
				EugestaBigqueryData.LastResults['Timestamp'] = pd.to_datetime(EugestaBigqueryData.LastResults['Timestamp'])
				EugestaBigqueryData.LastResults = EugestaBigqueryData.LastResults[EugestaBigqueryData.LastResults['Timestamp'] == self.RunDatetime.date()][['N37_BAR_KODAS', 'I17_KIEKIS', 'N37_PAV', 'N37_TRUM_PAV']]

				if EugestaBigqueryData.LastResults.shape[0] == 0:
					printlog("No Eugesta data for {}".format(self.RunDatetime.date().strftime("%Y-&m-%d")))
					return None

				#Map prieš sujungiant
				self.ResultData['BAR kodas'] = self.ResultData['BAR kodas'].map(lambda x: Nuostatos['EugestosBarkodaiMap'][x] if x in Nuostatos['EugestosBarkodaiMap'].keys() else x)

				#Merge
				self.ResultData = self.ResultData.merge(EugestaBigqueryData.LastResults, left_on='BAR kodas', right_on='N37_BAR_KODAS', how='outer').rename(columns={'N37_BAR_KODAS': 'TOPO Eugestos kodas', 'I17_KIEKIS': 'TOPO Eugestos kiekis', 'N37_PAV': 'TOPO Eugestos pavadinimas', 'N37_TRUM_PAV': 'TOPO Eugestos trumpas pavadinimas'})
				self.ResultData['Sujungimo Tipas'] = self.ResultData.apply(lambda x: "Nerasta TOPO sandėlyje" if str(x['TOPO Eugestos kodas']) == 'nan' else ("Nerasta Eugestos ataskaitoje" if str(x['BAR kodas']) == 'nan' else 'Sujungta'), axis=1)

				self.ResultData['RunDateTime'] = self.ResultData['RunDateTime'].map(lambda x: self.RunDatetime if str(x) == 'NaT' else x)
				self.ResultData['hash'] = self.ResultData['hash'].map(lambda x: None if str(x) == 'nan' else x)

				#Filter specific columns
				self.ResultData.fillna(value={'TOPO Eugestos kiekis': 0, 'Likutis': 0}, inplace=True)
				self.ResultData['Likučio skirtumas'] = self.ResultData.apply(lambda x: int(x['TOPO Eugestos kiekis']) - int(x['Likutis']), axis=1)
				self.ResultData['Likučio skirtumas sort'] = self.ResultData['Likučio skirtumas']
				self.ResultData['Likučio skirtumas sort'] = self.ResultData['Likučio skirtumas sort'].map(lambda x: ((int(x) * -1) if int(x) < 0 else x) if np.isnan(x) == False else 0)		

				columnsToExclude = ['hash', 'RunDateTime', 'Likučio skirtumas sort']
				self.ResultData.sort_values(by='Likučio skirtumas sort', ascending=False, inplace=True)
				self.ResultData = self.ResultData[[x for x in self.ResultData.columns if x not in columnsToExclude]]

				#Paskutinis išfiltravimas
				self.ResultData = self.ResultData[self.ResultData.apply(lambda x: False if (x["Sujungimo Tipas"] == "Nerasta Eugestos ataskaitoje" and int(x['TOPO Eugestos kiekis']) == 0) else True, axis=1) == True]
				self.ResultData = self.ResultData[self.ResultData.apply(lambda x: False if (x["Sujungimo Tipas"] == "Nerasta TOPO sandėlyje" and int(x['Likutis']) == 0) else True, axis=1) == True]


				#Produce summary dictionary
				Summary = {}
				Summary['Viso eilučių'] = self.ResultData.shape[0]
				Summary['Viso sujungtų'] = self.ResultData[self.ResultData['Sujungimo Tipas'] == 'Sujungta'].shape[0]
				Summary['Viso sujungtų %'] = round((Summary['Viso sujungtų'] / Summary['Viso eilučių']) * 100, 0)

				Summary["Nerasta TOPO sandėlyje"] = self.ResultData[self.ResultData['Sujungimo Tipas'] == "Nerasta TOPO sandėlyje"].shape[0]
				Summary['Nerasta TOPO sandėlyje %'] = round((Summary['Nerasta TOPO sandėlyje'] / Summary['Viso eilučių']) * 100, 0)

				Summary["Nerasta Eugestos ataskaitoje"] = self.ResultData[self.ResultData['Sujungimo Tipas'] == "Nerasta Eugestos ataskaitoje"].shape[0]
				Summary['Nerasta Eugestos ataskaitoje %'] = round((Summary['Nerasta Eugestos ataskaitoje'] / Summary['Viso eilučių']) * 100, 0)

				Summary["Viso vnt nesutapo"] = self.ResultData[self.ResultData['Sujungimo Tipas'] == 'Sujungta'].apply(lambda x: abs(int(x['TOPO Eugestos kiekis']) - int(x['Likutis'])), axis=1).sum()
				Summary["Eilučių pertekliaus"] = self.ResultData[(self.ResultData['Sujungimo Tipas'] == 'Sujungta') & (self.ResultData['Likučio skirtumas'] > 0)].shape[0]
				Summary["Eilučių trūkumo"] = self.ResultData[(self.ResultData['Sujungimo Tipas'] == 'Sujungta') & (self.ResultData['Likučio skirtumas'] < 0)].shape[0]


				#Paskutinis išfiltravimas




				#Convert to dictionary
				self.ResultData = self.ResultData.to_dict(orient='records')
				
				self.pickle.SavePickle({'data': self.ResultData, 'timestamp': self.RunDatetime, 'summary': Summary})
				del GlobalCache['EugestaUpload']
				return {'data': self.ResultData, 'timestamp': self.RunDatetime, 'summary': Summary}




	def parseFiles(self):
	
		xlsx_failai = [x for x in os.listdir(script_path + "Upload/") if x[-4:] == 'xlsx' and 'NERASTAS_Tiekimo_Lapas----' not in x and 'IMPORT_COMPLETED' not in x]
		csv_failai = [x for x in os.listdir(script_path + "Upload/") if x[-3:] == 'csv' and 'NERASTAS_Tiekimo_Lapas----' not in x and 'IMPORT_COMPLETED' not in x]

		print(xlsx_failai)
		print(csv_failai)
		
		if len(xlsx_failai) == 0 and len(csv_failai) == 0:
			return False
		
		#Process csv files
		for xls in xlsx_failai:
			self.data[xls] = {}
			
			for sheet in pd.ExcelFile(script_path + 'Upload/' + xls).sheet_names:
				self.data[xls][sheet.lower().strip()] = {'data': pd.read_excel(script_path + 'Upload/' + xls, sheet)}
				self.data[xls][sheet.lower().strip()]['data'] = self.data[xls][sheet.lower().strip()]['data'].rename(columns = {x:str(x).strip() for x in self.data[xls][sheet.lower().strip()]['data'].columns})
				self.data[xls][sheet.lower().strip()]['hash'] = hashlib.md5(self.data[xls][sheet.lower().strip()]['data'].values.tobytes()).hexdigest()
				
		#Process xls files
		for csvfile in csv_failai:
			self.data[csvfile] = {'No Sheet': {'data': pd.DataFrame.from_csv(script_path + "Upload/" + csvfile)}}
			self.data[csvfile]['No Sheet']['data'] = self.data[csvfile]['No Sheet']['data'].rename(columns={x:str(x).strip() for x in self.data[csvfile]['No Sheet']['data'].columns})
		
		self.FilterOutwithRelevantSheetsElseRename()
		self.stripbarcodes()
		return True
		
		
	def FilterOutwithRelevantSheetsElseRename(self):
		for file in self.data.keys():
			if self.SheetName in [x for x in self.data[file].keys()]:
				if self.ResultData == None:
					self.ResultData = self.data[file][self.SheetName]['data']
					self.ResultData['hash'] = self.data[file][self.SheetName]['hash']
					self.ResultData['RunDateTime'] = self.RunDatetime
					os.rename(script_path + "Upload/" + file, script_path + 'Upload/' + "IMPORT_COMPLETED----" + file)
				else:
					temp = self.ResultData.append(self.data[file][self.SheetName]['data'])
					temp['hash'] = self.data[file][self.SheetName]['hash']  
					temp['RunDateTime'] = self.RunDatetime
					self.ResultData = self.ResultData.append(temp)
					del temp
					os.rename(script_path + "Upload/" + file, script_path + 'Upload/' + "INPORT_COMPLETED----" + file)
				
			
			else:
				os.rename(script_path + "Upload/" + file, script_path + 'Upload/' + "NERASTAS_Tiekimo_Lapas----" + file)
				
	def stripbarcodes(self):
		#self.ResultData['BAR kodas'] = self.ResultData['BAR kodas'].astype(str)
		self.ResultData = self.ResultData[(self.ResultData['BAR kodas'].notnull())]
		self.ResultData['BAR kodas'] = self.ResultData['BAR kodas'].map(lambda x: (12 - len(str(int(x)))) * "0" + str(x) ) 
		self.ResultData['BAR kodas'] = self.ResultData['BAR kodas'].map(lambda x: str(x).replace('.', '').strip()[:12]) 


class EshopReport:

	def __init__(self):
		def ReturnNearestMonday(datetimeObject):
			return (datetimeObject - datetime.timedelta(days=datetimeObject.weekday()))

		#
		#(x['kada pirktas'].date() + datetime.timedelta(days=(0 - x['kada pirktas'].weekday()))).strftime("%m-%d")
		self.timestamp = datetime.datetime.now().date()
		self.timestamp = ReturnNearestMonday(self.timestamp)
		self.weekScope = 10
		self.start = self.timestamp - datetime.timedelta(days = (self.weekScope * 7))
		self.end = self.timestamp
		assert self.end.weekday() == 0
		assert self.start.weekday() == 0

		print("Start {}".format(self.start))
		print("End {}".format(self.end))
		self.ListOfCities = ['Akmenė', 'Alytus', 'Anykščiai', 'Ariogala', 'Baltoji Vokė', 'Birštonas', 'Biržai', 'Daugai', 'Druskininkai', 'Dūkštas', 'Dusetos', 'Eišiškės', 'Elektrėnai',
							'Ežerėlis', 'Gargždai', 'Garliava', 'Gelgaudiškis', 'Grigiškės', 'Ignalina', 'Jieznas', 'Jonava', 'Joniškėlis', 'Joniškis', 'Jurbarkas', 'Kaišiadorys', 'Kalvarija',
							'Kaunas', 'Kavarskas', 'Kazlų Rūda', 'Kėdainiai', 'Kelmė', 'Kybartai', 'Klaipėda', 'Kretinga', 'Kudirkos Naumiestis', 'Kupiškis', 'Kuršėnai', 'Lazdijai',
							'Lentvaris', 'Linkuva', 'Marijampolė', 'Mažeikiai', 'Molėtai', 'Naujoji Akmenė', 'Nemenčinė', 'Neringa', 'Nida', 'Obeliai', 'Pabradė', 'Pagėgiai', 'Pakruojis',
							'Palanga', 'Pandėlys', 'Panemuzė', 'Panevėžys', 'Pasvalys', 'Plungė', 'Priekulė', 'Prienai', 'Radviliškis', 'Ramygala', 'Raseiniai', 'Rietavas', 'Rokiškis',
							'Rūdiškės', 'Salantai', 'Seda', 'Simnas', 'Skaudvilė', 'Skuodas', 'Smalininkai', 'Subačius', 'Šakiai', 'Šalčininkai', 'Šeduva', 'Šiauliai', 'Šilalė',
							'Šilutė', 'Širvintos', 'Švenčionėliai', 'Švenčionys', 'Tauragė', 'Telšiai', 'Tytuvėnai', 'Trakai', 'Troškūnai', 'Ukmergė', 'Utena', 'Užventis', 'Vabalninkas', 'Varėna',
							'Varniai', 'Veisiejai', 'Venta', 'Viekšniai', 'Vievis', 'Vilkaviškis', 'Vilkija', 'Vilnius', 'Virbalis', 'Visaginas', 'Zarasai', 'Žagarė', 'Žiežmariai']
		
		self.ListOfShops = [{'Address': "Ukmergės g. 240", 'City': "Vilnius", "AndCity": False, 'Equal': False},
							{'Address': 'Žirmūnų g. 64', 'City': "Vilnius", "AndCity": False, 'Equal': False},
							{'Address': 'Ozo g. 18', 'City': "Vilnius", "AndCity": False, 'Equal': False},
							{'Address': 'Upės g. 9', 'City': "Vilnius", "AndCity": False, 'Equal': False},
							{'Address': 'Ozo g. 25', 'City': "Vilnius", "AndCity": False, 'Equal': False},
							{'Address': 'Ukmergės g. 369', 'City': "Vilnius", "AndCity": False, 'Equal': False},

							{'Address': 'Savanorių pr. 206', 'City': "Kaunas", "AndCity": False, 'Equal': False},
							{'Address': 'Karaliaus Mindaugo pr. 49', 'City': "Kaunas", "AndCity": False, 'Equal': False},
							{'Address': 'Islandijos pl. 32', 'City': "Kaunas", "AndCity": False, 'Equal': False},
							{'Address': 'Jonavos g. 60', 'City': "Kaunas", "AndCity": False, 'Equal': False},

							{'Address': 'Taikos pr. 64', 'City': "Klaipėda", "AndCity": False, 'Equal': False},
							{'Address': 'Klaipėdos g. 143', 'City': "Panevėžys", "AndCity": False, 'Equal': False},
							{'Address': 'Aido g. 8', 'City': "Šiauliai", "AndCity": False, 'Equal': False},

							{'Address': 'Ūdrijos g. 1', 'City': "Alytus", "AndCity": False, 'Equal': False},
							

							{'Address': 'Čiurlionio g. 55', 'City': "Druskininkai", "AndCity": False, 'Equal': False},
							

							{'Address': 'Basanavičiaus g. 93', 'City': "Kėdainiai", "AndCity": False, 'Equal': False},
							

							{'Address': 'S.Dariaus ir S.Girėno g. 3A', 'City': "Marijampolė", "AndCity": False, 'Equal': False},

							{'Address': 'Laisvės g. 7 ', 'City': "Mažeikiai", "AndCity": False, 'Equal': False},
							{'Address': 'Vilniaus g. 126', 'City': "Raseiniai", "AndCity": False, 'Equal': False},

							{'Address': 'Respublikos g. 106', 'City': "Rokiškis", "AndCity": False, 'Equal': False},
							{'Address': 'Dariaus ir Girėno g. 9', 'City': "Tauragė", "AndCity": False, 'Equal': False},
							{'Address': 'Kęstučio g. 20-1', 'City': "Telšiai", "AndCity": False, 'Equal': False},
							{'Address': 'Aušros g. 21', 'City': "Utena", "AndCity": False, 'Equal': False},
							{'Address': 'Baranausko g. 44', 'City': "Utena", "AndCity": False, 'Equal': False},

							{'Address': 'Kęstučio g. 2', 'City': "Biržai", 'Equal': False, "AndCity": False},
		
							
							{'Address': 'Basanavičiaus g. 6', 'City': "Jonava", 'Equal': False, "AndCity": True},
					


							{'Address': 'Algirdo g. 1A, Jurbarkas - PC Maxima . ', 'City': "Jurbarkas", 'Equal': True, "AndCity": False},
							{'Address': 'Žaltakalnio g. 20A', 'City': "Plungė", 'Equal': False, "AndCity": True},
							{'Address': 'Vytauto g. 17B', 'City': "Prienai", 'Equal': False, "AndCity": True},
							{'Address': 'Tilžės g. 109', 'City': "Šiauliai", "AndCity": False, 'Equal': False},

							{'Address': 'J.Basanavičiaus g. 3', 'City': "Panevėžys", "AndCity": True, 'Equal': False},

							{'Address': 'Žemaitijos g. 38', 'City': "Mažeikiai", "AndCity": False, 'Equal': False},
							{'Address': 'V. Kudirkos g. 3', 'City': "Marijampolė", "AndCity": True, 'Equal': False}


							#V. Kudirkos g. 3

							#J.Basanavičiaus g. 3

							]



	def Run(self):
		if os.path.isfile(script_path + 'data_EshopReport.pickle') == True:
			with open(script_path + 'data_EshopReport.pickle', 'rb') as handle:
				PickleData = pickle.load(handle)

			if (self.timestamp - PickleData['timestamp']).days >= 1:
				self.GetReport()
				self.PushBigqueryVisualsData()
				return self.OutputWebData()
			else:
				return PickleData['data_savaite'], PickleData['data_savaitgalis'], PickleData['timestamp'].strftime("%Y-%m-%d")
		else:
			self.GetReport()
			self.PushBigqueryVisualsData()
			return self.OutputWebData()


	def GetReport(self):
		EshopSQLReport = SQLObject(Servers[0]['Address'])
		query = """SELECT CASE WHEN h.companyid = 1000 THEN 'TC' ELSE 'EU'  END as eshop, h.OrderStatus as 'Užsakymo būsena', h.PaymentStatus as 'Apmokėjimo būsena',  h.BillFirstName Vardas, h.BillLastName 'Pavardė', odt.Descr as 'Atsiemimas', h.BillStreet + ' '+ ISNULL(h.BillCountry, '') AS Adresas, h.BillEmail as 'El paštas'
					, ISNULL(h.BillPhone, h.DelPhone) as 'Telefonas', [Guarantee]'Gamintojo garantija', CONVERT(VARCHAR(10),h.OrderDate,102) 'kada pirktas',
					h.OrderNr 'Pirkimo nr.', ISNULL(p.Descr,d.supplierArticleid) 'Prekė', h.Remark 'Pastaba', d.Price 'Galutine kaina', Count(d.Id) 'Kiekis uzsakymu'
					  FROM [ERP].[dbo].[OrderProxyD] d inner join [ERP].[dbo].[OrderProxyH] h  on d.hId = h.id
					   -- inner join erp.dbo.product p on d.ProductId = p.id 
						left join erp.dbo.product p on d.ProductId = p.id
						LEFT JOIN [OxDeliveryType] odt ON h.DeliveryType = odt.OXID AND h.CompanyId = odt.TradeNetworkId
					  where 1=1
					   -- and d.N17Type = 1 
					   and h.OrderDate between '{}' and '{}'
					  group by h.BillFirstName , h.BillLastName , h.BillStreet ,h.BillCountry,
					h.BillPhone, h.DelPhone, [Guarantee], h.OrderDate,
					h.OrderNr, p.Descr , h.Remark, d.Price, h.companyid,  h.BillEmail, h.OrderStatus, h.PaymentStatus, odt.Descr, d.supplierArticleid
					order by h.OrderDate""".format(self.start.strftime("%Y-%m-%d"), self.end.strftime("%Y-%m-%d"))

		#Removed from query h.Remark 'Pastaba'
		print(query)
		EshopSQLReport.getquery("ERP", query, False)
		self.data = EshopSQLReport.data
		self.data = self.data[self.data['Užsakymo būsena'] != 'wait_payment']


		#Global Data Modifications
		for metric in ['Galutine kaina', 'Kiekis uzsakymu']:
						self.data[metric] = self.data[metric].astype(float)
		
		#----To Datetime
		self.data['kada pirktas'] = pd.to_datetime(self.data['kada pirktas'])
		
		#Additional Dimensions
		self.data['Savaite'] = self.data.apply(lambda x: x['kada pirktas'].isocalendar()[1], axis=1)
		self.data['Savaites xx:xx'] = self.data.apply(lambda x: (x['kada pirktas'].date() + datetime.timedelta(days=(0 - x['kada pirktas'].weekday()))).strftime("%m-%d") + " iki " + (x['kada pirktas'].date() + datetime.timedelta(days=(6 - x['kada pirktas'].weekday()))).strftime("%m-%d"), axis=1)
		Apsipirkimai_dict = self.data.groupby('Pirkimo nr.')['Pirkimo nr.'].count().to_dict()
		#print(Apsipirkimai_dict)
		self.data['Apsipirkimų kiekis'] = self.data.apply(lambda x: (1 / Apsipirkimai_dict[x['Pirkimo nr.']]) if x['Pirkimo nr.'] != None else 0, axis=1)
		del Apsipirkimai_dict
		self.data['Bendra suma'] = self.data.apply(lambda x: x['Galutine kaina'] * x['Kiekis uzsakymu'], axis=1)

		self.data['Adresas'] = self.data['Adresas'].map(lambda x: x.strip())
		self.data.to_csv('debug_data.csv', index=False)
		
	def PushBigqueryVisualsData(self):
		#Save unprocessed raw data to bigquery for google data studio visualisations
		#Later will need to check whether to apply some of the modifications
		dbBigqueryInstance = Database('Eshop')

		try:
			dbBigqueryInstance.AddTable()
		except:
			logging.info("Table already exists")
			print('Unable to add new table it already exists')
		
		dbBigqueryInstance.DeleteFromTable('Eshop')
		

		#Copy data for bigquery upload and convert to the right dimensions for graphs
		BigqueryData = self.data.copy()
		
		#Get City
		def GetCity(adresas):
			for city in self.ListOfCities:
				if city.lower().strip() in adresas.lower().strip():
					return city
			return 'Kiti miestai'

		BigqueryData['Miestas'] = BigqueryData.apply(lambda x: GetCity(x['Adresas']), axis=1)

		#Buckets
		def PriceBuckets(x):
			if x<=50:
				return "(0) 0 iki 50 Eur"
			elif x>50 and x<=100:
				return "(1) 51 iki 100 Eur"
			elif x>100 and x<=180:
				return "(2) 101 iki 180 Eur"
			elif x>180 and x<=200:
				return "(3) 181 iki 200 Eur"
			elif x>200 and x<=250:
				return "(4) 201 iki 250 Eur"
			elif x>250 and x<=1000:
				return "(5) 251 iki 1000 Eur"

			elif x>1000:
				return "(6) 1000 ir daugiau Eur"
			else:
				"Error"


		def QuantityBuckets(x):
			if x == 1:
				return "(0) Viena prekė"
			elif x==2:
				return "(1) Dvi prekės"
			elif x>2 and x<=5:
				return "(2) Nuo 3 iki 5 prekių"
			elif x>5:
				return "(3) 6 ir daugiau preki7"

			else:
				return "Error" 


		BigqueryData['PriceBucket'] = BigqueryData.apply(lambda x: PriceBuckets(x['Galutine kaina']), axis=1)
		BigqueryData['QuantityBucket'] = BigqueryData.apply(lambda x: QuantityBuckets(x['Kiekis uzsakymu']), axis=1)


		BigqueryData = BigqueryData.groupby(['Savaite', 'Savaites xx:xx', 'eshop', 'Užsakymo būsena', 'Miestas', 'Atsiemimas', 'Prekė', 'PriceBucket', 'QuantityBucket']).agg({'Galutine kaina': 'mean', 'Kiekis uzsakymu': 'sum', 'Apsipirkimų kiekis': 'sum', 'Bendra suma': 'sum'}).reset_index()

		def removeSpecialcharacters(stringText):
			stringText = stringText.encode()
			chars = [{'\xc5\xb3': 'ų'}, ',', ':', '\t', '\n', """\a""", """\b""", """\cx""", """\C-x""", """\e""", """\f""", """\M-\C-x""", """\nnn""", """\r""", """\s""", r"""\v""", r"""\x""", r"""\xnn""", """'""","""`""","""~""", '''"''', '+', '/', '?', '%', '!', '&', '*', '^', '$', '@', '#', '.']
			if stringText != None:
				if str(stringText).isalnum() == True:
					for char in chars:
						if type(char) == str:
							stringText = str(stringText).replace(char, '')
						else:
							stringText = str(stringText).replace(list(char.keys())[0], list(char.values())[0])
					stringText = str(stringText).strip()
				return str(stringText)
			else:
				return str(stringText)

		BigqueryData['Prekė'] = BigqueryData['Prekė'].map(lambda x: x.encode() if x != None else "")
		dbBigqueryInstance.load_data_from_file(BigqueryData)
		del BigqueryData
		logging.info("Pushed data to Bigquery")
		print('Pushed data to bigquery')
				
	
	def OutputWebData(self):
		assert hasattr(self, 'data') == True, "Please RunReport method first"
			
		Metrikos = ['Kiekis uzsakymu', 'Apsipirkimų kiekis']
		Dimensijos = ['Savaite', 'Savaites xx:xx', 'Savaitgalis xx:xx', 'Savaitgalis True/False', 'Adresas', 'Užsakymo būsena', 'eshop', 'Atsiemimas']

		self.data['Savaitgalis xx:xx'] = self.data.apply(lambda x: (x['kada pirktas'].date() + datetime.timedelta(days=(4 - x['kada pirktas'].weekday()))).strftime("%m-%d") + " iki " + (x['kada pirktas'].date() + datetime.timedelta(days=(6 - x['kada pirktas'].weekday()))).strftime("%m-%d"), axis=1)
		self.data['Savaitgalis True/False'] = self.data.apply(lambda x: True if x['kada pirktas'].weekday() >= 4 else False, axis=1)

		#Fix issue of streen number after dot missing space
		self.data['Adresas'] = self.data['Adresas'].map(lambda x: x.replace('.', '. ') if ('.' in x and '. ' not in x.strip()) else x)

		def CalculateMetrics(DataFrame):

			for key, value in Calculations.items():
				DataFrame[key] = DataFrame.apply(value, axis=1)
				DataFrame[key] = DataFrame[key].astype(int)

			return DataFrame


		def CalculateMetricsCities(DataFrame):
			PristatymasTipas = ['atsiemimas parduotuveje', 'atsiemimas parduotuve', 'savitarna']

			for addr in self.ListOfShops:
				if addr['Equal'] == True:
					if addr['AndCity'] == True:
						DataFrame[addr['Address'] + ", " + addr['City']] = DataFrame.apply(lambda x: int(x['Apsipirkimų kiekis']) if (addr['Address'].lower().strip() == x['Adresas'].lower().strip()) and (addr['City'].lower().strip() in x['Adresas'].lower().strip()) and (x['Atsiemimas'].lower().strip() in PristatymasTipas) else 0, axis=1)
					else:
						DataFrame[addr['Address'] + ", " + addr['City']] = DataFrame.apply(lambda x: int(x['Apsipirkimų kiekis']) if (addr['Address'].lower().strip() == x['Adresas'].lower().strip()) and (x['Atsiemimas'].lower().strip() in PristatymasTipas) else 0, axis=1)
				else:
					if addr['AndCity'] == True:
						DataFrame[addr['Address'] + ", " + addr['City']] = DataFrame.apply(lambda x: int(x['Apsipirkimų kiekis']) if (addr['Address'].lower().strip() in x['Adresas'].lower().strip()) and (addr['City'].lower().strip() in x['Adresas'].lower().strip()) and (x['Atsiemimas'].lower().strip() in PristatymasTipas) else 0, axis=1)
					else:
						DataFrame[addr['Address'] + ", " + addr['City']] = DataFrame.apply(lambda x: int(x['Apsipirkimų kiekis']) if (addr['Address'].lower().strip() in x['Adresas'].lower().strip()) and (x['Atsiemimas'].lower().strip() in PristatymasTipas) else 0, axis=1)

			return DataFrame


		#AdressObjects = ['Ozo g. 18', 'Ukmergės g. 240', 'Ukmergės g. 369', 'Upės g. 9', 'Žirmūnų g. 64',
		#						 'Ozo g. 25', 'Karaliaus Mindaugo pr. 49', 'Savanorių pr. 206', 'Islandijos pl. 32',
		#						 'Jonavos g. 60', 'J. Basanavičiaus g. 3', 'Klaipėdos g. 143', 'Tilžės g. 109', 'Aido g. 8',
		#						'Laisvės g. 7', 'Žemaitijos g. 38', 'S. Dariaus ir S. Girėno g. 3', 'V. Kudirkos g. 3']
		#
		#AdressObjects = AdressObjects + self.ListOfCities
								

		Calculations = {'Gauta užsakymų': lambda x: x['Apsipirkimų kiekis'],
			'Atšauktų': lambda x: x['Apsipirkimų kiekis'] if x['Užsakymo būsena'] == 'canceled' else 0,
			'Eksportuoti': lambda x: x['Apsipirkimų kiekis'] if x['Užsakymo būsena'] == 'ORDEFOLDER_PREPARING' else 0,
			'Paruošta atsiėmimui': lambda x: x['Apsipirkimų kiekis'] if x['Užsakymo būsena'] in ['holded', 'ORDEFOLDER_READYTOTAKE'] else 0,
			'Nauji': lambda x: x['Apsipirkimų kiekis'] if x['Užsakymo būsena'] == 'processing' else 0,
			'Laukia apmokėjimo': lambda x: x['Apsipirkimų kiekis'] if x['Užsakymo būsena'] == 'wait_payment' else 0,
			'Užbaigti': lambda x: x['Apsipirkimų kiekis'] if x['Užsakymo būsena'] in ['complete', 'ORDEFOLDER_COMPLETE'] else 0,
			'Skubus į namus': lambda x: x['Apsipirkimų kiekis'] if x['Atsiemimas'] == 'LP Express' else 0, 
			'Skubus C&C': lambda x: x['Apsipirkimų kiekis'] if x['Atsiemimas'] in ['atsiemimas parduotuve ', 'atsiemimas parduotuveje'] else 0,
			'Parduotuvėms perduota po darbo valandų': lambda x: x['Apsipirkimų kiekis'], #<>
			'Atsiėmimas parduotuvėje': lambda x: x['Apsipirkimų kiekis'] if x['Atsiemimas'] == 'atsiemimas parduotuveje' else 0, #Savitarna arba parduotuves adresas
			'Pristatymas nurodytu adresu ': lambda x: x['Apsipirkimų kiekis'] if x['Atsiemimas'].lower().strip() == 'pristatymas i namus' else 0,
			'Omniva': lambda x: x['Apsipirkimų kiekis'] if 'omniva' in x['Atsiemimas'].lower() else 0,
			'LP Express': lambda x: x['Apsipirkimų kiekis'] if x['Atsiemimas'] == 'LP Express' else 0,
			'DPD terminalas': lambda x: x['Apsipirkimų kiekis'] if x['Atsiemimas'] == 'DPD terminalas' else 0,
			'Savitarna': lambda x: x['Apsipirkimų kiekis'] if x['Atsiemimas'] == 'DPD terminalas' else 0} #savitarna pristaty


		data_savaite = self.data.groupby(Dimensijos)[Metrikos].sum().reset_index()
		data_savaitgalis = self.data[self.data['Savaitgalis True/False'] == True].groupby(Dimensijos)[Metrikos].sum().reset_index()
		
		#Perform lambda calculations for metrics pair 1
		data_savaite = CalculateMetrics(data_savaite)
		data_savaitgalis = CalculateMetrics(data_savaitgalis)

		#Perform lambda calculations for metrics pair 2, i.e. address objects
		data_savaite = CalculateMetricsCities(data_savaite)
		data_savaitgalis = CalculateMetricsCities(data_savaitgalis)


		data_savaite = data_savaite.groupby(['Savaite', 'Savaites xx:xx'])[[x for x in Calculations.keys()] + [x['Address'] + ", " + x['City'] for x in self.ListOfShops]].sum().reset_index()
		data_savaite.sort_values(by=['Savaite'], inplace=True)

		data_savaitgalis = data_savaitgalis.groupby(['Savaite', 'Savaitgalis xx:xx'])[[x for x in Calculations.keys()] + [x['Address'] + ", " + x['City'] for x in self.ListOfShops]].sum().reset_index()
		data_savaitgalis.sort_values(by=['Savaite'], inplace=True)

		#Convert to record type dictionaries
		data_savaitgalis = data_savaitgalis.to_dict(orient='records')
		data_savaite = data_savaite.to_dict(orient='records')


		#Save to pickle
		with open(script_path + 'data_EshopReport.pickle', 'wb') as handle:
			pickle.dump({'data_savaite': data_savaite, 'data_savaitgalis': data_savaitgalis, 'timestamp': self.timestamp}, handle, protocol=pickle.HIGHEST_PROTOCOL)

		return data_savaite, data_savaitgalis, self.timestamp.strftime("%Y-%m-%d")


class PardavimaiReport:

	def __init__(self):
		self.timestamp = datetime.datetime.now()

	def Run(self):
		if os.path.isfile(script_path + 'data_PardavimaiReport.pickle') == True:
			with open(script_path + 'data_PardavimaiReport.pickle', 'rb') as handle:
				SQLReportUpload = SQLObject(Servers[1]['Address'])
				SQLReportUpload.data = pickle.load(handle)

			if (self.timestamp - SQLReportUpload.data['timestamp']).days >= 1:
				return self.GetReport()
			else:
				return SQLReportUpload.data['data']['MaxDate'], SQLReportUpload.data['data']
		else:
			return self.GetReport()

	def GetReport(self):
		SQLReportUpload = SQLObject(Servers[1]['Address'])
		SQLReportMaxDate = SQLObject(Servers[1]['Address'])
		queryMaxDate = """SELECT max(FullDate) FullDate from Pardavimai_factAll"""

		query = """SELECT Month, FACRegion, FACCompany, ClickNCollect, FACName,

							  /* Checkout */
							  SUM(Year0_minus_2_Checkout) Year0_minus_2_Checkout,
							  SUM(Year0_minus_1_Checkout) Year0_minus_1_Checkout,
							  SUM(Year0_checkout) Year0_Checkout,

							   /* sTotal */
							   CAST(round(sum(Year0_minus_2_sTotal), 0) as integer) Year0_minus_2_sTotal,
							   CAST(round(sum(Year0_minus_1_sTotal), 0) as integer) Year0_minus_1_sTotal,
							   CAST(round(sum(Year0_sTotal), 0) as integer) Year0_sTotal,
							
							   /* PeopleIn */
								CAST(round(sum(Year0_minus_2_PeopleIN), 0) as integer) Year0_minus_2_PeopleIN,
							   CAST(round(sum(Year0_minus_1_PeopleIN), 0) as integer) Year0_minus_1_PeopleIN,
							   CAST(round(sum(Year0_PeopleIN), 0) as integer) Year0_PeopleIN


							   FROM Pardavimai_factAll GROUP BY Month, FACRegion, FACCompany, ClickNCollect, FACName ORDER BY Month ASC, FACRegion ASC, FACName ASC, ClickNCollect DESC"""
		

		#Run Queries
		SQLReportUpload.getquery('PreparedReports', query)
		SQLReportMaxDate.getquery('PreparedReports', queryMaxDate)

		CurrentMonth = int((datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%m"))
		TopoShops = ['TOPO1', 'TOPO2', 'TOPO3', 'TOPO4']
		MetricColumns = ['Year0_minus_2_sTotal', 'Year0_minus_1_sTotal', 'Year0_sTotal',  'Year0_minus_2_PeopleIN', 'Year0_minus_1_PeopleIN', 'Year0_PeopleIN', 'Year0_minus_2_Checkout', 'Year0_minus_1_Checkout', 'Year0_Checkout']
		AverageMetricColumns = ['Year0_minus_2_Happyness', 'Year0_minus_1_Happyness', 'Year0_Happyness', 'Year0_minus_2_Basket', 'Year0_minus_1_Basket', 'Year0_Basket']
		SQLReportUpload.data = SQLReportUpload.data[(SQLReportUpload.data['Month'] == CurrentMonth) & (SQLReportUpload.data['FACCompany'] == 'TC LT')].groupby(['FACRegion', 'ClickNCollect'])[MetricColumns].sum().reset_index()



		#STAGE 2: Produce calculatons that are dimension dependent, i.e. need to be repeated if dimensions will be changed
		def ProduceCalculations(DataFrameObject):

			DataFrameObject['sTotalNetDifference'] = DataFrameObject.apply(lambda x: int(round(x['Year0_sTotal'] - x['Year0_minus_1_sTotal'], 0)), axis=1)
			DataFrameObject['sTotalPercentDifference'] = DataFrameObject.apply(lambda x: int(round(((x['Year0_sTotal'] - x['Year0_minus_1_sTotal']) / x['Year0_minus_1_sTotal']) * 100, 0)) if x['Year0_minus_1_sTotal'] != 0 else 0, axis=1)

			DataFrameObject['PeopleInNetDifference'] = DataFrameObject.apply(lambda x: int(round(x['Year0_PeopleIN'] - x['Year0_minus_1_PeopleIN'], 0)), axis=1)
			DataFrameObject['PeopleInPercentDifference'] = DataFrameObject.apply(lambda x: int(round(((x['Year0_PeopleIN'] - x['Year0_minus_1_PeopleIN']) / x['Year0_minus_1_PeopleIN']) * 100, 0)) if x['Year0_minus_1_PeopleIN'] != 0 else 0, axis=1)

			DataFrameObject['CheckoutNetDifference'] = DataFrameObject.apply(lambda x: int(round(x['Year0_Checkout'] - x['Year0_minus_1_Checkout'], 0)), axis=1)
			DataFrameObject['CheckoutPercentDifference'] = DataFrameObject.apply(lambda x: int(round(((x['Year0_Checkout'] - x['Year0_minus_1_Checkout']) / x['Year0_minus_1_Checkout']) * 100, 0)) if x['Year0_minus_1_Checkout'] != 0 else 0, axis=1)

			DataFrameObject['Year0_minus_2_Happyness'] = DataFrameObject.apply(lambda x: int(round((x['Year0_minus_2_Checkout'] / x['Year0_minus_2_PeopleIN']) * 100, 0)) if x['Year0_minus_2_PeopleIN'] != 0 else 0, axis=1)
			DataFrameObject['Year0_minus_1_Happyness'] = DataFrameObject.apply(lambda x: int(round((x['Year0_minus_1_Checkout'] / x['Year0_minus_1_PeopleIN']) * 100, 0)) if x['Year0_minus_1_PeopleIN'] != 0 else 0, axis=1)
			DataFrameObject['Year0_Happyness'] = DataFrameObject.apply(lambda x: int(round((x['Year0_Checkout'] / x['Year0_PeopleIN']) * 100, 0)) if x['Year0_PeopleIN'] != 0 else 0, axis=1)
			DataFrameObject['HappynessNetDifference'] = DataFrameObject.apply(lambda x: int(round(x['Year0_Happyness'] - x['Year0_minus_1_Happyness'], 0)), axis=1)
			DataFrameObject['HappynessPercentDifference'] = DataFrameObject.apply(lambda x: int(round(((x['Year0_Happyness'] - x['Year0_minus_1_Happyness']) / x['Year0_minus_1_Happyness']) * 100, 0)) if x['Year0_minus_1_Happyness'] != 0 else 0, axis=1)

			DataFrameObject['Year0_minus_2_Basket'] = DataFrameObject.apply(lambda x: int(round(x['Year0_minus_2_sTotal'] / x['Year0_minus_2_Checkout'], 0)) if x['Year0_minus_2_Checkout'] != 0 else 0, axis=1)
			DataFrameObject['Year0_minus_1_Basket'] = DataFrameObject.apply(lambda x: int(round(x['Year0_minus_1_sTotal'] / x['Year0_minus_1_Checkout'], 0)) if x['Year0_minus_1_Checkout'] != 0 else 0, axis=1)
			DataFrameObject['Year0_Basket'] = DataFrameObject.apply(lambda x: int(round(x['Year0_sTotal'] / x['Year0_Checkout'], 0)) if x['Year0_Checkout'] != 0 else 0, axis=1)
			DataFrameObject['BasketNetDifference'] = DataFrameObject.apply(lambda x: int(round(x['Year0_Basket'] - x['Year0_minus_1_Basket'], 0)), axis=1)
			DataFrameObject['BasketPercentDifference'] = DataFrameObject.apply(lambda x: int(round(((x['Year0_Basket'] - x['Year0_minus_1_Basket']) / x['Year0_minus_1_Basket']) * 100, 0)) if x['Year0_minus_1_Basket'] != 0 else 0, axis=1)

			return DataFrameObject


		SQLReportUpload.data = ProduceCalculations(SQLReportUpload.data)


		ClickNCollectUnique = [x for x in SQLReportUpload.data['ClickNCollect'].unique()]

		#All TOPO In-month
		AllTOPO = SQLReportUpload.data[SQLReportUpload.data['FACRegion'].isin(TopoShops)]
		AllTOPO['sum'] = 'sum'
		AllTOPO = AllTOPO.groupby('sum')[MetricColumns].sum().reset_index()
		AllTOPO = ProduceCalculations(AllTOPO)
		AllTOPO = AllTOPO.sum().to_dict()


		#All TOPO Up-to-date
		AllTOPOUpToDate = SQLReportUpload.dataBackup[SQLReportUpload.dataBackup['FACRegion'].isin(TopoShops)].groupby(['FACRegion', 'ClickNCollect'])[MetricColumns].sum().reset_index()
		AllTOPOUpToDate['sum'] = 'sum'
		AllTOPOUpToDate = AllTOPOUpToDate.groupby('sum')[MetricColumns].sum().reset_index()
		AllTOPOUpToDate = ProduceCalculations(AllTOPOUpToDate)
		AllTOPOUpToDate = AllTOPOUpToDate.sum().to_dict()


		Online = SQLReportUpload.data[SQLReportUpload.data['FACRegion'] == 'E-prekyba'].groupby('ClickNCollect')[MetricColumns].sum().reset_index()
		Online = ProduceCalculations(Online)
		Online = Online.set_index('ClickNCollect').to_dict()

		OnlineUpToDate = SQLReportUpload.dataBackup[SQLReportUpload.dataBackup['FACRegion'] == 'E-prekyba']
		OnlineUpToDate['sum'] = 'sum'
		OnlineUpToDate = OnlineUpToDate.groupby('sum')[MetricColumns].sum().reset_index()
		OnlineUpToDate = ProduceCalculations(OnlineUpToDate)
		OnlineUpToDate = OnlineUpToDate.sum().to_dict()

		B2B = SQLReportUpload.data[SQLReportUpload.data['FACRegion'] == 'B2B']
		B2B['sum'] = 'sum' 
		B2B = B2B.groupby('sum')[MetricColumns].sum().reset_index()
		B2B = ProduceCalculations(B2B)
		B2B = B2B.sum().to_dict()


		KitiKanalai = ['B2B']
		KitiKanalaiUpToDate = SQLReportUpload.dataBackup[SQLReportUpload.dataBackup['FACRegion'].isin(KitiKanalai)]
		KitiKanalaiUpToDate['sum'] = 'sum'
		KitiKanalaiUpToDate = KitiKanalaiUpToDate.groupby('sum')[MetricColumns].sum().reset_index()
		KitiKanalaiUpToDate = ProduceCalculations(KitiKanalaiUpToDate)
		KitiKanalaiUpToDate = KitiKanalaiUpToDate.sum().to_dict()

		TOPOGrupe = SQLReportUpload.data[MetricColumns]
		TOPOGrupe['sum'] = 'sum'
		TOPOGrupe = TOPOGrupe.groupby('sum')[MetricColumns].sum().reset_index()
		TOPOGrupe = ProduceCalculations(TOPOGrupe)
		TOPOGrupe = TOPOGrupe.sum().to_dict()


		TOPOGrupeUpToDate = SQLReportUpload.dataBackup[SQLReportUpload.dataBackup['FACCompany'] == 'TC LT']
		TOPOGrupeUpToDate['sum'] = 'sum'
		TOPOGrupeUpToDate = TOPOGrupeUpToDate.groupby('sum')[MetricColumns].sum().reset_index()
		TOPOGrupeUpToDate = ProduceCalculations(TOPOGrupeUpToDate)
		TOPOGrupeUpToDate = TOPOGrupeUpToDate.sum().to_dict()


		CalculatedAllTOPO = SQLReportUpload.data[SQLReportUpload.data['FACRegion'].isin(TopoShops)].groupby('FACRegion')[MetricColumns].sum().reset_index()
		#Overide average figures to get accurate calculation
		CalculatedAllTOPO = ProduceCalculations(CalculatedAllTOPO)
		
		

		for calculation in MetricColumns:
			CalculatedAllTOPO[calculation + 'TotalPercent'] = CalculatedAllTOPO.apply(lambda x: int(round((x[calculation] / AllTOPO[calculation]) * 100, 0)), axis=1)  
		
		#separate top level comparisons for happynesss index
		for calculation in AverageMetricColumns:
			CalculatedAllTOPO[calculation + 'TotalPercent'] = CalculatedAllTOPO.apply(lambda x: int(round(x[calculation] - AllTOPO[calculation], 0)), axis=1)
		
		#Drop unneded columns
		CalculatedAllTOPO = CalculatedAllTOPO[[x for x in CalculatedAllTOPO.columns if 'percent' in x.lower() or 'facregion' in x.lower()]]

		#del AllTOPO

		#Overall metrics for TOPO total values
		AllTOPOInMonthTable = SQLReportUpload.data[SQLReportUpload.data['FACRegion'].isin(TopoShops)].groupby('ClickNCollect')[MetricColumns].sum().reset_index()
		AllTOPOInMonthTable = ProduceCalculations(AllTOPOInMonthTable)
		AllTOPOInMonthTable = AllTOPOInMonthTable.set_index('ClickNCollect').to_dict()

		#Make sure keys are available even if data might not contain all of it
		for key, value in AllTOPOInMonthTable.items():
			for x in ClickNCollectUnique:
				if x not in value.keys():
					AllTOPOInMonthTable[key][x] = 0

		#PRODUCE VILNIUS / KAUNAS BREAKDOWN
		VilniusKaunas = SQLReportUpload.dataBackup[(SQLReportUpload.dataBackup['FACRegion'].isin(TopoShops)) & (SQLReportUpload.dataBackup['Month'] == CurrentMonth)].groupby('FACName')[MetricColumns].sum().reset_index()
		VilniusKaunas['VilniusOrKaunas'] = VilniusKaunas.apply(lambda x: "Vilnius" if "vilnius" in x['FACName'].lower() else ("Kaunas" if "kaunas" in x['FACName'].lower() else "Kiti miestai"), axis=1)
		VilniusKaunas = VilniusKaunas.groupby('VilniusOrKaunas')[MetricColumns].sum().reset_index().set_index('VilniusOrKaunas')
		VilniusKaunas = ProduceCalculations(VilniusKaunas)
		VilniusKaunas = VilniusKaunas.to_dict()

		#ADDING DATA TO FINAL DICTIONARY

		#Main dataset to be used, later addigtional calculations appended as keys
		SQLReportUpload.data = SQLReportUpload.data.set_index(['FACRegion', 'ClickNCollect']).to_dict()

		#print("Main dictionary____------------.--.-.-.-.-..--.-")
		#print(SQLReportUpload.data['Year0_minus_2_Basket'])
		
		#Bellow doictionary is for TOPO01/04 vs TC calculations in month appendesd to the main data
		SQLReportUpload.data['AllTOPOCalculated'] = CalculatedAllTOPO.set_index('FACRegion').to_dict()
		
		#Bellow is for overall TOPO pefromance in month
		SQLReportUpload.data['AllTOPOInMonth'] = AllTOPOInMonthTable

		#All topo in month
		SQLReportUpload.data['AllTOPO'] = AllTOPO

		#All TOPO Up to date
		SQLReportUpload.data['AllTOPOUpToDate'] = AllTOPOUpToDate

		#Ecomentce in month
		SQLReportUpload.data['Online'] = Online

		#print(SQLReportUpload.data['Online'])

		#Ecomenrce up to date
		SQLReportUpload.data['OnlineUpToDate'] = OnlineUpToDate

		#Kiti Kanalai
		SQLReportUpload.data['B2B'] = B2B

		SQLReportUpload.data['KitiKanalaiUpToDate'] = KitiKanalaiUpToDate


		#Visa topo grupe
		SQLReportUpload.data['TOPOGrupe'] = TOPOGrupe
		SQLReportUpload.data['TOPOGrupeUpToDate'] = TOPOGrupeUpToDate

		#Vilnius or Kaunas or Kiti miestai
		SQLReportUpload.data['VilniusKaunas'] = VilniusKaunas



		del AllTOPOInMonthTable, AllTOPO, AllTOPOUpToDate, VilniusKaunas, SQLReportUpload.dataBackup, OnlineUpToDate, Online




		#Save maxdate
		SQLReportUpload.data['MaxDate'] = SQLReportMaxDate.data['FullDate'][0]
		#Save to pickle
		with open(script_path + 'data_PardavimaiReport.pickle', 'wb') as handle:
			pickle.dump({'data': SQLReportUpload.data, 'timestamp': self.timestamp}, handle, protocol=pickle.HIGHEST_PROTOCOL)


		return SQLReportUpload.data['MaxDate'], SQLReportUpload.data


#Flast related
app = Flask(__name__)
app.secret_key = 'SecretTopo'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
GlobalCache = {}



@app.route("/")
def indexPage():
	return render_template('index.html')

@app.route("/Eugesta/EugestaUpload", methods=['POST'])
def EugestaUpload():
	
	def allowed_file(filename):
		return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

	if request.method == 'POST':
		# check if the post request has the file part
		if 'file' not in request.files:
			flash('No file part')
			return redirect(url_for('Eugesta'))
		file = request.files['file']
		# if user does not select file, browser also
		# submit an empty part without filename
		if file.filename == '':
			flash('No selected file.')
			return redirect(url_for('Eugesta'))
		if file and allowed_file(file.filename):
			filename = secure_filename(file.filename)
			file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
			GlobalCache['EugestaUpload'] = {'LastFileName': filename, 'LastUpload': datetime.datetime.now()}
			return redirect(url_for('Eugesta'))
		else:
			flash('Bad file extension.')
			return redirect(url_for('Eugesta'))




@app.route("/Eugesta")
def Eugesta():
	data = EugestaParse().run()

	return render_template('Eugesta.html', data = data['data'] if data != None else None, Maxdate=data['timestamp'].strftime("%Y-%m-%d") if data != None else None, summary = data['summary'] if data != None else None)

@app.route("/EugestaDownload")
def EugestaDownload():
	filenameDownload = 'EugestaParseData.xlsx'
	data = EugestaParse().run()
	data = pd.DataFrame.from_dict(data['data']) if data != None else pd.DataFrame(columns=['Error: No Data'])
	data.to_excel(script_path + 'Downloads/' + filenameDownload, index=False)
	return send_from_directory(directory=script_path + 'Downloads/', filename=filenameDownload)

@app.route("/Pardavimai")
def Pardavimai():
	PardavimaiReportInstance = PardavimaiReport()
	Maxdate, data = PardavimaiReportInstance.Run()
	return render_template('Pardavimai.html', Maxdate=Maxdate, data=data)

@app.route("/Marguard")
def Marguard():
	return render_template('Marguard.html')

@app.route("/Eshop")
def Eshop():
	EshopReportInstance = EshopReport()
	data_savaite, data_savaitgalis, Maxdate = EshopReportInstance.Run()
	return render_template('Eshop.html', data_savaite=data_savaite, data_savaitgalis=data_savaitgalis, Maxdate=Maxdate)

@app.route("/EshopPdf")
def EshopPdf():

	#Inner fucntion to create pdf object
	def create_pdf(pdf_data, outputfile):
		#pdf = StringIO()
		with open('Downloads/' + outputfile, 'wb') as writefile:
			pisa.CreatePDF(pdf_data, dest=writefile)

	EshopReportInstance = EshopReport()
	data_savaite, data_savaitgalis, Maxdate = EshopReportInstance.Run()
	filename='Eshop' + Maxdate + '.pdf'
	create_pdf(render_template('EshopPdf.html', data_savaite=data_savaite, data_savaitgalis=data_savaitgalis, Maxdate=Maxdate), filename)
	return send_from_directory(directory=script_path + 'Downloads/', filename=filename)




@app.route("/EshopGraphs")
def EshopGraphs():
	EshopReportInstance = EshopReport()
	data_savaite, data_savaitgalis, Maxdate = EshopReportInstance.Run()
	return render_template('Eshop_graphs.html', data_savaite=data_savaite, data_savaitgalis=data_savaitgalis, Maxdate=Maxdate)



if __name__ == '__main__':
	app.run(host='0.0.0.0', port=5000, debug=False)
