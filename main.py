#!/usr/bin/python3

from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage
from selenium import webdriver
from datetime import datetime   
from glob import glob 
import pandas as pd
import zipfile
import shutil 
import time 
import sys
import os

p_id = 'project-database'

f_format = "{:%Y_%m_%d}".format(datetime.now())
f_format_2 = "{:%B_%Y}".format(datetime.now())

bucket_name = f"bucket_name_{f_format}"
# bucket_name = "req_2022_05_13_777"
dataset_name = f"dataset_name_{f_format}"
# dataset_name = "REQ_Quebec_2022_05_13"

dataset_name2 = "ALL_NEW_COMPANIES"
# create client 
storage_client = storage.Client(project=p_id)
bigquery_client = bigquery.Client(project=p_id, location="US")

path = os.getcwd()
zip_files = glob('*.zip')
if len(zip_files) == 1:
	fileName, fileExtension = os.path.splitext(zip_files[0])
	shutil.copy2(zip_files[0], fileName + '_old' + fileExtension)

# BROWSER CONFIGURATION
def configure_browser():
	options = Options()
	options.add_argument("--no-sandbox")
	options.add_argument("start-maximized")
	options.add_argument("--headless")
	options.add_argument("--disable-dev-shm-usage")
	options.add_experimental_option("excludeSwitches", ["enable-automation"])
	options.add_experimental_option('useAutomationExtension', False)

	browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

	return browser

def delete_file(csv_rem):
	os.remove(csv_rem)
	print('Task Complete')

def bucket_func(csv_file):
	try:
		bucket = storage_client.get_bucket(bucket_name)
		print(f'{bucket_name} Exists.')
		
		if storage.Blob(bucket=bucket, name=csv_file).exists(storage_client):
			print(f'{bucket}/{csv_file} Exists.')
		else:
			blobs = bucket.blob(csv_file)
			blobs.upload_from_filename(filename=csv_file)
			print(f'{csv_file} Uploaded')

	except NotFound as e:
		new_bucket = storage_client.create_bucket(bucket_name)
		print(f'New {new_bucket} Created.')
		new_blob = new_bucket.blob(csv_file)
		new_blob.upload_from_filename(filename=csv_file)
		print(f'{csv_file} Uploading Now.')


def bigquery_func(dataset_names, csv_file):
	df = pd.read_csv(csv_file, low_memory=False)
	base=os.path.basename(csv_file)
	t_name = os.path.splitext(base)[0]

	dataset = "{}.{}".format(p_id, dataset_names)
	table_id = "{}.{}.{}".format(p_id, dataset_names, t_name)

	# determine wheter a dataset exists or not 
	try:
		bigquery_client.get_dataset(dataset)  # Make an API request.
		try:
			bigquery_client.get_table(table_id)  # Make an API request.

			print("Table {} already exists.".format(table_id))
		except NotFound:
			table = bigquery.Table(table_id)
			table = bigquery_client.create_table(table) 
			bigquery_client.load_table_from_dataframe(df, table).result()

	except NotFound:
		bigquery_client.create_dataset(dataset)

		try:
			bigquery_client.get_table(table_id)  # Make an API request.

			print("Table {} Created.".format(table_id))
		except NotFound:
			table = bigquery.Table(table_id)
			table = bigquery_client.create_table(table) 
			bigquery_client.load_table_from_dataframe(df, table).result()

def uploader(csv_file):
	try:
		if csv_file == f"ALL_NEW_COMPANIES_by_{f_format_2}__unique_neq.csv":
			pass
		elif csv_file == "Entreprise_merged__uniqueq.csv":
			pass
		else:
			bucket_func(csv_file)
	except Exception as e:
		pass
		print(e)

	try:
		if csv_file == f"ALL_NEW_COMPANIES_by_{f_format_2}__unique_neq.csv":
			bigquery_func(dataset_name2, csv_file)
		else:
			bigquery_func(dataset_name, csv_file)
	except:
		pass

	delete_file(csv_file)

def all_csv_file():
	csv_list = glob('*.csv')
	for i in csv_list:
		uploader(i)

all_csv_file()

def all_csv_file():
	csv_list = glob('*.csv')
	for cs in csv_list:
		uploader(cs)
	
def duplicate_remover():
	increase = 1
	csv_name = f"ALL_NEW_COMPANIES_by_{f_format_2}__unique_neq"
	df = pd.read_csv('Nom_Entreprise_merged__unique_neq.csv', low_memory=False)

	df['DAT_INIT_NOM_ASSUJ'] = pd.to_datetime(df.DAT_INIT_NOM_ASSUJ, infer_datetime_format = True, errors = 'coerce')
	df.sort_values(by = 'DAT_INIT_NOM_ASSUJ', ascending = True, inplace = True)
	df = df.drop_duplicates(subset='NEQ', keep='first')

	df['Date1']=df['DAT_INIT_NOM_ASSUJ']+pd.DateOffset(days=increase)
	df['Date2']=df['Date1']+pd.DateOffset(months=increase)

	df['Date11'] = df['Date1'].astype(str)
	df['Date22'] = df['Date2'].astype(str)

	df[["year1", "month1", "day1"]] = df["Date11"].str.split("-", expand=True)
	df[["year2", "month2", "day2"]] = df["Date22"].str.split("-", expand=True)

	df['DAT_INIT_NOM_ASSUJ'].dt.month_name()

	df['Month'] = df['day1'] + "th "+ df['Date1'].dt.month_name() + " " + df["year1"] + " - " + df['day2'] + "th "+ df['Date2'].dt.month_name() + " " + df["year2"] 

	df.drop("Date1", axis=1, inplace=True)
	df.drop("Date2", axis=1, inplace=True)
	df.drop("month1", axis=1, inplace=True)
	df.drop("month2", axis=1, inplace=True)
	df.drop("Date11", axis=1, inplace=True)
	df.drop("Date22", axis=1, inplace=True)
	df.drop("day1", axis=1, inplace=True)
	df.drop("day2", axis=1, inplace=True)
	df.drop("year1", axis=1, inplace=True)
	df.drop("year2", axis=1, inplace=True)
	
	df.to_csv(csv_name + '.csv', index=False)

	all_csv_file()

def merge_csv(csv1, csv2):
	a = pd.read_csv(csv1, low_memory=False)
	b = pd.read_csv(csv2, low_memory=False)

	# b = b.dropna(axis=1)
	merged = a.merge(b, on='NEQ')
	merged.to_csv("Nom_Entreprise_merged__unique_neq.csv", index=False)

	duplicate_remover()

def ll():
	csv_list = glob('*.csv')
	for csvv in csv_list:
		if csvv == 'Nom.csv':
			a = csvv
		elif csvv == 'Entreprise.csv':
			b = csvv
	merge_csv(a, b)


def un_zipFiles(filePath):
	zip_file = zipfile.ZipFile(filePath)
	for names in zip_file.namelist():
		zip_file.extract(names)
	zip_file.close()
	ll()
 

def dup_checker(path):
	data = []
	for item in os.scandir(path):
		if item.name.endswith('.rar'):
			if len(data) == 0:
				data.append([item.name, item.path, item.stat().st_size, item.stat().st_atime])
			else:
				for i in range(len(data)):
					if (data[i][2] == item.stat().st_size) and (data[i][3] == item.stat().st_atime):
						if data[i][3] < item.stat().st_atime:
							os.remove(data[i][1])
						else:
							os.remove(item.path)
						print('Done')
				else:
					data.append([item.name, item.path, item.stat().st_size, item.stat().st_atime])

		elif item.name.endswith('.zip'):
			if len(data) == 0:
				data.append([item.name, item.path, item.stat().st_size, item.stat().st_atime])
			else:
				for i in range(len(data)):
					if (data[i][2] == item.stat().st_size):
						if data[i][3] < item.stat().st_atime:
							os.remove(data[i][1])
						else:
							os.remove(item.path)
						print('Done')
					else:
						if data[i][3] < item.stat().st_atime:
							os.remove(data[i][1])
							un_zipFiles(item.path)

						else:
							os.remove(item.path)
							print(data[i][1])
							# print(type(data[i][1]))
							un_zipFiles(data[i][1])
						data.append([item.name, item.path, item.stat().st_size, item.stat().st_atime])

## SCRAPING STARTS HERE
def scrapping():
	url = ''
	try:
		browser.get(url)
	except:
		browser = configure_browser()
		browser.get(url)

	time.sleep(5)

	browser.find_element(By.ID, 'CPHContenuGR_btnDonnees').click()
	for x in range(0,600):
		time.sleep(1)
		print(f'Sleep - {x}')

	dup_checker(path)

if __name__ == '__main__':
	scrapping()
