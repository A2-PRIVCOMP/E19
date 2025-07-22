from pathlib import Path
import sys
#path_root = print(Path(__file__).parents)#[1]
#sys.path.append(str(path_root))
sys.path.append('../updated_crawling_utils')
from meta_utils import *
import pickle
import csv
import time
import json
import pymongo
import tqdm
from datetime import datetime
from datetime import timedelta

client_tokens = pymongo.MongoClient('localhost')
db_tokens = client_tokens['prokanon']
col_tokens = db_tokens['tokens']

client_combs = pymongo.MongoClient('localhost')
db_combs = client_combs['prokanon']
col_combs = db_combs['mt_combs4_newrel']

#cookies, access_token and actid available from mongo collection 'tokens', 'instage' database, updated automatically with cron task

index = 0
number_accounts = col_tokens.count_documents({})
creds = list(col_tokens.find({}))

rel_statuses = ["2","3","12","4","9","11","1","10","13","6"]

new_rel_statuses = ["4","2","3","1","6"]

n_america = ["BM","CA","US","GL","MX","PM"]
europe = ["AL","DE","AD","AT","BE","BY","BA","BG","HR","DK","SK","SI","ES","EE","FI","FR","GI","GR","GG","HU","IE","IM","IS","FO","IT","JE","XK","LV","LI","LT","LU","MK","MT","MD","MC","ME","NO","NL","PL","PT","GB","CZ","RO","RU","SM","RS","SE","CH","SJ","UA"]
asia = ["AF","SA","AM","AZ","BD","BH","BN","BT","KH","CN","CY","KR","AE","PH","GE","HK","IN","ID","IQ","IL","JP","JO","KZ","KG","KW","LA","LB","MO","MY","MV","MN","MM","NP","OM","PK","PS","QA","SG","LK","TH","TW","TJ","TL","TM","TR","UZ","VN","YE"]
l_america = ["AR","BO","BR","CL","CO","EC","GF","GY","FK","PY","PE","SR","UY","VE"]
c_america = ["BZ","CR","SV","GT","HN","NI","PA"]
caribe = ["AG","AW","BB","VG","KY","CW","DM","DO","GD","GP","HT","JM","MQ","MS","PR","KN","MF","VC","SX","LC","BS","TT","TC","VI"]
oceania = ["AS","AU","CK","FM","FJ","PF","GU","KI","MH","NR","NC","NZ","NU","NF","MP","PW","PG","PN","WS","SB","TK","TO","TV","VU","WF"]
africa = ["AO","DZ","BJ","BW","BF","BI","CV","CM","TD","CI","EG","ER","ET","GA","GM","GH","GN","GQ","GW","KM","KE","LS","LR","LY","MG","MW","ML","MA","MU","MR","YT","MZ","NA","NE","NG","CF","CG","CD","RE","RW","EH","SH","ST","SN","SC","SL","SO","SZ","ZA","SS","TZ","TG","TN","UG","DJ","ZM","ZW"]

countries = n_america + europe + asia + l_america + c_america + caribe + oceania + africa
total_queries = len(rel_statuses)*len(countries)*len(list(range(18,64)))*len([1,2])
print(f'total queries: {total_queries}')

for country in countries:
	for age in range(18,65):
		for gender in [1,2]:
			for rel_status in new_rel_statuses:
				link = linkgen_andcomb_age_ct_interests([creds[index]['act_id'],creds[index]['token']], [], num_interests=0, age=age, country=country, gender=gender, rel_status=rel_status)
				link_array = {'age':age,'country':country, 'rel_status':rel_status, 'gender':gender, 'link':link, 'cookies':creds[index]['cookies']}
				re = col_combs.find_one({'age':link_array['age'],'country':link_array['country'],'gender':link_array['gender'],'rel_status':link_array['rel_status']})
				if re == None:
					resp = query_slow(link_array)
					response = json.loads(resp['resp'])
					try:
						col_combs.insert_one({'age':link_array['age'],'country':link_array['country'],'gender':link_array['gender'],'rel_status':link_array['rel_status'],'mau':response['data'][0]['estimate_mau'],'dau':response['data'][0]['estimate_dau']})
					except:
						print(link)
						print(response)
						if(response['error']['code'] == 368):
							exit(0)
						if(response['error']['code'] == 190):
							exit(0)

				#update tokens from token database
				number_accounts = col_tokens.count_documents({})
				creds = col_tokens.find()
				index = index + 1
				if index >= number_accounts:
					index = 0
