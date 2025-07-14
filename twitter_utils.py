import requests
import json
import aiohttp
import asyncio
import time
import numpy as np
from urllib.parse import quote
from urllib.parse import unquote
from urllib.parse import urlencode
from yarl import URL
from urllib.request import urlopen
import logging
import tqdm
import pandas as pd
import pickle

loc_dict = {
    'Austria':'30410557050f13a5',
    'Belgium':'78bfaf3f12c05982',
    'Czech Republic':'6b5d375c346e3be9',
    'Denmark':'c29833e68a86e703',
    'Egypt':'c659cb4912229666',
    'Finland':'e7c97cdfef3a741a',
    'France': 'f3bfc7dcc928977f',
    'Germany':'fdcd221ac44fa326',
    'Greece':'2ee7eeaa84dbe65a',
    'Hungary':'81b8dcbe189773f2',
    'Ireland':'ea679934779f45c7',
    'Italy':'c799e2d3a79f810e',
    'Korea':'c00e5392b3fa46fb',
    'Kuwait':'d303522079ceb9a7',
    'Morocco':'b5fc43481ea8b89a',
    'Netherlands':'879d7cfc66c9c290',
    'Norway':'0ce8b9a7b2742f7e',
    'Poland':'d9074951d5976bdf',
    'Portugal':'8198e85105936d3c',
    'Qatar':'a54c21f6aedb2967',
    'Romania':'f7531639e8db5e12',
    'Saudi Arabia':'08b2d74428e2ca88',
    'South Africa':'dd9c0d7d7e07eb49',
    'Spain': 'ecdce75d48b13b64',
    'Sweden':'82b141af443cb1b8',
    'Switzerland':'4e7c21fd2af027c6',
    'Turkey':'682c5a667856ef42',
    'Ukraine':'084d0d0155787e9d',
    'United Arab Emirates':'3f63906fc8aa5a7d',
    'United Kingdom':'6416b8512febefc9',
    "Cambodia":'df4a2798d032e321',
    "Indonesia":'ce7988d3a8b6f49f',
    "Malaysia":'8633ee56a589f49c',
    "Philippines":'fb151ef38fa2ac0d',
    "Singapore":'2509b9adc1fedfd2',
    "Thailand":'974c290e10850494'
}

countries = [ #Countries available in TikTok
    'Austria',
    'Belgium',
    'Czech Republic',
    'Denmark',
    'Egypt',
    'Finland',
    'France',
    'Germany',
    'Greece',
    'Hungary',
    'Ireland',
    'Italy',
    'Korea',
    'Kuwait',
    'Morocco',
    'Netherlands',
    'Norway',
    'Poland',
    'Portugal',
    'Qatar',
    'Romania',
    'Saudi Arabia',
    'South Africa',
    'Spain',
    'Sweden',
    'Switzerland',
    'Turkey',
    'Ukraine',
    'United Arab Emirates',
    'United Kingdom',
    "Cambodia",
    "Indonesia",
    "Malaysia",
    "Philippines",
    "Singapore",
    "Thailand"
]

as_ct = [
    "Cambodia",
    "Indonesia",
    "Malaysia",
    "Philippines",
    "Singapore",
    "Thailand"
]

actID = open('TW.cnf').read().split(',')[0]

access_token = open('TW.cnf').read().split(',')[1]

cookies = open('TW.cnf',encoding='utf-8').read().split(',')[2]

authorization = open('TW.cnf',encoding='utf-8').read().split(',')[3]

headers = {'x-csrf-token':access_token,
'authorization': authorization,
}

cookies = cookies.split(';')

cookies = [cookie.split('=',1) for cookie in cookies]

for i in range(len(cookies)):
    cookies[i] = [cookie.strip() for cookie in cookies[i]]
cookies = np.array(cookies)

cookies = dict(zip(cookies[:,0],cookies[:,1]))

async def getCountryTargetingValue(data,session):

    #consider checking for geo

    params = '{"account_id":'+actID+',"domains":"Geo","query":"'+data['country']+'"}'

    url = 'https://api.twitter.com/graphql/p__5t0aZnLP6CKMk0j1rVQ/TargetingSearchQuery?variables='+quote(params,safe='')
    print(url)

    try:
        async with session.get(url=url, headers=headers,cookies=cookies) as response:
            time.sleep(1.8)
            resp = await response.read()
            try:
                found = json.loads(resp.decode('utf-8'))['data']['targeting_catalog_search']
                for element in found:
                    if(element['metadata']['location_type'] == 'Countries'):
                        targeting_value = element['api_targeting_value']
                        count = element['audience_size']
                        break
                data['targetingValue'] = targeting_value
                data['count'] = count
                return data
            except:
                data['targetingValue'] = -resp.status
                return data
    except Exception as e:
        print("Unable to get url {} due to {}.".format(data, e.__class__))

        

async def getKeywordGlobalCount(data,session):

    #consider checking for geo

    params = '{"account_id":'+actID+',"criteria":[{"domain":"BroadMatchKeyword","domain_id":0,"label":"'+data['keyword']+'"}]}'

    url = 'https://api.twitter.com/graphql/aRLqbpw0e_6ICIu3gKatLw/TargetingCriteriaItemsQuery?variables='+quote(params,safe='')



    try:
        async with session.get(url=url, headers=headers,cookies=cookies) as response:
            time.sleep(1.8)
            resp = await response.read()
            try:
                count = json.loads(resp.decode('utf-8'))['data']['targeting_catalog_by_criteria'][0]['audience_size']
                data['count'] = count
                return data
            except:
                data['count'] = -resp.status
                return data
    except Exception as e:
        print("Unable to get url {} due to {}.".format(data, e.__class__))


async def getKeywordLocalCount(data,session):
    url = 'https://ads-api.twitter.com/11/accounts/18ce55j1k5q/audience_estimate'
    form = {"targeting_criteria":[]}
    if('locations' in data):
        for location in data['locations']:
            form['targeting_criteria'] += [{"targeting_value":loc_dict[location],"targeting_type":"LOCATION"}]
    
    form['targeting_criteria'] += [{"targeting_value":data['keyword'],"targeting_type":"BROAD_KEYWORD"}]

    try:
        async with session.post(url=url, headers=headers,cookies=cookies,json=form) as response:
            resp = await response.read()
            try:
                temp = json.loads(resp.decode('utf-8'))['data']['audience_size']
                count = int(temp['max'])#we consider this max equivalent to MAU in Meta
                data['count'] = count
                return data
            except:
                try:
                    error = json.loads(resp.decode('utf-8'))['errors'][0]['code']
                    if(error == 'AUDIENCE_ESTIMATE_TOO_SMALL'):
                       data['count'] = 1000
                except:
                    print(resp)
                    data['count'] = -response.status
                return data
    except Exception as e:
        print("Unable to get url {} due to {}.".format(data, e))


async def paralellize_queries(forms,limit,query):
    connector = aiohttp.TCPConnector(limit=limit)
    timeout = aiohttp.ClientTimeout(total=100000)
    jar = aiohttp.CookieJar(unsafe=False)
    async with aiohttp.ClientSession(connector=connector,timeout=timeout,cookie_jar=jar,skip_auto_headers=['user-agent','accept-encoding','accept']) as session:
        ret = await asyncio.gather(*[query(form,session) for form in forms])
    print("Finalized all. Return is a list of len {} outputs.".format(len(ret)))
    return ret


superarray = [{'keyword':'fifa','locations':['Spain']}]

df = pd.read_csv('../data/fb_tk_joint.csv')

names = list(df['name'])

superarray = [{'keyword': name.lower(),'locations':[country]} for name in names for country in countries]
#superarray = [{'country':country} for country in countries]

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
start = time.time()
ret = asyncio.run(paralellize_queries(superarray,25,getKeywordLocalCount))
end = time.time()
counter = 0

with open('../data/twitter_local_as+eu_audiences.pickle', 'wb') as handle:
    pickle.dump(ret,handle)

with open('../data/twitter_local_as+eu_audiences.pickle', 'rb') as handle:
    ret = pickle.load(handle)


print(ret)