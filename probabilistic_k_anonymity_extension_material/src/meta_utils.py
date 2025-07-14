import numpy
import pycountry
import traceback
from urllib.parse import quote
from ratelimit import limits, sleep_and_retry
import requests
from requests.exceptions import ConnectTimeout
import os
import pymongo

#DIFFERENT ACCOUNTS FOR ROUND ROBIN QUERYING WITH PROXIES   #each element is a dict: {'actID':actID,access_token:access_token,'cookies':{cookies}}
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client['prokanon']
tokens_col = db['tokens']

headers = {
    'authority': 'adsmanager-graph.facebook.com',
    'accept': '*/*',
    'accept-language': 'en;q=0.9',
    'content-type': 'application/x-www-form-urlencoded',
    'origin': 'https://adsmanager.facebook.com',
    'referer': 'https://adsmanager.facebook.com/',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'sec-gpc': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
}

def cookie_processing(cookies):
    cookies = cookies.split(';')
    cookies = [cookie.split('=',1) for cookie in cookies]
    for i in range(len(cookies)):
        cookies[i] = [cookie.strip() for cookie in cookies[i]]
    cookies = numpy.array(cookies)
    cookies = dict(zip(cookies[:,0],cookies[:,1]))
    return cookies

def linkgen_andcomb_age_ct_interests(logging, interests, num_interests, age=0, gender=0, country='', platform='',rel_status=0):
    [actID,access_token] = logging
    begining_part='https://adsmanager-graph.facebook.com/v15.0/act_'
    logging_part=actID+'/delivery_estimate?access_token='+access_token
    Goal_part='&method=get&optimization_goal=IMPRESSIONS&'
    agemax_part='"age_max":'+str(age)+',' if age!=0 else ''
    agemin_part='"age_min":'+str(age)+',' if age!=0 else ''
    gender_part=f'"genders":[{gender}],'
    if(country == ''):
        geolocation_part='"geo_locations":{"country_groups":["worldwide"],"location_types":["home"]},'
    else:
        geolocation_part='"geo_locations":{"countries":["'+country+'"],"location_types":["home"]},'
    if(platform == 'instagram'):
        platform_part = '"publisher_platforms":["instagram"],"instagram_positions":["stream","story","explore","reels"],"device_platforms":["mobile","desktop"],'
    elif(platform == 'facebook'):
        platform_part = '"publisher_platforms":["facebook"],"facebook_positions":["feed","instant_article","instream_video","video_feeds","marketplace","story","facebook_reels_overlay","search","groups_feed","facebook_reels"],"device_platforms":["mobile","desktop"],'
    else:
        platform_part = ''

    interest_part='"flexible_spec":['

    for i in interests:
        interest_part +='{"interests":[{"id":"' +i+'"}]},'


    relstatus_part = '{"relationship_statuses":["'+rel_status+'"]}' if rel_status!=0 else ''
    interest_part = interest_part + relstatus_part if relstatus_part != '' else interest_part[:-1] #to trim the last comma
    interest_part += ']'
    link=begining_part+logging_part+Goal_part+'targeting_spec={'+geolocation_part+gender_part+agemin_part+agemax_part+platform_part+interest_part+'}'
    return link

def linkgen_interest_validation(logging, interest):
    [actID,access_token] = logging
    begining_part='https://graph.facebook.com/v15.0/search?access_token='+access_token
    interest_part='&interest_fbid_list=['+interest+']&type=adinterestvalid'
    link = begining_part+interest_part
    return link

def linkgen_item_search(logging, item):
    [actID,access_token] = logging
    begining_part='https://graph.facebook.com/v15.0/act_'+actID+'/targetingsearch?access_token='+access_token
    locale_part='&locale=en_US'
    item_part='&q'+quote(item)
    link = begining_part+locale_part+item_part
    return link

@sleep_and_retry
@limits(calls=1, period=1)
def query_slow(data):
    try:
        response = requests.get(data['link'],cookies=cookie_processing(data['cookies']),timeout=30,headers=headers)
        data['resp'] = response.text
    except ConnectTimeout:
        print('Request has timed out')
    return data

async def query(data,session):
    #print("in request_query")
    try:
        async with session.get(url=data['link'],cookies=cookie_processing(cookies)) as response:
            resp = await response.read()
            data['resp'] = resp
            return data
    except Exception as e:
        print("Unable to get url {} due to {}.".format(data['link'], e))
        print(traceback.format_exc())

async def paralellize_queries(urls,limit,query):
    connector = aiohttp.TCPConnector(limit=limit)
    timeout = aiohttp.ClientTimeout(total=30000000)
    async with aiohttp.ClientSession(connector=connector,timeout=timeout) as session:
        ret = await asyncio.gather(*[query(url,session) for url in urls])
    print("Finalized all. Return is a list of len {} outputs.".format(len(ret)))
    return ret

'''

with open('../data/fb_tk_joint.csv','r',encoding='utf-8') as input:
    reader = csv.reader(input, delimiter=',')
    headers = next(reader)
    rows = []
    dictio = {}
    for row in reader:
        dictio = {}
        for i in range(len(row)):
            dictio[headers[i]] = row[i]
        rows += [dictio]

newrows = []
for row in rows:
    for country in countries:
            newrow = row.copy()
            newrow['country'] = country
            newrows += [newrow]

newrows = []
for country in countries:
    newrows += [{'country':country}]

for row in newrows:
    row['link'] = linkgen_andcomb_age_ct_interests(logging=[actID,access_token],country=country_codes[row['country']], interests=[[]], num_interests=0,platform='instagram')
'''
'''

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
start = time.time()
ret = asyncio.run(paralellize_queries(newrows,15,query))
end = time.time()

print(ret)


with open('../data/ig_country_audiences.pickle', 'wb') as handle:
    pickle.dump(ret,handle)

with open('../data/ig_country_audiences.pickle', 'rb') as handle:
    ret = pickle.load(handle)

'''
