import numpy as np
import urllib.parse
import aiohttp
import asyncio
import json
import sqlite3
import sys

countries_dict = {
        "Austria":2782113,
        "Belgium":2802361,
        "Czechia":3077311,
        "Denmark":2623032,
        "Egypt":357994,
        "Finland":660013,
        "France":3017382,
        "Germany":2921044,
        "Greece":390903,
        "Hungary":719819,
        "Ireland":2963597,
        "Italy":3175395,
        "Korea":1835841,
        "Kuwait":285570,
        "Morocco":2542007,
        "Netherlands":2750405,
        "Norway":3144096,
        "Poland":798544,
        "Portugal":2264397,
        "Qatar":289688,
        "Romania":798549,
        "Saudi Arabia":102358,
        "South Africa":953987,
        "Spain":2510769,
        "Sweden":2661886,
        "Switzerland":2658434,
        "Turkey":298795,
        "Ukraine":690791,
        "United Arab Emirates":290557,
        "United Kingdom":2635167,
        "Cambodia":1831722,
        "Indonesia":1643084,
        "Malaysia":1733045,
        "Philippines":1694008,
        "Singapore":1880251,
        "Thailand":1605651
    }

countries_dict_as = {
    "Cambodia":1831722,
    "Indonesia":1643084,
    "Malaysia":1733045,
    "Philippines":1694008,
    "Singapore":1880251,
    "Thailand":1605651
}

actID = open('creds.txt').read().split('\n')[0]

csrftoken = open('creds.txt').read().split('\n')[1]

cookies = open('creds.txt').read().split('\n')[2].split(';')
cookies = [cookie.split('=',1) for cookie in cookies]
for i in range(len(cookies)):
    cookies[i] = [cookie.strip() for cookie in cookies[i]]
cookies = np.array(cookies)
cookies = dict(zip(cookies[:,0],cookies[:,1]))

headers = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36',
    "x-csrftoken": csrftoken,
    "cookie": cookies
}

def formgen_count_interests(countries=[],interests=[],additional_interests=[]):
    countries = [countries_dict[i] for i in countries]
    if(countries == []):
        countries = [i for i in countries_dict.values()]

    form = {
        "with_signature":False,
        "inventory_flow":["3000"],
        "ad_ref_app_id":"",
        "ad_ref_pixel_id":"",
        "op_sys_filter":0,
        "external_type":602,
        "objective_type":5,
        "audience":{
            "ad_tag_v2":[str(i) for i in interests],
            "interest_keywords_i18n":additional_interests,
            "in_market_tags":[],
            "action_scenes_v2":[],
            "action_categories_v2":[],
            "action_days_v2":[],
            "contextual_tags":[],
            "platform":["0"],
            "device_models":[],
            "automated_targeting":0,
            "target_device_version":0,
            "retargeting_tags":[],
            "retargeting_tags_exclude":[],
            "districts":[],
            "city":[],
            "country":countries,
            "province":[],
            "particle_locations":[str(i) for i in countries],
            "gender":"0",
            "spending_power":1,
            "ac":[],
            "language_list":[],
            "household_income":[],
            "android_osv":"",
            "ios_osv":"",
            "carriers":[],
            "targeting_expansion":{
                "expansion_enabled":False,
                "expansion_types":[]
            },
            "launch_price":[],
            "flow_package_include":[]
            ,"flow_package_exclude":[],
            "app_retargeting_install":False,
            "app_retargeting_type":None,
            "retargeting_audience_rule":None
        }
    }

    return form

def linkgen_get_keyid(keyword, type):
    keyword = urllib.parse.quote(keyword,safe='')
    scenes_part = ''
    if(type == 2):
        url = 'https://ads.tiktok.com/api/v3/i18n/optimizer/tool/interest_keywords_i18n/search/?aadvid='
        scenes_part = 'scenes=[10,12]'#10 means interests, and 12 means additional interests (those not predefined)
    else:
        url = 'https://ads.tiktok.com/api/v3/i18n/optimizer/tool/keywords_i18n/search/?aadvid='
        scenes_part = 'scenes=13'#13 means hastags
    

    url = url+actID+'&req_src=ad_creation&industry_types=%5B%5D&keywords='+keyword+'&mode=1&limit=50&'+scenes_part

    return url

async def query_get(data,session):
    #print("in request_query")
    try:
        async with session.get(url=data['link'],cookies=cookies,headers=headers) as response:
            resp = await response.read()
            if(response.status != 200):
                print('error: ',response.status)
            data['resp'] = resp
            return data
    except Exception as e:
        print("Unable to get url {} due to {}.".format(data['link'], e.__class__))

async def getCriteriaCount(data,session):
    #print("in request_query")
    url = 'https://ads.tiktok.com/api/v3/i18n/optimizer/audience/user/estimate/?aadvid='+actID
    try:
        async with session.post(url=url,json=data['form'],cookies=cookies,headers=headers) as response:
            resp = await response.read()
            if(response.status != 200):
                print('error: ',response.status)
            data['resp'] = resp
            return data
    except Exception as e:
        print("Unable to get url {} due to {}.".format(data['form'], e.__class__))


async def paralellize_queries(query,data,limit=63):
    connector = aiohttp.TCPConnector(limit=limit)
    timeout = aiohttp.ClientTimeout(total=100000)
    async with aiohttp.ClientSession(connector=connector,timeout=timeout) as session:
            ret = await asyncio.gather(*[query(element,session) for element in data])

    print("Finalized all. Return is a list of len {} outputs.".format(len(ret)))
    return ret

