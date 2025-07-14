import pymongo
from urllib.parse import quote
import requests
import json
import random
import pycountry
import re
import time

client = pymongo.MongoClient('localhost')
db = client['prokanon']
col_countries = db['tw_countries']
col_combs = db['tw_combs3_gen_pla_cty']

tw_cookies = ''
x_csrf_token = ''
act = ''
actID = ''
graphqlID = ''

headers = {
  "accept": "*/*",
  "authorization": f"Bearer AAAAAAAAAAAAAAAAAAAAANBSYwEAAAAAOlt9bKS2BoTpbL%2FSNHNmN0ejfBc%3DVFV0ENqnQ927hPgNRQsihKOMwPfpPeO8Ds9toqkIMqj6PIj8dn",
  "cache-control": "no-cache",
  "content-type": "application/json; charset=UTF-8",
  "x-csrf-token": x_csrf_token,
  "cookie": tw_cookies,
  "pragma": "no-cache",
  "Host": "ads-api.twitter.com",
  "Origin": "https://ads.twitter.com",
  "Referer": "https://ads.twitter.com/",
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": "\"Windows\"",
  "sec-fetch-dest": "empty",
  "sec-fetch-mode": "cors",
  "sec-fetch-site": "same-origin",
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
}


#Elements format example: 
#{'targeting_type':'AGE','targeting_value':'AGE_OVER_13'}
#GENDER,LOCATION (Locations have a 'hashed' targeting value, similar to the URN on LD)


### Age Ranges: 18 to 24, 25 to 34, 35 to 54, 55+
age_ranges = ['AGE_13_TO_24','AGE_18_TO_24','AGE_18_TO_34','AGE_21_TO_34','AGE_25_TO_49','AGE_35_TO_49','AGE_OVER_50']

# urn:urn:li:ageRange:(35,54)
countries = col_countries.find({})

### Genders: MALE, FEMALE
genders = ['MALE','FEMALE']

platforms = ['0','1','3','4']

countries = col_countries.find({})

#Audience query example:
#{"targeting_criteria":[{"targeting_type":"AGE_18_TO_24","targeting_value":"AGE_OVER_13"},{"targeting_type":"LOCATION","targeting_value":"96683cc9126741d1"},{"targeting_type":"GENDER","targeting_value":"FEMALE"}]}
#endpoint: https://ads-api.twitter.com/11/accounts/18ce55mgj6i/audience_estimate

#targeting values query example: (url encoded, GET) ?variables={"account_id":4503599734562586,"domains":["Geo"],"query":"estados unidos"}
#endpoint: https://api.twitter.com/graphql/pkE1MXvewwh5wjXrzqFdoQ/TargetingSearchQuery

for country in countries:
#for age_range in age_ranges:
    for gender in genders:
        for platform in platforms:#
            if col_combs.find_one({'country':country["metadata"]["country_code"],'platform':platform,'gender':gender}) != None:
                continue
            form = {'targeting_criteria':[
                {"targeting_type":"LOCATION","targeting_value":country["api_targeting_value"]},
                #{"targeting_type":"AGE","targeting_value":age_range},
                {"targeting_type":"GENDER","targeting_value":gender},
                {"targeting_type":"PLATFORM","targeting_value":platform}
            ]}
            url = f'https://ads-api.twitter.com/11/accounts/18ce55mgj6i/audience_estimate'
            time.sleep(1.5)
            response = requests.post(url,headers=headers,json=form)
            try:
                response = json.loads(response.text)
            except:
                print(response)
                #exit(0)
            try:
                if 'errors' in response:
                    if response["errors"][0]['code'] == 'AUDIENCE_ESTIMATE_TOO_SMALL':
                        col_combs.update_one({
                            'country':country["metadata"]["country_code"],
                            #'age_range':age_range,
                            'gender':gender,
                            'platform':platform
                        },{'$set':{'count_min':0,'count_max':1000}},upsert=True)
                        continue
                    if response["errors"][0]['code'] == 'SERVICE_UNAVAILABLE':
                        time.sleep(2000)
                        response = requests.post(url,headers=headers,json=form)
                        try:
                            response = json.loads(response.text)
                        except:
                            print(response)
                            #exit(0)

                count_min = response["data"]["audience_size"]['min']
                count_max = response["data"]["audience_size"]['max']
                col_combs.update_one({
                    'country':country["metadata"]["country_code"],
                    #'age_range':age_range,
                    'gender':gender,
                    'platform':platform
                },{'$set':{'count_min':count_min,'count_max':count_max}},upsert=True)
            except:
                print(response)
                print(response.headers)
                raise Exception("cant obtain count")

print('sacabao')



'''
#Facebook countries
n_america = ["BM","CA","US","GL","MX","PM"]
europe = ["AL","DE","AD","AT","BE","BY","BA","BG","HR","DK","SK","SI","ES","EE","FI","FR","GI","GR","GG","HU","IE","IM","IS","FO","IT","JE","XK","LV","LI","LT","LU","MK","MT","MD","MC","ME","NO","NL","PL","PT","GB","CZ","RO","RU","SM","RS","SE","CH","SJ","UA"]
asia = ["AF","SA","AM","AZ","BD","BH","BN","BT","KH","CN","CY","KR","AE","PH","GE","HK","IN","ID","IQ","IL","JP","JO","KZ","KG","KW","LA","LB","MO","MY","MV","MN","MM","NP","OM","PK","PS","QA","SG","LK","TH","TW","TJ","TL","TM","TR","UZ","VN","YE"]
l_america = ["AR","BO","BR","CL","CO","EC","GF","GY","FK","PY","PE","SR","UY","VE"]
c_america = ["BZ","CR","SV","GT","HN","NI","PA"]
caribe = ["AG","AW","BB","VG","KY","CW","DM","DO","GD","GP","HT","JM","MQ","MS","PR","KN","MF","VC","SX","LC","BS","TT","TC","VI"]
oceania = ["AS","AU","CK","FM","FJ","PF","GU","KI","MH","NR","NC","NZ","NU","NF","MP","PW","PG","PN","WS","SB","TK","TO","TV","VU","WF"]
africa = ["AO","DZ","BJ","BW","BF","BI","CV","CM","TD","CI","EG","ER","ET","GA","GM","GH","GN","GQ","GW","KM","KE","LS","LR","LY","MG","MW","ML","MA","MU","MR","YT","MZ","NA","NE","NG","CF","CG","CD","RE","RW","EH","SH","ST","SN","SC","SL","SO","SZ","ZA","SS","TZ","TG","TN","UG","DJ","ZM","ZW"]

fb_countries = n_america + europe + asia + l_america + c_america + caribe + oceania + africa
print(len(fb_countries))
found =[]
for country in fb_countries:
    try:
        full_pais = pycountry.countries.get(alpha_2=country)
        pais_name = re.split(r"[,()\-!?:]+", full_pais.name)[0]
        
    except:
        print(country)
        #kosovo
    
    try:
        tmp = col_countries.find_one({'name':pais_name})
        assert found != None
        if tmp not in found: found.append()
        else: print(pais_name)
    except:
        urn_url = f'https://api.twitter.com/graphql/{graphqlID}/TargetingSearchQuery?variables=%7B%22account_id%22%3A4503599734562586%2C%22domains%22%3A%5B%22Geo%22%5D%2C%22query%22%3A%22{quote(pais_name)}%22%7D'
        response = requests.get(urn_url,headers=headers)
        try:
            response = json.loads(response.text)
            print(response)
            for element in response["data"]["targeting_catalog_search"]:
                if element["metadata"]["location_type"] == 'Countries':
                    col_countries.update_one({'metadata.country_code':element["metadata"]["country_code"]}, {"$set":element}, upsert=True)
        except:
            print(response)
            print(pais_name)
print(len(found))
'''