import pymongo
from urllib.parse import quote
import requests
import json
import random
import pycountry
import re

client = pymongo.MongoClient('localhost')
db = client['prokanon']
col_countries = db['ld_countries']
col_combs = db['ld_combs3_sen_age_cty']

li_cookies = ''
csrf_token = ''
actID = ''

def genForms(dataArray2d):
    cmTargetingCriteria = "(include:(and:List((or:List((facet:(urn:urn%3Ali%3AadTargetingFacet%3AinterfaceLocales,name:Idioma%20del%20perfil),segments:List((urn:urn%3Ali%3Alocale%3Aen_US,name:Ingl%C3%A9s,facetUrn:urn%3Ali%3AadTargetingFacet%3AinterfaceLocales)))))"

    epilogue = ",exclude:(or:List()))"

    for i in dataArray2d:
        for index_j, j in enumerate(i):
            try :
              ancestorsList = j['ancestorUrns']
            except:
              ancestorsList = []
            urn = quote(j['urn'],safe='')
            facetUrn = quote(j['facetUrn'],safe='')

            differentType = False

            if (index_j != 0):
                try:
                    if (previousType != facetUrn):
                        differentType = True
                except:
                    pass
            first = f',(or:List((facet:(urn:{facetUrn}),segments:List((urn:{urn},facetUrn:{facetUrn})'

            middle = f',(urn:{urn},facetUrn:{facetUrn})'

            middleDifferentType = f')),(facet:(urn:{facetUrn}),segments:List((urn:{urn},facetUrn:{facetUrn})'

            last = f',(urn:{urn},facetUrn:{facetUrn})))))'

            lastDifferentType = f')),(facet:(urn:{facetUrn}),segments:List((urn:{urn},facetUrn:{facetUrn})))))'

            unique = f',(or:List((facet:(urn:{facetUrn}),segments:List((urn:{urn},facetUrn:{facetUrn})))))'

            firstWithAncestor = f',(or:List((facet:(urn:{facetUrn}),segments:List((urn:{urn},facetUrn:{facetUrn},ancestorUrns:List('

            ancestors = ''
            for ancestor in ancestorsList:
                ancestor_safe = quote(ancestor,safe='')
                if ancestor == ancestorsList[-1]:
                  ancestors += f'{ancestor_safe}'
                else:
                  ancestors += f'{ancestor_safe},'


            firstWithAncestor += f'{ancestors}))'

            middleOrWithAncestor = f',(urn:{urn},facetUrn:{facetUrn},ancestorUrns:List('

            middleOrWithAncestorDifferentType = f')),(facet:{facetUrn}),segments:List((urn:{urn},facetUrn:{facetUrn},ancestorUrns:List('

            middleOrWithAncestor += f'{ancestors}))'
            middleOrWithAncestorDifferentType += f'{ancestors}))'

            lastWithAncestor = f',(urn:{urn},facetUrn:{facetUrn},ancestorUrns:List('
            lastWithAncestorDifferentType = f')),(facet:(urn:{facetUrn}),segments:List((urn:{urn},facetUrn:{facetUrn},ancestorUrns:List('

            lastWithAncestor += f'{ancestors}))))))'
            lastWithAncestorDifferentType += f'{ancestors}))))))'

            uniqueWithAncestor = f',(or:List((facet:(urn:{facetUrn}),segments:List((urn:{urn},facetUrn:{facetUrn},ancestorUrns:List('

            uniqueWithAncestor += f'{ancestors}))))))'

            if (len(i) - 1 == 0):
                if (len(ancestorsList) > 0):
                    cmTargetingCriteria += uniqueWithAncestor
                else: cmTargetingCriteria += unique
            else:
                if (index_j == 0):
                    if (len(ancestorsList) > 0):
                        cmTargetingCriteria += firstWithAncestor
                    else: cmTargetingCriteria += first

                if (index_j == len(i) - 1):
                    if (len(ancestorsList) > 0):
                        if (differentType):
                            cmTargetingCriteria += lastWithAncestorDifferentType
                        else: cmTargetingCriteria += lastWithAncestor
                    else:
                        if (differentType): cmTargetingCriteria += lastDifferentType
                        else: cmTargetingCriteria += last

                else:
                    if (index_j > 0):
                        if (len(ancestorsList) > 0):
                            if (differentType): cmTargetingCriteria += middleOrWithAncestorDifferentType
                            else: cmTargetingCriteria += middleOrWithAncestor
                        else:
                            if (differentType): cmTargetingCriteria += middleDifferentType
                            else: cmTargetingCriteria += middle

        previousType = facetUrn

    cmTargetingCriteria += "))" + epilogue
    return cmTargetingCriteria

headers = {
  "accept": "*/*",
  "cache-control": "no-cache",
  "content-type": "application/x-www-form-urlencoded",
  "csrf-token": csrf_token,
  "cookie": li_cookies,
  "pragma": "no-cache",
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": "\"Windows\"",
  "sec-fetch-dest": "empty",
  "sec-fetch-mode": "cors",
  "sec-fetch-site": "same-origin",
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
  "x-http-method-override": "GET",
  "x-li-er-key": f"urn:li:sponsoredAccount:{actID}",
  "x-li-page-instance": "urn:li:page:d_campaign_details;DkAvyudeRvi74f7eYCS/TA==",
  "x-li-lang": "en_US",
  "x-restli-protocol-version": "2.0.0"
}

options = {"headers": headers, "method": "POST", "mode": "cors", "credentials": "include"}

#Elements format example: 
#{'urn':urn_part,'facetUrn':'urn:li:adTargetingFacet:skills'}

### Seniorities: 1 Unpaid, 2 Training, 3 Entry, 4 Senior, 5 Manager, 6 Director, 7 VP, 8 CXO, 9 Partner, 10 Owner.
seniorities = ['1','2','3','4','5','6','7','8','9','10']
# urn:urn:li:seniority:10

### Age Ranges: 18 to 24, 25 to 34, 35 to 54, 55+
age_ranges = ['(18,24)','(25,34)','(35,54)','(55,2147483647)']
# urn:urn:li:ageRange:(35,54)
countries = col_countries.find({})

### Genders: MALE, FEMALE
genders = ['MALE','FEMALE']


for country in countries:
    '''
    if col_countries.find_one({'name':country['name'],'count':{'$exists':True}}) != None:
        continue#
    else:
        arr2d = [[{'urn':country['urn'],'facetUrn':'urn:li:adTargetingFacet:locations'}]]
        cmTargetingCriteria = genForms(arr2d)
        url = f'https://www.linkedin.com/campaign-manager-api/campaignManagerAudienceCounts?q=targetingCriteria&cmTargetingCriteria={cmTargetingCriteria}'
        response = requests.get(url,headers=headers)
        response = json.loads(response.text)
        try:
            count = response["elements"][0]['count']
            col_countries.update_one({
                'name':country['name'],
            },{'$set':{'count':count}},upsert=True)
        except:
            raise Exception("cant obtain count")
    '''
    for seniority in seniorities:
        for age_range in age_ranges:
        #for gender in genders:#
            if col_combs.find_one({'country':country['name'],'age_range':age_range,'seniority':seniority}) != None:
                continue#
            arr2d = [
                [{'urn':country['urn'],'facetUrn':'urn:li:adTargetingFacet:locations'}],
                [{'urn':f'urn:li:seniority:{seniority}','facetUrn':'urn:li:adTargetingFacet:seniorities'}],
                [{'urn':f'urn:li:ageRange:{age_range}','facetUrn':'urn:li:adTargetingFacet:ageRanges'}]
                #[{'urn':f'urn:li:gender:{gender}','facetUrn':'urn:li:adTargetingFacet:genders'}]
            ]
            cmTargetingCriteria = genForms(arr2d)
            url = f'https://www.linkedin.com/campaign-manager-api/campaignManagerAudienceCounts?q=targetingCriteria&cmTargetingCriteria={cmTargetingCriteria}'
            response = requests.get(url,headers=headers)
            response = json.loads(response.text)
            try:
                count = response["elements"][0]['count']
                col_combs.update_one({
                    'country':country['name'],
                    'seniority':seniority,
                    'age_range':age_range
                    #'gender':gender
                },{'$set':{'count':count}},upsert=True)
            except:
                raise Exception("cant obtain count")
                
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
        urn_url = f'https://www.linkedin.com/campaign-manager-api/campaignManagerAdTargetingEntities?query={pais_name}&accountId={actID}&facets=List(urn%3Ali%3AadTargetingFacet%3Alocations)&q=queryAndMultiFacetTypeahead'
        response = requests.get(urn_url,headers=headers)
        try:
            response = json.loads(response.text)
            for element in response["elements"]:
                if pais_name.lower() == element["name"].lower():
                    element["alpha2"] = country
                    col_countries.update_one({'name':pais_name}, {"$set":element}, upsert=True)
        except:
            print(pais_name)
print(len(found))
'''