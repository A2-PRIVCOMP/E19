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
import pymongo
import pandas as pd
import pickle
import traceback

client = pymongo.MongoClient('localhost')
db = client['as_descending_by_countries']
col = db['urns_paises']

'''
with open('../data/json_urns.json','r',encoding='utf-8') as input:
    reg = json.load(input)
'''

arrIn = [
    "locations",
    "titles",
    "industries",
    "genders",
    "ageRanges",
    "jobFunctions",
    "skills",
    "employers",
    "degrees",
    "schools",
    "titles",
    "groups",
    "interests",
    "fieldsOfStudy",
    "ageRanges",
]

arrOut = [
    "geo",
    "title",
    "industry",
    "gender",
    "ageRange",
    "function",
    "skill",
    "company",
    "degree",
    "school",
    "title",
    "group",
    "interest",
    "fieldOfStudy",
    "ageRange",
]

arrDict = dict(zip(arrIn,arrOut))


actID = open('LD.cnf').read().split(',')[0]

access_token = open('LD.cnf').read().split(',')[1]

cookies = open('LD.cnf',encoding='utf-8').read().split(',')[2]


headers = {'csrf-token':access_token,
'COOKIE': cookies,
#'content-type':'application/x-www-form-urlencoded',
#'x-http-method-override': 'GET',
'x-restli-protocol-version':'2.0.0'
}


cookies = cookies.split(';')

cookies = [cookie.split('=',1) for cookie in cookies]

for i in range(len(cookies)):
    cookies[i] = [cookie.strip(' "') for cookie in cookies[i]]

cookies = np.array(cookies)

cookies = dict(zip(cookies[:,0],cookies[:,1]))

print(cookies)


def getURNsFromNames(arr2ofdicts):#substitutes the names with urns in a 2d array containing dicts with the type of data and the name (ej:{'type':'location','name':'Spain'}
    newarr = []
    for index1,row in enumerate(arr2ofdicts):
        newrow = []
        for index2, element in enumerate(row):
            try:
                newrow += [getURNfromReg(element)]
            except:
                newrow += [asyncio.run(paralellize_queries([element],1,getURN))]
        newarr += [newrow]
    return newarr

def getURNfromReg(element):#ej:{'type':'location','name':'Spain'}
    uncodedName = element['name'].lower()
    solicitedType = element['type'].lower()
    typeOut = ''
    try:
        document = col.find_one({'name': uncodedName,'type':arrDict[solicitedType]})
        urn = quote(document['urn'], safe='')
        typeOut = document['type']
        name = quote(uncodedName, safe='')
        ancestorList = document['ancestorList']
    except Exception as e: 
        raise(e)

    if(typeOut != arrDict[solicitedType]):
            print("error: solicited and returned types don't match", element)
            return {}
    else:
        return {'type':typeOut,'urn':urn,'ancestorList':ancestorList,'name':unquote(name)}

async def getURN(dict,session):#ej:{'type':'location','name':'Spain'}
    url = 'https://www.linkedin.com/campaign-manager-api/campaignManagerAdTargetingEntities?query='+quote(dict['name'])+'&accountId='+actID+'&facets=List(urn%3Ali%3AadTargetingFacet%3A'+dict['type']+')&q=queryAndMultiFacetTypeahead'
    #url = 'https://postman-echo.com/get'
    try:
        async with session.get(url=URL(url,encoded=True), headers=headers) as response:
            resp = await response.read()

            try:
                data = json.loads(resp.decode('utf-8'))['elements']
                element_index = 0
                for index, element in enumerate(data):
                    if element['name'].lower() == dict['name'].lower():
                        element_index = index
                data = data[element_index]
                dict['urn'] = data['urn'].split(':')[-1]
                try:
                    dict['ancestorList'] = [i.split(':')[-1] for i in data['ancestorUrns']]
                except:
                    dict['ancestorList'] = []
                dict['name'] = data['name'].lower()
                dict['type'] = data['urn'].split(':')[-2]
                try:
                    col.insert_one({'type':dict['type'],'name':dict['name'],'urn':dict['urn'],'ancestorList':dict['ancestorList']})
                except:
                    traceback.print_exc()
            except Exception as e:
                traceback.print_exc()
    
    except Exception as e:
        traceback.print_exc()

    return dict    
def genForms(dataArray3d):#,dbMongo):Implementing lookup on mongo db using type and name
    retarray3d = []
    for element in dataArray3d:
        preamb = "(include:(and:List("
        epilogue = ",exclude:(or:List()))"
        lang = "(or:List((facet:(urn:urn%3Ali%3AadTargetingFacet%3AinterfaceLocales,name:Idiomas%20de%20la%20interfaz),segments:List((urn:urn%3Ali%3Alocale%3Aen_US,name:Ingl%C3%A9s,facetUrn:urn%3Ali%3AadTargetingFacet%3AinterfaceLocales)))))"
        cmTargetingCriteria = preamb + lang

        cmTargetingCriteria = preamb + lang
        for i in element:
            for index_j, j in enumerate(i):
                ancestorList = j['ancestorList']
                urn = j['urn']
                name = j['name']

                typeOut = j['type']
                typeIn = arrOut.index(typeOut)

                differentType = False

                if (index_j != 0):
                    try:
                        if (previousType != typeOut): 
                            differentType = True
                    except:
                        pass
                first = (
                    ",(or:List((facet:(urn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    "),segments:List((urn:urn%3Ali%3A" +
                    arrOut[typeIn] +
                    "%3A" +
                    urn +
                    ",facetUrn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    ")"
                )
                middle = (
                    ",(urn:urn%3Ali%3A" +
                    arrOut[typeIn] +
                    "%3A" +
                    urn +
                    ",facetUrn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    ")"
                )
                middleDifferentType = (
                    ")),(facet:(urn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    "),segments:List((urn:urn%3Ali%3A" +
                    arrOut[typeIn] +
                    "%3A" +
                    urn +
                    ",facetUrn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    ")"
                )
                last = (
                    ",(urn:urn%3Ali%3A" +
                    arrOut[typeIn] +
                    "%3A" +
                    urn +
                    ",facetUrn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    ")))))"
                )
                lastDifferentType = (
                    ")),(facet:(urn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    "),segments:List((urn:urn%3Ali%3A" +
                    arrOut[typeIn] +
                    "%3A" +
                    urn +
                    ",facetUrn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    ")))))"
                )
                unique = (
                    ",(or:List((facet:(urn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    "),segments:List((urn:urn%3Ali%3A" +
                    arrOut[typeIn] +
                    "%3A" +
                    urn +
                    ",facetUrn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    ")))))"
                )

                firstWithAncestor = (
                    ",(or:List((facet:(urn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    "),segments:List((urn:urn%3Ali%3A" +
                    arrOut[typeIn] +
                    "%3A" +
                    urn +
                    ",facetUrn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    ",ancestorUrns:List("
                )

                for index, k in enumerate(ancestorList):
                    firstWithAncestor += "urn%3Ali%3Ageo%3A" + k
                    if(index != len(ancestorList) - 1): 
                        firstWithAncestor += ","

                firstWithAncestor += "))"

                middleOrWithAncestor = (
                    ",(urn:urn%3Ali%3A" +
                    arrOut[typeIn] +
                    "%3A" +
                    urn +
                    ",facetUrn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    ",ancestorUrns:List("
                )
                middleOrWithAncestorDifferentType = (
                    ")),(facet:(urn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    "),segments:List((urn:urn%3Ali%3A" +
                    arrOut[typeIn] +
                    "%3A" +
                    urn +
                    ",facetUrn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    ",ancestorUrns:List("
                )
                for index, k in enumerate(ancestorList):
                    middleOrWithAncestor += "urn%3Ali%3Ageo%3A" + k
                    if (index != len(ancestorList) - 1): middleOrWithAncestor += ","
                    middleOrWithAncestorDifferentType += "urn%3Ali%3Ageo%3A" + k
                    if (index != len(ancestorList) - 1): middleOrWithAncestorDifferentType += ","

                middleOrWithAncestor += "))"
                middleOrWithAncestorDifferentType += "))"

                lastWithAncestor = (
                    ",(urn:urn%3Ali%3A" +
                    arrOut[typeIn] +
                    "%3A" +
                    urn +
                    ",facetUrn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    ",ancestorUrns:List("
                )
                lastWithAncestorDifferentType = (
                    ")),(facet:(urn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    "),segments:List((urn:urn%3Ali%3A" +
                    arrOut[typeIn] +
                    "%3A" +
                    urn +
                    ",facetUrn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    ",ancestorUrns:List("
                )
                for index, k in enumerate(ancestorList):
                    lastWithAncestor += "urn%3Ali%3Ageo%3A" + k
                    if (index != len(ancestorList) - 1): lastWithAncestor += ","
                    lastWithAncestorDifferentType += "urn%3Ali%3Ageo%3A" + k
                    if (index != len(ancestorList) - 1):
                        lastWithAncestorDifferentType += ","

                lastWithAncestor += "))))))"
                lastWithAncestorDifferentType += "))))))"

                uniqueWithAncestor = (
                    ",(or:List((facet:(urn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    "),segments:List((urn:urn%3Ali%3A" +
                    arrOut[typeIn] +
                    "%3A" +
                    urn +
                    ",facetUrn:urn%3Ali%3AadTargetingFacet%3A" +
                    arrIn[typeIn] +
                    ",ancestorUrns:List("
                )

                for index, k in enumerate(ancestorList):
                    uniqueWithAncestor += "urn%3Ali%3Ageo%3A" + k
                    if (index != len(ancestorList) - 1): uniqueWithAncestor += ","

                uniqueWithAncestor += "))))))"

                if (len(i) - 1 == 0):
                    if (len(ancestorList) > 0):
                        cmTargetingCriteria += uniqueWithAncestor
                    else: cmTargetingCriteria += unique
                else:
                    if (index_j == 0):
                        if (len(ancestorList) > 0):
                            cmTargetingCriteria += firstWithAncestor
                        else: cmTargetingCriteria += first
                    
                    if (index_j == len(i) - 1):
                        if (len(ancestorList) > 0):
                            if (differentType):
                                cmTargetingCriteria += lastWithAncestorDifferentType
                            else: cmTargetingCriteria += lastWithAncestor
                        else:
                            if (differentType): cmTargetingCriteria += lastDifferentType
                            else: cmTargetingCriteria += last

                    else:
                        if (index_j > 0):
                            if (len(ancestorList) > 0):
                                if (differentType): cmTargetingCriteria += middleOrWithAncestorDifferentType
                                else: cmTargetingCriteria += middleOrWithAncestor
                            else:
                                if (differentType): cmTargetingCriteria += middleDifferentType
                                else: cmTargetingCriteria += middle

            previousType = typeOut

        cmTargetingCriteria += "))" + epilogue
        retarray3d += [cmTargetingCriteria]
    return retarray3d
async def getAudienceCounts(data,session):
    print(data)
    #consider checking for geo
    cmTargetingCriteria = data['form']

    form = 'q=targetingCriteria'
    form += '&cmTargetingCriteria='

    form += cmTargetingCriteria
    form += '&withValidation=true'

    url =  'https://www.linkedin.com/campaign-manager-api/campaignManagerAudienceCounts'

    try:
        async with session.post(url=url, headers=headers,data=form.encode('utf-8'), cookies=cookies) as response:
            resp = await response.read()
            try:
                count = json.loads(resp.decode('utf-8'))['elements'][0]['count']
                data['count'] = count
                del data['form']
                return data
            except:
                data['count'] = -resp.status
                return data
    except Exception as e:
        print("Unable to get url {} due to {}{}.".format(url, e.__class__,e))



async def getBidSuggestion(data, bidType, session):

    #consider checking for some geo in data
    cmTargetingCriteria = data['form']

    form = "q=criteriaV2"
    form += "&adFormats=List(STANDARD_SPONSORED_CONTENT)"
    form += "&accountId=" + actID
    form += "&targetingCriteria="

    form += cmTargetingCriteria

    form += "&bidType=" + bidType
    form += "&currency=USD"
    form += "&matchType=EXACT"
    form += "&productType=MARKETING_SOLUTIONS"
    form += "&roadblockType=NONE"
    form += "&optimizationTargetType=NONE"
    form += "&dailyBudget=(amount:50.00,currencyCode:USD)"
    form += "&objectiveType=WEBSITE_VISIT"
    form += "&runSchedule=(start:" + str(time.time()).split('.')[0]+str(time.time()).split('.')[1][0:3] + ")"

    url =  "https://www.linkedin.com/campaign-manager-api/campaignManagerLimits"

    try:
        async with session.post(url=url, headers=headers,data=form.encode('utf-8'),cookies=cookies) as response:
            resp = await response.read()
            data['resp'] = resp
            return data
    except Exception as e:
        print("Unable to get url {} due to {}{}.".format(url, e.__class__,e))

async def paralellize_queries(forms,limit,query):
    connector = aiohttp.TCPConnector(limit=limit)
    timeout = aiohttp.ClientTimeout(total=3000)
    jar = aiohttp.CookieJar(unsafe=False)
    async with aiohttp.ClientSession(connector=connector,timeout=timeout,cookie_jar=jar,skip_auto_headers=['user-agent','accept-encoding','accept']) as session:
        ret = await asyncio.gather(*[query(form,session) for form in forms])
    print("Finalized all. Return is a list of len {} outputs.".format(len(ret)))
    return ret


'''

countries = ['Spain','France','Italy','Russia']

interests = ['3D Printing']#leer de la lista
'''

'''


interests = pd.read_csv('interests.csv',names=['interest'])['interest'].tolist()
countries = pd.read_csv('countries.csv',names=['country'])['country'].tolist()

arr_toget = []
for interest in interests:
    arr_toget += [{'type':'interests','name':interest}]
for country in countries:
    arr_toget += [{'type':'locations','name':country}]

ret = asyncio.run(paralellize_queries(arr_toget,5,getURN))
'''


'''
arr_for_counts = []

interests = pd.read_csv('valid_interests_ld.csv',names=['interest'])['interest'].tolist()
countries = pd.read_csv('valid_countries_ld.csv',names=['country'])['country'].tolist()

for interest in interests:
#for country in countries:
    arr = [#esto es la query
        #[{'type':'locations','name':country}],
        [{'type':'interests','name':interest}]
    ]
    query = getURNsFromNames(arr)
    arr_for_counts += [{'query':arr,'form':genFormsCount([query])[0]}]

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

print(len(arr_for_counts))
ret = asyncio.run(paralellize_queries(arr_for_counts,5,getAudienceCounts))


with open('ret_insterest_base.pickle', 'wb') as handle:
    pickle.dump(ret, handle, protocol=pickle.HIGHEST_PROTOCOL)
'''