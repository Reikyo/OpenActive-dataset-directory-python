import copy
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from flask import Flask, jsonify, request
from inspect import stack
from os.path import exists
# from termcolor import colored

# ----------------------------------------------------------------------------------------------------

application = Flask(__name__)

catalogueCollectionUrl = 'https://openactive.io/data-catalogs/data-catalog-collection.jsonld'

dirNameCache = './cache/'
fileNameCatalogueUrls = 'catalogueUrls.json'
fileNameDatasetUrls = 'datasetUrls.json'
fileNameFeeds = 'feeds.json'
fileNameOpportunities = 'opportunities.json'

# ----------------------------------------------------------------------------------------------------

kwargsDefault = {
    'doRefresh': (bool, False),
    'doFlatten': (bool, False),
    'doMetadata': (bool, False),
    'doPath': (bool, False),
    'doLimitCatalogues': (int, None),
    'doLimitDatasets': (int, None),
    'doLimitFeeds': (int, None),
    'doLimitOpportunities': (int, None),
}

kwargsGlobal = {}

def set_kwargs_global(**kwargs):

    # print(datetime.now(), colored('This is', stack()[0].function, 'called by', stack()[1].function, 'green'))

    # This option is for a Flask call:
    if (stack()[2].function == 'dispatch_request'):
        for key,val in kwargsDefault.items():
            if (val[0] == bool):
                kwargsGlobal[key] = request.args.get(key, type=lambda arg: arg.lower()=='true', default=val[1])
            elif (val[0] == int):
                kwargsGlobal[key] = request.args.get(key, type=val[0], default=val[1])
    # This option is for a direct call if the code has been imported:
    elif (stack()[2].function == '<module>'):
        for key,val in kwargsDefault.items():
            kwargsGlobal[key] = kwargs[key] if key in kwargs.keys() else val[1]
    # We don't need a default option, as other calls would be for functions deeper in the stack, and
    # only the first function in the stack (either 'dispatch_request' or '<module>') dictates the
    # setting of global kwargs

# ----------------------------------------------------------------------------------------------------

def try_requests(url):

    r = requests.get(url)

    numTries = 1
    numTriesMax = 10

    while (r.status_code == 403):
        r = requests.get(url)
        numTries += 1
        if (numTries == numTriesMax):
            print('Max. tries reached')
            break;

    return r, numTries

# ----------------------------------------------------------------------------------------------------

if (exists(dirNameCache + fileNameCatalogueUrls)):
    catalogueUrls = json.load(open(dirNameCache + fileNameCatalogueUrls, 'r'))
else:
    catalogueUrls = None

@application.route('/catalogueurls')
def get_catalogue_urls(**kwargs):

    # print(datetime.now(), colored('This is', stack()[0].function, 'called by', stack()[1].function, 'green'))

    if (stack()[1].function in ['dispatch_request', '<module>']):
        set_kwargs_global(**kwargs)

    # ----------------------------------------------------------------------------------------------------

    global catalogueUrls

    if (    not catalogueUrls
        or  kwargsGlobal['doRefresh']
    ):

        catalogueUrls = {
            'metadata': {
                'counts': 0,
                'timeLastUpdated': None,
            },
            'data': [],
        }

        # ----------------------------------------------------------------------------------------------------

        try:
            r1, r1NumTries = try_requests(catalogueCollectionUrl)
            # print(datetime.now(), 'Got response in {} {}'.format(r1NumTries, 'try' if r1NumTries == 1 else 'tries'))
        except:
            print(datetime.now(), colored('ERROR: Can\'t get collection of catalogues', 'yellow'))

        # ----------------------------------------------------------------------------------------------------

        if (    r1.status_code == 200
            and r1.json()
            and type(r1.json()) == dict
            and 'hasPart' in r1.json().keys()
            and type(r1.json()['hasPart']) == list
        ):
            for catalogueUrl in r1.json()['hasPart'][0:kwargsGlobal['doLimitCatalogues']]:
                if (    type(catalogueUrl) == str
                    and catalogueUrl not in catalogueUrls['data']
                ):
                    catalogueUrls['data'].append(catalogueUrl)

        # ----------------------------------------------------------------------------------------------------

        catalogueUrls['metadata']['counts'] = len(catalogueUrls['data'])
        catalogueUrls['metadata']['timeLastUpdated'] = str(datetime.now())

    # ----------------------------------------------------------------------------------------------------

    if (    not exists(dirNameCache + fileNameCatalogueUrls)
        or  kwargsGlobal['doRefresh']
    ):
        json.dump(catalogueUrls, open(dirNameCache + fileNameCatalogueUrls, 'w'))

    # ----------------------------------------------------------------------------------------------------

    if (kwargsGlobal['doMetadata']):
        return catalogueUrls
    else:
        return catalogueUrls['data']

# ----------------------------------------------------------------------------------------------------

if (exists(dirNameCache + fileNameDatasetUrls)):
    datasetUrls = json.load(open(dirNameCache + fileNameDatasetUrls, 'r'))
else:
    datasetUrls = None

@application.route('/dataseturls')
def get_dataset_urls(**kwargs):

    # print(datetime.now(), colored('This is', stack()[0].function, 'called by', stack()[1].function, 'green'))

    if (stack()[1].function in ['dispatch_request', '<module>']):
        set_kwargs_global(**kwargs)

    # ----------------------------------------------------------------------------------------------------

    global datasetUrls

    if (    not datasetUrls
        or  kwargsGlobal['doRefresh']
    ):

        get_catalogue_urls()

        # ----------------------------------------------------------------------------------------------------

        datasetUrls = {
            'metadata': {
                'counts': 0,
                'timeLastUpdated': None,
            },
            'data': {},
        }

        # ----------------------------------------------------------------------------------------------------

        for catalogueUrl in catalogueUrls['data']:

            catalogueDatasetUrls = {
                'metadata': {
                    'counts': 0,
                    'timeLastUpdated': None,
                },
                'data': [],
            }

            # ----------------------------------------------------------------------------------------------------

            try:
                r2, r2NumTries = try_requests(catalogueUrl)
                # print(datetime.now(), 'Got response in {} {}'.format(r2NumTries, 'try' if r2NumTries == 1 else 'tries'))
            except:
                print(datetime.now(), colored('ERROR: Can\'t get catalogue {}'.format(catalogueUrl), 'yellow'))
                continue

            # ----------------------------------------------------------------------------------------------------

            if (    r2.status_code == 200
                and r2.json()
                and type(r2.json()) == dict
                and 'dataset' in r2.json().keys()
                and type(r2.json()['dataset']) == list
            ):
                for datasetUrl in r2.json()['dataset'][0:kwargsGlobal['doLimitDatasets']]:
                    if (    type(datasetUrl) == str
                        and datasetUrl not in catalogueDatasetUrls['data']
                    ):
                        catalogueDatasetUrls['data'].append(datasetUrl)

            # ----------------------------------------------------------------------------------------------------

            catalogueDatasetUrls['metadata']['counts'] = len(catalogueDatasetUrls['data'])
            catalogueDatasetUrls['metadata']['timeLastUpdated'] = str(datetime.now())

            datasetUrls['data'][catalogueUrl] = catalogueDatasetUrls

        # ----------------------------------------------------------------------------------------------------

        datasetUrls['metadata']['counts'] = sum([
            val['metadata']['counts']
            for val in datasetUrls['data'].values()
        ])
        datasetUrls['metadata']['timeLastUpdated'] = str(datetime.now())

    # ----------------------------------------------------------------------------------------------------

    if (    not exists(dirNameCache + fileNameDatasetUrls)
        or  kwargsGlobal['doRefresh']
    ):
        json.dump(datasetUrls, open(dirNameCache + fileNameDatasetUrls, 'w'))

    # ----------------------------------------------------------------------------------------------------

    if (kwargsGlobal['doFlatten']):
        return [
            val2
            for val1 in datasetUrls['data'].values()
            for val2 in val1['data']
        ]
    elif (kwargsGlobal['doMetadata']):
        return datasetUrls
    else:
        return {
            key: val['data']
            for key,val in datasetUrls['data'].items()
        }

# ----------------------------------------------------------------------------------------------------

if (exists(dirNameCache + fileNameFeeds)):
    feeds = json.load(open(dirNameCache + fileNameFeeds, 'r'))
else:
    feeds = None

@application.route('/feeds')
def get_feeds(**kwargs):

    # print(datetime.now(), colored('This is', stack()[0].function, 'called by', stack()[1].function, 'green'))

    if (stack()[1].function in ['dispatch_request', '<module>']):
        set_kwargs_global(**kwargs)

    # ----------------------------------------------------------------------------------------------------

    global feeds

    if (    not feeds
        or  kwargsGlobal['doRefresh']
    ):

        get_dataset_urls()

        # ----------------------------------------------------------------------------------------------------

        feeds = {
            'metadata': {
                'counts': 0,
                'timeLastUpdated': None,
            },
            'data': {},
        }

        # ----------------------------------------------------------------------------------------------------

        for catalogueUrl in datasetUrls['data'].keys():

            feeds['data'][catalogueUrl] = {
                'metadata': {
                    'counts': 0,
                    'timeLastUpdated': None,
                },
                'data': {},
            }

            # ----------------------------------------------------------------------------------------------------

            for datasetUrl in datasetUrls['data'][catalogueUrl]['data']:

                datasetFeeds = {
                    'metadata': {
                        'counts': 0,
                        'timeLastUpdated': None,
                    },
                    'data': [],
                }

                # ----------------------------------------------------------------------------------------------------

                try:
                    r3, r3NumTries = try_requests(datasetUrl)
                    # print(datetime.now(), 'Got response in {} {}'.format(r3NumTries, 'try' if r3NumTries == 1 else 'tries'))
                except:
                    print(datetime.now(), colored('ERROR: Can\'t get dataset {} -> {}'.format(catalogueUrl, datasetUrl), 'yellow'))
                    continue

                # ----------------------------------------------------------------------------------------------------

                if (    r3.status_code == 200
                    and r3.text
                    and type(r3.text) == str
                ):

                    soup = BeautifulSoup(r3.text, 'html.parser')

                    if (not soup.head):
                        continue

                    for val in soup.head.find_all('script'):
                        if (    'type' in val.attrs.keys()
                            and val['type'] == 'application/ld+json'
                        ):

                            jsonld = json.loads(val.string)

                            if (    type(jsonld) == dict
                                and 'distribution' in jsonld.keys()
                                and type(jsonld['distribution']) == list
                            ):
                                for feedInfo in jsonld['distribution'][0:kwargsGlobal['doLimitFeeds']]:
                                    if (type(feedInfo) == dict):

                                        datasetFeed = {}

                                        try: datasetFeed['url'] = feedInfo['contentUrl']
                                        except: pass
                                        # This is intentionally labelled as 'kind' to match opportunity info, and to avoid 'type' which is
                                        # preferable but used in other contexts:
                                        try: datasetFeed['kind'] = feedInfo['name']
                                        except: pass
                                        try: datasetFeed['datasetName'] = jsonld['name']
                                        except: pass
                                        try: datasetFeed['datasetPublisherName'] = jsonld['publisher']['name']
                                        except: pass
                                        try: datasetFeed['discussionUrl'] = jsonld['discussionUrl']
                                        except: pass
                                        try: datasetFeed['licenseUrl'] = jsonld['license']
                                        except: pass

                                        if (len(datasetFeed.keys()) > 0):
                                            datasetFeeds['data'].append(datasetFeed)

                # ----------------------------------------------------------------------------------------------------

                datasetFeeds['metadata']['counts'] = len(datasetFeeds['data'])
                datasetFeeds['metadata']['timeLastUpdated'] = str(datetime.now())

                feeds['data'][catalogueUrl]['data'][datasetUrl] = datasetFeeds

            # ----------------------------------------------------------------------------------------------------

            feeds['data'][catalogueUrl]['metadata']['counts'] = sum([
                val['metadata']['counts']
                for val in feeds['data'][catalogueUrl]['data'].values()
            ])
            feeds['data'][catalogueUrl]['metadata']['timeLastUpdated'] = str(datetime.now())

        # ----------------------------------------------------------------------------------------------------

        feeds['metadata']['counts'] = sum([
            val['metadata']['counts']
            for val in feeds['data'].values()
        ])
        feeds['metadata']['timeLastUpdated'] = str(datetime.now())

    # ----------------------------------------------------------------------------------------------------

    if (    not exists(dirNameCache + fileNameFeeds)
        or  kwargsGlobal['doRefresh']
    ):
        json.dump(feeds, open(dirNameCache + fileNameFeeds, 'w'))

    # ----------------------------------------------------------------------------------------------------

    if (not kwargsGlobal['doPath']):
        output = feeds
    else:
        output = copy.deepcopy(feeds)
        for catalogueUrl in output['data'].keys():
            for datasetUrl in output['data'][catalogueUrl]['data'].keys():
                for feed in output['data'][catalogueUrl]['data'][datasetUrl]['data']:
                    feed.update({
                        'catalogueUrl': catalogueUrl,
                        'datasetUrl': datasetUrl,
                    })

    if (kwargsGlobal['doFlatten']):
        return [
            val3
            for val1 in output['data'].values()
            for val2 in val1['data'].values()
            for val3 in val2['data']
        ]
    elif (kwargsGlobal['doMetadata']):
        return output
    else:
        return {
            key1: {
                key2: val2['data']
                for key2,val2 in val1['data'].items()
            }
            for key1,val1 in output['data'].items()
        }

# ----------------------------------------------------------------------------------------------------

feedUrls = None

@application.route('/feedurls')
def get_feed_urls(**kwargs):

    # print(datetime.now(), colored('This is', stack()[0].function, 'called by', stack()[1].function, 'green'))

    if (stack()[1].function in ['dispatch_request', '<module>']):
        set_kwargs_global(**kwargs)

    # ----------------------------------------------------------------------------------------------------

    global feedUrls

    if (    not feedUrls
        or  kwargsGlobal['doRefresh']
    ):

        get_feeds()

        # ----------------------------------------------------------------------------------------------------

        feedUrls = copy.deepcopy(feeds)

        # ----------------------------------------------------------------------------------------------------

        for catalogueUrl in feedUrls['data'].keys():
            for datasetUrl in feedUrls['data'][catalogueUrl]['data'].keys():
                feedUrls['data'][catalogueUrl]['data'][datasetUrl]['data'] = [
                    feed['url']
                    for feed in feedUrls['data'][catalogueUrl]['data'][datasetUrl]['data']
                ]

    # ----------------------------------------------------------------------------------------------------

    if (kwargsGlobal['doFlatten']):
        return [
            val3
            for val1 in feedUrls['data'].values()
            for val2 in val1['data'].values()
            for val3 in val2['data']
        ]
    elif (kwargsGlobal['doMetadata']):
        return feedUrls
    else:
        return {
            key1: {
                key2: val2['data']
                for key2,val2 in val1['data'].items()
            }
            for key1,val1 in feedUrls['data'].items()
        }

# ----------------------------------------------------------------------------------------------------

if (exists(dirNameCache + fileNameOpportunities)):
    opportunities = json.load(open(dirNameCache + fileNameOpportunities, 'r'))
else:
    opportunities = None

@application.route('/opportunities')
def get_opportunities(**kwargs):

    t1 = datetime.now()

    # print(datetime.now(), colored('This is', stack()[0].function, 'called by', stack()[1].function, 'green'))

    if (stack()[1].function in ['dispatch_request', '<module>']):
        set_kwargs_global(**kwargs)

    # ----------------------------------------------------------------------------------------------------

    global opportunities

    if (    not opportunities
        or  kwargsGlobal['doRefresh']
    ):

        get_feed_urls()

        # ----------------------------------------------------------------------------------------------------

        opportunities = {
            'metadata': {
                'counts': 0,
                'timeLastUpdated': None,
            },
            'data': {},
        }

        # ----------------------------------------------------------------------------------------------------

        catalogueUrlCtr = 1
        catalogueUrlNum = len(feedUrls['data'])

        for catalogueUrl in feedUrls['data'].keys():

            print(datetime.now(), colored('catalogueUrl ({}/{}): {}'.format(catalogueUrlCtr, catalogueUrlNum, catalogueUrl), 'blue'))
            catalogueUrlCtr += 1

            opportunities['data'][catalogueUrl] = {
                'metadata': {
                    'counts': 0,
                    'timeLastUpdated': None,
                },
                'data': {},
            }

            # ----------------------------------------------------------------------------------------------------

            datasetUrlCtr = 1
            datasetUrlNum = len(feedUrls['data'][catalogueUrl]['data'])

            for datasetUrl in feedUrls['data'][catalogueUrl]['data'].keys():

                print(datetime.now(), colored('datasetUrl ({}/{}): {}'.format(datasetUrlCtr, datasetUrlNum, datasetUrl), 'blue'))
                datasetUrlCtr += 1

                opportunities['data'][catalogueUrl]['data'][datasetUrl] = {
                    'metadata': {
                        'counts': 0,
                        'timeLastUpdated': None,
                    },
                    'data': {},
                }

                # ----------------------------------------------------------------------------------------------------

                feedUrlCtr = 1
                feedUrlNum = len(feedUrls['data'][catalogueUrl]['data'][datasetUrl]['data'])

                for feedUrl in feedUrls['data'][catalogueUrl]['data'][datasetUrl]['data']:

                    print(datetime.now(), colored('feedUrl ({}/{}): {}'.format(feedUrlCtr, feedUrlNum, feedUrl), 'blue'))
                    feedUrlCtr += 1

                    feedOpportunities = {
                        'metadata': {
                            'counts': 0,
                            'timeLastUpdated': None,
                        },
                        'data': {},
                    }

                    # ----------------------------------------------------------------------------------------------------

                    feedPageUrl = feedUrl
                    feedPageUrlCtr = 1

                    while (feedPageUrl):

                        print(datetime.now(), 'feedPageUrl ({}): {}'.format(feedPageUrlCtr, feedPageUrl))
                        feedPageUrlCtr += 1

                        try:
                            r4, r4NumTries = try_requests(feedPageUrl)
                            print(datetime.now(), 'Got response in {} {}'.format(r4NumTries, 'try' if r4NumTries == 1 else 'tries'))
                        except:
                            print(datetime.now(), colored('ERROR: Can\'t get feed page {} -> {} -> {}'.format(catalogueUrl, datasetUrl, feedPageUrl), 'yellow'))
                            break

                        # ----------------------------------------------------------------------------------------------------

                        if (    r4.status_code == 200
                            and r4.json()
                            and type(r4.json()) == dict
                        ):
                            if (    'items' in r4.json().keys()
                                and type(r4.json()['items']) == list
                            ):

                                itemCtr = 1
                                itemNum = len(r4.json()['items'])

                                for opportunityInfo in r4.json()['items']:

                                    print('\ritem ({}/{}): {}'.format(itemCtr, itemNum, opportunityInfo['id']), end='')
                                    itemCtr += 1

                                    if (    type(opportunityInfo) == dict
                                        and 'state' in opportunityInfo.keys()
                                        and type(opportunityInfo['state']) in [int, str]
                                        and 'id' in opportunityInfo.keys()
                                        and type(opportunityInfo['id']) in [int, str]
                                        and 'modified' in opportunityInfo.keys()
                                        and type(opportunityInfo['modified']) in [int, str]
                                        and (   opportunityInfo['id'] not in feedOpportunities['data'].keys()
                                            or  opportunityInfo['modified'] > feedOpportunities['data'][opportunityInfo['id']]['modified'] )
                                    ):

                                        if (opportunityInfo['state'] == 'deleted'):
                                            if (opportunityInfo['id'] in feedOpportunities['data'].keys()):
                                                del(feedOpportunities['data'][opportunityInfo['id']])
                                            continue

                                        feedOpportunity = {}

                                        # Most states should be 'updated', so only output the outliers to check what they are:
                                        if (opportunityInfo['state'] != 'updated'):
                                            feedOpportunity['state'] = opportunityInfo['state']
                                        feedOpportunity['id'] = opportunityInfo['id']
                                        feedOpportunity['modified'] = opportunityInfo['modified']
                                        try: feedOpportunity['kind'] = opportunityInfo['kind']
                                        except: pass
                                        try: feedOpportunity['name'] = opportunityInfo['data']['name']
                                        except: pass
                                        try: feedOpportunity['activityPrefLabel'] = opportunityInfo['data']['activity'][0]['prefLabel']
                                        except: pass
                                        try: feedOpportunity['activityId'] = opportunityInfo['data']['activity'][0]['id']
                                        except: pass
                                        try: feedOpportunity['latitude'] = opportunityInfo['data']['location']['geo']['latitude']
                                        except: pass
                                        try: feedOpportunity['longitude'] = opportunityInfo['data']['location']['geo']['longitude']
                                        except: pass

                                        # These were just to check the available keys, but take up a lot of cache file space:
                                        # feedOpportunity['keys'] = list(opportunityInfo.keys())
                                        # try: feedOpportunity['keysData'] = list(opportunityInfo['data'].keys())
                                        # except: pass

                                        feedOpportunities['data'][opportunityInfo['id']] = feedOpportunity

                                        if (len(feedOpportunities['data']) == kwargsGlobal['doLimitOpportunities']):
                                            break
                                            # TODO: Think we need to ensure this takes us out of the for-loop as well as the while-loop

                                if (itemNum > 0):
                                    print()
                                print(datetime.now(), 'Processed this feed page, we now have {} items for this feed'.format(len(feedOpportunities['data'])))

                            if (    'next' in r4.json().keys()
                                and type(r4.json()['next']) == str
                                and r4.json()['next'] != feedPageUrl
                                and len(feedOpportunities['data']) != kwargsGlobal['doLimitOpportunities']
                            ):
                                print(datetime.now(), 'Going to the next feed page')
                                feedPageUrl = r4.json()['next']
                            else:
                                print(datetime.now(), 'Going to the next feed')
                                feedPageUrl = None

                        else:
                            print(datetime.now(), colored('ERROR: Problem with feed page {} -> {} -> {}'.format(catalogueUrl, datasetUrl, feedPageUrl), 'yellow'))
                            print(datetime.now(), 'Going to the next feed')
                            feedPageUrl = None

                    # ----------------------------------------------------------------------------------------------------

                    feedOpportunities['data'] = list(feedOpportunities['data'].values())
                    # feedOpportunities['data'] = list(feedOpportunities['data'].keys())

                    # ----------------------------------------------------------------------------------------------------

                    feedOpportunities['metadata']['counts'] = len(feedOpportunities['data'])
                    feedOpportunities['metadata']['timeLastUpdated'] = str(datetime.now())

                    opportunities['data'][catalogueUrl]['data'][datasetUrl]['data'][feedUrl] = feedOpportunities

                # ----------------------------------------------------------------------------------------------------

                opportunities['data'][catalogueUrl]['data'][datasetUrl]['metadata']['counts'] = sum([
                    val['metadata']['counts']
                    for val in opportunities['data'][catalogueUrl]['data'][datasetUrl]['data'].values()
                ])
                opportunities['data'][catalogueUrl]['data'][datasetUrl]['metadata']['timeLastUpdated'] = str(datetime.now())

            # ----------------------------------------------------------------------------------------------------

            opportunities['data'][catalogueUrl]['metadata']['counts'] = sum([
                val['metadata']['counts']
                for val in opportunities['data'][catalogueUrl]['data'].values()
            ])
            opportunities['data'][catalogueUrl]['metadata']['timeLastUpdated'] = str(datetime.now())

        # ----------------------------------------------------------------------------------------------------

        opportunities['metadata']['counts'] = sum([
            val['metadata']['counts']
            for val in opportunities['data'].values()
        ])
        opportunities['metadata']['timeLastUpdated'] = str(datetime.now())

    # ----------------------------------------------------------------------------------------------------

    if (    not exists(dirNameCache + fileNameOpportunities)
        or  kwargsGlobal['doRefresh']
    ):
        json.dump(opportunities, open(dirNameCache + fileNameOpportunities, 'w'))

    # ----------------------------------------------------------------------------------------------------

    if (not kwargsGlobal['doPath']):
        output = opportunities
    else:
        output = copy.deepcopy(opportunities)
        for catalogueUrl in output['data'].keys():
            for datasetUrl in output['data'][catalogueUrl]['data'].keys():
                for feedUrl in output['data'][catalogueUrl]['data'][datasetUrl]['data'].keys():
                    for opportunity in output['data'][catalogueUrl]['data'][datasetUrl]['data'][feedUrl]['data']:
                        opportunity.update({
                            'catalogueUrl': catalogueUrl,
                            'datasetUrl': datasetUrl,
                            'feedUrl': feedUrl,
                        })

    t2 = datetime.now()
    print(datetime.now(), 'Time taken:', t2-t1)

    if (kwargsGlobal['doFlatten']):
        return [
            val4
            for val1 in output['data'].values()
            for val2 in val1['data'].values()
            for val3 in val2['data'].values()
            for val4 in val3['data']
        ]
    elif (kwargsGlobal['doMetadata']):
        return output
    else:
        return {
            key1: {
                key2: {
                    key3: val3['data']
                    for key3,val3 in val2['data'].items()
                }
                for key2,val2 in val1['data'].items()
            }
            for key1,val1 in output['data'].items()
        }

# ----------------------------------------------------------------------------------------------------

if (__name__ == '__main__'):
    application.run()
