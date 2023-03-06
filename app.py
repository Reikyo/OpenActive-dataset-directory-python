import copy
import datetime
import json
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request
from inspect import stack
from os.path import exists

# ----------------------------------------------------------------------------------------------------

application = Flask(__name__)

catalogueCollectionUrl = 'https://openactive.io/data-catalogs/data-catalog-collection.jsonld'

dirNameCache = './cache/'
fileNameCatalogueUrls = 'catalogueUrls.json'
fileNameDatasetUrls = 'datasetUrls.json'
fileNameFeeds = 'feeds.json'
fileNameOpportunities = 'opportunities.json'

# ----------------------------------------------------------------------------------------------------

def try_requests(url):

    r = requests.get(url)

    numTries = 1
    numTriesMax = 10

    while (r.status_code == 403):
        r = requests.get(url)
        numTries += 1
        if (numTries == numTriesMax):
            break;

    return r

# ----------------------------------------------------------------------------------------------------

if (exists(dirNameCache + fileNameCatalogueUrls)):
    catalogueUrls = json.load(open(dirNameCache + fileNameCatalogueUrls, 'r'))
else:
    catalogueUrls = None

@application.route('/catalogueurls')
def get_catalogue_urls(
    doRefresh = False,
    doMetadata = False,
    doLimitCatalogues = None,
):

    if (stack()[1].function == 'dispatch_request'):
        doRefresh = request.args.get('doRefresh', default=False, type=lambda arg: arg.lower()=='true')
        doMetadata = request.args.get('doMetadata', default=False, type=lambda arg: arg.lower()=='true')
        doLimitCatalogues = request.args.get('doLimitCatalogues', default=None, type=int)

    # ----------------------------------------------------------------------------------------------------

    global catalogueUrls

    if (    not catalogueUrls
        or  doRefresh
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
            r1 = try_requests(catalogueCollectionUrl)
        except:
            print('ERROR: Can\'t get collection of catalogues')

        # ----------------------------------------------------------------------------------------------------

        if (    r1.status_code == 200
            and r1.json()
            and type(r1.json()) == dict
            and 'hasPart' in r1.json().keys()
            and type(r1.json()['hasPart']) == list
        ):
            for catalogueUrl in r1.json()['hasPart'][0:doLimitCatalogues]:
                if (    type(catalogueUrl) == str
                    and catalogueUrl not in catalogueUrls['data']
                ):
                    catalogueUrls['data'].append(catalogueUrl)

        # ----------------------------------------------------------------------------------------------------

        catalogueUrls['metadata']['counts'] = len(catalogueUrls['data'])
        catalogueUrls['metadata']['timeLastUpdated'] = str(datetime.datetime.now())

    # ----------------------------------------------------------------------------------------------------

    if (    not exists(dirNameCache + fileNameCatalogueUrls)
        or  doRefresh
    ):
        json.dump(catalogueUrls, open(dirNameCache + fileNameCatalogueUrls, 'w'))

    # ----------------------------------------------------------------------------------------------------

    if (doMetadata):
        return catalogueUrls
    else:
        return catalogueUrls['data']

# ----------------------------------------------------------------------------------------------------

if (exists(dirNameCache + fileNameDatasetUrls)):
    datasetUrls = json.load(open(dirNameCache + fileNameDatasetUrls, 'r'))
else:
    datasetUrls = None

@application.route('/dataseturls')
def get_dataset_urls(
    doRefresh = False,
    doFlatten = False,
    doMetadata = False,
    doLimitCatalogues = None,
    doLimitDatasets = None,
):

    if (stack()[1].function == 'dispatch_request'):
        doRefresh = request.args.get('doRefresh', default=False, type=lambda arg: arg.lower()=='true')
        doFlatten = request.args.get('doFlatten', default=False, type=lambda arg: arg.lower()=='true')
        doMetadata = request.args.get('doMetadata', default=False, type=lambda arg: arg.lower()=='true')
        doLimitCatalogues = request.args.get('doLimitCatalogues', default=None, type=int)
        doLimitDatasets = request.args.get('doLimitDatasets', default=None, type=int)

    # ----------------------------------------------------------------------------------------------------

    global datasetUrls

    if (    not datasetUrls
        or  doRefresh
    ):

        get_catalogue_urls(
            doRefresh = doRefresh,
            doLimitCatalogues = doLimitCatalogues,
        )

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
                r2 = try_requests(catalogueUrl)
            except:
                print('ERROR: Can\'t get catalogue', catalogueUrl)
                continue

            # ----------------------------------------------------------------------------------------------------

            if (    r2.status_code == 200
                and r2.json()
                and type(r2.json()) == dict
                and 'dataset' in r2.json().keys()
                and type(r2.json()['dataset']) == list
            ):
                for datasetUrl in r2.json()['dataset'][0:doLimitDatasets]:
                    if (    type(datasetUrl) == str
                        and datasetUrl not in catalogueDatasetUrls['data']
                    ):
                        catalogueDatasetUrls['data'].append(datasetUrl)

            # ----------------------------------------------------------------------------------------------------

            catalogueDatasetUrls['metadata']['counts'] = len(catalogueDatasetUrls['data'])
            catalogueDatasetUrls['metadata']['timeLastUpdated'] = str(datetime.datetime.now())

            datasetUrls['data'][catalogueUrl] = catalogueDatasetUrls

        # ----------------------------------------------------------------------------------------------------

        datasetUrls['metadata']['counts'] = sum([
            val['metadata']['counts']
            for val in datasetUrls['data'].values()
        ])
        datasetUrls['metadata']['timeLastUpdated'] = str(datetime.datetime.now())

    # ----------------------------------------------------------------------------------------------------

    if (    not exists(dirNameCache + fileNameDatasetUrls)
        or  doRefresh
    ):
        json.dump(datasetUrls, open(dirNameCache + fileNameDatasetUrls, 'w'))

    # ----------------------------------------------------------------------------------------------------

    if (doFlatten):
        return [
            val2
            for val1 in datasetUrls['data'].values()
            for val2 in val1['data']
        ]
    elif (doMetadata):
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
def get_feeds(
    doRefresh = False,
    doFlatten = False,
    doMetadata = False,
    doLimitCatalogues = None,
    doLimitDatasets = None,
    doLimitFeeds = None,
    doPath = False,
):

    if (stack()[1].function == 'dispatch_request'):
        doRefresh = request.args.get('doRefresh', default=False, type=lambda arg: arg.lower()=='true')
        doFlatten = request.args.get('doFlatten', default=False, type=lambda arg: arg.lower()=='true')
        doMetadata = request.args.get('doMetadata', default=False, type=lambda arg: arg.lower()=='true')
        doLimitCatalogues = request.args.get('doLimitCatalogues', default=None, type=int)
        doLimitDatasets = request.args.get('doLimitDatasets', default=None, type=int)
        doLimitFeeds = request.args.get('doLimitFeeds', default=None, type=int)
        doPath = request.args.get('doPath', default=False, type=lambda arg: arg.lower()=='true')

    # ----------------------------------------------------------------------------------------------------

    global feeds

    if (    not feeds
        or  doRefresh
    ):

        get_dataset_urls(
            doRefresh = doRefresh,
            doLimitCatalogues = doLimitCatalogues,
            doLimitDatasets = doLimitDatasets,
        )

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
                    r3 = try_requests(datasetUrl)
                except:
                    print('ERROR: Can\'t get dataset', catalogueUrl, '->', datasetUrl)
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
                                for feedInfo in jsonld['distribution'][0:doLimitFeeds]:
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
                datasetFeeds['metadata']['timeLastUpdated'] = str(datetime.datetime.now())

                feeds['data'][catalogueUrl]['data'][datasetUrl] = datasetFeeds

            # ----------------------------------------------------------------------------------------------------

            feeds['data'][catalogueUrl]['metadata']['counts'] = sum([
                val['metadata']['counts']
                for val in feeds['data'][catalogueUrl]['data'].values()
            ])
            feeds['data'][catalogueUrl]['metadata']['timeLastUpdated'] = str(datetime.datetime.now())

        # ----------------------------------------------------------------------------------------------------

        feeds['metadata']['counts'] = sum([
            val['metadata']['counts']
            for val in feeds['data'].values()
        ])
        feeds['metadata']['timeLastUpdated'] = str(datetime.datetime.now())

    # ----------------------------------------------------------------------------------------------------

    if (    not exists(dirNameCache + fileNameFeeds)
        or  doRefresh
    ):
        json.dump(feeds, open(dirNameCache + fileNameFeeds, 'w'))

    # ----------------------------------------------------------------------------------------------------

    if (not doPath):
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

    if (doFlatten):
        return [
            val3
            for val1 in output['data'].values()
            for val2 in val1['data'].values()
            for val3 in val2['data']
        ]
    elif (doMetadata):
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
def get_feed_urls(
    doRefresh = False,
    doFlatten = False,
    doMetadata = False,
    doLimitCatalogues = None,
    doLimitDatasets = None,
    doLimitFeeds = None,
):

    if (stack()[1].function == 'dispatch_request'):
        doRefresh = request.args.get('doRefresh', default=False, type=lambda arg: arg.lower()=='true')
        doFlatten = request.args.get('doFlatten', default=False, type=lambda arg: arg.lower()=='true')
        doMetadata = request.args.get('doMetadata', default=False, type=lambda arg: arg.lower()=='true')
        doLimitCatalogues = request.args.get('doLimitCatalogues', default=None, type=int)
        doLimitDatasets = request.args.get('doLimitDatasets', default=None, type=int)
        doLimitFeeds = request.args.get('doLimitFeeds', default=None, type=int)

    # ----------------------------------------------------------------------------------------------------

    global feedUrls

    if (    not feedUrls
        or  doRefresh
    ):

        get_feeds(
            doRefresh = doRefresh,
            doLimitCatalogues = doLimitCatalogues,
            doLimitDatasets = doLimitDatasets,
            doLimitFeeds = doLimitFeeds,
        )

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

    if (doFlatten):
        return [
            val3
            for val1 in feedUrls['data'].values()
            for val2 in val1['data'].values()
            for val3 in val2['data']
        ]
    elif (doMetadata):
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
def get_opportunities(
    doRefresh = False,
    doFlatten = False,
    doMetadata = False,
    doLimitCatalogues = None,
    doLimitDatasets = None,
    doLimitFeeds = None,
    doLimitOpportunities = None,
    doPath = False,
):

    if (stack()[1].function == 'dispatch_request'):
        doRefresh = request.args.get('doRefresh', default=False, type=lambda arg: arg.lower()=='true')
        doFlatten = request.args.get('doFlatten', default=False, type=lambda arg: arg.lower()=='true')
        doMetadata = request.args.get('doMetadata', default=False, type=lambda arg: arg.lower()=='true')
        doLimitCatalogues = request.args.get('doLimitCatalogues', default=None, type=int)
        doLimitDatasets = request.args.get('doLimitDatasets', default=None, type=int)
        doLimitFeeds = request.args.get('doLimitFeeds', default=None, type=int)
        doLimitOpportunities = request.args.get('doLimitOpportunities', default=None, type=int)
        doPath = request.args.get('doPath', default=False, type=lambda arg: arg.lower()=='true')

    # ----------------------------------------------------------------------------------------------------

    global opportunities

    if (    not opportunities
        or  doRefresh
    ):

        get_feed_urls(
            doRefresh = doRefresh,
            doLimitCatalogues = doLimitCatalogues,
            doLimitDatasets = doLimitDatasets,
            doLimitFeeds = doLimitFeeds,
        )

        # ----------------------------------------------------------------------------------------------------

        opportunities = {
            'metadata': {
                'counts': 0,
                'timeLastUpdated': None,
            },
            'data': {},
        }

        # ----------------------------------------------------------------------------------------------------

        for catalogueUrl in feedUrls['data'].keys():

            opportunities['data'][catalogueUrl] = {
                'metadata': {
                    'counts': 0,
                    'timeLastUpdated': None,
                },
                'data': {},
            }

            # ----------------------------------------------------------------------------------------------------

            for datasetUrl in feedUrls['data'][catalogueUrl]['data'].keys():

                opportunities['data'][catalogueUrl]['data'][datasetUrl] = {
                    'metadata': {
                        'counts': 0,
                        'timeLastUpdated': None,
                    },
                    'data': {},
                }

                # ----------------------------------------------------------------------------------------------------

                for feedUrl in feedUrls['data'][catalogueUrl]['data'][datasetUrl]['data']:

                    feedOpportunities = {
                        'metadata': {
                            'counts': 0,
                            'timeLastUpdated': None,
                        },
                        'data': {},
                    }

                    # ----------------------------------------------------------------------------------------------------

                    feedUrlCurrent = feedUrl

                    while (feedUrlCurrent):

                        try:
                            r4 = try_requests(feedUrlCurrent)
                        except:
                            print('ERROR: Can\'t get feed', catalogueUrl, '->', datasetUrl, '->', feedUrlCurrent)
                            continue

                        # ----------------------------------------------------------------------------------------------------

                        if (    r4.status_code == 200
                            and r4.json()
                            and type(r4.json()) == dict
                        ):
                            if (    'items' in r4.json().keys()
                                and type(r4.json()['items']) == list
                            ):
                                for opportunityInfo in r4.json()['items']:
                                    if (    type(opportunityInfo) == dict
                                        and 'state' in opportunityInfo.keys()
                                        and 'id' in opportunityInfo.keys()
                                        and 'modified' in opportunityInfo.keys()
                                        and (   opportunityInfo['id'] not in feedOpportunities['data'].keys()
                                            or  opportunityInfo['modified'] > feedOpportunities['data'][opportunityInfo['id']]['modified'] )
                                    ):

                                        if (opportunityInfo['state'] == 'deleted'):
                                            if (opportunityInfo['id'] in feedOpportunities['data'].keys()):
                                                del(feedOpportunities['data'][opportunityInfo['id']])
                                            continue

                                        feedOpportunity = {}

                                        # Most states should be 'updated', so only output the outliers to check what they are:
                                        # if (opportunityInfo['state'] != 'updated'):
                                        #     feedOpportunity['state'] = opportunityInfo['state']
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

                                        if (len(feedOpportunities['data']) == doLimitOpportunities):
                                            break

                            if (    'next' in r4.json().keys()
                                and type(r4.json()['next']) == str
                                and r4.json()['next'] != feedUrlCurrent
                                and len(feedOpportunities['data']) != doLimitOpportunities
                            ):
                                feedUrlCurrent = r4.json()['next']
                            else:
                                feedUrlCurrent = None

                        else:
                            print('ERROR: Problem with feed', catalogueUrl, '->', datasetUrl, '->', feedUrlCurrent)
                            feedUrlCurrent = None

                    # ----------------------------------------------------------------------------------------------------

                    feedOpportunities['data'] = list(feedOpportunities['data'].values())
                    # feedOpportunities['data'] = list(feedOpportunities['data'].keys())

                    # ----------------------------------------------------------------------------------------------------

                    feedOpportunities['metadata']['counts'] = len(feedOpportunities['data'])
                    feedOpportunities['metadata']['timeLastUpdated'] = str(datetime.datetime.now())

                    opportunities['data'][catalogueUrl]['data'][datasetUrl]['data'][feedUrl] = feedOpportunities

                # ----------------------------------------------------------------------------------------------------

                opportunities['data'][catalogueUrl]['data'][datasetUrl]['metadata']['counts'] = sum([
                    val['metadata']['counts']
                    for val in opportunities['data'][catalogueUrl]['data'][datasetUrl]['data'].values()
                ])
                opportunities['data'][catalogueUrl]['data'][datasetUrl]['metadata']['timeLastUpdated'] = str(datetime.datetime.now())

            # ----------------------------------------------------------------------------------------------------

            opportunities['data'][catalogueUrl]['metadata']['counts'] = sum([
                val['metadata']['counts']
                for val in opportunities['data'][catalogueUrl]['data'].values()
            ])
            opportunities['data'][catalogueUrl]['metadata']['timeLastUpdated'] = str(datetime.datetime.now())

        # ----------------------------------------------------------------------------------------------------

        opportunities['metadata']['counts'] = sum([
            val['metadata']['counts']
            for val in opportunities['data'].values()
        ])
        opportunities['metadata']['timeLastUpdated'] = str(datetime.datetime.now())

    # ----------------------------------------------------------------------------------------------------

    if (    not exists(dirNameCache + fileNameOpportunities)
        or  doRefresh
    ):
        json.dump(opportunities, open(dirNameCache + fileNameOpportunities, 'w'))

    # ----------------------------------------------------------------------------------------------------

    if (not doPath):
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

    if (doFlatten):
        return [
            val4
            for val1 in output['data'].values()
            for val2 in val1['data'].values()
            for val3 in val2['data'].values()
            for val4 in val3['data']
        ]
    elif (doMetadata):
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
