import datetime
import json
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify

# ----------------------------------------------------------------------------------------------------

application = Flask(__name__)

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

@application.route('/distributions')
def get_distributions():

    distributions = []

    # ----------------------------------------------------------------------------------------------------

    try:
        r1 = try_requests('https://openactive.io/data-catalogs/data-catalog-collection.jsonld')
    except:
        print('ERROR: Can\'t get collection of catalogues')

    # ----------------------------------------------------------------------------------------------------

    if (    r1.status_code == 200
        and r1.json()
        and type(r1.json()) == dict
        and 'hasPart' in r1.json().keys()
        and type(r1.json()['hasPart']) == list
    ):

        for sUrlCatalogue in r1.json()['hasPart']: # Enable to do all catalogues
        # for sUrlCatalogue in [r1.json()['hasPart'][0]]: # Enable to do only one catalogue for a test
            if (type(sUrlCatalogue) == str):

                try:
                    r2 = try_requests(sUrlCatalogue)
                except:
                    print('ERROR: Can\'t get catalogue', sUrlCatalogue)
                    continue

                # ----------------------------------------------------------------------------------------------------

                if (    r2.status_code == 200
                    and r2.json()
                    and type(r2.json()) == dict
                    and 'dataset' in r2.json().keys()
                    and type(r2.json()['dataset']) == list
                ):

                    for sUrlDataset in r2.json()['dataset']: # Enable to do all datasets
                    # for sUrlDataset in [r2.json()['dataset'][0]]: # Enable to do only one dataset for a test
                        if (type(sUrlDataset) == str):

                            try:
                                r3 = try_requests(sUrlDataset)
                            except:
                                print('ERROR: Can\'t get dataset', sUrlCatalogue, '->', sUrlDataset)
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

                                            for jsonldDistribution in jsonld['distribution']: # Enable to do all distributions
                                            # for jsonldDistribution in [distributions[0]]: # Enable to do only one distribution for a test
                                                if (type(jsonldDistribution) == dict):

                                                    distribution = {}

                                                    # Should match sUrlCatalogue from r1.json()['hasPart']:
                                                    distribution['urlCatalogue'] = r2.json()['id'] if 'id' in r2.json().keys() else r2.json()['@id'] if '@id' in r2.json().keys() else ''
                                                    # The catalogue publisher name is the closest we have to a catalogue name proper:
                                                    distribution['namePublisherCatalogue'] = r2.json()['publisher']['name'] if 'publisher' in r2.json().keys() and 'name' in r2.json()['publisher'].keys() else ''
                                                    # Should match sUrlDataset from r2.json()['dataset']:
                                                    distribution['urlDataset'] = jsonld['url'] if 'url' in jsonld.keys() else ''
                                                    distribution['nameDataset'] = jsonld['name'] if 'name' in jsonld.keys() else ''
                                                    distribution['namePublisherDataset'] = jsonld['publisher']['name'] if 'publisher' in jsonld.keys() and 'name' in jsonld['publisher'].keys() else ''
                                                    distribution['urlDistribution'] = jsonldDistribution['contentUrl'] if 'contentUrl' in jsonldDistribution.keys() else ''
                                                    distribution['nameDistribution'] = jsonldDistribution['name'] if 'name' in jsonldDistribution.keys() else ''
                                                    distribution['urlDiscussion'] = jsonld['discussionUrl'] if 'discussionUrl' in jsonld.keys() else ''
                                                    distribution['urlLicense'] = jsonld['license'] if 'license' in jsonld.keys() else ''
                                                    distribution['numActivities'] = 0
                                                    distribution['numActivitiesWithName1'] = 0
                                                    distribution['numActivitiesWithName2'] = 0
                                                    distribution['numActivitiesWithId'] = 0
                                                    distribution['activities'] = []

                                                    # ----------------------------------------------------------------------------------------------------

                                                    if (    'contentUrl' in jsonldDistribution.keys()
                                                        and type(jsonldDistribution['contentUrl'] == str)
                                                    ):

                                                        sUrlDistribution = jsonldDistribution['contentUrl']

                                                        try:
                                                            r4 = try_requests(sUrlDistribution)
                                                        except:
                                                            print('ERROR: Can\'t get distribution', sUrlCatalogue, '->', sUrlDataset, '->', sUrlDistribution)
                                                            continue

                                                        # ----------------------------------------------------------------------------------------------------

                                                        if (    r4.status_code == 200
                                                            and r4.json()
                                                            and type(r4.json()) == dict
                                                            and 'items' in r4.json().keys()
                                                            and type(r4.json()['items']) == list
                                                        ):

                                                            distribution['numActivities'] = len(r4.json()['items'])

                                                            for distributionActivity in r4.json()['items']:

                                                                activity = {}

                                                                if (    type(distributionActivity) == dict
                                                                    and 'data' in distributionActivity.keys()
                                                                    and type(distributionActivity['data']) == dict
                                                                ):

                                                                    if (    'name' in distributionActivity['data'].keys()
                                                                        and type(distributionActivity['data']['name']) == str
                                                                    ):
                                                                        activity['name1'] = distributionActivity['data']['name']
                                                                        distribution['numActivitiesWithName1'] += 1

                                                                    if (    'beta:sportsActivityLocation' in distributionActivity['data'].keys()
                                                                        and type(distributionActivity['data']['beta:sportsActivityLocation']) == list
                                                                        and len(distributionActivity['data']['beta:sportsActivityLocation']) == 1
                                                                        and type(distributionActivity['data']['beta:sportsActivityLocation'][0]) == dict
                                                                        and 'name' in distributionActivity['data']['beta:sportsActivityLocation'][0].keys()
                                                                        # and type(distributionActivity['data']['beta:sportsActivityLocation'][0]['name']) == str # Sometimes this name field is a string, sometimes it is a list of strings ...
                                                                    ):
                                                                        activity['name2'] = distributionActivity['data']['beta:sportsActivityLocation'][0]['name']
                                                                        distribution['numActivitiesWithName2'] += 1

                                                                    if (    'activity' in distributionActivity['data'].keys()
                                                                        and type(distributionActivity['data']['activity']) == list
                                                                        and len(distributionActivity['data']['activity']) == 1
                                                                        and type(distributionActivity['data']['activity'][0]) == dict
                                                                        and 'id' in distributionActivity['data']['activity'][0].keys()
                                                                        and type(distributionActivity['data']['activity'][0]['id']) == str
                                                                    ):
                                                                        activity['id'] = distributionActivity['data']['activity'][0]['id']
                                                                        distribution['numActivitiesWithId'] += 1

                                                                if (len(activity.keys()) > 0):
                                                                    distribution['activities'].append(activity)

                                                    # ----------------------------------------------------------------------------------------------------

                                                    distributions.append(distribution)

    # ----------------------------------------------------------------------------------------------------

    # return distributions # Key order made alphabetical
    # return jsonify(distributions) # Key order made alphabetical
    return json.dumps(distributions) # Key order maintained as inserted, but have to select JSON in PostMan

# ----------------------------------------------------------------------------------------------------

@application.route('/keycounts')
def get_keycounts():

    keycounts = {
        'metadata': {
            'keysDistributions': {},
            'keysContents': {},
        },
        'catalogues': {},
    }

    # ----------------------------------------------------------------------------------------------------

    print('\rStep 1/5: Processing ...', end='')
    t1 = datetime.datetime.now()

    try:
        r1 = try_requests('https://openactive.io/data-catalogs/data-catalog-collection.jsonld')
    except:
        print('ERROR: Can\'t get collection of catalogues')

    t2 = datetime.datetime.now()
    print('\rStep 1/5:', t2-t1)

    # ----------------------------------------------------------------------------------------------------

    print('\rStep 2/5: Processing ...', end='')
    t1 = datetime.datetime.now()

    if (    r1.status_code == 200
        and r1.json()
        and type(r1.json()) == dict
        and 'hasPart' in r1.json().keys()
        and type(r1.json()['hasPart']) == list
    ):

        for sUrlCatalogue in r1.json()['hasPart']: # Enable to do all catalogues
        # for sUrlCatalogue in [r1.json()['hasPart'][0]]: # Enable to do only one catalogue for a test
            if (    type(sUrlCatalogue) == str
                and sUrlCatalogue not in keycounts['catalogues'].keys()
            ):
                keycounts['catalogues'][sUrlCatalogue] = {
                    'metadata': {
                        'keysDistributions': {},
                        'keysContents': {},
                    },
                    'datasets': {},
                }

    t2 = datetime.datetime.now()
    print('\rStep 2/5:', t2-t1)

    # ----------------------------------------------------------------------------------------------------

    print('\rStep 3/5: Processing ...', end='')
    t1 = datetime.datetime.now()

    for sUrlCatalogue,catalogue in keycounts['catalogues'].items():

        try:
            r2 = try_requests(sUrlCatalogue)
        except:
            print('ERROR: Can\'t get catalogue', sUrlCatalogue)
            continue

        if (    r2.status_code == 200
            and r2.json()
            and type(r2.json()) == dict
            and 'dataset' in r2.json().keys()
            and type(r2.json()['dataset']) == list
        ):

            for sUrlDataset in r2.json()['dataset']: # Enable to do all datasets
            # for sUrlDataset in [r2.json()['dataset'][0]]: # Enable to do only one dataset for a test
                if (    type(sUrlDataset) == str
                    and sUrlDataset not in catalogue['datasets'].keys()
                ):
                    catalogue['datasets'][sUrlDataset] = {
                        'metadata': {
                            'keysDistributions': {},
                            'keysContents': {},
                        },
                        'distributions': {},
                    }

    t2 = datetime.datetime.now()
    print('\rStep 3/5:', t2-t1)

    # ----------------------------------------------------------------------------------------------------

    print('\rStep 4/5: Processing ...', end='')
    t1 = datetime.datetime.now()

    for sUrlCatalogue,catalogue in keycounts['catalogues'].items():
        for sUrlDataset,dataset in catalogue['datasets'].items():

            try:
                r3 = try_requests(sUrlDataset)
            except:
                print('ERROR: Can\'t get dataset', sUrlCatalogue, '->', sUrlDataset)
                continue

            if (    r3.status_code == 200
                and r3.text
                and type(r3.text) == str
            ):

                soup = BeautifulSoup(r3.text, 'html.parser')
                distributions = []

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
                            distributions += jsonld['distribution']

                for distribution in distributions:
                    if (    type(distribution) == dict
                        and 'contentUrl' in distribution.keys()
                        and type(distribution['contentUrl']) == str
                        and distribution['contentUrl'] not in dataset['distributions'].keys()
                    ):
                        dataset['distributions'][distribution['contentUrl']] = {
                            'metadata': {
                                'keys': list(distribution.keys()),
                            },
                            'contents': {
                                'metadata': {
                                    'keys': {},
                                },
                                'names': {},
                            },
                        }
                        if (    'name' in distribution.keys()
                            and type(distribution['name']) == str
                        ):
                            dataset['distributions'][distribution['contentUrl']]['metadata']['name'] = distribution['name']

    t2 = datetime.datetime.now()
    print('\rStep 4/5:', t2-t1)

    # ----------------------------------------------------------------------------------------------------

    print('\rStep 5/5: Processing ...', end='')
    t1 = datetime.datetime.now()

    for sUrlCatalogue,catalogue in keycounts['catalogues'].items():
        for sUrlDataset,dataset in catalogue['datasets'].items():
            for sUrlDistribution,distribution in dataset['distributions'].items():

                try:
                    r4 = try_requests(sUrlDistribution)
                except:
                    print('ERROR: Can\'t get distribution', sUrlCatalogue, '->', sUrlDataset, '->', sUrlDistribution)
                    continue

                if (    r4.status_code == 200
                    and r4.json()
                    and type(r4.json()) == dict
                    and 'items' in r4.json().keys()
                    and type(r4.json()['items']) == list
                ):
                    # Each item is a different sporting activity:
                    for item in r4.json()['items']:
                        if (    type(item) == dict
                            and 'data' in item.keys()
                            and type(item['data']) == dict
                        ):
                            for key in item['data'].keys():
                                if (key not in distribution['contents']['metadata']['keys'].keys()):
                                    distribution['contents']['metadata']['keys'][key] = 1
                                else:
                                    distribution['contents']['metadata']['keys'][key] += 1
                            if (    'name' in item['data'].keys()
                                and type(item['data']['name']) == str
                            ):
                                if (item['data']['name'] not in distribution['contents']['names'].keys()):
                                    distribution['contents']['names'][item['data']['name']] = 1
                                else:
                                    distribution['contents']['names'][item['data']['name']] += 1

    t2 = datetime.datetime.now()
    print('\rStep 5/5:', t2-t1)

    # ----------------------------------------------------------------------------------------------------

    for catalogue in keycounts['catalogues'].values():
        for dataset in catalogue['datasets'].values():
            for distribution in dataset['distributions'].values():

                # ----------------------------------------------------------------------------------------------------

                for key in distribution['metadata']['keys']:
                    if (key not in dataset['metadata']['keysDistributions'].keys()):
                        dataset['metadata']['keysDistributions'][key] = 1
                    else:
                        dataset['metadata']['keysDistributions'][key] += 1

                for key,val in distribution['contents']['metadata']['keys'].items():
                    if (key not in dataset['metadata']['keysContents'].keys()):
                        dataset['metadata']['keysContents'][key] = val
                    else:
                        dataset['metadata']['keysContents'][key] += val

            # ----------------------------------------------------------------------------------------------------

            for superKey in ['keysDistributions', 'keysContents']:
                for key,val in dataset['metadata'][superKey].items():
                    if (key not in catalogue['metadata'][superKey].keys()):
                        catalogue['metadata'][superKey][key] = val
                    else:
                        catalogue['metadata'][superKey][key] += val

        # ----------------------------------------------------------------------------------------------------

        for superKey in ['keysDistributions', 'keysContents']:
            for key,val in catalogue['metadata'][superKey].items():
                if (key not in keycounts['metadata'][superKey].keys()):
                    keycounts['metadata'][superKey][key] = val
                else:
                    keycounts['metadata'][superKey][key] += val

    # ----------------------------------------------------------------------------------------------------

    return json.dumps(keycounts)

# ----------------------------------------------------------------------------------------------------

# Old:

# @application.route('/keycounts')
# def get_keycounts():
#
#     d = {
#         'metadata': {
#             'keysDistributions': {},
#             'keysContents': {},
#         },
#         'catalogues': {},
#     }
#
#     # ----------------------------------------------------------------------------------------------------
#
#     print('\rStep 1/5: Processing ...', end='')
#     t1 = datetime.datetime.now()
#
#     try:
#         r1 = try_requests('https://openactive.io/data-catalogs/data-catalog-collection.jsonld')
#     except:
#         print('ERROR: Can\'t get collection of catalogues')
#
#     t2 = datetime.datetime.now()
#     print('\rStep 1/5:', t2-t1)
#
#     # ----------------------------------------------------------------------------------------------------
#
#     print('\rStep 2/5: Processing ...', end='')
#     t1 = datetime.datetime.now()
#
#     if (    r1.status_code == 200
#         and r1.json()
#         and type(r1.json()) == dict
#         and 'hasPart' in r1.json().keys()
#         and type(r1.json()['hasPart']) == list
#     ):
#
#         # for sUrlCatalogue in r1.json()['hasPart']: # Enable to do all catalogues
#         for sUrlCatalogue in [r1.json()['hasPart'][0]]: # Enable to do only one catalogue for a test
#             if (    type(sUrlCatalogue) == str
#                 and sUrlCatalogue not in d['catalogues'].keys()
#             ):
#                 d['catalogues'][sUrlCatalogue] = {
#                     'metadata': {
#                         'keysDistributions': {},
#                         'keysContents': {},
#                     },
#                     'datasets': {},
#                 }
#
#     t2 = datetime.datetime.now()
#     print('\rStep 2/5:', t2-t1)
#
#     # ----------------------------------------------------------------------------------------------------
#
#     print('\rStep 3/5: Processing ...', end='')
#     t1 = datetime.datetime.now()
#
#     for sUrlCatalogue in d['catalogues'].keys():
#
#         try:
#             r2 = try_requests(sUrlCatalogue)
#         except:
#             print('ERROR: Can\'t get catalogue', sUrlCatalogue)
#             continue
#
#         if (    r2.status_code == 200
#             and r2.json()
#             and type(r2.json()) == dict
#             and 'dataset' in r2.json().keys()
#             and type(r2.json()['dataset']) == list
#         ):
#
#             for sUrlDataset in r2.json()['dataset']:
#                 if (    type(sUrlDataset) == str
#                     and sUrlDataset not in d['catalogues'][sUrlCatalogue]['datasets'].keys()
#                 ):
#                     d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset] = {
#                         'metadata': {
#                             'keysDistributions': {},
#                             'keysContents': {},
#                         },
#                         'distributions': {},
#                     }
#
#     t2 = datetime.datetime.now()
#     print('\rStep 3/5:', t2-t1)
#
#     # ----------------------------------------------------------------------------------------------------
#
#     print('\rStep 4/5: Processing ...', end='')
#     t1 = datetime.datetime.now()
#
#     for sUrlCatalogue in d['catalogues'].keys():
#         for sUrlDataset in d['catalogues'][sUrlCatalogue]['datasets'].keys():
#
#             try:
#                 r3 = try_requests(sUrlDataset)
#             except:
#                 print('ERROR: Can\'t get dataset', sUrlCatalogue, '->', sUrlDataset)
#                 continue
#
#             if (    r3.status_code == 200
#                 and r3.text
#                 and type(r3.text) == str
#             ):
#
#                 soup = BeautifulSoup(r3.text, 'html.parser')
#                 distributions = []
#
#                 for val in soup.head.find_all('script'):
#                     if (    'type' in val.attrs.keys()
#                         and val['type'] == 'application/ld+json'
#                     ):
#                         jsonld = json.loads(val.string)
#                         if (    type(jsonld) == dict
#                             and 'distribution' in jsonld.keys()
#                             and type(jsonld['distribution']) == list
#                         ):
#                             distributions += jsonld['distribution']
#
#                 for distribution in distributions:
#                     if (    type(distribution) == dict
#                         and 'contentUrl' in distribution.keys()
#                         and type(distribution['contentUrl']) == str
#                         and distribution['contentUrl'] not in d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'].keys()
#                     ):
#                         d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'][distribution['contentUrl']] = {
#                             'metadata': {
#                                 'keys': {},
#                             },
#                             'contents': {},
#                         }
#                         d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'][distribution['contentUrl']]['metadata']['keys'] = list(distribution.keys())
#                         if (    'name' in distribution.keys()
#                             and type(distribution['name']) == str
#                         ):
#                             d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'][distribution['contentUrl']]['metadata']['name'] = distribution['name']
#                         d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'][distribution['contentUrl']]['contents'] = {
#                             'metadata': {
#                                 'keys': {},
#                             },
#                             'names': {},
#                         }
#
#     t2 = datetime.datetime.now()
#     print('\rStep 4/5:', t2-t1)
#
#     # ----------------------------------------------------------------------------------------------------
#
#     print('\rStep 5/5: Processing ...', end='')
#     t1 = datetime.datetime.now()
#
#     for sUrlCatalogue in d['catalogues'].keys():
#         for sUrlDataset in d['catalogues'][sUrlCatalogue]['datasets'].keys():
#             for sUrlDistribution in d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'].keys():
#
#                 try:
#                     r4 = try_requests(sUrlDistribution)
#                 except:
#                     print('ERROR: Can\'t get distribution', sUrlCatalogue, '->', sUrlDataset, '->', sUrlDistribution)
#                     continue
#
#                 if (    r4.status_code == 200
#                     and r4.json()
#                     and type(r4.json()) == dict
#                     and 'items' in r4.json().keys()
#                     and type(r4.json()['items']) == list
#                 ):
#                     for item in r4.json()['items']:
#                         if (    type(item) == dict
#                             and 'data' in item.keys()
#                             and type(item['data']) == dict
#                         ):
#                             for key in item['data'].keys():
#                                 if (key not in d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'][sUrlDistribution]['contents']['metadata']['keys'].keys()):
#                                     d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'][sUrlDistribution]['contents']['metadata']['keys'][key] = 1
#                                 else:
#                                     d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'][sUrlDistribution]['contents']['metadata']['keys'][key] += 1
#                             if (    'name' in item['data'].keys()
#                                 and type(item['data']['name']) == str
#                             ):
#                                 if (item['data']['name'] not in d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'][sUrlDistribution]['contents']['names'].keys()):
#                                     d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'][sUrlDistribution]['contents']['names'][item['data']['name']] = 1
#                                 else:
#                                     d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'][sUrlDistribution]['contents']['names'][item['data']['name']] += 1
#
#     t2 = datetime.datetime.now()
#     print('\rStep 5/5:', t2-t1)
#
#     # ----------------------------------------------------------------------------------------------------
#
#     for sUrlCatalogue in d['catalogues'].keys():
#         for sUrlDataset in d['catalogues'][sUrlCatalogue]['datasets'].keys():
#             for sUrlDistribution in d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'].keys():
#
#                 for key in d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'][sUrlDistribution]['metadata']['keys']:
#
#                     if (key not in d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['metadata']['keysDistributions'].keys()):
#                         d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['metadata']['keysDistributions'][key] = 1
#                     else:
#                         d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['metadata']['keysDistributions'][key] += 1
#
#                     if (key not in d['catalogues'][sUrlCatalogue]['metadata']['keysDistributions'].keys()):
#                         d['catalogues'][sUrlCatalogue]['metadata']['keysDistributions'][key] = d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['metadata']['keysDistributions'][key]
#                     else:
#                         d['catalogues'][sUrlCatalogue]['metadata']['keysDistributions'][key] += d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['metadata']['keysDistributions'][key]
#
#                     if (key not in d['metadata']['keysDistributions'].keys()):
#                         d['metadata']['keysDistributions'][key] = d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['metadata']['keysDistributions'][key]
#                     else:
#                         d['metadata']['keysDistributions'][key] += d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['metadata']['keysDistributions'][key]
#
#                 for key in d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'][sUrlDistribution]['contents']['metadata']['keys'].keys():
#
#                     if (key not in d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['metadata']['keysContents'].keys()):
#                         d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['metadata']['keysContents'][key] = d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'][sUrlDistribution]['contents']['metadata']['keys'][key]
#                     else:
#                         d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['metadata']['keysContents'][key] += d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['distributions'][sUrlDistribution]['contents']['metadata']['keys'][key]
#
#                     if (key not in d['catalogues'][sUrlCatalogue]['metadata']['keysContents'].keys()):
#                         d['catalogues'][sUrlCatalogue]['metadata']['keysContents'][key] = d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['metadata']['keysContents'][key]
#                     else:
#                         d['catalogues'][sUrlCatalogue]['metadata']['keysContents'][key] += d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['metadata']['keysContents'][key]
#
#                     if (key not in d['metadata']['keysContents'].keys()):
#                         d['metadata']['keysContents'][key] = d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['metadata']['keysContents'][key]
#                     else:
#                         d['metadata']['keysContents'][key] += d['catalogues'][sUrlCatalogue]['datasets'][sUrlDataset]['metadata']['keysContents'][key]
#
#     # ----------------------------------------------------------------------------------------------------
#
#     return json.dumps(d)

# ----------------------------------------------------------------------------------------------------

if (__name__ == '__main__'):
    application.run()
