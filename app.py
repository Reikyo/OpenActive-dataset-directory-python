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

    output = []

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

        # for sUrlCatalogue in r1.json()['hasPart']: # Enable to do all catalogues
        for sUrlCatalogue in [r1.json()['hasPart'][0]]: # Enable to do only one catalogue for a test
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

                    # for sUrlDataset in r2.json()['dataset']: # Enable to do all datasets
                    for sUrlDataset in [r2.json()['dataset'][0]]: # Enable to do only one dataset for a test
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

                                for val in soup.head.find_all('script'):
                                    if (    'type' in val.attrs.keys()
                                        and val['type'] == 'application/ld+json'
                                    ):

                                        jsonld = json.loads(val.string)

                                        # ----------------------------------------------------------------------------------------------------

                                        if (    type(jsonld) == dict
                                            and 'distribution' in jsonld.keys()
                                            and type(jsonld['distribution']) == list
                                        ):
                                            distributions = jsonld['distribution']
                                        else:
                                            distributions = []

                                        for distribution in distributions:
                                            if (type(distribution) == dict):

                                                # First attempt, no protection from missing fields:
                                                # output.append({
                                                #     'urlCatalogue': r2.json()['id'], # Should match sUrlCatalogue from r1.json()['hasPart']
                                                #     'namePublisherCatalogue': r2.json()['publisher']['name'], # The catalogue publisher name is the closest we have to a catalogue name proper
                                                #
                                                #     'urlDataset': jsonld['url'], # Should match sUrlDataset from r2.json()['dataset']
                                                #     'namePublisherDataset': jsonld['publisher']['name'], # We also have jsonld['name'], but here using the publisher name for consistency with namePublisherCatalogue
                                                #
                                                #     'urlContent': distribution['contentUrl'],
                                                #     'nameContent': distribution['name'],
                                                #
                                                #     'urlDiscussion': jsonld['discussionUrl'],
                                                #     'urlLicense': jsonld['license'],
                                                # })

                                                # Second attempt, a bit long-winded:
                                                # content = {}
                                                # try:
                                                #     content['urlCatalogue'] = r2.json()['id'] # Should match sUrlCatalogue from r1.json()['hasPart']
                                                # except:
                                                #     content['urlCatalogue'] = ''
                                                # try:
                                                #     content['namePublisherCatalogue'] = r2.json()['publisher']['name'] # The catalogue publisher name is the closest we have to a catalogue name proper
                                                # except:
                                                #     content['namePublisherCatalogue'] = ''
                                                # try:
                                                #     content['urlDataset'] = jsonld['url'] # Should match sUrlDataset from r2.json()['dataset']
                                                # except:
                                                #     content['urlDataset'] = ''
                                                # try:
                                                #     content['nameDataset'] = jsonld['name']
                                                # except:
                                                #     content['nameDataset'] = ''
                                                # try:
                                                #     content['namePublisherDataset'] = jsonld['publisher']['name']
                                                # except:
                                                #     content['namePublisherDataset'] = ''
                                                # try:
                                                #     content['urlContent'] = distribution['contentUrl']
                                                # except:
                                                #     content['urlContent'] = ''
                                                # try:
                                                #     content['nameContent'] = distribution['name']
                                                # except:
                                                #     content['nameContent'] = ''
                                                # try:
                                                #     content['urlDiscussion'] = jsonld['discussionUrl']
                                                # except:
                                                #     content['urlDiscussion'] = ''
                                                # try:
                                                #     content['urlLicense'] = jsonld['license']
                                                # except:
                                                #     content['urlLicense'] = ''
                                                # output.append(content)

                                                content = {}
                                                # Should match sUrlCatalogue from r1.json()['hasPart']:
                                                content['urlCatalogue'] = r2.json()['id'] if 'id' in r2.json().keys() else ''
                                                # The catalogue publisher name is the closest we have to a catalogue name proper:
                                                content['namePublisherCatalogue'] = r2.json()['publisher']['name'] if 'publisher' in r2.json().keys() and 'name' in r2.json()['publisher'].keys() else ''
                                                # Should match sUrlDataset from r2.json()['dataset']:
                                                content['urlDataset'] = jsonld['url'] if 'url' in jsonld.keys() else ''
                                                content['nameDataset'] = jsonld['name'] if 'name' in jsonld.keys() else ''
                                                content['namePublisherDataset'] = jsonld['publisher']['name'] if 'publisher' in jsonld.keys() and 'name' in jsonld['publisher'].keys() else ''
                                                content['urlContent'] = distribution['contentUrl'] if 'contentUrl' in distribution.keys() else ''
                                                content['nameContent'] = distribution['name'] if 'name' in distribution.keys() else ''
                                                content['urlDiscussion'] = jsonld['discussionUrl'] if 'discussionUrl' in jsonld.keys() else ''
                                                content['urlLicense'] = jsonld['license'] if 'license' in jsonld.keys() else ''
                                                output.append(content)

    # ----------------------------------------------------------------------------------------------------

    # return output # Key order made alphabetical
    # return jsonify(output) # Key order made alphabetical
    return json.dumps(output) # Key order maintained as inserted, but have to select JSON in PostMan

# ----------------------------------------------------------------------------------------------------

@application.route('/keycounts')
def get_keycounts():

    output = {
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
                and sUrlCatalogue not in output['catalogues'].keys()
            ):
                output['catalogues'][sUrlCatalogue] = {
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

    for sUrlCatalogue,catalogue in output['catalogues'].items():

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

    for sUrlCatalogue,catalogue in output['catalogues'].items():
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

    for sUrlCatalogue,catalogue in output['catalogues'].items():
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

    for catalogue in output['catalogues'].values():
        for dataset in catalogue['datasets'].values():
            for distribution in dataset['distributions'].values():

                for key in distribution['metadata']['keys']:

                    if (key not in dataset['metadata']['keysDistributions'].keys()):
                        dataset['metadata']['keysDistributions'][key] = 1
                    else:
                        dataset['metadata']['keysDistributions'][key] += 1

                    if (key not in catalogue['metadata']['keysDistributions'].keys()):
                        catalogue['metadata']['keysDistributions'][key] = dataset['metadata']['keysDistributions'][key]
                    else:
                        catalogue['metadata']['keysDistributions'][key] += dataset['metadata']['keysDistributions'][key]

                    if (key not in output['metadata']['keysDistributions'].keys()):
                        output['metadata']['keysDistributions'][key] = dataset['metadata']['keysDistributions'][key]
                    else:
                        output['metadata']['keysDistributions'][key] += dataset['metadata']['keysDistributions'][key]

                for key in distribution['contents']['metadata']['keys'].keys():

                    if (key not in dataset['metadata']['keysContents'].keys()):
                        dataset['metadata']['keysContents'][key] = distribution['contents']['metadata']['keys'][key]
                    else:
                        dataset['metadata']['keysContents'][key] += distribution['contents']['metadata']['keys'][key]

                    if (key not in catalogue['metadata']['keysContents'].keys()):
                        catalogue['metadata']['keysContents'][key] = dataset['metadata']['keysContents'][key]
                    else:
                        catalogue['metadata']['keysContents'][key] += dataset['metadata']['keysContents'][key]

                    if (key not in output['metadata']['keysContents'].keys()):
                        output['metadata']['keysContents'][key] = dataset['metadata']['keysContents'][key]
                    else:
                        output['metadata']['keysContents'][key] += dataset['metadata']['keysContents'][key]

    # ----------------------------------------------------------------------------------------------------

    return json.dumps(output)

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
