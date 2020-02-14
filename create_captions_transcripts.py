#!/usr/bin/python3

import csv
import json
import logging
import sys
import traceback
import yaml

import requests
from tqdm import tqdm

'''
1.) For each archival object URI, create 2 child records, 1 entitled 'Caption' and 1 entitled 'Transcript'
2.) Create 2 digital object records - one for the transcript and one for the caption
3.) Link archival object and digital object records via an instance subrecord
'''

#Open a CSV in dictreader mode
def opencsvdict(input_csv=None):
    """Opens a CSV in DictReader mode."""
    try:
        if input_csv is None:
            input_csv = input('Please enter path to CSV: ')
        if input_csv == 'quit':
            quit()
        file = open(input_csv, 'r', encoding='utf-8')
        csvin = csv.DictReader(file)
        return csvin
    except:
        logging.exception('Error: ')
        logging.debug('Trying again...')
        print('CSV not found. Please try again. Enter "quit" to exit')
        c = opencsvdict()
        return c


def error_log(filepath=None):
    """Initiates an error log."""
    if sys.platform == "win32":
        if filepath == None:
            logger = '\\Windows\\Temp\\error_log.log'
        else:
            logger = filepath
    else:
        if filepath == None:
            logger = '/tmp/error_log.log'
        else:
            logger = filepath
    logging.basicConfig(filename=logger, level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(name)s %(message)s')
    return logger


def get_config(cfg=None):
    """Gets a config file"""
    if cfg != None:
        if type(cfg) is str:
            cfg_file = yaml.load(open(cfg, 'r', encoding='utf-8'), Loader=yaml.FullLoader)
    else:
        cfg_file = yaml.load(open('config.yml', 'r', encoding='utf-8'), Loader=yaml.FullLoader)
        return cfg_file
    return cfg_file


def login(url=None, username=None, password=None):
    """Logs into the ArchivesSpace API"""
    try:
        if url is None and username is None and password is None:
            url = input('Please enter the ArchivesSpace API URL: ')
            username = input('Please enter your username: ')
            password = input('Please enter your password: ')
        auth = requests.post(url+'/users/'+username+'/login?password='+password).json()
        #if session object is returned then login was successful; if not it failed.
        if 'session' in auth:
            session = auth["session"]
            h = {'X-ArchivesSpace-Session':session, 'Content_Type': 'application/json'}
            print('Login successful!')
            logging.debug('Success!')
            return (url, h)
        else:
            print('Login failed! Check credentials and try again.')
            logging.debug('Login failed')
            logging.debug(auth.get('error'))
            #try again
            u, heads = login()
            return u, heads
    except:
        print('Login failed! Check credentials and try again!')
        logging.exception('Error: ')
        u, heads = login()
        return u, heads


def new_ao(title, parent_uri, resource_uri, repo_uri):
    return {'jsonmodel_type': 'archival_object', 'title': title, 
                'level': 'file', 'publish': True,
                'parent': {'ref': parent_uri},
                'resource': {'ref': resource_uri},
                'repository': {'ref': repo_uri}}


def new_do(do_id, do_title, do_file_version):
    return {'jsonmodel_type': 'digital_object', 'publish': False,
    'title': do_title, 'digital_object_id': do_id,
    'file_versions': [{'jsonmodel_type': 'file_version', 'file_uri': do_file_version, 'publish': False, 'xlink_show_attribute': 'new'}]}


def new_instance(api_url, headers, new_ao_uri, new_do_uri):
    record_json = requests.get(api_url + new_ao_uri, headers=headers).json()
    new_ao_instance = {'jsonmodel_type': 'instance', 'instance_type': 'digital_object', 'digital_object': {'ref': new_do_uri}}
    record_json['instances'].append(new_ao_instance)
    record_post = requests.post(api_url + new_ao_uri, headers=headers, json=record_json).json()
    #print(record_post)


def create_objects(api_url, headers, title, parent_uri, resource_uri, repo_uri, do_id, do_title, do_file_version):
    new_archival_object = new_ao(title, parent_uri, resource_uri, repo_uri)
    new_ao_post = requests.post(api_url + repo_uri + '/archival_objects', headers=headers, json=new_archival_object).json()
    #print(new_ao_post)
    if 'status' in new_ao_post:
        new_ao_uri = new_ao_post['uri']
        new_digital_object = new_do(do_id, do_title, do_file_version)
        new_do_post = requests.post(api_url + repo_uri + '/digital_objects', headers=headers, json=new_digital_object).json()
        #print(new_do_post)
        if 'status' in new_do_post:
            new_do_uri = new_do_post['uri']
            new_instance(api_url, headers, new_ao_uri, new_do_uri)
        elif 'error' in new_do_post:
            logging.debug(f"{parent_uri}: {new_do_post.get('error')}")
            #print(f"{parent_uri}: {new_do_post.get('error')}")
    elif 'error' in new_ao_post:
        logging.debug(f"{parent_uri}: {new_do_post.get('error')}")
        #print(f"{parent_uri}: {new_do_post.get('error')}")


def loop_and_create(api_url, headers, csv_data, row_count):
    with tqdm(total=row_count) as pbar:
        for csv_row in csv_data:
            pbar.update(1)
            try:
                if (csv_row['DigitalObjectIdentifier-Caption'] != '' 
                    and csv_row['DigitalObjectTitle-Caption'] != '' 
                    and csv_row['DigitalObjectFileVersionFileURI-Caption'] != '' 
                    and csv_row['DigitalObjectIdentifier-Transcript'] != '' 
                    and csv_row['DigitalObjectTitle-Transcript'] != '' 
                    and csv_row['DigitalObjectFileVersionFileURI-Transcript'] != ''):
                    caption_data = create_objects(api_url, headers, 'Caption', csv_row['parent_uri'], csv_row['resource_uri'], csv_row['repo_uri'], csv_row['DigitalObjectIdentifier-Caption'],
                        csv_row['DigitalObjectTitle-Caption'], csv_row['DigitalObjectFileVersionFileURI-Caption'])
                    transcript_data = create_objects(api_url, headers, 'Transcript', csv_row['parent_uri'], csv_row['resource_uri'], csv_row['repo_uri'], csv_row['DigitalObjectIdentifier-Transcript'], csv_row['DigitalObjectTitle-Transcript'], csv_row['DigitalObjectFileVersionFileURI-Transcript'])
                else:
                    logging.debug(f"MISSING DATA: {csv_row['parent_uri']}")
                    #print(f"MISSING DATA: {csv_row['parent_uri']}")
            except Exception:
                logging.debug(csv_row['parent_uri'])
                logging.debug(traceback.format_exc())
                #print(csv_row['parent_uri'])
                #print(traceback.format_exc())
                continue


def main():
    error_log(filepath='log.log')
    cnfg = get_config(cfg='config.yml')
    print(cnfg)
    csv_data = opencsvdict(input_csv=cnfg['input_csv'])
    api_url, headers = login(url=cnfg['api_url'], username=cnfg['api_username'], password=cnfg['api_password'])
    row_count = sum(1 for line in open(cnfg['input_csv']).readlines()) - 1
    confirm = input(f"""Enter 'Y' to confirm that your inputs are correct
              and run the update. Enter any key to quit:
                ArchivesSpace instance: {api_url}
                CSV input file: {cnfg['input_csv']}: \n\n
                Y?: """)
    if confirm == 'Y':
        loop_and_create(api_url, headers, csv_data, row_count)
    else:
        print('Aborted.')


if __name__ == "__main__":
    main()