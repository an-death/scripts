#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import unicode_literals

import httplib2
import os
import json
import codecs
import argparse


from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage


flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()


SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'monitoringTableParcer'


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        credentials = tools.run_flow(flow, store, flags)
    return credentials


def main():
    d = {}
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)

    spreadsheetId = '1TtPEa9F4Hlw1gb6LHIaKkILOCFdFtAZj5C7YBdyWS4M'
    rangeHeadTable = 'A3:S35'
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range=rangeHeadTable).execute()
    values = result.get('values', [])
    if not values:
        print('No data found.')
    else:

        for row in values:
            row_len_max = 19
            while len(row) < row_len_max:
                row.append(None)
            mysql_rows = {"port on gate": row[7], "login": row[8], "password": row[9], "database": row[10]}
            table_dict = {"Сервак": row[0],
                          "network": row[1],
                          "name": row[2],
                          "vpn": row[3],
                          "dns name/server/cred": row[4],
                          "auth": row[5],
                          "URL": row[6],
                          "mysql": mysql_rows,
                          "monitoring": row[11],
                          "v": row[12],
                          "adminka": row[13],
                          "witsml": row[14],
                          "клиентский dashboard": row[15],
                          "GBOX ssh": row[16],
                          "reporting": row[17],
                          "Itillium": row[18]
                          }
            d[table_dict['name']] = table_dict

    with codecs.open("monitoring_data.json", 'w', encoding='utf-8') as fd:
        fd.write(json.dumps(d, ensure_ascii=False))

if __name__ == '__main__':
    main()
