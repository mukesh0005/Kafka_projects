import requests
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import json

load_dotenv()

client_key = os.getenv('client_key')
secret_key = os.getenv('secret_key')


data = {
    'grant_type': 'password',
    'username' : os.getenv('use_name'),
'password' : os.getenv('password')
}
auth = requests.auth.HTTPBasicAuth(client_key,secret_key)
headers = {'User-Agent':'Batman/0.0.1'}
res = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)
TOKEN = res.json()['access_token']
headers['Authorization'] = f'bearer {TOKEN}'



