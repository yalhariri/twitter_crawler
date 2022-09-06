# -*- coding: utf-8 -*-
"""
Created on Wed Feb  2 16:50:13 2022

@author: y.alhariri@outlook.com
"""

import yaml

documents = {'name': 'TwitterV2',
 'BEARER_TOKEN': '',
 'OUTPUT_FOLDER': './../data_sample22/',
 'START_DATE': '2022/1/20',
 'END_DATE': '2022/2/1',
 'WAIT_TIME': 10,
 'LOG': './../.logs',
 'QUERY': 'q1, #q1',
 'EXTRACT_DATA_INPUT': './../data_sample22/',
 'EXTRACT_DATA_OUTPUT': './../data_sample22/data/',
 'next_token': True
 }

import os

output_folder = "../.config/"

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

with open(output_folder + ".configs.yml", 'w') as file:
    yaml.dump(documents, file)