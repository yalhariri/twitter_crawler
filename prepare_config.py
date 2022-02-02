# -*- coding: utf-8 -*-
"""
Created on Wed Feb  2 16:50:13 2022

@author: yalha
"""



import yaml

documents = {'name': 'TwitterV2',
 'BEARER_TOKEN': '',
 'OUTPUT_FOLDER': './../data_sample',
 'START_DATE': '2022/1/20',
 'END_DATE': '2022/2/1',
 'WAIT_TIME': 10,
 'LOG': './../.logs',
 'QUERY': '#kindness'}

import os

output_folder = "../.config/"

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

with open(output_folder + ".config_proj1.yml", 'w') as file:
    documents = yaml.dump(documents, file)