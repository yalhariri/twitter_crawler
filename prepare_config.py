# -*- coding: utf-8 -*-
"""
Created on Wed Feb  2 16:50:13 2022

@author: yalha
"""



import yaml


with open(r'.config_kind.yml') as file:
    documents = yaml.full_load(file)

    for item, doc in documents.items():
        print(item, ":", doc)

output_folder = "../.config"


import os

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

with open(output_folder + ".config_proj1.yml", 'w') as file:
    documents = yaml.dump(documents, file)