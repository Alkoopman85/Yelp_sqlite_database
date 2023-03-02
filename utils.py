"""
database utility functions
"""
import yaml
import json
from pathlib import Path
from typing import List



def load_config(file_path:str='config.yaml', category:str|None=None) -> dict|None:
    """Loads the config.yaml file

    Args:
        file_path (str, optional): File path to config file. 
                                   Defaults to 'config.yaml'.
        category (str | None, optional): A subcategory to load from the config file. 
                                         Defaults to None.

    Returns:
        dict | None: config dictionary object or None if the category 
                     is not None and not found in the config object
    """
    with open(file_path, 'r') as config_file:
        config_obj = yaml.safe_load(config_file)
    if category is None:
        return config_obj
    
    try:
        return config_obj[category]
    except KeyError:
        print(f'{category} not found in config')
        return None


def flatten_dict(attribute_dict:dict) -> dict:
    """flattens a dictionarry with max depth of 2.

    Args:
        attribute_dict (dict): The dict to flatten

    Returns:
        dict: flattened dict, all values returned as strings

    Example:
        input_dict = {
            'entry1': 1,
            'entry2': {
                'sub_entry1': 'second',
                'sub_entry2': 'third'
            }
        }
        
        output_dict = flatten_dict(input_dict)
        
        print(output_dict)
        
        {
            'entry1': '1',
            'entry2_sub_entry1': 'second',
            'entry2_sub_entry2': 'third'
        }
    """
    if attribute_dict is None:
        return None
    attributes = {}
    for key, value in attribute_dict.items():
        value = eval(value)
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                attributes[key + '_' + sub_key] = str(sub_value)
        else:
            attributes[key] = str(value)
    return attributes

def file_line_generator(file_name:str|Path, file_type:str='json'):
    """loads a file line by line

    Args:
        file_name (str | Path): path to file
        file_type (str, optional): 'json' or 'txt'. Defaults to 'json'.

    Yields:
        dict | str: yields dict from json.loads(line) if file_type is 'json' 
                    otherwise it strips the new line character and 
                    returns the line as a string
    """

    with open(file_name, 'r') as user_file:
        while True:
            line = user_file.readline()
            if line:
                if file_type == 'json':
                    yield json.loads(line)
                else:
                    yield line.rstrip('\n').strip()
            else:
                break
