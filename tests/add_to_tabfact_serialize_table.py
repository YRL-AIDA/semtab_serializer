import sys
import os
from datasets import load_from_disk

# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.dirname(os.path.abspath('/media/research/yrl_aida_users/poddubny/poddubnyy/postgraduate/semtab_serializer/tests')))
from datasets import Dataset
import json
import pandas as pd
from io import StringIO
from tqdm import tqdm
from utils.utils import serialize_table, load_config
from utils.doduo.doduo import Doduo
from utils.cossim.cossim import Qwen3EmbeddingMatcher
from utils.defgen.defgen import ColumnDefGenerator
import argparse
from functools import partial
import torch

            
def add_to_dataset_table_serialization(data,name=None,include_description=False,**kwargs):
    
    
    df = pd.read_csv(StringIO(data['table_text']), delimiter='#')
    try:
        if include_description:
            kwargs['description'] = data['table_caption']
            #print(type(data))
        kwargs['add_data'] = data
        data[name] = serialize_table(df,**kwargs)
    #try:
     #   data['semtab_query'] = data['statement'] + ' ' + serialize_table(df,model=model, description=data['table_caption'],basedir='../utils/doduo/')
    except Exception as e:
        print ('ERRORRRRRRRRRRRRRRRRRRRRRRRRR', e)
        data[name] = 'None'
    return data



def main():
    parser = argparse.ArgumentParser(description='Скрипт с настраиваемыми параметрами')
    parser.add_argument('--inputdata', type=str, default='./tab_fact_test')
    parser.add_argument('--outputdata', type=str, default='./tab_fact_test_xml')
    parser.add_argument('--conf-file', type=str, default='./convert_conf.yaml')
    parser.add_argument('--num_proc', type=int, default=1, 
                      help='Количество процессов')
    
    args = parser.parse_args()
    
    inputdata = args.inputdata
    outputdata = args.outputdata
    num_proc = args.num_proc 
    config_data = load_config(args.conf_file)
    dataset = load_from_disk(inputdata)
    print(f"inputdata: {inputdata}")
    print(f"outputdata: {outputdata}")
    for config_name in config_data.keys():
        config = config_data[config_name]
        print(config_name)
        print(f"num-proc: {num_proc}")
        print(config_data)
        model = None
        num_proc = config.get('num_proc',None) 
        if config['serialization_type']=='semtab':
            if config['include_semantic_types']:
                if config['semantic_method'] == 'Doduo':
                    model = Doduo(argparse.Namespace(**{'model': config['model_type'], 'device': config['device']}),basedir=config['model_base_dir'])#'../utils/doduo/')
                
        # Ваш основной код здесь
                    config['model'] = model
                elif config['semantic_method'] == 'CosSim':
                    model = Qwen3EmbeddingMatcher(**config)
                    config['model'] = model
                elif config['semantic_method'] == 'DefGen':
                    model = ColumnDefGenerator()
                    config['model'] = model
                num_proc = None
        
        add_to_dataset_table_serialization_partial = partial(add_to_dataset_table_serialization,name=config_name,**config)
    
        
        dataset2 = dataset.map(add_to_dataset_table_serialization_partial,num_proc=num_proc)
        dataset2.save_to_disk(outputdata)
        dataset = dataset2
        del model
        del config
        with torch.no_grad():
            torch.cuda.empty_cache()


if __name__ == "__main__":
    main()          