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
from utils.utils import serialize_table
from utils.doduo.doduo import Doduo
import argparse
from functools import partial

def convert_dataset_to_nlsep_format(model,data):
    df = pd.read_csv(StringIO(data['table_text']), delimiter='#')
    data['nlsep_query'] = data['statement'] + ' ' +  serialize_table_to_tapex_format(df)
    try:
        data['semtab_query'] = data['statement'] + ' ' + serialize_table(df,model=model, description=data['table_caption'],basedir='../utils/doduo/')
    except Exception as e:
        print ('ERRORRRRRRRRRRRRRRRRRRRRRRRRR', e)
        data['semtab_query'] = 'None'
    return data

def serialize_table_to_tapex_format(df):
    head_pattern = " col : "
    row_pattern = " row {num} : "
    coll_delimetr = " | "
    
    lin_table = head_pattern+coll_delimetr.join(df.columns)
    for i,row in df.iterrows():
        #print(row_pattern.format(num=i+1))
        lin_table+=row_pattern.format(num=i+1)+coll_delimetr.join(str(r) for r in row.values)
    
    return lin_table

def main():
    parser = argparse.ArgumentParser(description='Скрипт с настраиваемыми параметрами')
    parser.add_argument('--inputdata', type=str, default='./tab_fact_test')
    parser.add_argument('--outputdata', type=str, default='./tab_fact_test_xml')
    #parser.add_argument('--num_proc', type=int, default=2, 
    #                  help='Количество процессов')
    
    args = parser.parse_args()
    
    inputdata = args.inputdata
    outputdata = args.outputdata
    #NUM_PROC = args.num_proc 
    model_type = "viznet"
    device = 'cuda'
    print(f"inputdata: {inputdata}")
    print(f"outputdata: {outputdata}")
    #print(f"num-proc: {NUM_PROC}")
    model = Doduo(argparse.Namespace(**{'model': model_type, 'device': device}),basedir='../utils/doduo/')
    # Ваш основной код здесь
    doduo_convert_dataset_to_nlsep_format = partial(convert_dataset_to_nlsep_format,model)

    dataset = load_from_disk(inputdata)
    dataset2 = dataset.map(doduo_convert_dataset_to_nlsep_format)
    dataset2.save_to_disk(outputdata)


if __name__ == "__main__":
    main()          