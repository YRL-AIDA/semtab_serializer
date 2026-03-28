import pandas as pd
import json

from typing import List, Dict, Tuple, Optional
import re
class ColumnDefGenerator:
    """
    
    """
    
    def __init__(
        self,
    ):
        """
        Args:
            
        """
        pass


    def _get_table_columns_defenitions(self,table_name: str):
        if table_name == None:
            return None
        table_def = pd.read_parquet("/home/poddubny/notebooks/poddubnyy/postgraduate/semtab_serializer/utils/defgen/df_pantabfact_train.parquet.gzip")
        return json.loads(table_def[table_def['table_csv']==table_name]['column_defenition_json'].to_list()[0])
        
    def annotate_columns(self, df: pd.DataFrame, top_k: int= 1, threshold: float = 0.5, add_data = None)-> List[List[Tuple[str,float]]]:
        if add_data != None:
            col_def = self._get_table_columns_defenitions(add_data.get('table_csv', None))
            
            # Создаем новый словарь с нормализованными ключами
            normalized_col_def = {}
            for key, value in col_def.items():
                normalized_key = re.sub(' ', '', key).lower()
                normalized_col_def[normalized_key] = value
                # Сохраняем и оригинальный ключ, если нужно
                normalized_col_def[key] = value
            
            return [[(normalized_col_def.get(re.sub(' ', '', col_name).lower(), ''), None)]
                   for col_name in df.columns]
        else:
            #print('!'*50)
            return [[(None, None)]
                   for col_name in df.columns]



