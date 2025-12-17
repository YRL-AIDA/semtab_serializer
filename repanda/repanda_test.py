from datasets import load_from_disk
import pandas as pd

# Загружаем датасеты
datasets = {
    'all': pd.DataFrame(load_from_disk('none_filtered_new_dataset2')),
    'train': pd.DataFrame(load_from_disk('tab_fact_train')),
    'val': pd.DataFrame(load_from_disk('tab_fact_val')),
    'test': pd.DataFrame(load_from_disk('tab_fact_test'))
}

# Удаляем дубликаты
unique_ds = {k: v.drop_duplicates(subset=['table_text', 'statement']) for k, v in datasets.items()}

# Размеры
print("Размеры уникальных датасетов:")
for name, ds in unique_ds.items():
    print(f"{name}: {len(ds)}")

# Пересечения
intersections = {}
for name in ['train', 'val', 'test']:
    intersections[f'all_{name}'] = len(pd.merge(
        unique_ds['all'], unique_ds[name],
        on=['table_text', 'statement'],
        how='inner'
    ))

# Пересечения между train, val, test
pairs = [('train', 'val'), ('train', 'test'), ('test', 'val')]
for ds1, ds2 in pairs:
    intersections[f'{ds1}_{ds2}'] = len(pd.merge(
        unique_ds[ds1], unique_ds[ds2],
        on=['table_text', 'statement'],
        how='inner'
    ))

print("\nПересечения:")
for key, value in intersections.items():
    print(f"{key}: {value}")