# mimic_iv_etl/data_insights.py
import os
import yaml
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, clear_output, Markdown


def get_dataset_description(file_path):
    with open(file_path, 'r') as file:
        description = yaml.safe_load(file)
    
    dataset_description = description.get('dataset_description', 'No description available.')
    schema_df = pd.DataFrame(description['schema']).T
    schema_df['field'] = schema_df.index
    schema_df.reset_index(drop=True, inplace=True)
    schema_df = schema_df[['field', 'type', 'description']]
    
    return dataset_description, schema_df


def display_datasets():
    description_path = '../data/descriptions'
    subdirs = ['hosp', 'icu']
    pd.set_option('display.max_colwidth', None)
    
    datasets = {}
    for subdir in subdirs:
        subdir_path = os.path.join(description_path, subdir)
        for file in os.listdir(subdir_path):
            if file.endswith('.yml'):
                file_name_without_ext = os.path.splitext(file)[0]
                full_path = os.path.join(subdir_path, file)
                datasets[f'{subdir}.{file_name_without_ext}'] = full_path

    dropdown = widgets.Dropdown(options=list(datasets.keys()), description='Dataset:')
    output = widgets.Output()

    def on_change(change):
        if change['type'] == 'change' and change['name'] == 'value':
            with output:
                clear_output(wait=True)
                file_path = datasets[change['new']]
                description_text, df = get_dataset_description(file_path)
                display(Markdown(f"**Description:** {description_text}"))
                display(df)

    dropdown.observe(on_change)
    display(dropdown, output)