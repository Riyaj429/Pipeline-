import json
import pandas as pd
import geopandas as gpd
from pipeline_code3 import create_pipeline

with open('config.json') as f:
    config = json.load(f)

pipeline = create_pipeline(config)
# To add new columns
for i, file in enumerate(config['file_columns']):
    df = pd.read_json(file)
    latitude1_col = config['latitude1_cols'][i]
    longitude1_col = config['longitude1_cols'][i]
    if latitude1_col in df.columns:
        pipeline[latitude1_col] = df[latitude1_col]
    else:
        pipeline[latitude1_col] = None
    if longitude1_col in df.columns:
        pipeline[longitude1_col] = df[longitude1_col]
    else:
        pipeline[longitude1_col] = None


# To delete the column
# config.pop('longitude1', None)
# updated_pipeline = create_pipeline(config)

# print(updated_pipeline.head())


# To Rename the columnn
# pipeline = pipeline.rename(columns={'Latitude': 'Lat'})

# print(pipeline.head())

print(pipeline.head())


print(pipeline.columns)


# To run the code without changing anything
# import pandas as pd
# import geopandas as gpd
# import json
# from pipeline_code3 import create_pipeline

# with open('config.json') as f:
#     config.json.load(f)

# pipeline = create_pipeline(config)

# print(pipeline.head())
