import pandas as pd
import geopandas as gpd

def create_pipeline(config):
    file_columns = config['file_columns']
    state_cols = config['state_cols']
    district_cols = config['district_cols']
    country_cols = config['country_cols']
    type_cols = config['type_cols']
    latitude_cols = config['latitude_cols']
    longitude_cols = config['longitude_cols']

    def create_pipeline_inner(file_columns, state_cols, district_cols, country_cols, type_cols, latitude_cols, longitude_cols):
        State = []
        District = []
        Country = []
        Type = []
        Latitude = []
        Longitude = []
        for i, file in enumerate(file_columns):
            df = pd.read_json(file)

            if state_cols[i] in df.columns:
                State.append(df[state_cols[i]])
            else:
                State.append(None)

            if district_cols[i] in df.columns:
                District.append(df[district_cols[i]])
            else:
                District.append(None)

            if country_cols[i] in df.columns:
                Country.append(df[country_cols[i]])
            else:
                Country.append(None)

            if type_cols[i] in df.columns:
                Type.append(df[type_cols[i]])
            else:
                Type.append(None)

            if latitude_cols[i] in df.columns:
                Latitude.append(df[latitude_cols[i]])
            else:
                Latitude.append(None)

            if longitude_cols[i] in df.columns:
                Longitude.append(df[longitude_cols[i]])
            else:
                Longitude.append(None)
        merged_gdf = pd.DataFrame({
            'Country': pd.concat(Country),
            'State': pd.concat(State),
            'District': pd.concat(District),
            'Type': pd.concat(Type),
            'Latitude': pd.concat(Latitude), 
            'Longitude': pd.concat(Longitude)
        })
        merged_gdf = gpd.GeoDataFrame(merged_gdf)

        return merged_gdf
    
    return create_pipeline_inner(file_columns, state_cols, district_cols, country_cols, type_cols, latitude_cols, longitude_cols)
