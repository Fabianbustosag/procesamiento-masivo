import pandas as pd
import xml.etree.ElementTree as ET

ns = {'ns': 'http://www.sii.cl/SiiDte'}

# Funcion para obtener un elemento
def get_element_value(element, tag):
    return element.find(f'ns:{tag}', ns).text if element.find(f'ns:{tag}', ns) is not None else None

# Funcion para combinar columnas
def combinar_columnas(df, col1, col2, nueva_columna):
    df[nueva_columna] = df[col1].astype(str) + " " + df[col2].astype(str)
    df = df.drop(columns=[col1, col2])
    return df

# Funcion para cambiar de posicion columnas
def cambiar_lugar_columnas(df, col1, col2):
    if col1 in df.columns and col2 in df.columns:
        cols = list(df.columns)
        col1_index, col2_index = cols.index(col1), cols.index(col2)
        
        # Intercambiar los nombres de las columnas en la lista
        cols[col1_index], cols[col2_index] = cols[col2_index], cols[col1_index]
        
        # Reordenar el DataFrame según la nueva lista de columnas
        df = df[cols]
        print(f"Columnas '{col1}' y '{col2}' han sido intercambiadas.")
    else:
        print(f"Una o ambas columnas '{col1}' y '{col2}' no existen en el DataFrame.")
    return df