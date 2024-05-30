import psycopg2
import pandas as pd
from query_sql import query_POSTGRESQL 
import os, sys
import xml.etree.ElementTree as ET
from query_sql import query_POSTGRESQL, DataFrame2DataBase

# Especifica la ruta al archivo Excel
# archivo_excel = 'extract_glosa.xlsx'

path_files = os.getcwd()
# cambiar Tarea1 por el nombre de su carpeta
path_sql = os.path.join(os.path.normpath(os.getcwd() + os.sep + os.pardir), "Tarea1") 
sys.path.insert(0, path_sql)

# -------------------------  EXTRACCION DE DATOS
query_sql = '''SELECT * FROM dtes'''
# tener en la misma carpeta in archivo llamado database.ini
df = query_POSTGRESQL(path_sql, 'USM', query_sql)

# ------------------------- TRANSFORMACION DE DATOS

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


filas_sin = []
filas_con = []

# Recorrer el dataframe
for xml in df['convert_from']:

    root = ET.fromstring(xml)
    namespace = {'default': 'http://www.sii.cl/SiiDte'}

    # Obtener los valores "globales" del xml
    Folio = root.find('.//{http://www.sii.cl/SiiDte}Folio').text
    RutEmisor = root.find('.//{http://www.sii.cl/SiiDte}RutEmisor').text

    # Obtener todos los detalles del xml <detalle>
    detalles = root.findall('.//ns:Detalle', ns)

    # Recorrer cada detalle de los detalles
    for detalle in detalles:
        NroLinDet = get_element_value(detalle, 'NroLinDet')
        NmbItem = get_element_value(detalle, 'NmbItem')
        MontoItem = get_element_value(detalle, 'MontoItem')
        DscItem = get_element_value(detalle, 'DscItem')
        # Crear una fila para agregar al dataframe
        # Si esta el elemento <DscItem> se agrega al df que contiene las intancias con DscItem
        if DscItem is not None:
            nuevaFila = {
            "rut_emisor": RutEmisor,
            "folio": Folio,
            "n_item":NroLinDet,
            "nmb_item": NmbItem,
            "dsc_item": DscItem,
            "monto_item": MontoItem,
            }
            filas_con.append(nuevaFila)
        # Si no esta el elemento <DscItem> se agrega la fila al df que no tiene 
        else:
            nuevaFila = {
            "rut_emisor": RutEmisor,
            "folio": Folio,
            "n_item":NroLinDet,
            "nmb_item": NmbItem,
            "dsc_item": None,
            "monto_item": MontoItem,
            }
            filas_sin.append(nuevaFila)

# agregar las nuevas filas a los distintos df
df_con = pd.DataFrame(filas_con) # df con DscItem
df_sin = pd.DataFrame(filas_sin) # df sin DscItem


# Dejar df_sin <DscItem> con el formato requerido
# Elimino la columna dsc_item porque no tiene nada en ese campo
df_sin = df_sin.drop(columns=['dsc_item'])
# Renombro las columna nmb_item que es la unica glosa que hay
df_sin = df_sin.rename(columns={'nmb_item': 'glosa'})

# Dejar df_con <DscItem> con el formato requerido
df_con = combinar_columnas(df_con,'nmb_item','dsc_item','glosa')
df_con = cambiar_lugar_columnas(df_con, 'monto_item', 'glosa')

# Juntar los dataframe
df_new = pd.concat([df_con, df_sin], axis=0, ignore_index=False)

# Agregar indice
df_new.index.name = 'id'
df_new = df_new.reset_index()

print(df_new.info())

# Si el script salio bien debe dar este resultado
# <class 'pandas.core.frame.DataFrame'>  
# RangeIndex: 1102 entries, 0 to 1101    
# Data columns (total 6 columns):        
#  #   Column      Non-Null Count  Dtype 
# ---  ------      --------------  ----- 
#  0   id          1102 non-null   int64 
#  1   rut_emisor  1102 non-null   object
#  2   folio       1102 non-null   object
#  3   n_item      1102 non-null   object
#  4   glosa       1102 non-null   object
#  5   monto_item  1102 non-null   object

# -------------------------  CARGA DE DATOS

# esto es para subirlo a la base de datos al schema python
# table_name = 'mendoza_bustos_fonseca_ramos'
# DataFrame2DataBase(path_sql, 'USM', df_new, table_name, 'python')