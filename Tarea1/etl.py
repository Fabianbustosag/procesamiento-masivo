#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 25 17:33:24 2024

@author: jorge.ortiz
"""
import os, sys

#%% RUTAS

path_files = os.getcwd()
# path_sql = os.path.join(os.path.normpath(os.getcwd() + os.sep + os.pardir), "funciones_sql") 
path_sql = os.path.join(os.path.normpath(os.getcwd() + os.sep + os.pardir), "Tarea2") 


#%% IMPORT DE FUNCIONES

sys.path.insert(0, path_sql)
from query_sql import query_POSTGRESQL, DataFrame2DataBase

#%% EXTRACCION DE DATOS

query_sql = '''SELECT * FROM dtes'''
data = query_POSTGRESQL(path_sql, 'USM', query_sql)

#%% TRANSFORMACION DE DATOS


# ACA VA SU CODIGO

# generar un dataframe que contenga el id, rut_emisor_folio, glosa, monto

# id, rut_emisor, folio, monto, glosa1
# id, rut_emisor, folio, monto, glosa2



#%% CARGA DE DATOS

# table_name = 'bustos_fonseca_'
# DataFrame2DataBase(path_sql, 'USM', data, table_name, 'public')