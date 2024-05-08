from query_sql import query_POSTGRESQL 

path = 'C:\\Users\\Fabian\\Documents\\Homelab\\Notebooks\\DB\\Tarea2'

query = '''SELECT * FROM dtes;'''

dtes = query_POSTGRESQL(path,'USM',query)


