# pandas

""" 
para trabajar con datos tabulares tipo tabla: 
- excel
- tabla de una base de datos
- otros (ej: tablas pyflink)

estructuras principales: Series y Data Frames
"""

# - series: un vector o una columna en el excel o una columna en tabla de BBDD
import pandas as pd
serie = pd.Series([1,2,3])
print (serie)
serie.name ='nombre'
print (serie)

# - data fames: una tabla en formato diccionario con elementos con columnas que son series
df = pd.DataFrame ({'col1': [1,2,3,4], 'col2': ['a','b','c','d']})
print (df)
print (df.shape)

# carga del dataframe desde un fichero csv (hay más tipos de conectores y cargas)
df =pd.read_csv ('data/iris.data', header = None )
print (df)
print (df.head(3))
print (df.tail(3))

# nombre de columnas
nombres = ['long_sepalo', 'ancho_sepalo', 'long_petalo', 'ancho_petalo', 'clase']
df.columns = nombres
print (df.head())
print (df.columns)
print (df.index)
print (df.shape)
# estadisticas describre() ojo no describe (da tb los cuartiles o dispersion de datos, el 50% es la mediana)
print (df.describe())

# cuenta por series o columnas
print (df['clase'].value_counts())

# transposicion
print (df.T)

# ordenacion por ancho_sepalo
print (df.sort_values('ancho_sepalo', ascending=False))

#seleccion de columnas
print (df[['long_sepalo', 'long_petalo']])

# indices (mirar como devolver distintos indices en python)
print (df[:3])

# devuelve filas 4,10 y colunas 0,2
print (df.iloc [[4,10], [0,2]])

# long sepalo > 5
print (df[df['long_sepalo']>5])
# operaciones entre columnas
print (df['long_sepalo'] - df ['long_petalo'])
# valores perdidos
print (df.isna().sum())
# mas estadiscas
print (df ['long_petalo'].mean())
print (df ['long_petalo'].median())

#funcion map com lambdas con apply
print (df ['ancho_sepalo'].apply (lambda x: -x))

#agrupaciones
df_group = df.groupby('clase')['ancho_petalo'].mean()
df_group.name = 'media_ancho_petalo'
print (df_group)

#join
df_join = df.join (df_group, on=['clase'], how='inner')
print (df_join.head())
print (df_join.tail(12))


