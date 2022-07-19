#!/usr/bin/env python
# coding: utf-8

# ### Importa pacotes

# In[19]:


import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime


# ### Define funções

# In[3]:


#Função de nome dos meses
def func_mes(row):
    if row['MES_EXT'] == 'Jan':
        return '01'
    elif row['MES_EXT'] =='Fev':
        return '02'
    elif row['MES_EXT'] =='Mar':
        return '03'
    elif row['MES_EXT'] =='Abr':
        return '04'
    elif row['MES_EXT'] =='Mai':
        return '05'
    elif row['MES_EXT'] =='Jun':
        return '06'
    elif row['MES_EXT'] =='Jul':
        return '07'
    elif row['MES_EXT'] =='Ago':
        return '08'
    elif row['MES_EXT'] =='Set':
        return '09'
    elif row['MES_EXT'] =='Out':
        return '10'
    elif row['MES_EXT'] =='Nov':
        return '11'
    elif row['MES_EXT'] =='Dez':
        return '12'
    else:
        return row['MES_EXT']

#Função de códigos de UF
def func_uf(row):
    if row['ESTADO_TRIM'] == 'RONDÔNIA':
        return 'RO'
    elif row['ESTADO_TRIM'] =='ACRE':
        return 'AC'
    elif row['ESTADO_TRIM'] =='AMAZONAS':
        return 'AM'
    elif row['ESTADO_TRIM'] =='RORAIMA':
        return 'RR'
    elif row['ESTADO_TRIM'] =='PARÁ':
        return 'PA'
    elif row['ESTADO_TRIM'] =='AMAPÁ':
        return 'AP'
    elif row['ESTADO_TRIM'] =='TOCANTINS':
        return 'TO'
    elif row['ESTADO_TRIM'] =='MARANHÃO':
        return 'MA'
    elif row['ESTADO_TRIM'] =='PIAUÍ':
        return 'PI'
    elif row['ESTADO_TRIM'] =='CEARÁ':
        return 'CE'
    elif row['ESTADO_TRIM'] =='RIO GRANDE DO NORTE':
        return 'RN'
    elif row['ESTADO_TRIM'] =='PARAÍBA':
        return 'PB'
    elif row['ESTADO_TRIM'] =='PERNAMBUCO':
        return 'PE'
    elif row['ESTADO_TRIM'] =='ALAGOAS':
        return 'AL'
    elif row['ESTADO_TRIM'] =='SERGIPE':
        return 'SE'
    elif row['ESTADO_TRIM'] =='BAHIA':
        return 'BA'
    elif row['ESTADO_TRIM'] =='MINAS GERAIS':
        return 'MG'
    elif row['ESTADO_TRIM'] =='ESPÍRITO SANTO':
        return 'ES'
    elif row['ESTADO_TRIM'] =='RIO DE JANEIRO':
        return 'RJ'
    elif row['ESTADO_TRIM'] =='SÃO PAULO':
        return 'SP'
    elif row['ESTADO_TRIM'] =='PARANÁ':
        return 'PR'
    elif row['ESTADO_TRIM'] =='SANTA CATARINA':
        return 'SC'
    elif row['ESTADO_TRIM'] =='RIO GRANDE DO SUL':
        return 'RS'
    elif row['ESTADO_TRIM'] =='MATO GROSSO DO SUL':
        return 'MS'
    elif row['ESTADO_TRIM'] =='MATO GROSSO':
        return 'MT'
    elif row['ESTADO_TRIM'] =='GOIÁS':
        return 'GO'
    elif row['ESTADO_TRIM'] =='DISTRITO FEDERAL':
        return 'DF'
    else:
        return row['ESTADO_TRIM']


# ### Importa dados

# #### Sales of oil derivative fuels by UF and product

# In[4]:


#Lendo excel
df_Oil_UF_prod = pd.read_excel (r'C:\Users\cysb\OneDrive\Documentos\Python Scripts\Raizen\data\vendas-combustiveis-m3-Libre.xls', sheet_name = 'DPCache_m3', usecols='A:Q', nrows=4537)


# #### Sales of diesel by UF and type

# In[5]:


#Lendo excel
df_Oil_UF_type = pd.read_excel (r'C:\Users\cysb\OneDrive\Documentos\Python Scripts\Raizen\data\vendas-combustiveis-m3-Libre.xls', sheet_name = 'DPCache_m3_2', usecols='A:Q', nrows=1081)


# ### Despivoteia dados

# #### Sales of oil derivative fuels by UF and product

# In[6]:


#Colunas viram linhas
df_Oil_UF_prod_unpivot = df_Oil_UF_prod.melt(id_vars = ['COMBUSTÍVEL','ANO','REGIÃO','ESTADO'], var_name = 'MES_EXT', value_name = 'QTDE')


# #### Sales of diesel by UF and type

# In[7]:


#Colunas viram linhas
df_Oil_UF_type_unpivot = df_Oil_UF_type.melt(id_vars = ['COMBUSTÍVEL','ANO','REGIÃO','ESTADO'], var_name = 'MES_EXT', value_name = 'QTDE')


# ### Campos auxiliares

# #### Sales of oil derivative fuels by UF and product

# In[8]:


#Limpa linhas de totais
df_Oil_UF_prod_unpivot = df_Oil_UF_prod_unpivot[(df_Oil_UF_prod_unpivot['MES_EXT'] != 'TOTAL')]

#Limpa espaços
df_Oil_UF_prod_unpivot['ESTADO_TRIM'] = df_Oil_UF_prod_unpivot['ESTADO'].str.strip()

#Aplicando nome dos meses
df_Oil_UF_prod_unpivot['NR_MES'] = df_Oil_UF_prod_unpivot.apply(func_mes, axis=1)


# #### Sales of diesel by UF and type

# In[9]:


#Limpa linhas de totais
df_Oil_UF_type_unpivot = df_Oil_UF_type_unpivot[(df_Oil_UF_type_unpivot['MES_EXT'] != 'TOTAL')]

#Limpa espaços
df_Oil_UF_type_unpivot['ESTADO_TRIM'] = df_Oil_UF_type_unpivot['ESTADO'].str.strip()

#Aplicando nome dos meses
df_Oil_UF_type_unpivot['NR_MES'] = df_Oil_UF_type_unpivot.apply(func_mes, axis=1)


# ### Campos finais

# #### Sales of oil derivative fuels by UF and product

# In[10]:


#Concatenando data
df_Oil_UF_prod_unpivot['year_month'] = pd.to_datetime(df_Oil_UF_prod_unpivot["ANO"].astype(str) + df_Oil_UF_prod_unpivot["NR_MES"] + '01', format='%Y%m%d', errors='coerce')

#Aplicando códigos de UF
df_Oil_UF_prod_unpivot['uf'] = df_Oil_UF_prod_unpivot.apply(func_uf, axis=1)

#Formatando campos
df_Oil_UF_prod_unpivot['product'] = df_Oil_UF_prod_unpivot['COMBUSTÍVEL'].apply(lambda st: st[:st.find("(")]).str.strip()
df_Oil_UF_prod_unpivot['unit'] = df_Oil_UF_prod_unpivot['COMBUSTÍVEL'].apply(lambda st: st[st.find("(")+1:st.find(")")]).str.strip()
df_Oil_UF_prod_unpivot['volume'] = df_Oil_UF_prod_unpivot['QTDE']
df_Oil_UF_prod_unpivot['created_at'] = pd.to_datetime("today")

#Datas para string
df_Oil_UF_prod_unpivot['created_at'] = df_Oil_UF_prod_unpivot['created_at'].astype(str)
df_Oil_UF_prod_unpivot['year_month'] = df_Oil_UF_prod_unpivot['year_month'].astype(str)


# #### Sales of diesel by UF and type

# In[11]:


#Concatenando data
df_Oil_UF_type_unpivot['year_month'] = pd.to_datetime(df_Oil_UF_type_unpivot["ANO"].astype(str) + df_Oil_UF_type_unpivot["NR_MES"] + '01', format='%Y%m%d', errors='coerce')

#Aplicando códigos de UF
df_Oil_UF_type_unpivot['uf'] = df_Oil_UF_type_unpivot.apply(func_uf, axis=1)

#Formatando campos
df_Oil_UF_type_unpivot['product'] = df_Oil_UF_type_unpivot['COMBUSTÍVEL'].apply(lambda st: st[:st.find("(")]).str.strip()
df_Oil_UF_type_unpivot['unit'] = df_Oil_UF_type_unpivot['COMBUSTÍVEL'].apply(lambda st: st[st.find("(")+1:st.find(")")]).str.strip()
df_Oil_UF_type_unpivot['volume'] = df_Oil_UF_type_unpivot['QTDE']
df_Oil_UF_type_unpivot['created_at'] = pd.to_datetime("today")

#Datas para string
df_Oil_UF_type_unpivot['created_at'] = df_Oil_UF_type_unpivot['created_at'].astype(str)
df_Oil_UF_type_unpivot['year_month'] = df_Oil_UF_type_unpivot['year_month'].astype(str)


# ### Testa totais

# #### Sales of oil derivative fuels by UF and product

# In[96]:


#Teste de totais
df_Oil_UF_prod_unpivot[(df_Oil_UF_prod_unpivot['ANO'] == 2020)].groupby(['MES_EXT','NR_MES','ANO']).sum().sort_values(by='NR_MES')


# #### Sales of diesel by UF and type

# In[97]:


#Teste de totais
df_Oil_UF_type_unpivot[(df_Oil_UF_type_unpivot['ANO'] == 2013)].groupby(['MES_EXT','NR_MES','ANO']).sum().sort_values(by='NR_MES')


# ### Dataframe final

# #### Sales of oil derivative fuels by UF and product

# In[12]:


#Dataframe no modelo definido
df_Oil_By_UF_prod_final = df_Oil_UF_prod_unpivot[['year_month','uf','product','unit','volume','created_at']].fillna(0)

df_Oil_By_UF_prod_final


# #### Sales of diesel by UF and type¶

# In[13]:


#Dataframe no modelo definido
df_Oil_By_UF_type_final = df_Oil_UF_type_unpivot[['year_month','uf','product','unit','volume','created_at']].fillna(0)

df_Oil_By_UF_type_final


# ### Insere no banco

# #### Sales of oil derivative fuels by UF and product

# In[100]:


try:
    con = mysql.connector.connect(host='localhost', database='raizen', user='UserCarga', password='UserCarga')
    if con.is_connected():
        cursor = con.cursor()
        print(datetime.now(), ": You're connected to database... Wait for the tb_Oil_By_UF_prod load to finish...")
        for i,row in df_Oil_By_UF_prod_final.iterrows():
            sql = "REPLACE INTO raizen.tb_Oil_By_UF_prod (yearmonth ,uf ,product ,unit ,volume ,created_at) VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            con.commit()
        print(datetime.now(), ": Load finish")
except Error as e:
            print("Error while connecting to MySQL", e)


# #### Sales of diesel by UF and type

# In[20]:


try:
    con = mysql.connector.connect(host='localhost', database='raizen', user='UserCarga', password='UserCarga')
    if con.is_connected():
        cursor = con.cursor()
        print(datetime.now(), ": You're connected to database... Wait for the tb_Oil_By_UF_type load to finish...")
        for i,row in df_Oil_By_UF_type_final.iterrows():
            sql = "REPLACE INTO raizen.tb_Oil_By_UF_type (yearmonth ,uf ,product ,unit ,volume ,created_at) VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            con.commit()
        print(datetime.now(), ": Load finish")
except Error as e:
            print("Error while connecting to MySQL", e)


# In[ ]:




