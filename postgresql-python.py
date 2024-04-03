from sqlalchemy import create_engine

import psycopg2

conexao_pg = create_engine('postgres://user:password@host/database').connect()

#importando o Pandas
import pandas as pd
#Lendo o csv
ideb_brasil = pd.read_csv(
    filepath_or_buffer = "https://raw.githubusercontent.com/jonates/opendata/master/INEP/ideb_ensino_medio_brasil.csv",
    sep = ";")


ideb_brasil.to_sql( 
    name = 'teste_ideb_brasil',
    con = conexao_pg,
    index = False,
    if_exists = 'replace')


conexao = psycopg2.connect(
    host="10.28.***.***",
    database = "nome_da_base_de_dados",
    port= 5432,
    user="meu_usuario",
    password="minha_senha")


# Criando um cursor
cursor = conexao.cursor()
# Realizando a consulta na tabela do postgres
cursor.execute(
    '''
    SELECT ano , ideb 
    FROM teste_ideb_brasil
    WHERE rede='Estadual';
    '''
)


cursor.fetchall()
Out[4]: 
[(2005, '3,0'),
 (2007, '3,2'),
 (2009, '3,4'),
 (2011, '3,4'),
 (2013, '3,4'),
 (2015, '3,5'),
 (2017, '3,5'),
 (2019, '3,9'),
 (2021, None)]



#fechando o cursor
cursor.close()
#fechando a conexao
conexao.close()
