#BIBLIOTECAS
import pandas as pd


#Importando os dados
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/seu repositorio registrado no  data bricks/tabela_contratos.csv")
tabela_contratos = df1.toPandas()
tabela_contratos.head(5)

#tabela de datas
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/seu email.com/tabela_datas.csv")
tabela_data = df1.toPandas()
tabela_data.head()

#Tabela de empresas
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/seu email registrado no databricks.com/tabela_empresas.csv")
tabela_empresas = df1.toPandas()
tabela_empresas.tail()


# realizando a junção de colunas com o merge
contratos_att = tabela_contratos.merge(tabela_empresas,
                                      left_on='fk_empresa_contratada',
                                      right_on='id_empresa',
                                      how='left')
contratos_att.head(5)

#excluindo as colunas desnecessarias
contratos_att.drop(columns=['fk_empresa_contratada', 'id_empresa'],inplace=True)
contratos_att


# outra junção de colunas sendo elas: id_data na tabela_datas e transformamos em inicio_vigencia
contratos_dt = contratos_att.merge(tabela_data,
                                      left_on='inicio_vigencia',
                                      right_on='id_data',
                                      how='left')
contratos_att

#Ultimo merge pegando  id_data da tabela_datas e transformamos em termino_vigencia
contrato_final = contratos_dt.merge(tabela_data,
                                      left_on='termino_vigencia',
                                      right_on='id_data',
                                      how='left')
contrato_final.head(5)

#excluindo as colunas de datas desnessarias
contrato_final.drop(columns=['inicio_vigencia', 'id_data_x'], inplace=True)
contrato_final.drop(columns=['termino_virgencia', 'id_data_y'], inplace=True)

#renomeando o nome das colunas
contrato_final.rename(columns={'data': 'data_inicio_vigencia'}, inplace=True)
contrato_final.rename(columns={'data': 'data_termino_vigencia'}, inplace=True)
contrato_final.head(5)

#confirmando se nao tem nenhum dados nulo etc...
contrato_final.count()

#verificando se os dados estao corretos no caso não e iremos fazer algumas alterações
contrato_final.dtypes

#transformando a coluna valor_contrato que esta com object em float por conta de ser uma coluna decimal, numerica etc..
contrato_final['valor_contrato'] = contrato_final['valor_contrato'].astype('float')
#id em  numero inteiro
contrato_final['id_contrato'] = contrato_final['id_contrato'].astype('int')

#um comando muito bom para mudar o tipo para data e o: dt.date, para modificar a coluna que esta em string
contrato_final.data_inicio_vigencia = pd.to_datetime(contrato_final.data_inicio_vigencia,
                                                    format='%d/%m/%Y').dt.date

#tentei repetir o mesmo comando acima mais deu um erro, provavel que seja string no meio ou data inexistente
contrato_final.data_termino_vigencia = pd.to_datetime(contrato_final.data_termino_vigencia,
                                                    format='%d/%m/%Y').dt.date

#for para percorrer toda coluna e parar quando ocorer o erro 
for i in contrato_final.data_termino_vigencia:
  print(i)
  print(pd.to_datetime(i))

# Nesse caso o erro foi que no mes 09 não vai até o dia 31 so fazer a correção e ver se foi resolvido
contrato_final.data_termino_vigencia = contrato_final.data_termino_vigencia.str.replace('31/09/2017', '29/09/2022')
#repeti o mesmo processo e deu certo
contrato_final.data_termino_vigencia = pd.to_datetime(contrato_final.data_termino_vigencia,
                                                    format='%d/%m/%Y').dt.date
contrato_final.head()

#Criando uma nova coluna com o tempo de contrato em dias 
contrato_final['Tempo_de_contrato'] = (contrato_final.data_termino_vigencia - contrato_final.data_inicio_vigencia).dt.days
contrato_final.head()

contrato_final.nome_contrato.value_counts()

#aqui conferindo se tinha algum dados nulo ou difente na nossa coluna recem criada
contrato_final.Tempo_de_contrato.value_counts()
#tudo certo sem problemas nessa coluna

contrato_final.tail()

#Um ponto muito importante importar essa tabela so fazendo o dowload "contrato_final" e trazer o projeto que foi feito todo em pandas para um dataframe e transformar ele em spark e sql 
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/seu email repositorio no data bricks/contratos_atualizados.csv")
df1

#AQUI JA FOI FEITO UMA CONFIGURAÇÃO DIRETA NO DATABRICKS SO MUDANDO ALGUMAS FUNÇÕES

# File location and type
file_location = "dbfs:/FileStore/shared_uploads/seu email ou repositorio no databricks/contratos_atualizados.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df1 = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("sep", delimiter) \
  .load(file_location)

display(df1)

# Criando a tabela

temp_table_name = "contratado"

df1.createOrReplaceTempView(temp_table_name)

#e aqui transformei tudo em uma tabela em sql o database foi criado com todos dados acima e ja conseguimos deixar esses dados visiveis  para que todo time possa utiliza-los

%sql
create database rh

create table rh.contratados

select *from contratado