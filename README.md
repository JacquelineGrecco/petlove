# Petlove : O desafio


Este projeto consiste em extrair e também transformar dados fictícios para que, hipoteticamente, a área de negócios da Petlove possa tomar decisões. O Objetivo desse projeto consiste em mostrar a possibilidade de usar PySpark para normalizar os dataframes e assim ingerir os dados em um Postgresql. 

# Relacionamento das tabelas 

![petlove (1)](https://user-images.githubusercontent.com/22913973/70939621-5c258e80-2027-11ea-89c6-4de54498b7e2.jpg)


# Execução

## Ingestão de Dados

```spark-submit --driver-class-path seu_path/petlove/jars/postgresql-42.2.9.jar seu_path/petlove/ingestion/spark_ingestion.py```

## ETLs 
```spark-submit --driver-class-path seu_path/petlove/jars/postgresql-42.2.9.jar seu_path/petlove/etl/spark_etl.py```


# Arquitetura de Dados 

A proposta de arquitetura de dados se encontra disponível neste [link](https://github.com/JacquelineGrecco/petlove/tree/master/data_architecture)


# Observações 

### No caso de houver algum problema com a função `round`:

- Abra o seu postgres usando o terminal e use o comando abaixo.  
- ```\df *round*``` 


### No caso de houver problema com o arquivo `spark_etl.py`: 

 - Abra o seu postgres usando o terminal e use o comando abaixo.  
 - Conceda privilegios ao seu usuário com os comandos. 
    - ```GRANT ALL PRIVILEGES ON tb_faturamento postgres to seu_usuario;```
    - ```GRANT ALL PRIVILEGES ON tb_frete postgres to seu_usuario;```
    - ```GRANT ALL PRIVILEGES ON tb_peso_unitario postgres to seu_usuario;```
    - ```GRANT ALL PRIVILEGES ON tb_familia_setor postgres to seu_usuario;```