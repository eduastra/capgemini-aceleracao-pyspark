# Capgemini - Aceleração PySpark 2022

Este projeto é parte do Programa de Aceleração [PySpark](https://spark.apache.org) da [Capgemini Brasil](https://www.capgemini.com/br-pt).
[<img src="https://www.capgemini.com/wp-content/themes/capgemini-komposite/assets/images/logo.svg" align="right" width="140">](https://www.capgemini.com/br-pt)

## Sobre

Este projeto consiste em realizar tarefas que buscam garantir a qualidade dos dados para responder perguntas de negócio a fim de gerar relatórios de forma assertiva. As tarefas são essencialmente apontar inconsistências nos dados originais, e realizar transformações que permitam tratar as inconsistências e enriquecer os dados. Em resumo, o projeto está organizado em três módulos: (1) qualidade, (2) transformação, e (3) relatório.

## Dependências

Para executar os Jupyter Notebooks deste repositório é necessário ter o [Spark instalado localmente](https://spark.apache.org/downloads.html) e também as seguintes dependências:

`pip install pyspark findspark`

Para executar scripts deste repositório é necessário ter o [Spark instalado localmente](https://spark.apache.org/downloads.html) e também um OS contendo ubuntu configurado para spark e pyspark:8080

## Estrutura de diretórios

```
├── LICENSE
├── README.md
├── csv                       <- Diretório contendo os dados brutos.
│   ├── airports.csv
│   ├── planes.csv
│   ├── flights.csv
│   │
│   ├── census-income.csv
│   │   ├── census-income.NAMES
│   │
│   ├── communities.csv
│   │   ├── communities.NAMES
│   │
│   ├── online-retail.csv
│   │   ├── online-retail.NAMES
│
├── semana_1.ipynb          <- Contém apontamentos de dados inconsistêntes.
├── semana_2.ipynb   <- Contem tratamentos dos dados.
├── semana_3.ipynb           <- Contém respostas de negócio baseadas em dados.
│
├── python scripts
├── Semana_5.py              <- Perguntas para Census Income
├── Semana_6.py              <- Perguntas para Communities and Crime
├── Semana_7.py              <- Perguntas para Online Retail

```
