# link2postgresql

This is a collection of methods for uploading or downloading data with different formats.
1. Using SQL to manage DB
1. Spark DataFrame <-> DB
1. Pandas DataFrame <-> DB
1. Excel/csv/json -->DB
* Automatically add an ID column when uploading

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

What things you need to install the software and how to install them

```
spark
jdk
hadoop
PostgreSQL

# python package
pip install psycopg2
pip install pandas
pip install numpy
pip install findspark
pip install pyspark
pip install pyarrow
```

### Installing

A step by step series of examples that tell you how to get a development env running

Say what the step will be
Step1: 
```
copy 'link2postgresql' folder and paste it into 'python/Lib/site-packages'
```
Step2: open a terminal or command to open a python having a test.
``` python
from link2postgresql import Link2postgresql as link
```

End with an example of getting some data out of the system or using it for a little demo

## Running the tests

Explain how to run the automated tests for this system.
### initialization
```python
aim_link=link(user="postgres", password="1234", ip="localhost", port="5432",database="sample_database")
```
### table2spark_df

Download table from PostgreSQL database as spark dataframe (transformation).

```python
cmd="select * from sample_table where id=1"
pandas_df=aim_link.table2spark_df(table_name="sample_table",cmd=cmd)
```

### table2spark_df_slow

Download table from PostgreSQL database as spark dataframe (action). All the data in the table are stored in the memory.

```python
cmd="select * from sample_table where id=1"
pandas_df=aim_link.table2spark_df_slow(table_name="sample_table",cmd=cmd)
```
### spark_df2table

Upload spark dataframe into PostgreSQL database. mode is the same as in `JDBC`

```python
aim_link.spark_df2table(df=spark_df,table_name='sample_table',mode="append"):
```

### execute

Using sql to control the database. eg: delete the records with id equal to 1 in sample_table.

```python
aim_link.execute("delete from sample_table where id=1;")
```

### fetch_execute

Download the data (String) from DB 

```python
cmd="select * from sample_table;"
temp=aim_link.fetch_execute(cmd=cmd)
```
Download the data and title (Tuple) from DB 

```python
cmd="select * from sample_table;"
temp=aim_link.fetch_execute(cmd=cmd,title=True)
data=temp[0]
title=temp[1]
```
### tablemaxcount

Inner-class function. Getting the max value of id from the defined table and columns.

```python
def tablemaxcount(self,id_name,table_name):
    ......
```

### table2pandas_df

Download table from PostgreSQL database as spark dataframe. All the data in the table are stored in the memory. 

```python
cmd="select * from sample_table where id=1"
pandas_df=aim_link.table2pandas_df(table_name='sample_table',cmd=cmd)
```

### table2pandas_df_slow

Using `JDBC` to download to spark dataframe and then transform it into pandas dataframe. `Pyarrow` is used to accelerate the action.

```python
cmd="select * from sample_table where id=1"
pandas_df=aim_link.table2pandas_df_slow(table_name='sample_table',cmd=cmd)
```
### emptytable

Create a new empty table in PostgreSQL database
```python
schema="(a bigint,b real,c text)"
aim_link.emptytable(table_name='sample_table', schema)
```
### insert_s

Insert values into an extant table. Supporting multiline. the schema is different from the above.

```python
schema="a,b,c"
values="(1,1.0,'1.0'),(2,2.0,'2.0'),(3,3.0,'3.0')"
aim_link.insert_s(table_name='sample_table', schema=schema, values=values)
```
### pandas_df2table
upload pandas dataframe to PostgreSQL database.
1. `table_name` should not contian `uppercase` letters!!
2. `if_exists='append'`: Continuoulsy insert table without wawrnings.
3. `id='True'`: create a ID column in the first. At the same time, a creasing sequence and a primary key constraint will be created.

* Known error: if the current table is without the sequence and constraint, an error will appear. To fix it:
```python
tablename='sample_table'
# create the constraint of primary key
aim_link.execute("ALTER TABLE %s ADD CONSTRAINT %s_id_pk PRIMARY KEY (id);"%(table_name,table_name))
# create sequence of creasing
aim_link.execute('''
CREATE SEQUENCE %s_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
alter table %s alter column id set default nextval('%s_id_seq');
'''%(table_name,table_name,table_name))
```

2. `clean="True"`:Special marks in title may casue some errors with a high risk!! So I use 'clean' to fix it.
3. `clean="True"`:single quota marks in the content will also cause error. So set check="True".

```python
aim_link.pandas_df2table(df=pandas_df, table_name='sample_table', if_exists='append', id='True', check="True", clean="True")
```
### pandas_df2table_slow

Using  `emptytable` and `insert_s` to realize uploading pandas dataframe to Postgresql database. The best advantage: it supports postgis that is geo-information. The coordinate system is 4326.
`geo_schema`:the column label of cooridinate information. It must be in the format of WKT, eg: POINT(113.1 22.3)
```python
aim_link.pandas_df2table_slow(df=pandas_df, table_name='sample_table', geo_schema="geometry", check="Yes"):
```
### pandas_df2table_lite

Inner-class function. it suits for building a lightweight form in DB. The machine will in a high probability be stuck when big data. 

```python
def pandas_df2table_lite(self, df, table_name,if_exists='append',clean='False', *args, **kwargs): 
    ......
```
### excel2table

if your original data are ``clean enough``, you can choose it! otherwise, do data clean first

```python
aim_link.excel2table(excelpath='./sample_table.xls',table_name='sample_table', if_exists='fail')
```
### csv2table

if your original data are ``clean enough``, you can choose it! otherwise, do data clean first

```python
aim_link.csv2table(csvpath='./sample_table.csv',table_name='sample_table', if_exists='fail')
```
### json2table

if your original data are ``clean enough``, you can choose it! otherwise, do data clean first

```python
aim_link.json2table(jsonpath='./sample_table.json',table_name='sample_table', if_exists='fail')
```

## Authors

* **GUO Zijian**

## License
```
MIT License

Copyright (c) 2018-present GUO ZIJIAN from PolyU

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
