# link2postgresql

This is a collection of methods for uploading or downloading data with different formats.

1. Using SQL to manage DB
2. Spark DataFrame <-> DB
3. Pandas DataFrame <-> DB
4. Excel/csv/json -->DB

- Automatically add an ID column when uploading

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

What things you need to install the software and how to install them

```
PostgreSQL          # required for any DB operation
# python package (minimum for the pandas upload path)
pip install psycopg2
pip install pandas
pip install numpy
pip install sqlalchemy     # required by pandas_df2table / *_lite / excel-csv-json2table

# only needed for the Spark path (spark_df2table / table2spark_df*)
pip install findspark
pip install pyspark
pip install pyarrow
# spark / jdk / hadoop  (only for the Spark path)

# only needed for the PostGIS path
pip install geoalchemy2
pip install geopandas      # only for geopandas_df2table / table2pandas_df(geo_scheme=...)
```

> **Note:** the package no longer imports Spark / psycopg2 / geopandas at the top
>   
> level. Simply `import link2postgresql` works without Spark, JDK or Hadoop
>   
> installed. The heavy dependencies above are imported lazily only when the
>   
> corresponding method is actually called.

### Installing

A step by step series of examples that tell you how to get a development env running

Say what the step will be
  
Step1:

```
copy 'link2postgresql' folder and paste it into 'python/Lib/site-packages'
```

Step2: open a terminal or command to open a python having a test.

```python
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

Upload spark dataframe into PostgreSQL database. mode is the same as in `JDBC`.
  
When `wkt_column` is given, the Spark DataFrame must carry that WKT text column;
  
it is written as TEXT and then converted to a PostGIS geometry column (same as
  
the pandas path), and optionally dropped with `drop_wkt='True'`.

```python
# plain upload
aim_link.spark_df2table(df=spark_df, table_name='sample_table', mode="append")

# with PostGIS geometry (spark_df must contain the wkt_column text column,
# written in WKT, e.g. 'POINT(113.1 22.3)')
aim_link.spark_df2table(df=spark_df, table_name='sample_table', mode="append",
                        wkt_column='wkt', geo_type='POINT', drop_wkt='False')
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

### addgeocolumn

Manually transform a column about wkt[^1] into geometry.

```python
aim_link.addgeocolumn(table_name='sample_table', wkt_column='wkt_column', geo_type='POINT')
```

Notes:

- The PostGIS extension is created automatically when missing (`CREATE EXTENSION IF NOT EXISTS postgis`).
- If the `geometry` column does not exist yet it is added with type `geometry(<geo_type>,4326)`.
- The geometry is (re)built only for rows where `geometry IS NULL`, so it is safe to call repeatedly (e.g. after every append) and will not re-parse the whole table.
- If the table already has a `geometry` column and you want to refresh it from changed WKT for *all* rows, call this after clearing the existing geometry, or use a custom `UPDATE`.

### drop_wkt_column

Explicitly drop a raw WKT text column once you no longer need it (e.g. after the geometry column is built and you are done appending). Safe to call multiple times (`IF EXISTS`).

```python
aim_link.drop_wkt_column(table_name='sample_table', wkt_column='wkt')
```

### pandas_df2table

upload pandas dataframe to PostgreSQL database.

1. `table_name` should not contian `uppercase` letters!!
2. `if_exists='append'`: Continuoulsy insert table without wawrnings.
3. `id='True'`: an auto-increment `id` primary key column is created (PostgreSQL `bigserial`). The id is generated by PostgreSQL, so it always continues correctly even across multiple appends.
4. `clean="True"`:Special marks in title may casue some errors with a high risk!! So I use 'clean' to fix it. Column names are cleaned (spaces / special marks stripped, lower-cased) before upload. If you pass `wkt_column`, it is translated to its cleaned name automatically.
5. `check` is **no longer needed**: single quotes and other special characters in the content are now handled safely by the parameterised SQLAlchemy writer, so you can omit `check` (it is kept only for backward compatibility).

PostGIS support (all optional):

- `wkt_column`: name of the column that holds WKT text (e.g. `'POINT(113.1 22.3)'`). When set, a `geometry` column is created and filled from it.
- `geo_type`: geometry type for the column, default `'POINT'` (use `'MULTIPOLYGON'`, `'GEOMETRY'`, etc. for other shapes). SRID is fixed to `4326`.
- `drop_wkt='False'`: set `'True'` to drop the raw WKT text column after the geometry is built, to save storage. The text column is re-created automatically on the next load, so appending still works.

```python
# plain upload
aim_link.pandas_df2table(df=pandas_df, table_name='sample_table',
                         if_exists='append', id='True', clean='True')

# with PostGIS geometry
aim_link.pandas_df2table(df=pandas_df, table_name='sample_table',
                         if_exists='append', id='True', clean='True',
                         wkt_column='wkt', geo_type='POINT', drop_wkt='False')
```

### pandas_df2table_slow

A convenience wrapper that delegates to `pandas_df2table`, so it shares the same
  
behaviour (id, clean, parameterised writing, etc.) and supports PostGIS. The
  
coordinate system is 4326.
  
`geo_schema`: the column label of the WKT coordinate information. It must be in
  
the format of WKT, e.g. `POINT(113.1 22.3)`. `drop_wkt`: set `'True'` to drop
  
the raw WKT text column after the geometry is built (saves storage).

```python
# geo_schema is the name of the WKT text column in your dataframe
aim_link.pandas_df2table_slow(df=pandas_df, table_name='sample_table',
                              geo_schema="wkt", drop_wkt='False')
```

### pandas_df2table_lite

Lightweight pandas upload (no auto `id` column). It suits for building a simple
  
form in DB. When `wkt_column` is supplied it generates a PostGIS geometry column
  
exactly like `pandas_df2table` (including append-after-drop support via
  
`drop_wkt`).

```python
# plain
aim_link.pandas_df2table_lite(df=pandas_df, table_name='sample_table',
                              if_exists='append', clean='False')

# with PostGIS geometry
aim_link.pandas_df2table_lite(df=pandas_df, table_name='sample_table',
                              wkt_column='wkt', geo_type='POINT', drop_wkt='False')
```

### excel2table

if your original data are `clean enough`, you can choose it! otherwise, do data clean first

```python
aim_link.excel2table(excelpath='./sample_table.xls',table_name='sample_table', if_exists='fail')
```

### csv2table

if your original data are `clean enough`, you can choose it! otherwise, do data clean first

```python
aim_link.csv2table(csvpath='./sample_table.csv',table_name='sample_table', if_exists='fail')
```

### json2table

if your original data are `clean enough`, you can choose it! otherwise, do data clean first

```python
aim_link.json2table(jsonpath='./sample_table.json',table_name='sample_table', if_exists='fail')
```

### PostGIS compatibility notes

- Works with PostGIS >= 1.5 (the `geometry(<type>,4326)` typmod and `ST_GeomFromText` are supported by all versions).


- **SRID is fixed to 4326.** Data in another SRID must be transformed beforehand.
- The geometry column is always named **`geometry`**.
- `geo_type` defaults to **`POINT`**; pass `'MULTIPOLYGON'`, `'LINESTRING'`, `'GEOMETRY'`, etc. when your WKT is not a point, otherwise the typmod will reject the geometry.
- On managed PostgreSQL (RDS / Cloud SQL / Aurora) the `CREATE EXTENSION postgis` may require specific privileges or a pre-installed extension; if it fails the geometry operations will then error.

## License
```
MIT License

Copyright (c) 2018-present from NENU

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

[^1]: https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry