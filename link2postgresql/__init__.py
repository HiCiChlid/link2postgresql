# author: GUO ZIJIAN from PolyU
import psycopg2
import io
import os
import pandas as pd
import numpy as np
from numpy import NaN
from sqlalchemy import create_engine
import findspark # https://blog.csdn.net/sinat_26599509/article/details/51895999
findspark.init()
from pyspark import *
from pyspark.sql import *
from pyspark.conf import SparkConf
#import jaydebeapi
from link2postgresql.progressbar import ShowProcess
import re
import link2postgresql
from urllib.request import quote
class Link2postgresql(object):

    def __init__(self, user="postgres", password="postgres", ip="localhost", port="5432",database="postgres", *args, **kwargs):
        '''DB means postgresql database\n
        (1)``spark``: DB to dataframe(spark); dataframe(spark) to DB\n
        (2)``pandas``: DB to dataframe(pandas); dataframe(pandas) to DB <normal ,slow ,light>\n
        (3)others: build empty table; insert values; csv,excel,json to DB \n
        '''
        self.user=user
        self.password=password
        self.ip=ip
        self.port=port
        self.database=database 

# when meeting fatal error restart spark service
    def startspark(self):
        if SparkContext._active_spark_context:
            self.sc=SparkContext._active_spark_context
            self.sqlContext=SQLContext(self.sc)
            self.sqlContext.sql("set spark.sql.execution.arrow.enabled=true")
        else:
            self.sc=self.buildspark()
            self.sqlContext = SQLContext(self.sc)
            self.sqlContext.sql("set spark.sql.execution.arrow.enabled=true")

    def restartspark(self):
        self.sc.stop()
        self.sc=self.buildspark()
        self.sqlContext = SQLContext(self.sc) 

# about spark        
    def buildspark(self,appname="sparkapp"):
        conf = SparkConf().setAppName(appname)
        conf = conf.setAll\
        ([('spark.executor.memory', '2g'),\
            ('spark.cores.max', '4'),\
            ('spark.executor.cores','4'),\
            ('spark.driver.memory','8g'),\
            ('spark.default.parallelism','4'),\
            ('spark.sql.warehouse.dir',r"%s/spark-warehouse"% os.path.dirname(link2postgresql.__file__)),\
            ('spark.driver.extraClassPath',r"%s/driver/postgresql-42.2.2.jar"% os.path.dirname(link2postgresql.__file__)),\
            ('spark.driver.allowMultipleContexts', 'true'),\
            ('spark.network.timeout', '10000000'),\
            ('spark.core.connection.ack.wait.timeout', '10000000'),\
            ('spark.storage.blockManagerSlaveTimeoutMs','10000000'), \
            ('spark.shuffle.io.connectionTimeout','10000000'), \
            ('spark.rpc.askTimeout or spark.rpc.lookupTimeout','10000000')
            ])      
        sc = SparkContext(conf=conf) 
        return sc

    def table2spark_df(self,table_name,cmd=""):
        '''
        use for reading ``postgresql database`` tables and transforming it into spark dataframe.
        '''
        self.startspark()
        #try:
        url="jdbc:postgresql://%s:%s/%s?user=%s&password=%s"%(quote(self.ip),quote(self.port),quote(self.database),quote(self.user),quote(self.password))
        if cmd=="":
            spark_df = self.sqlContext.read.format("jdbc").option("url", url).option("dbtable", table_name).load()
            return spark_df
        elif [True for i in ["select","Select","SELECT"] if i in cmd]:
            #print("an sql command!")
            spark_df = self.sqlContext.read.format("jdbc").option("url", url).option("dbtable", table_name).option("dbtable", "(%s) as tmp"%cmd).load()
            return spark_df
        elif [True for i in ["where","Where","WHERE"] if i in cmd]:
            #print("a condition!")
            condition=cmd
            spark_df = self.sqlContext.read.format("jdbc").option("url", url).option("dbtable", table_name).option("dbtable", "(SELECT * FROM %s %s) as tmp"%(table_name,condition)).load()
            return spark_df
        else:
            print("error")
            return
        #except:# the same database will not appearing error but with danger
            #self.restartspark()
            #self.table2spark_df(table_name,cmd)

    def table2spark_df_slow(self,table_name, cmd=""): # have downloaded
        pandas_df=self.table2pandas_df_slow(table_name,cmd)
        spark_df=self.sqlContext.createDataFrame(pandas_df)
        return spark_df

    def spark_df2table(self,df,table_name,mode="append"):
        '''
        use for updating spark dataframe into ``postgresql``table in the servers.
        '''
        self.startspark()
        url="jdbc:postgresql://%s:%s/%s?user=%s&password=%s"%(quote(self.ip),quote(self.port),quote(self.database),quote(self.user),quote(self.password))
        df.write.jdbc(url,table=table_name,mode=mode)

# base methods
    def execute(self,cmd): # execute the cmd to control Postgis
        try:
            pgisCon = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.ip ,port=self.port)
            try:
                pgisCursor = pgisCon.cursor()
                # pgisCursor.execute("CREATE EXTENSION postgis;") must be by hand
                pgisCursor.execute(cmd)
                pgisCon.commit() # don't forget
            except psycopg2.OperationalError:
                print("connect bd successfully, but execute cmd failure!")
            else:
                pgisCursor.close()
        except psycopg2.OperationalError:
            print('connect bd failure')
        else:
            # print('execute successfully!')
            pgisCon.close()
            
    def fetch_execute(self,cmd,title=False):
        try:
            pgisCon = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.ip ,port=self.port)
            try:
                pgisCursor = pgisCon.cursor()
                # pgisCursor.execute("CREATE EXTENSION postgis;") must be by hand
                pgisCursor.execute(cmd)
                results=pgisCursor.fetchall()
                pgisCon.commit() # don't forget
            except psycopg2.OperationalError:
                print("connect bd successfully, but execute cmd failure!")
            else:
                pgisCursor.close()
        except psycopg2.OperationalError:
            print('connect bd failure')
        else:
            # print('execute successfully!')
            pgisCon.close()
            if title==True:
                col_names = []
                for elt in pgisCursor.description:
                    col_names.append(elt[0])
                return(results,col_names)
            else:
                return results

# from database to local
    def tablemaxcount(self,id_name,table_name):
        cmd='SELECT max(%s) FROM %s'% (id_name,table_name)
        results=self.fetch_execute(cmd)
        return results[0][0]

    def table2pandas_df(self,table_name,cmd=""):# from postgis DB to local but no fileds just values
        # print('downloading data from table[%s]-->pandas dataframe'%table_name)
        if cmd=="":
            cmd='SELECT * FROM %s'%table_name
        elif [True for i in ["select","Select","SELECT"] if i in cmd]:
            #print("an sql command!")
            cmd=cmd
        elif [True for i in ["where","Where","WHERE"] if i in cmd]:
            #print("a condition")
            condition=cmd
            cmd='SELECT * FROM %s %s'%(table_name,condition)
        else:
            print("error")
            return
        results=self.fetch_execute(cmd,title=True)
        return pd.DataFrame(results[0],columns=results[1])

    def table2pandas_df_slow(self,table_name,cmd=""):
        spark_df=self.table2spark_df(table_name,cmd)
        pandas_df=spark_df.toPandas()
        return pandas_df

# from local to database        
    def emptytable(self, table_name, schema): # build a table 
        self.execute("create table IF NOT EXISTS %s(id bigserial not null, %s);"%(table_name,schema))

    def insert_s(self,table_name, schema, values): # insert values into postgis, multilines
        self.execute("INSERT INTO %s (%s) VALUES %s;"%(table_name, schema, values))

    def addgeocolumn(self,table_name,wkt_column="",geo_type="POINT"):
        # create database geo-extension
        try:
            self.execute("create extension postgis;")
        except:
            pass

        # check table existing or not
        have_table=len([True for i in [i[0] for i in self.fetch_execute("select tablename from pg_tables;")] if table_name == i])
        if have_table == 0:
            print('no table')
            return
        else:
            pass

        # check geometry column
        column_list=[i[0] for i in self.fetch_execute("select column_name from information_schema.columns where table_schema='public' and table_name='%s';"%table_name)]
        have_geometry=len([i for i in column_list if 'geometry' == i])

        column_list=[i[0] for i in self.fetch_execute("select column_name from information_schema.columns where table_schema='public' and table_name='%s';"%table_name)]
        if wkt_column !="": #geometry column is wkt
            # check wkt columns existing or not
            if wkt_column in column_list:
                try:
                    if have_geometry == 0: 
                        self.execute('alter table %s add column geometry geometry(%s,4326);'%(table_name,geo_type))
                    else:
                        pass
                    self.execute("update %s set geometry=st_geomfromtext(%s, 4326);"%(table_name,wkt_column))
                except:
                    print('wkt is illegal')
                    print('''
                    Example:
                    POINT (30 10)
                    LINESTRING (30 10, 10 30, 40 40)
                    POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))
                    POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),
(20 30, 35 35, 30 20, 20 30))
                    ''')
                    self.execute('alter table %s drop column geometry;'%table_name)
            else:
                print('wrong wkt name')
                return
        else:
            print('no wkt')
            return

        
            
    def pandas_df2table(self, df, table_name, if_exists='append', id='True', check='False', clean='True',wkt_column="",geo_type="POINT"): # completely insert all data in pandas into table
        '''
        1) table name should not contian ``uppercase letters``!!\
        2) ``special marks`` are in title may casue some errors with a high risk!! --> clean="True"\
        3)``single quota`` marks in the content will also --> check="True"
        '''
        df=df.copy()
        column_list=[i[0] for i in self.fetch_execute("select column_name from information_schema.columns where table_schema='public' and table_name='%s';"%table_name)]
        have_geometry=len([i for i in column_list if 'geometry' == i])
        if have_geometry != 0:
            df['geometry']=np.nan
        else:
            pass
        # print("start to input data to [%s]-->[%s]"%(self.database,table_name))
        if _judgecorrect(id):
            df=self._makeid(df,table_name,head='True') #add title and id
        if _judgecorrect(check):
            df=_checksinglequote(df)
        if _judgecorrect(clean):
            df=_cleanspecialmark(df)
        #print(np.issubdtype(df['id'][0], np.int))
        engineurl='postgresql://%s:%s@%s:%s/%s'%(self.user,self.password,self.ip,self.port,self.database)
        db_engine=create_engine(engineurl)
        
        string_data_io = io.StringIO()
        df.to_csv(string_data_io, sep='|', index=False)
        pd_sql_engine = pd.io.sql.pandasSQL_builder(db_engine)
        if _judgecorrect(id):
            table = pd.io.sql.SQLTable(table_name, pd_sql_engine, frame=df, index=True, index_label='id',keys='id',if_exists=if_exists)  # schema = 'goods_code'
        else:
            table = pd.io.sql.SQLTable(table_name, pd_sql_engine, frame=df, index=False, if_exists=if_exists) #schema = 'goods_code'
        table.create()
        string_data_io.seek(0)
        #string_data_io.readline()  # remove header
        with db_engine.connect() as connection:
            with connection.connection.cursor() as cursor:
                copy_cmd = "COPY %s FROM STDIN HEADER DELIMITER '|' CSV" %table_name #goods_code.
                cursor.copy_expert(copy_cmd, string_data_io)
            connection.connection.commit()
        if wkt_column != "": 
            self.addgeocolumn(table_name=table_name,wkt_column=wkt_column,geo_type=geo_type)
        else:
            print("Finish inputing!")

    def pandas_df2table_slow(self, df, table_name, geo_schema="", check="Yes"): # no more maintenance
        if _judgecorrect(check):
            df=_checksinglequote(df)
        schema=""
        schema_cmd=""
        info=""
        infolist=[]
        # print("start to input data to [%s]-->[%s]"%(self.database,table_name))
        process_bar = ShowProcess(len(df),"Finish inputing!")
        for a in df.columns:
            b=_cleanspecialmark(a)
            if a==geo_schema:
                schema_cmd="%s,%s geometry"%(schema_cmd,b)
            elif df[a].dtypes=='int64':
                schema_cmd="%s,%s bigint"%(schema_cmd,b)
            elif df[a].dtypes=='float64':
                schema_cmd="%s,%s real"%(schema_cmd,b)
            else:
                schema_cmd="%s,%s text"%(schema_cmd,b)
            schema="%s,%s"%(schema,b)
        schema=schema.lstrip(",")
        schema_cmd=schema_cmd.lstrip(',')
        self.emptytable(table_name,schema_cmd) #build an empty table
        for temp in range(len(df)): # input values into the table
            for a in df.columns:
                if a!=geo_schema or geo_schema=="" :
                    info="%s,'%s'"%(info,str(df.iloc[temp][a]))
                else: # with geometry or geography information
                    info="%s,%s"%(info,"st_geomfromtext(\'%s\', 4326)"%str(df.iloc[temp][a]))
            info="(%s)"%info.lstrip(",").replace("\\","")
            infolist.append(info)
            info=""
            if len(infolist)>=500: # each 500 to input
                #print(str(infolist).replace('"','').replace('[','').replace(']',''))
                self.insert_s(table_name,schema,str(infolist).replace('"','').replace('[','').replace(']',''))
                infolist=[]
            process_bar.show_process()
        #input the rest data
        self.insert_s(table_name,schema,str(infolist).replace('"','').replace('[','').replace(']',''))

    def pandas_df2table_lite(self, df, table_name,if_exists='append',clean='False', *args, **kwargs):  
        #print("start to input data to [%s]-->[%s]"%(self.database,table_name))
        engineurl='postgresql://%s:%s@%s:%s/%s'%(self.user,self.password,self.ip,self.port,self.database)
        db_engine=create_engine(engineurl)
        # clean title
        if _judgecorrect(clean):
            df=_cleanspecialmark(df)
        df.to_sql(table_name, con=db_engine,index=False,if_exists=if_exists)
        print("Finish inputing!")

    def excel2table(self,excelpath,table_name, if_exists='fail' ,*args, **kwargs):
        '''
        if your original data are ``clean enough``, you can choose it! otherwise, do data clean first:-)
        '''
        df=pd.read_excel(excelpath)
        self.pandas_df2table(df, table_name, if_exists,*args, **kwargs)

    def csv2table(self,csvpath,table_name, if_exists='fail',*args, **kwargs):
        '''
        if your original data are ``clean enough``, you can choose it! otherwise, do data clean first:-)
        '''
        df=pd.read_csv(csvpath)
        self.pandas_df2table(df, table_name, if_exists,*args, **kwargs)

    def json2table(self,jsonpath,table_name, if_exists='fail',*args, **kwargs):
        '''
        if your original data are ``clean enough``, you can choose it! otherwise, do data clean first:-)
        '''
        df=pd.read_json(jsonpath)
        self.pandas_df2table(df, table_name, if_exists,*args, **kwargs)
# others
    def _makeid(self,df,table_name,head=False):
        df.reset_index(inplace=True, drop=True)
        df2=df.copy()
        df3=df.copy()
        lengthcount=len(df3) 
    
        have_table=len([True for i in [i[0] for i in self.fetch_execute("select tablename from pg_tables;")] if table_name == i])
        if have_table != 0: # table exists
            column_list=[i[0] for i in self.fetch_execute("select column_name from information_schema.columns where table_schema='public' and table_name='%s';"%table_name)]
            id_list=[i for i in column_list if 'id' == i or 'ID' == i or 'Id' == i]
            have_id=len(id_list)

            if have_id != 0: # table exists, ID column exists
                id_column=id_list[0]

                # id primary key
                try:
                    self.execute('alter table %s add constraint %s_id_pk primary key (%s);'%(table_name,table_name,id_column))
                except:
                    pass

                #check seq existing or not
                con_name=[j for j in [i[0] for i in self.fetch_execute("SELECT c.relname FROM pg_class c WHERE c.relkind = 'S';")] if table_name in j]
                seq_id_list=[j for j in con_name if id_column in j]
                have_seq=len(seq_id_list)  
                
                if have_seq != 0: #table exists, ID column exists, id sequence exists
                    seq_id=seq_id_list[0]
                    try:# if success, the id not null
                        maxcount=self.tablemaxcount(id_column,table_name) 
                        maxcount+=0
                    except: #null
                        self.execute("update %s set %s = nextval('%s');"%(table_name,id_column,seq_id))
                        try: 
                            maxcount=self.tablemaxcount(id_column,table_name)
                            maxcount+=0
                        except: #without any conetext but a title
                            maxcount=0
                    self.execute(
'''alter sequence %s
restart with %s
increment by 1
no minvalue
no maxvalue
cache 1;
alter table %s alter column %s set default nextval ('%s');'''
                        % (seq_id, maxcount+lengthcount+1, table_name,id_column, seq_id))
                else: #table exists, ID column exists, ID column do not exist
                    seq_id="%s_id_seq"%table_name
                    self.execute(
'''create sequence %s
start with 1
increment by 1
no minvalue
no maxvalue
cache 1;
alter table %s alter column %s set default nextval ('%s');'''
                %(seq_id,table_name,id_column,seq_id))             
                    try:#  id is null or not
                        maxcount=self.tablemaxcount(id_column,table_name) 
                        maxcount+=0
                    except: #id is null
                        self.execute("update %s set %s = nextval('%s');"%(table_name,id_column,seq_id))
                        try: #after updating, id is still null or not
                            maxcount=self.tablemaxcount(id_column,table_name)
                            maxcount+=0
                        except: #id is still null, table without any conetext but a title
                            maxcount=0
                    self.execute(
'''alter sequence %s
restart with %s
increment by 1
no minvalue
no maxvalue
cache 1;
alter table %s alter column %s set default nextval ('%s');'''
                        % (seq_id, maxcount+lengthcount+1, table_name,id_column, seq_id))

                id_pos=column_list.index(id_column)
                id_ser = pd.Series(range(maxcount+1,maxcount+lengthcount+1),dtype='int')
                df3.insert(id_pos,id_column,id_ser)

            else: # table exists, ID column do not exist
                #check sequence,delete the existing seq before table building
                con_name=[j for j in [i[0] for i in self.fetch_execute("SELECT c.relname FROM pg_class c WHERE c.relkind = 'S';")] if table_name in j]
                extra_id_seq=[i for i in con_name if 'id' in i or 'ID' in i or 'Id' in i ]
                for i in extra_id_seq:
                    cmd="DROP SEQUENCE IF EXISTS %s"% i
                    self.execute(cmd)

                self.execute("alter table %s add column id int;"%table_name)
                self.execute(
'''create sequence %s_id_seq
start with 1
increment by 1
no minvalue
no maxvalue
cache 1;
alter table %s alter column id set default nextval ('%s_id_seq');'''
                %(table_name,table_name,table_name))
                self.execute("update %s set id = nextval('%s_id_seq');"%(table_name, table_name))  
                self.execute('alter table %s add constraint %s_id_pk primary key (id);'%(table_name,table_name))
                try: #if table is single-title-no-data table
                    maxcount=self.tablemaxcount('id',table_name)
                    maxcount+=0
                except:
                    maxcount=0
                self.execute(
'''alter sequence %s_id_seq
restart with %s
increment by 1
no minvalue
no maxvalue
cache 1;
alter table %s alter column id set default nextval ('%s_id_seq');'''
                        % (table_name, maxcount+lengthcount+1, table_name, table_name))
                id_ser = pd.Series(range(maxcount+1,maxcount+lengthcount+1),dtype='int')
                df3['id']=id_ser

        else: # don't have table
            #check sequence,delete the existing seq before table building
            con_name=[j for j in [i[0] for i in self.fetch_execute("SELECT c.relname FROM pg_class c WHERE c.relkind = 'S';")] if table_name in j]
            for i in con_name:
                cmd="DROP SEQUENCE IF EXISTS %s"% i
                self.execute(cmd)

            id_ser = pd.Series(range(1,lengthcount+1),dtype='int')
            if _judgecorrect(head):
                df3.insert(0,'id',id_ser)
            else:
                df3['id']=id_ser  
                
            if _judgecorrect(head):
                df2.insert(0,'id',0)
            else:
                df2['id']=0
            self.pandas_df2table(df=df2.head(1),table_name=table_name,if_exists='fail',id=False,check=False,clean=False)
            self.execute('DELETE FROM %s where id=0'%table_name)
            self.execute('alter table %s add constraint %s_id_pk primary key (id);'%(table_name,table_name))
            self.execute(
'''create sequence %s_id_seq
start with 1
increment by 1
no minvalue
no maxvalue
cache 1;
alter table %s alter column id set default nextval ('%s_id_seq');''' 
            % (table_name, table_name, table_name))
        return df3
    
def _checksinglequote(df): #CHECK TEXT IN DATAFRAME. TEXT WITH SINGLE QUOTA CAN NOT BE INPUT INTO DB
    # print("start to clean single quotes")
    process_bar = ShowProcess(len(df.columns),"Finish cleaning them!")
    for a in df.columns:
        try: # single ' can not be input into db ,for double ''
            df[a]=df.apply(lambda x: x[a].replace("'","''").replace("\"","''"), axis=1)
        except:
            pass
        process_bar.show_process()
    return df

def _cleanspecialmark(df):
    try:
        for a in df.columns:
            pass
        # pandas type
        # print("start to clean special marks in titles")
        process_bar = ShowProcess(len(df.columns),"Finish cleaning them!")
        for a in df.columns:
            try: # single ' can not be input into db ,for double ''
                b=re.sub("[\s+\.\-:?!\/,$%^*()+\"\']+|[+——！，。？、~@#￥%……&*（）]+","",str(a).replace(" ","_").replace("[","").replace("]",""))
                b=b.lower()
                df=df.rename(columns={a:b})  
            except:
                pass
            process_bar.show_process()
        return df
    except:
        #str type
        return re.sub("[\s+\.\-:?!\/,$%^*()+\"\']+|[+——！，。？、~@#￥%……&*（）]+","",str(df).replace(" ","_").replace("[","").replace("]",""))
    else:
        return df

def _judgecorrect(_):
    if _ in ["True","TRUE","true","T","YES","yes","Yes","OK","ok","Ok",True]:#True,
        return True
    else:
        return False

