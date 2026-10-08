# author: GUO ZIJIAN from PolyU
import io
import os
import re
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from link2postgresql.progressbar import ShowProcess
import link2postgresql
from urllib.request import quote

# Heavy / optional dependencies (psycopg2, pyspark, geopandas) are imported
# lazily inside the methods that need them, so that simply importing this
# package no longer requires a full Spark / JDK / Hadoop / PostGIS stack.


class Link2postgresql(object):

    def __init__(self, user="postgres", password="postgres", ip="localhost", port="5432", database="postgres", *args, **kwargs):
        '''DB means postgresql database\n
        (1)``spark``: DB to dataframe(spark); dataframe(spark) to DB\n
        (2)``pandas``: DB to dataframe(pandas); dataframe(pandas) to DB <normal ,slow ,light>\n
        (3)others: build empty table; insert values; csv,excel,json to DB \n
        '''
        self.user = user
        self.password = password
        self.ip = ip
        self.port = port
        self.database = database

    # ------------------------------------------------------------------
    # internal DB helper: one robust connection/execute layer.
    # All SQL *values* are passed as bound parameters; *identifiers*
    # (table / column names) are quoted with _qi().
    # ------------------------------------------------------------------
    @staticmethod
    def _qi(name):
        """Safely quote a PostgreSQL identifier (table / column name)."""
        return '"' + str(name).replace('"', '""') + '"'

    def _connect(self):
        import psycopg2
        return psycopg2.connect(
            database=self.database, user=self.user,
            password=self.password, host=self.ip, port=self.port)

    def _run(self, cmd, params=None, title=False):
        """Run a SQL command. Returns fetched rows (tuple) when title=True,
        otherwise returns the rows or None on any failure. Connection and
        statement errors are caught and reported instead of crashing."""
        import psycopg2
        try:
            con = self._connect()
        except psycopg2.OperationalError as e:
            print('connect db failure: %s' % e)
            return None
        cur = None
        try:
            cur = con.cursor()
            cur.execute(cmd, params)
            if cur.description is not None:
                results = cur.fetchall()
                cols = [d[0] for d in cur.description]
            else:
                results, cols = None, []
            con.commit()
        except psycopg2.Error as e:
            try:
                con.rollback()
            except Exception:
                pass
            print('execute cmd failure: %s' % e)
            results, cols = None, []
        finally:
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass
            try:
                con.close()
            except Exception:
                pass
        if title:
            return (results, cols)
        return results

# about spark
    def startspark(self):
        from pyspark.context import SparkContext
        from pyspark.sql import SparkSession
        if SparkContext._active_spark_context:
            self.sc = SparkContext._active_spark_context
            self.spark = SparkSession.builder.getOrCreate()
            self.sqlContext = self.spark
        else:
            self.spark = self.buildspark()
            self.sc = self.spark.sparkContext
            self.sqlContext = self.spark

    def restartspark(self):
        try:
            self.sc.stop()
        except Exception:
            pass
        self.spark = self.buildspark()
        self.sc = self.spark.sparkContext
        self.sqlContext = self.spark

# about spark
    def buildspark(self, appname="sparkapp"):
        from pyspark.conf import SparkConf
        from pyspark.sql import SparkSession
        conf = SparkConf().setAppName(appname).setAll([
            ('spark.executor.memory', '2g'),
            ('spark.cores.max', '4'),
            ('spark.executor.cores', '4'),
            ('spark.driver.memory', '8g'),
            ('spark.default.parallelism', '4'),
            ('spark.sql.warehouse.dir', r"%s/spark-warehouse" % os.path.dirname(link2postgresql.__file__)),
            ('spark.driver.extraClassPath', r"%s/driver/postgresql-42.2.2.jar" % os.path.dirname(link2postgresql.__file__)),
            ('spark.driver.allowMultipleContexts', 'true'),
            ('spark.network.timeout', '10000000'),
            ('spark.core.connection.ack.wait.timeout', '10000000'),
            ('spark.storage.blockManagerSlaveTimeoutMs', '10000000'),
            ('spark.shuffle.io.connectionTimeout', '10000000'),
            ('spark.rpc.askTimeout', '10000000'),
            ('spark.rpc.lookupTimeout', '10000000'),
        ])
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        # enable arrow for faster pandas<->spark conversion (Spark 2 & 3 keys)
        try:
            spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        except Exception:
            pass
        try:
            spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        except Exception:
            pass
        return spark

    def table2spark_df(self, table_name, cmd=""):
        '''
        use for reading ``postgresql database`` tables and transforming it into spark dataframe.
        '''
        self.startspark()
        url = "jdbc:postgresql://%s:%s/%s?user=%s&password=%s" % (
            quote(self.ip), quote(self.port), quote(self.database),
            quote(self.user), quote(self.password))
        cmd = (cmd or "").strip()
        if cmd == "":
            dbtable = table_name
        elif cmd.lower().startswith("select"):
            dbtable = "(%s) as tmp" % cmd
        elif cmd.lower().startswith("where"):
            dbtable = "(SELECT * FROM %s %s) as tmp" % (self._qi(table_name), cmd)
        else:
            # not a clean keyword prefix: if it looks like a full query use it,
            # otherwise treat it as a WHERE condition.
            if "select" in cmd.lower():
                dbtable = "(%s) as tmp" % cmd
            else:
                dbtable = "(SELECT * FROM %s %s) as tmp" % (self._qi(table_name), cmd)
        spark_df = self.sqlContext.read.format("jdbc").option("url", url).option("dbtable", dbtable).load()
        return spark_df

    def table2spark_df_slow(self, table_name, cmd=""):  # have downloaded
        pandas_df = self.table2pandas_df_slow(table_name, cmd)
        spark_df = self.sqlContext.createDataFrame(pandas_df)
        return spark_df

    def spark_df2table(self, df, table_name, mode="append", wkt_column="", geo_type="POINT", drop_wkt='False'):
        '''
        use for updating spark dataframe into ``postgresql``table in the servers.
        When ``wkt_column`` is supplied, the Spark DataFrame must carry that WKT
        text column; it is written as TEXT and then converted to a PostGIS
        geometry column (same as the pandas path), and optionally dropped.
        '''
        self.startspark()
        url = "jdbc:postgresql://%s:%s/%s?user=%s&password=%s" % (
            quote(self.ip), quote(self.port), quote(self.database),
            quote(self.user), quote(self.password))
        # If the wkt text column was dropped earlier, re-add it (TEXT) so the
        # JDBC append matches the incoming DataFrame's schema.
        if wkt_column != "":
            self._ensure_text_column(table_name, wkt_column)
        df.write.jdbc(url, table=table_name, mode=mode)
        if wkt_column != "":
            self.addgeocolumn(table_name=table_name, wkt_column=wkt_column, geo_type=geo_type)
            if _judgecorrect(drop_wkt):
                self._run("ALTER TABLE %s DROP COLUMN IF EXISTS %s;" % (self._qi(table_name), self._qi(wkt_column)))

# base methods
    def execute(self, cmd):  # execute the cmd to control Postgis
        return self._run(cmd)

    def fetch_execute(self, cmd, title=False):
        return self._run(cmd, title=title)

# from database to local
    def tablemaxcount(self, id_name, table_name):
        cmd = 'SELECT max(%s) FROM %s' % (self._qi(id_name), self._qi(table_name))
        results = self._run(cmd)
        if not results:
            return 0
        return results[0][0]

    def table2pandas_df(self, table_name, cmd="", geo_scheme=""):  # from postgis DB to local but no fileds just values
        if geo_scheme == '':
            cmd = (cmd or "").strip()
            if cmd == "":
                cmd = 'SELECT * FROM %s' % self._qi(table_name)
            elif cmd.lower().startswith("select"):
                pass  # already a full query
            elif cmd.lower().startswith("where"):
                cmd = 'SELECT * FROM %s %s' % (self._qi(table_name), cmd)
            else:
                # assume a full query was supplied
                pass
            res = self.fetch_execute(cmd, title=True)
            if res is None:
                return None
            return pd.DataFrame(res[0], columns=res[1])
        else:
            from geopandas import read_postgis
            engineurl = 'postgresql://%s:%s@%s:%s/%s' % (self.user, self.password, self.ip, self.port, self.database)
            db_engine = create_engine(engineurl)
            return read_postgis(cmd, con=db_engine, geom_col=geo_scheme)

    def table2pandas_df_slow(self, table_name, cmd=""):
        spark_df = self.table2spark_df(table_name, cmd)
        pandas_df = spark_df.toPandas()
        return pandas_df

# from local to database
    def emptytable(self, table_name, schema):  # build a table
        self.execute("CREATE TABLE IF NOT EXISTS %s(id bigserial not null, %s);" % (self._qi(table_name), schema))

    def insert_s(self, table_name, schema, values):  # insert values into postgis, multilines
        self.execute("INSERT INTO %s (%s) VALUES %s;" % (self._qi(table_name), schema, values))

    def addgeocolumn(self, table_name, wkt_column="", geo_type="POINT"):
        t = self._qi(table_name)
        # create database geo-extension (CREATE EXTENSION IF NOT EXISTS is safe to re-run)
        try:
            self.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        except Exception:
            pass

        # check table existing or not (use a parameterised query, compare in python)
        tables = self._run("SELECT tablename FROM pg_tables WHERE tablename=%s;", (table_name,))
        if not tables:
            print('no table')
            return

        cols = self._run(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s;",
            (table_name,))
        col_names = [r[0] for r in cols] if cols else []
        have_geometry = any(c == 'geometry' for c in col_names)

        if wkt_column == "":
            print('no wkt')
            return
        if wkt_column not in col_names:
            print('wrong wkt name')
            return

        if not have_geometry:
            self._run("ALTER TABLE %s ADD COLUMN geometry geometry(%s,4326);" % (t, geo_type))
        # Only (re)build geometry for rows where it is still NULL. This makes
        # appends incremental (fast on large tables) and avoids re-parsing every
        # existing row -- which would otherwise raise an error and roll back the
        # whole append if any *old* row has bad/empty WKT after the first load.
        self._run("UPDATE %s SET geometry=ST_GeomFromText(%s,4326) WHERE geometry IS NULL;" % (t, self._qi(wkt_column)))

    def geopandas_df2table(self, df, table_name, if_exists='append'):
        engineurl = 'postgresql://%s:%s@%s:%s/%s' % (self.user, self.password, self.ip, self.port, self.database)
        db_engine = create_engine(engineurl)
        try:
            df.to_postgis(name=table_name, con=db_engine, if_exists=if_exists)
        except Exception as e:
            print('please input geopandas dataframe!! (%s)' % e)

    def _ensure_id_column(self, table_name):
        """Make sure the table has an auto-increment ``id`` primary key column.
        The auto-generated id is left to PostgreSQL's bigserial, so it always
        continues correctly even across multiple appends."""
        t = self._qi(table_name)
        res = self._run(
            "SELECT 1 FROM information_schema.columns WHERE table_schema='public' "
            "AND table_name=%s AND column_name='id';", (table_name,))
        if res:
            return
        self._run("ALTER TABLE %s ADD COLUMN id bigserial PRIMARY KEY;" % t)

    def _ensure_text_column(self, table_name, col_name):
        """Make sure the table has a TEXT column named ``col_name`` to carry the
        raw WKT before it is converted to a geometry column. If the table does
        not exist yet (first load), ``to_sql`` will create it together with this
        column, so we do nothing. If the table exists but the column is missing
        (e.g. it was dropped earlier to save space), we add it back so a later
        append can still derive geometry."""
        t = self._qi(table_name)
        tbl = self._run("SELECT 1 FROM pg_tables WHERE tablename=%s;", (table_name,))
        if not tbl:
            return
        res = self._run(
            "SELECT 1 FROM information_schema.columns WHERE table_schema='public' "
            "AND table_name=%s AND column_name=%s;", (table_name, col_name))
        if res:
            return
        self._run("ALTER TABLE %s ADD COLUMN %s TEXT;" % (t, self._qi(col_name)))

    def drop_wkt_column(self, table_name, wkt_column):
        """Explicitly drop a raw WKT text column once you no longer need it
        (e.g. after the geometry column is built and you are done appending).
        Safe to call multiple times (uses IF EXISTS)."""
        self._run("ALTER TABLE %s DROP COLUMN IF EXISTS %s;" % (self._qi(table_name), self._qi(wkt_column)))

    def pandas_df2table(self, df, table_name, if_exists='append', id='True', check='False', clean='True', wkt_column="", geo_type="POINT", drop_wkt='False'):
        '''
        1) table name should not contian ``uppercase letters``!!\
        2) ``special marks`` in title may cause errors --> clean="True"\
        3) ``single quote`` marks in the content are handled safely by the
           SQLAlchemy parameterised writer, so ``check`` is no longer needed.
        4) ``drop_wkt='True'`` removes the raw WKT text column after the
           geometry column is built, to save storage. The text column is
           re-created automatically on every load (see _ensure_text_column),
           so appending more rows keeps working.
        '''
        df = df.copy()
        orig_cols = list(df.columns)
        if _judgecorrect(clean):
            df = _cleanspecialmark(df)
        # column names are cleaned, so the user-supplied wkt_column must be
        # translated to its cleaned form, otherwise addgeocolumn can't find it.
        if wkt_column and wkt_column in orig_cols:
            wkt_column = _clean_name(wkt_column)

        engineurl = 'postgresql://%s:%s@%s:%s/%s' % (self.user, self.password, self.ip, self.port, self.database)
        db_engine = create_engine(engineurl)

        # If a wkt column is requested and the target table already exists but is
        # missing that column (e.g. it was dropped earlier via drop_wkt), re-add
        # it as TEXT so the load + geometry derivation can still run.
        if wkt_column != "":
            self._ensure_text_column(table_name, wkt_column)

        # When id is requested we let PostgreSQL generate it (bigserial), so we
        # must NOT write an explicit id column from the dataframe.
        if _judgecorrect(id):
            load_cols = [c for c in df.columns if str(c).lower() != 'id']
        else:
            load_cols = list(df.columns)

        sub = df[load_cols]
        # method='multi' batches INSERTs for speed; values are parameterised -> injection safe.
        sub.to_sql(table_name, con=db_engine, if_exists=if_exists, index=False, method='multi')

        if _judgecorrect(id):
            self._ensure_id_column(table_name)

        if wkt_column != "":
            self.addgeocolumn(table_name=table_name, wkt_column=wkt_column, geo_type=geo_type)
            # Once the geometry column is built, the raw WKT text is redundant.
            # Drop it to save storage (re-created automatically on next load).
            if _judgecorrect(drop_wkt):
                self._run("ALTER TABLE %s DROP COLUMN IF EXISTS %s;" % (self._qi(table_name), self._qi(wkt_column)))
        else:
            # If the target table already carries a PostGIS geometry column but
            # the caller did not pass a wkt_column, newly appended rows will end
            # up with NULL geometry (silent data loss). Warn about it.
            geo = self._run(
                "SELECT 1 FROM information_schema.columns WHERE table_schema='public' "
                "AND table_name=%s AND column_name='geometry';", (table_name,))
            if geo:
                print("Warning: '%s' already has a geometry column but no wkt_column was "
                      "supplied; appended rows will get NULL geometry." % table_name)
            print("Finish inputing!")

    def pandas_df2table_slow(self, df, table_name, geo_schema="", check="Yes", drop_wkt='False'):  # no more maintenance
        # The geometry column (geo_schema) is just a WKT text column; the main
        # path handles it via addgeocolumn after load, so we simply delegate.
        self.pandas_df2table(
            df, table_name, if_exists='append', id='True',
            check='False', clean='True', wkt_column=geo_schema, geo_type="POINT", drop_wkt=drop_wkt)

    def pandas_df2table_lite(self, df, table_name, if_exists='append', clean='False', wkt_column="", geo_type="POINT", drop_wkt='False', *args, **kwargs):
        '''Lightweight pandas upload (no id auto-column). When ``wkt_column`` is
        supplied it generates a PostGIS geometry column exactly like
        ``pandas_df2table`` (including append-after-drop support).'''
        df = df.copy()
        orig_cols = list(df.columns)
        if _judgecorrect(clean):
            df = _cleanspecialmark(df)
        if wkt_column and wkt_column in orig_cols:
            wkt_column = _clean_name(wkt_column)
        engineurl = 'postgresql://%s:%s@%s:%s/%s' % (self.user, self.password, self.ip, self.port, self.database)
        db_engine = create_engine(engineurl)
        if wkt_column != "":
            self._ensure_text_column(table_name, wkt_column)
        df.to_sql(table_name, con=db_engine, index=False, if_exists=if_exists)
        if wkt_column != "":
            self.addgeocolumn(table_name=table_name, wkt_column=wkt_column, geo_type=geo_type)
            if _judgecorrect(drop_wkt):
                self._run("ALTER TABLE %s DROP COLUMN IF EXISTS %s;" % (self._qi(table_name), self._qi(wkt_column)))
        print("Finish inputing!")

    def excel2table(self, excelpath, table_name, if_exists='fail', *args, **kwargs):
        '''
        if your original data are ``clean enough``, you can choose it! otherwise, do data clean first:-)
        '''
        df = pd.read_excel(excelpath)
        self.pandas_df2table(df, table_name, if_exists, *args, **kwargs)

    def csv2table(self, csvpath, table_name, if_exists='fail', *args, **kwargs):
        '''
        if your original data are ``clean enough``, you can choose it! otherwise, do data clean first:-)
        '''
        df = pd.read_csv(csvpath)
        self.pandas_df2table(df, table_name, if_exists, *args, **kwargs)

    def json2table(self, jsonpath, table_name, if_exists='fail', *args, **kwargs):
        '''
        if your original data are ``clean enough``, you can choose it! otherwise, do data clean first:-)
        '''
        df = pd.read_json(jsonpath)
        self.pandas_df2table(df, table_name, if_exists, *args, **kwargs)


def _judgecorrect(_):
    if _ in ["True", "TRUE", "true", "T", "YES", "yes", "Yes", "OK", "ok", "Ok", True]:
        return True
    else:
        return False


# One shared regex + transform so that a single column name and a whole
# DataFrame are cleaned identically (important for resolving wkt_column).
_COLNAME_PATTERN = re.compile(r"[\s+\.\-:?!/,$%^*()+\"']+|[+——！，。？、~@#￥%……&*（）]+")


def _clean_name(name):
    """Clean a single column/identifier name: strip spaces & special marks,
    replace spaces with '_', lower-case."""
    return _COLNAME_PATTERN.sub("", str(name).replace(" ", "_").replace("[", "").replace("]", "")).lower()


def _cleanspecialmark(df):
    """Clean column names (strip spaces / special marks, lower-case).
    Operates on a snapshot of the column list so renaming mid-iteration is safe."""
    try:
        rename_map = {}
        for a in list(df.columns):
            b = _clean_name(a)
            if b and b != a:
                rename_map[a] = b
        if rename_map:
            df = df.rename(columns=rename_map)
        return df
    except Exception:
        # str type fallback
        return _COLNAME_PATTERN.sub("", str(df).replace(" ", "_").replace("[", "").replace("]", ""))
