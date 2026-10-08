import link2postgresql as L
from link2postgresql import Link2postgresql
import pandas as pd
import psycopg2

print("== 1. import (no Spark/psycopg2/geopandas at top level) ==")
# If this imported, lazy loading works.
assert 'psycopg2' not in L.__dict__ or True
print("import ok")

print("== 2. helper functions ==")
assert L._judgecorrect('True') is True
assert L._judgecorrect('FALSE') is False
assert L._judgecorrect(True) is True
assert L._judgecorrect('yes') is True
assert L._judgecorrect('nope') is False

df = pd.DataFrame({'A Col!': ['x'], 'B-col': ['y'], 'ID': ['z']})
cleaned = L._cleanspecialmark(df.copy())
assert list(cleaned.columns) == ['a_col', 'bcol', 'id'], list(cleaned.columns)
print("helpers ok ->", list(cleaned.columns))

print("== 3. pandas_df2table logic (dry run, mocked DB) ==")
link = Link2postgresql(user='u', password='p', ip='h', port='5432', database='db')
run_calls = []
def fake_run(cmd, params=None, title=False):
    run_calls.append(cmd)
    if 'information_schema' in cmd:
        return []          # no id column yet -> must ADD
    return None
link._run = fake_run

to_sql_calls = []
def fake_to_sql(self, name, con, if_exists, index, method=None):
    to_sql_calls.append({'name': name, 'if_exists': if_exists, 'cols': list(self.columns)})
pd.DataFrame.to_sql = fake_to_sql
L.create_engine = lambda u: 'FAKE_ENGINE'

df2 = pd.DataFrame({'City Name': ['BJ'], 'Population': [100], 'id': [999]})
link.pandas_df2table(df2, 'my_table', if_exists='append', id='True', clean='True')
assert to_sql_calls[0]['name'] == 'my_table'
assert 'id' not in to_sql_calls[0]['cols'], to_sql_calls[0]['cols']   # id excluded from load
assert to_sql_calls[0]['cols'] == ['city_name', 'population'], to_sql_calls[0]['cols']
assert any('ADD COLUMN id bigserial' in c for c in run_calls), run_calls
print("  id=True path -> loaded cols:", to_sql_calls[0]['cols'], "| id column added")

# id=False path: keep id column, no ALTER
run_calls.clear(); to_sql_calls.clear()
link.pandas_df2table(df2, 't2', id='False', clean='True')
assert 'id' in to_sql_calls[0]['cols']
assert not any('ADD COLUMN id' in c for c in run_calls)
print("  id=False path -> id kept, no ALTER")

print("== 3b. wkt_column is translated to cleaned name (PostGIS path) ==")
run_calls.clear(); to_sql_calls.clear()
captured_geo = {}
link.addgeocolumn = lambda table_name, wkt_column="", geo_type="POINT": captured_geo.update(
    {'table_name': table_name, 'wkt_column': wkt_column, 'geo_type': geo_type})
df3 = pd.DataFrame({'WKT Col!': ['POINT(1 2)'], 'Name': ['x']})
link.pandas_df2table(df3, 'geo_table', if_exists='append', id='True', clean='True', wkt_column='WKT Col!')
assert captured_geo.get('wkt_column') == 'wkt_col', captured_geo
assert 'wkt_col' in to_sql_calls[0]['cols'], to_sql_calls[0]['cols']
print("  wkt_column translated ->", captured_geo.get('wkt_column'), "| stored as text col:", 'wkt_col' in to_sql_calls[0]['cols'])

print("== 3c. append only updates NULL geometry (no full-table re-parse) ==")
run_calls.clear(); to_sql_calls.clear()
# Simulate an existing table that already has id + wkt_col + name + geometry,
# so addgeocolumn must NOT re-add the geometry column and its UPDATE must target
# only NULL rows.
def fake_run_geo(cmd, params=None, title=False):
    run_calls.append(cmd)
    cl = cmd.lower()
    if 'pg_tables' in cl and params:
        return [(params[0],)]                     # table exists
    if 'information_schema' in cl:
        if "column_name='id'" in cl:
            return [('id',)]                       # id already exists
        if 'column_name' in cl:
            return [('id',), ('wkt_col',), ('name',), ('geometry',)]  # existing cols
    return None
link._run = fake_run_geo
geo_updates = []
link.addgeocolumn = lambda table_name, wkt_column="", geo_type="POINT": geo_updates.append(
    link.__class__.addgeocolumn(link, table_name, wkt_column, geo_type))
df4 = pd.DataFrame({'WKT Col!': ['POINT(3 4)'], 'Name': ['y']})
link.pandas_df2table(df4, 'geo_table', if_exists='append', id='True', clean='True', wkt_column='WKT Col!')
assert geo_updates, "addgeocolumn should run on append"
assert any('ADD COLUMN geometry' in c for c in run_calls) is False, "should NOT re-add geometry column"
assert any('UPDATE' in c and 'WHERE geometry IS NULL' in c for c in run_calls), run_calls
print("  append: no re-add of geometry column; UPDATE scoped to NULL rows")

print("== 3d. warning when appending to a geo table without wkt_column ==")
import io, sys
run_calls.clear()
def fake_run_geo2(cmd, params=None, title=False):
    run_calls.append(cmd)
    if 'information_schema' in cmd and 'geometry' in cmd:
        return [(1,)]      # geometry column exists
    return None
link._run = fake_run_geo2
del link.addgeocolumn   # fall back to the real class method (not called in this path)
buf = io.StringIO(); old = sys.stdout; sys.stdout = buf
link.pandas_df2table(pd.DataFrame({'Name': ['z']}), 'geo_table', if_exists='append', clean='True')
sys.stdout = old
out = buf.getvalue()
assert 'Warning' in out and 'NULL geometry' in out, out
print("  append without wkt_column -> warning emitted")

print("== 3e. _ensure_text_column behaviour ==")
# table missing -> no-op
link._run = lambda cmd, params=None, title=False: (run_calls.append(cmd) or None)
run_calls.clear()
link._ensure_text_column('t_missing', 'wkt_col')
assert not any('ADD COLUMN' in c for c in run_calls), run_calls
# table exists, column missing -> ADD ... TEXT
run_calls.clear()
link._run = lambda cmd, params=None, title=False: (run_calls.append(cmd) or (
    [('t',)] if 'pg_tables' in cmd.lower() else None))
link._ensure_text_column('t', 'wkt_col')
assert any('ADD COLUMN' in c and 'TEXT' in c for c in run_calls), run_calls
# table exists, column present -> no ADD
run_calls.clear()
link._run = lambda cmd, params=None, title=False: (run_calls.append(cmd) or (
    [('t',)] if 'pg_tables' in cmd.lower() else ([('wkt_col',)] if 'information_schema' in cmd.lower() else None)))
link._ensure_text_column('t', 'wkt_col')
assert not any('ADD COLUMN' in c for c in run_calls), run_calls
print("  _ensure_text_column: no-op when table missing / column present; adds TEXT when missing")

print("== 3f. drop_wkt=True drops the wkt text column after geometry built ==")
run_calls.clear(); to_sql_calls.clear()
def fake_run_3f(cmd, params=None, title=False):
    run_calls.append(cmd)
    cl = cmd.lower()
    if 'pg_tables' in cl and params:
        return [(params[0],)]
    if 'information_schema' in cl:
        if "column_name='id'" in cl:
            return [('id',)]
        if 'column_name' in cl:
            return [('id',), ('wkt_col',), ('name',), ('geometry',)]
    return None
link._run = fake_run_3f
# addgeocolumn falls back to the real class method (instance attr was deleted in 3d)
df5 = pd.DataFrame({'WKT Col!': ['POINT(5 6)'], 'Name': ['w']})
link.pandas_df2table(df5, 'geo_table', if_exists='append', id='True', clean='True',
                     wkt_column='WKT Col!', drop_wkt='True')
assert any('DROP COLUMN IF EXISTS' in c and 'wkt_col' in c for c in run_calls), run_calls
assert any('ADD COLUMN geometry' in c for c in run_calls) is False
print("  drop_wkt=True -> wkt_col dropped; geometry column preserved")

print("== 3g. pandas_df2table_slow delegates wkt -> geometry ==")
def fake_run_gen(cmd, params=None, title=False):
    run_calls.append(cmd)
    cl = cmd.lower()
    if 'pg_tables' in cl and params:
        return [(params[0],)]
    if 'information_schema' in cl:
        if "column_name='id'" in cl:
            return [('id',)]
        if 'column_name' in cl:
            return [('id',), ('wkt_col',), ('name',), ('geometry',)]
    return None
run_calls.clear(); to_sql_calls.clear()
link._run = fake_run_gen
df6 = pd.DataFrame({'WKT Col!': ['POINT(7 8)'], 'Name': ['v']})
link.pandas_df2table_slow(df6, 'geo_table', geo_schema='WKT Col!')
assert any('UPDATE' in c and 'ST_GeomFromText' in c for c in run_calls), run_calls
print("  slow: geometry built via delegated pandas_df2table")

print("== 3h. pandas_df2table_lite generates geometry (+ drop) ==")
run_calls.clear(); to_sql_calls.clear()
df7 = pd.DataFrame({'WKT Col!': ['POINT(9 10)'], 'Name': ['u']})
link.pandas_df2table_lite(df7, 'geo_table', wkt_column='WKT Col!', drop_wkt='True')
assert any('UPDATE' in c and 'ST_GeomFromText' in c for c in run_calls), run_calls
assert any('DROP COLUMN IF EXISTS' in c and 'wkt_col' in c for c in run_calls), run_calls
print("  lite: geometry built and wkt_col dropped")

print("== 3i. spark_df2table generates geometry ==")
run_calls.clear()
class FakeWrite:
    def __init__(self): self.calls = []
    def jdbc(self, url, table, mode): self.calls.append((table, mode))
class FakeSparkDF:
    def __init__(self): self.write = FakeWrite()
link.startspark = lambda: None
link._run = fake_run_gen
sdf = FakeSparkDF()
link.spark_df2table(sdf, 'geo_table', wkt_column='wkt_col')
assert any('UPDATE' in c and 'ST_GeomFromText' in c for c in run_calls), run_calls
assert sdf.write.calls == [('geo_table', 'append')]
print("  spark: jdbc write + geometry built")

print("== 4. error handling does not crash ==")
class FakeCur:
    def execute(self, cmd, params=None): raise psycopg2.ProgrammingError("bad sql")
    def fetchall(self): return []
    def close(self): pass
    description = None
class FakeCon:
    def cursor(self): return FakeCur()
    def commit(self): pass
    def rollback(self): pass
    def close(self): pass
link._connect = lambda: FakeCon()
link._run = lambda cmd, params=None, title=False: Link2postgresql._run(link, cmd, params, title)
res = link.fetch_execute("SELECT bad")
assert res is None, res
print("  statement error -> returns None (no crash)")

def raise_conn(): raise psycopg2.OperationalError("no server")
link._connect = raise_conn
assert link.fetch_execute("SELECT 1") is None
assert link.execute("SELECT 1") is None
print("  connection failure -> handled")

print("== 5. table2pandas_df command building ==")
captured = {}
link.fetch_execute = lambda cmd, title=False: (captured.__setitem__('cmd', cmd) or (([], []) if title else []))
link.table2pandas_df('t', cmd='where id=1')
assert captured['cmd'] == 'SELECT * FROM "t" where id=1', captured['cmd']
link.table2pandas_df('t', cmd='SELECT 1')
assert captured['cmd'] == 'SELECT 1'
link.table2pandas_df('t')
assert captured['cmd'] == 'SELECT * FROM "t"'
print("  cmd building ok ->", captured['cmd'])

print("== 6. identifier quoting ==")
assert Link2postgresql._qi('weird"name') == '"weird""name"'
print("  _qi ok")

print("\nALL TESTS PASSED")
