# CnosDB Python Connector

CnosDB Python Connector repository contains the Python client library for the [CnosDB](https://github.com/cnosdb/cnosdb). cnosdb_connector adapted for PEP249.

## Installation

use pip install it from pypi, **Python 3.6** or later is required.

```
pip install cnos-connector
```

Then import the package:

```
import cnosdb_connector
```

## Getting Started

#### Query use SQL

```python
from cnosdb_connector import connect

conn = connect(url="http://127.0.0.1:31007/", user="root", password="")
resp = conn.execute("SHOW DATABASES")
print(resp)
```

#### Query use interface

```python
from cnosdb_connector import connect

conn = connect(url="http://127.0.0.1:31007/", user="root", password="")
conn.create_database("air")
resp = conn.list_database()
print(resp)
```

#### Query use PEP-249

```python
from cnosdb_connector import connect

conn = connect(url="http://127.0.0.1:31007/", user="root", password="")
cursor = conn.cursor()

cursor.execute("SHOW DATABASES")
resp = cursor.fetchall()
print(resp)
```

#### Query use pandas

```python
import pandas as pd
from cnosdb_connector import connect

conn = connect(url="http://127.0.0.1:31007/", user="root", password="")

resp = pd.read_sql("SHOW DATABASES", conn)
print(resp)
```

#### Write use LineProtocol

```python
from cnosdb_connector import connect

line0 = "test_insert,ta=a1,tb=b1 fa=1,fb=2 1"
line1 = "test_insert,ta=a1,tb=b1 fa=3,fb=4 2"
line2 = "test_insert,ta=a1,tb=b1 fa=5,fb=6 3"

conn = connect(url="http://127.0.0.1:31007/", user="root", password="")

conn.create_database_with_ttl("test_database", "100000d")
conn.switch_database("test_database")

conn.write_lines([line0, line1, line2])

resp = conn.execute("SELECT * FROM test_insert;")
print(resp)
```

#### Write use SQL

```python
from cnosdb_connector import connect

conn = connect(url="http://127.0.0.1:31007/", user="root", password="")

query = "insert test_insert(TIME, column1, column2, column3, column4, column5, column6, column7) values (100, -1234, 'hello', 1234, false, 1.2, 'beijing', 'shanghai'); "

conn.execute(query)

resp = conn.execute("SELECT * FROM test_insert;")
print(resp)
```

#### Write use CSV

```python
from cnosdb_connector import connect
import os

query = "CREATE TABLE test_insert(column1 BIGINT CODEC(DELTA),\
                                  column2 BOOLEAN,\
                                  column3 DOUBLE CODEC(GORILLA),\
                                  TAGS(column4));"

conn = connect(url="http://127.0.0.1:31007/", user="root", password="")
# table schema must same with csv file
conn.execute(query)

path = os.path.abspath("test.csv")
conn.write_csv("test_insert", path)

resp = conn.execute("SELECT * FROM test_insert;")
print(resp)
```

# License

CnosDB Python Connector use MIT License