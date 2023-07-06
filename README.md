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

## LangChain
### connect to CnosDB
Use the cnosdb_connector and SQLDatabase to connect to CnosDB
First create the uri required for the SQLDatabase
```python
def make_cnosdb_langchain_uri(url: str = "127.0.0.1:8902",
                              user: str = "root",
                              password: str = "",
                              tenant: str = "cnosdb",
                              database: str = "public") -> str
```
Args:
1. url (str): The HTTP connection host name and port number of the CnosDB
                service, excluding "http://" or "https://", with a default value
                of "127.0.0.1:8902".
2. user (str): The username used to connect to the CnosDB service, with a
                default value of "root".
3. password (str): The password of the user connecting to the CnosDB service,
                with a default value of "".
4. tenant (str): The name of the tenant used to connect to the CnosDB service,
                with a default value of "cnosdb".
5. database (str): The name of the database in the CnosDB tenant.
### example
```python
from cnosdb_connector import make_cnosdb_langchain_uri
from langchain import SQLDatabase

uri = cnosdb_connector.make_cnosdb_langchain_uri()
db = SQLDatabase.from_uri(uri)
```
```python
from langchain.chat_models import ChatOpenAI

llm = ChatOpenAI(temperature=0, model_name="gpt-3.5-turbo")
```

### SQL Chain
```python
from langchain import SQLDatabaseChain

db_chain = SQLDatabaseChain.from_llm(llm, db, verbose=True)

db_chain.run(
  "What is the average fa of test table that time between November 3,2022 and November 4, 2022?"
)
```
```shell
> Entering new  chain...
What is the average fa of test table that time between November 3, 2022 and November 4, 2022?
SQLQuery:SELECT AVG(fa) FROM test WHERE time >= '2022-11-03' AND time < '2022-11-04'
SQLResult: [(2.0,)]
Answer:The average fa of the test table between November 3, 2022, and November 4, 2022, is 2.0.
> Finished chain.
```
### SQL Database Agent
```python
from langchain.agents import create_sql_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit

toolkit = SQLDatabaseToolkit(db=db, llm=llm)
agent = create_sql_agent(llm=llm, toolkit=toolkit, verbose=True)
```
```python
agent.run(
  "What is the average fa of test table that time between November 3, 2022 and November 4, 2022?"
)
```
```shell
> Entering new  chain...
Action: sql_db_list_tables
Action Input: ""
Observation: test
Thought:The relevant table is "test". I should query the schema of this table to see the column names.
Action: sql_db_schema
Action Input: "test"
Observation: 
CREATE TABLE test (
	time TIMESTAMP, 
	fa BIGINT
)

/*
3 rows from test table:
fa	time
1	2022-11-03T06:20:11
2	2022-11-03T06:20:11.000000001
3	2022-11-03T06:20:11.000000002
*/
Thought:The relevant column is "fa" in the "test" table. I can now construct the query to calculate the average "fa" between the specified time range.
Action: sql_db_query
Action Input: "SELECT AVG(fa) FROM test WHERE time >= '2022-11-03' AND time < '2022-11-04'"
Observation: [(2.0,)]
Thought:The average "fa" of the "test" table between November 3, 2022 and November 4, 2022 is 2.0.
Final Answer: 2.0

> Finished chain.
```
# License

CnosDB Python Connector use MIT License