import os

from cnosdb_connector import connect


def main():
    query_select = "select ALL * from test_database.test0;"

    conn = connect()
    conn.create_database_with_ttl("test_database", "100000d")
    conn.switch_database("test_database")

    path = os.path.abspath("test.csv")
    query0 = f"CREATE EXTERNAL TABLE test0 STORED AS CSV WITH HEADER ROW LOCATION \"{path}\";"

    # create external table without schema
    conn.execute(query0)

    resp = conn.execute(query_select)
    print(resp)
    # [{'column1': -1234, 'column2': 'hello', 'column3': 1234, 'column4': False, 'column5': 1.2, 'column6': 'beijing',
    #   'column7': 'shanghai', 'time': '1970-01-01T00:00:00.000000100'},
    #  {'column1': -1234, 'column2': 'hello', 'column3': 1234, 'column4': False, 'column5': 1.2, 'column6': 'beijing',
    #   'column7': 'shanghai', 'time': '1970-01-01T00:00:00.000000103'},
    #  {'column1': -1234, 'column2': 'hello', 'column3': 1234, 'column4': False, 'column5': 1.2, 'column6': 'beijing',
    #   'column7': 'shanghai', 'time': '1970-01-01T00:00:00.000000104'}]

    conn.drop_table("test0")

    query1 = f"CREATE EXTERNAL TABLE test0 (time STRING, col6 STRING, col7 STRING, col1 BIGINT, col2 STRING, " \
             f"col3 BIGINT UNSIGNED, col4 BOOLEAN, col5 DOUBLE) STORED AS CSV WITH HEADER ROW LOCATION \"{path}\"; "

    # create external table with schema
    conn.execute(query1)

    resp = conn.execute(query_select)
    print(resp)
    # [{'col1': -1234, 'col2': 'hello', 'col3': 1234, 'col4': False, 'col5': 1.2, 'col6': 'beijing', 'col7': 'shanghai',
    #   'time': '1970-01-01T00:00:00.000000100'},
    #  {'col1': -1234, 'col2': 'hello', 'col3': 1234, 'col4': False, 'col5': 1.2, 'col6': 'beijing', 'col7': 'shanghai',
    #   'time': '1970-01-01T00:00:00.000000103'},
    #  {'col1': -1234, 'col2': 'hello', 'col3': 1234, 'col4': False, 'col5': 1.2, 'col6': 'beijing', 'col7': 'shanghai',
    #   'time': '1970-01-01T00:00:00.000000104'}]

    conn.drop_database("test_database")


if __name__ == '__main__':
    main()
