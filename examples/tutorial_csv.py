from cnosdb_connector import connect
import os


def main():
    query0 = "CREATE TABLE test0(column1 BIGINT CODEC(DELTA),\
                                    column2 STRING CODEC(GZIP),\
                                    column3 BIGINT UNSIGNED CODEC(NULL),\
                                    column4 BOOLEAN,\
                                    column5 DOUBLE CODEC(GORILLA),\
                                    TAGS(column6, column7));"

    query1 = "select ALL * from test_database.test0;"

    conn = connect()

    resp = conn.create_database_with_ttl("test_database", "100000d")
    print(resp)
    # None

    conn.switch_database("test_database")

    resp = conn.execute(query0)
    print(resp)
    # {}

    path = os.path.abspath("test.csv")
    resp = conn.write_csv("test0", path)
    print(resp)
    # None

    resp = conn.execute(query1)
    print(resp)
    # [{'column1': -1234, 'column2': 'hello', 'column3': 1234, 'column4': False, 'column5': 1.2, 'column6': 'beijing',
    #   'column7': 'shanghai', 'time': '1970-01-01 00:00:00.000000100'},
    #  {'column1': -1234, 'column2': 'hello', 'column3': 1234, 'column4': False, 'column5': 1.2, 'column6': 'beijing',
    #   'column7': 'shanghai', 'time': '1970-01-01 00:00:00.000000103'},
    #  {'column1': -1234, 'column2': 'hello', 'column3': 1234, 'column4': False, 'column5': 1.2, 'column6': 'beijing',
    #   'column7': 'shanghai', 'time': '1970-01-01 00:00:00.000000104'}]

    conn.drop_database("test_database")


if __name__ == '__main__':
    main()
