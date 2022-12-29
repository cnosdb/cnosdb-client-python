import pandas as pd
from cnosdb_connector import connect


def main():
    conn = connect()
    resp = conn.create_database_with_ttl("test_database", "100000d")
    print(resp)
    # None

    conn.switch_database("test_database")
    line0 = "test_insert,ta=a1,tb=b1 fa=1,fb=2 1"
    line1 = "test_insert,ta=a1,tb=b1 fa=3,fb=4 2"
    line2 = "test_insert,ta=a1,tb=b1 fa=5,fb=6 3"

    resp = conn.write_lines([line0, line1, line2])
    print(resp)
    # None

    df = pd.read_sql("select * from test_insert", conn)
    print(df)
    #     fa   fb  ta  tb                           time
    # 0  1.0  2.0  a1  b1  1970-01-01 00:00:00.000000001
    # 1  3.0  4.0  a1  b1  1970-01-01 00:00:00.000000002
    # 2  5.0  6.0  a1  b1  1970-01-01 00:00:00.000000003

    resp = conn.execute("select * from test_insert")
    df = pd.DataFrame(resp)
    print(df)
    #     fa   fb  ta  tb                           time
    # 0  1.0  2.0  a1  b1  1970-01-01 00:00:00.000000001
    # 1  3.0  4.0  a1  b1  1970-01-01 00:00:00.000000002
    # 2  5.0  6.0  a1  b1  1970-01-01 00:00:00.000000003

    cursor = conn.cursor()
    cursor.execute("select * from test_insert")
    print(cursor.description)
    #[['fa', 'float'], ['fb', 'float'], ['ta', 'str'], ['tb', 'str'], ['time', 'str']]

    resp = cursor.fetchall()
    print(resp)
    # [{'fa': 1.0, 'fb': 2.0, 'ta': 'a1', 'tb': 'b1', 'time': '1970-01-01 00:00:00.000000001'},
    #  {'fa': 3.0, 'fb': 4.0, 'ta': 'a1', 'tb': 'b1', 'time': '1970-01-01 00:00:00.000000002'},
    #  {'fa': 5.0, 'fb': 6.0, 'ta': 'a1', 'tb': 'b1', 'time': '1970-01-01 00:00:00.000000003'}]

    conn.drop_database("test_database")


if __name__ == '__main__':
    main()
