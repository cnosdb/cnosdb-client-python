from cnosdb_connector import connect


def main():
    line0 = "test_insert,ta=a1,tb=b1 fa=1,fb=2 1"
    line1 = "test_insert,ta=a1,tb=b1 fa=3,fb=4 2"
    line2 = "test_insert,ta=a1,tb=b1 fa=5,fb=6 3"

    conn = connect()

    resp = conn.create_database_with_ttl("test_database", "100000d")
    print(resp)
    # None

    conn.switch_database("test_database")

    resp = conn.write_lines([line0, line1, line2])
    print(resp)
    # None

    query = "SELECT * FROM test_insert;"

    resp = conn.execute(query)
    print(resp)
    # [{'fa': 1.0, 'fb': 2.0, 'ta': 'a1', 'tb': 'b1', 'time': '1970-01-01 00:00:00.000000001'},
    #  {'fa': 3.0, 'fb': 4.0, 'ta': 'a1', 'tb': 'b1', 'time': '1970-01-01 00:00:00.000000002'},
    #  {'fa': 5.0, 'fb': 6.0, 'ta': 'a1', 'tb': 'b1', 'time': '1970-01-01 00:00:00.000000003'}]

    conn.drop_database("test_database")


if __name__ == '__main__':
    main()
