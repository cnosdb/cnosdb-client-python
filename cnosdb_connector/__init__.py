from .conn import CnosDBConnection


def connect(**kwargs) -> CnosDBConnection:
    """
    Keyword Arguments
    ========================================================================
    url: str
        optional, default_value: "http://127.0.0.1:31007/"
        description: the url to connect db server

    database: str
        optional, default_value: public
        description: database name for client to send message

    user: str
        optional, default_value: "root"
        description: default user

    password: str
        optional, default_value: ""
        description: the password of user, default user root password is ""
    ========================================================================
    ```
    example:
        import cnos_connector

        conn = cnos_connector.connect(url="http://192.168.0.11/", database="public", user="root", password="123456")
    ```
    """
    return CnosDBConnection(**kwargs)
