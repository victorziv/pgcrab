from pgcrab.dbadmin import DBAdmin


def test_create_db():
    host = 'localhost'
    port = 5442
    dbname = 'ivttest'
    user = 'ivt'
    password = 'ivt'
    dba = DBAdmin()
    dba.create_db(
        host=host,
        port=port,
        dbname=dbname,
        dbuser=user,
        dbpassword=password
    )
