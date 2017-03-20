import os
import datetime
import psycopg2
import psycopg2.extras
from psycopg2 import DatabaseError, IntegrityError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, AsIs
from flask import current_app


class DBAdmin(object):

    @staticmethod
    def create_db(host, port, dbuser, dbname):
        conn = psycopg2.connect(
                host=host,
                port=port,
                dbname='postgres',
                user=dbuser
            )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        try:
            query = """CREATE DATABASE %(dbname)s WITH OWNER %(user)s"""
            params = {'dbname': AsIs(dbname), 'user': AsIs(dbuser)}
            cur.execute(query, params)
        except psycopg2.ProgrammingError as pe:
            if 'already exists' in repr(pe):
                pass
            else:
                raise
        finally:
            cur.close()
            conn.close()
    # ___________________________

    @staticmethod
    def drop_db(host, port, dbuser, dbname):
        conn = psycopg2.connect(
                host=host,
                port=port,
                dbname='postgres',
                user=dbuser,
            )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        try:
            query = """DROP DATABASE IF EXISTS %(dbname)s"""
            params = {'dbname': AsIs(dbname), 'user': AsIs(dbuser)}
            cur.execute(query, params)
        finally:
            cur.close()
            conn.close()

    # ___________________________

    def connectdb(self, host, port, dbname, dbuser):
        self.conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=dbuser
            )
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return self.conn, self.cur
    # ___________________________

    def close_connection(self):
        self.cur.close()
        self.conn.close()
    # ___________________________

    def create_changelog_table(self):
        """
        """

        query = """
            CREATE TABLE IF NOT EXISTS changelog (
                id serial PRIMARY KEY,
                name VARCHAR(64) UNIQUE,
                filenumber VARCHAR(4),
                dateapplied TIMESTAMP,
                comment VARCHAR(255)
            );
        """
        params = {}

        self.cur.execute(query, params)
        self.conn.commit()
    # _____________________________

    def create_table_roles(self):
        """
        class models.Role
        id = db.Column(db.Integer, primary_key=True)
        name = db.Column(db.String(64), unique=True)
        default = db.Column(db.Boolean, default=False, index=True)
        permissions = db.Column(db.Integer)
        """

        query = """
            CREATE TABLE IF NOT EXISTS roles (
                id serial PRIMARY KEY,
                name VARCHAR(64) UNIQUE,
                isdefault BOOLEAN DEFAULT FALSE,
                permissions INTEGER
            );
        """
        params = {}

        self.cur.execute(query, params)
        self.conn.commit()
    # _____________________________

    def create_table_changelog(self):

        query = """
            CREATE TABLE IF NOT EXISTS changelog (
                id serial PRIMARY KEY,
                major VARCHAR(2),
                minor VARCHAR(2),
                patch VARCHAR(4),
                name VARCHAR(100) UNIQUE,
                applied TIMESTAMP
            );
        """
        params = {}

        self.cur.execute(query, params)
        self.conn.commit()
    # _____________________________

    def create_table_installationstep(self):
        """
        class models.InstallationStep
        id = db.Column(db.Integer, primary_key=True)
        name = db.Column(db.String(32), unique=True)
        display_name = db.Column(db.String(64), unique=True)
        priority = INTEGER
        """

        table = 'installationstep'

        query = """
            CREATE TABLE IF NOT EXISTS %(table)s (
                id serial PRIMARY KEY,
                name VARCHAR(32) UNIQUE,
                display_name VARCHAR(64),
                priority INTEGER
            );
        """
        params = {'table': AsIs(table)}

        self.cur.execute(query, params)
        self.conn.commit()

        # Create an index on priority column
        query = """ CREATE INDEX priority_ind ON %(table)s (priority); """
        params = {'table': AsIs(table)}

        self.cur.execute(query, params)
        self.conn.commit()
    # _____________________________

    def create_table_users(self):

        query = """
            CREATE TABLE IF NOT EXISTS users (
                id serial PRIMARY KEY,
                email VARCHAR(64) UNIQUE,
                username VARCHAR(64) UNIQUE,
                password_hash VARCHAR(128),
                role_id INTEGER REFERENCES roles(id)
            );
        """
        params = {}

        self.cur.execute(query, params)
        self.conn.commit()
    # _____________________________

    def grant_access_to_table(self, table):
        query = """GRANT ALL ON TABLE %(table)s TO %(user)s"""
        params = {'table': AsIs(table), 'user': AsIs('ivt')}

        self.cur.execute(query, params)
        self.conn.commit()

    # ___________________________

    def drop_table(self, table):
        print("DB: %r" % self.__dict__)
        print("Table to drop: %r" % table)

        self.cur.execute("""
            DROP TABLE IF EXISTS %s CASCADE
        """ % table)

        self.conn.commit()

    # _____________________________

    def create_baseline(self):
        baseline = Baseline(db=self)
        baseline.create_tables()
        baseline.insert_base_version()
    # _____________________________

    def drop_all(self):
        for table in self.all_tables:
            self.drop_table(table)
    # _____________________________

    def insert_changelog_record(self, filename):

        """
        """
        try:

            print("XXXX File: %s" % filename)
            patch_name = os.path.splitext(filename)[0]
            print("Patch name: %s " % patch_name)
            patchver, name = patch_name.split('.')
            print("Patchver, name: %s %s " % (patchver, name))

            query = """
                INSERT INTO changelog
                    (major, minor, patch, name, applied)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """

            params = ('01', '00', patchver, name, datetime.datetime.now())

            self.cur.execute(query, params)
            self.conn.commit()
            fetch = self.cur.fetchone()
            return fetch['id']

        except Exception as e:
            print('ERROR: %s' % e)
            self.conn.rollback()
            return
    # ____________________________


class Baseline(object):

    def __init__(self, db):
        self.db = db

    # ____________________________

    def create_tables(self):
        tables = current_app.config['DB_TABLES_BASELINE']
        for table in tables:
            self.db.drop_table(table)
            getattr(self.db, "create_table_%s" % table)()
            self.db.grant_access_to_table(table)
    # ____________________________

    def insert_base_version(self):

        """
        """

        query = """
            INSERT INTO changelog
                (major, minor, patch, name, applied)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """

        params = (
                '01', '00', '0000',
                'initial_baseline',
                datetime.datetime.now()
        )

        try:
            self.db.cur.execute(query, params)
            self.db.conn.commit()
            fetch = self.db.cur.fetchone()
            return fetch['id']

        except IntegrityError as ie:
            print('ERROR: %s' % ie)
            self.db.conn.rollback()
            return
        except DatabaseError as dbe:
            print('ERROR: %s' % dbe)
            self.db.conn.rollback()
            return
    # ____________________________
