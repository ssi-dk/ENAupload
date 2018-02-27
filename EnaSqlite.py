#!/usr/bin/env python3

import sqlite3

class Sqlitedb:
    def __init__(self, dbname):
        self.connection = sqlite3.connect(dbname)
        self.c = self.connection.cursor()
        self.create_tables()

    def create_project_table(self):
        """Create the projects table"""
        self.c.execute('CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY, alias TEXT unique, accession TEXT unique);')

    def add_project(self, alias):
        self.c.execute("insert or ignore into projects (alias) VALUES (?);",(alias,))
        
    def add_project_accession(self, alias, accession):
        self.c.execute("update projects set accession=? where alias=?;", (accession, alias))

    def get_project_accession(self, alias):
        self.c.execute("select accession from projects where alias=?;",(alias,))
        return self.c.fetchone()[0]
    
    def add_sample(self, alias):
        self.c.execute("insert or ignore into samples (alias) VALUES (?);",(alias,))

    def add_experiment(self, alias, project_alias, sample_alias):
        self.c.execute("insert or ignore into experiments (alias, project_id, sample_id) VALUES (?,(SELECT id FROM projects WHERE alias=?),(SELECT id FROM samples WHERE alias=?));",(alias, project_alias, sample_alias))

    def add_run(self, alias, experiment_alias):
        self.c.execute("insert or ignore into runs (alias, experiment_id) VALUES (?,(SELECT id FROM experiments WHERE alias=?));",(alias, experiment_alias))

    def add_accession(self, alias, accession, table):
        self.c.execute("update " + table + " set accession=? where alias=?;", (accession, alias))

    def get_accession(self, alias, table):
        self.c.execute("select accession from " + table + " where alias=?;", (alias, ))
        return self.c.fetchone()[0]

    def create_sample_table(self):
        """Create the samples table"""
        self.c.execute('CREATE TABLE IF NOT EXISTS samples (id INTEGER PRIMARY KEY, alias TEXT, accession TEXT);')

    def create_run_table(self):
        """Create the run table"""
        self.c.execute('CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY, alias TEXT, accession TEXT, experiment_id INTEGER);')
        
    def create_experiment_table(self):
        """Create the run table"""
        self.c.execute('CREATE TABLE IF NOT EXISTS experiments (id INTEGER PRIMARY KEY, alias TEXT, accession TEXT, project_id INTEGER, sample_id INTEGER);')

    def create_tables(self):
        self.create_project_table()
        self.create_sample_table()
        self.create_experiment_table()
        self.create_run_table()

    def start_transaction(self):
        """Begin a transaction group"""
        self.c.execute('BEGIN TRANSACTION;')

    def commit_transaction(self):
        """End a transaction group"""
        self.c.execute('COMMIT TRANSACTION;')

    def commit(self):
        """End a transaction group"""
        self.connection.commit()
