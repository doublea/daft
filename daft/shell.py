# -*- coding: utf-8 -*-
import IPython 
import sqlite3
import bs4

import load
import daft_types

if __name__ == '__main__':
    conn = daft_types.get_db_connection()
    cur = conn.execute('SELECT * FROM listings')
    listings = cur.fetchall()
    IPython.embed()