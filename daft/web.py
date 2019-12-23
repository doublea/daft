# -*- coding: utf-8 -*-
from flask import Flask, g, render_template
import sqlite3

import daft_types

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = daft_types.get_db_connection()
    return db

app = Flask(__name__)

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

@app.route('/')
def hello_world():
    cur = get_db().execute('SELECT * from listings;')
    return render_template('list.html', listings=cur.fetchall())


if __name__ == '__main__':
    app.run()