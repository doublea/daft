#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import Flask, g, render_template, send_from_directory
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
    get_db().row_factory = daft_types.Listing.row_factory
    listings = get_db().execute('SELECT * from listing;').fetchall()

    get_db().row_factory = daft_types.Distance.row_factory
    distance = get_db().execute('SELECT * from distance;').fetchall()
    distances = {d.listing_url: d for d in distance}
    return render_template(
        'index.html', listings=listings, distances=distances)


if __name__ == '__main__':
    app.run(host='0.0.0.0')
