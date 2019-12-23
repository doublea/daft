# -*- coding: utf-8 -*-
import dataclasses
import sqlite3

DATABASE = 'db.sqlite'

@dataclasses.dataclass
class Location:
    x: float
    y: float

    @staticmethod
    def adapter(location):
        return '%f;%f' % (location.x, location.y)

    @classmethod
    def converter(cls, s):
        x, y = map(float, s.split(b';'))
        return cls(x, y)

sqlite3.register_converter('Location', Location.converter)
sqlite3.register_adapter(Location, Location.adapter)


@dataclasses.dataclass
class Listing:
    url: str = dataclasses.field(metadata={'sqlite_type': 'text PRIMARY KEY'})
    doc: str = dataclasses.field(metadata={'sqlite_type': 'text'})
    price: int = dataclasses.field(metadata={'sqlite_type': 'integer'})
    address: str = dataclasses.field(metadata={'sqlite_type': 'text'})
    location: Location = dataclasses.field(metadata={'sqlite_type': 'Location'})
    beds: int = dataclasses.field(metadata={'sqlite_type': 'integer'})
    bathrooms: int = dataclasses.field(metadata={'sqlite_type': 'integer'})
    area: int = dataclasses.field(metadata={'sqlite_type': 'integer'})

    @classmethod
    def row_factory(cls, cursor, row):
        fields = {f.name: None for f in dataclasses.fields(cls)}
        for idx, col in enumerate(cursor.description):
            if col[0] in fields:
                fields[col[0]] = row[idx]
        return cls(**fields)

    @classmethod
    def create_table_statement(cls):
        cols = ', '.join(['%s %s' % (f.name, f.metadata['sqlite_type'])
                         for f in dataclasses.fields(cls)])
        return 'CREATE TABLE IF NOT EXISTS listings (%s);' % cols


def get_db_connection():
    conn = sqlite3.connect(DATABASE, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = Listing.row_factory
    return conn