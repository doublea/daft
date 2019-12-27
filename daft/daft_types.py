# -*- coding: utf-8 -*-
import dataclasses
import sqlite3

DATABASE = 'db.sqlite'
_SQLITE_TYPE_MD_KEY = 'sqlite_type'

@dataclasses.dataclass(frozen=True)
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

    def astuple(self):
        return (self.x, self.y)

sqlite3.register_converter('Location', Location.converter)
sqlite3.register_adapter(Location, Location.adapter)

class SQLMappingMixin:
    @classmethod
    def tablename(cls):
        return cls.__name__.lower()

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
        return 'CREATE TABLE IF NOT EXISTS %s (%s);' % (cls.tablename(), cols)

    @classmethod
    def insert_stmt(cls):
        fields = dataclasses.fields(cls)
        return "INSERT OR IGNORE INTO %s(%s) values (%s);" % (
            cls.tablename(),
            ','.join(f.name for f in fields),
            ','.join(['?'] * len(fields)))

    def astuple(self):
        fields = dataclasses.fields(self)
        return tuple([getattr(self, f.name) for f in fields])

def sqltype(sqlite_type: str):
    return dataclasses.field(metadata={_SQLITE_TYPE_MD_KEY: sqlite_type})

@dataclasses.dataclass(frozen=True)
class Listing(SQLMappingMixin):
    url: str = sqltype('text PRIMARY KEY')
    doc: str = sqltype('text')
    price: int = sqltype('integer')
    address: str = sqltype('text')
    location: Location = sqltype('Location')
    beds: int = sqltype('integer')
    bathrooms: int = sqltype('integer')
    area: int = sqltype('integer')

@dataclasses.dataclass(frozen=True)
class Distance(SQLMappingMixin):
    listing_url: str = sqltype('text')
    mode: str = sqltype('text')
    origin: str = sqltype('Location')
    destination: str = sqltype('Location')
    duration_secs: int = sqltype('int')
    duration_text: str = sqltype('text')
    distance_m: int = sqltype('int')
    distance_text: str = sqltype('text')

def get_db_connection(for_type=None):
    conn = sqlite3.connect(DATABASE, detect_types=sqlite3.PARSE_DECLTYPES)
    if for_type is not None:
        conn.row_factory = for_type.row_factory
    return conn
