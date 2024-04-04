from sqlalchemy import Table, Column, String,Date, Float, Text, Integer, MetaData, create_engine
import csv
import contextlib
import sqlite3




engine = create_engine('sqlite:///train_db.db')

meta = MetaData()

stations = Table(
    'Stations', meta,
    Column('id', Integer, primary_key=True),
    Column('station', Text),
    Column('latitude', Float),
    Column('longitude', Text),
    Column('elevation', Text),
    Column('name', Text),
    Column('country', String),
    Column('state', String)
)


measure = Table(
    'Measure', meta,
    Column('id', Integer, primary_key=True),
    Column('station', Text),
    Column('date', Text),
    Column('precip', Float),
    Column('tobs', Integer),
)

meta.create_all(engine)

conn = engine.connect()

insert_station = stations.insert()
insert_measure = measure.insert()

with open('clean_stations.csv') as f:
    reader = csv.DictReader(f, delimiter=',')
    conn.execute(
        insert_station,
        [{'station': row['station'], 'latitude': row['latitude'],'longitude': row['longitude'],
           'elevation': row['elevation'], 'name': row['name'], 'country': row['country'], 'state': row['state']}
             for row in reader]
    ) 

with open('clean_measure.csv') as f:
    reader = csv.DictReader(f, delimiter=',')
    conn.execute(
        insert_measure,
        [{'station': row['station'], 'date': row['date'],'precip': row['precip'],
           'tobs': row['tobs']} for row in reader]
    )