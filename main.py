import csv 

from sqlalchemy import create_engine, MetaData, Integer, String, Float, Table, Column, ForeignKey

cl_st = [*csv.DictReader(open('clean_stations.csv'))]
cl_me = [*csv.DictReader(open('clean_measure.csv'))]

engine = create_engine('sqlite:///database.db')

meta = MetaData()

stations = Table(
   'stations', meta,
   #Column('id', Integer),
   Column('station', String, primary_key=True),
   Column('latitude', Float),
   Column('longitude', Float),
   Column('elevation', Float),
   Column('name', String),
   Column('country', String),
   Column('state', String)
)

meassures = Table(
   'meassures', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String, ForeignKey("stations.station"), nullable=False),
   Column('date', String),
   Column('precip', Float),
   Column('tobs', Integer)
)

# Wyszukanie stacji położonych na wysokości powyżej 100
elev = stations.select().where(stations.c.elevation > 100)

if __name__ == "__main__":
    meta.create_all(engine)

    conn = engine.connect()
    conn.execute(stations.insert(), cl_st)
    conn.execute(meassures.insert(), cl_me)

    result = conn.execute(elev)

    for row in result:
        print(row)
