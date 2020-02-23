import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import datetime as dt

from flask import Flask, jsonify
# ------------------------------------------------------------------------

# Database Setup
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Flask Setup
app = Flask(__name__)
# ------------------------------------------------------------------------

# Flask Routes
@app.route("/")
def homepage():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start_date><br/>"
        f"/api/v1.0/<start_date>/<end_date>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session(engine)
    results = session.query(Measurement.date, Measurement.prcp).all()
    session.close()

    all_precipitation = []
    for date, prcp in results:
        precipitation_dict = {}
        precipitation_dict[date] = prcp
        all_precipitation.append(precipitation_dict)

    return jsonify(all_precipitation)

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)
    results = session.query(Measurement.station).all()
    session.close()

    all_stations = []
    for station in results:
        station_dict = {}
        station_dict = station
        all_stations.append(station_dict)

    return jsonify(all_stations)

# Calculate the date 1 year ago from the last data point in the database


@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)    
    max_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

    result = []
    for d in max_date:
        result = d
    year = int(result.split('-')[0])
    month = int(result.split('-')[1])
    day = int(result.split('-')[2])

    twelve_month_start = dt.date(year,month,day) - dt.timedelta(days=365)
    twelve_month_end = dt.date(year,month,day)

    results = session.query(Measurement.date, Measurement.tobs).filter(Measurement.date>=twelve_month_start, Measurement.date<=twelve_month_end).all()
    session.close()

    all_tobs = []
    for date, tobs in results:
        tobs_dict = {}
        tobs_dict[date] = tobs
        all_tobs.append(tobs_dict)

    return jsonify(all_tobs)


@app.route("/api/v1.0/<start_date>")
def start_only(start_date):
    session = Session(engine)
    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).all()
    session.close()

    all_start = []
    for tmin, tavg, tmax in results:
        start_dict = {}
        start_dict['min_temp'] = tmin
        start_dict['avg_temp'] = tavg
        start_dict['max_temp'] = tmax

        all_start.append(start_dict)  

    return jsonify(all_start)

@app.route("/api/v1.0/<start_date>/<end_date>")
def start_end(start_date, end_date):
    session = Session(engine)
    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
    session.close()

    all_start_end = []
    for tmin, tavg, tmax in results:
        start_end_dict = {}
        start_end_dict['min_temp'] = tmin
        start_end_dict['avg_temp'] = tavg
        start_end_dict['max_temp'] = tmax

        all_start_end.append(start_end_dict)  

    return jsonify(all_start_end)

if __name__ == '__main__':
    app.run(debug=True)