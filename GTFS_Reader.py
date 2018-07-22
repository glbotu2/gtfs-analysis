import pandas as pd
import os
import os.path
import osgeo
import ogr,csv,sys
from itertools import compress
from datetime import datetime

def point_in_polygon(point, polygon):
    """
    point : [longitude, latitude]

    polygon : [(lon1, lat1), (lon2, lat2), ..., (lonn, latn)]

    """
    # Create spatialReference
    spatialReference = osgeo.osr.SpatialReference()
    spatialReference.SetWellKnownGeogCS("WGS84")
    # Create ring
    ring = osgeo.ogr.Geometry(osgeo.ogr.wkbLinearRing)
    # Add points
    #for lon, lat in polygon:
    #    ring.AddPoint(lon, lat)
    # Create polygon
    #poly = osgeo.ogr.Geometry(osgeo.ogr.wkbPolygon)
    #poly.AssignSpatialReference(spatialReference)
    #poly.AddGeometry(ring)
    # Create point
    pt = osgeo.ogr.Geometry(osgeo.ogr.wkbPoint)
    pt.AssignSpatialReference(spatialReference)
    pt.SetPoint(0, point[0], point[1])
    # test
    return pt.Within(polygon)




path = 'C:\\Users\\Benjamin\\Dropbox (PBA)\\Side Projects\\GTFS Importer\\Victoria\\4\\'
files = ['agency', 'calendar', 'calendar_dates', 'routes', 'shapes', 'stop_times', 'stops', 'trips']

lga_to_check = ['Whitehorse (C)', 'Monash (C)', 'Yarra (C)', 'Boroondara (C)', 'Melbourne (C)']





for x in files:
    test_path = path + x + '.txt'
    if os.path.isfile(test_path) and os.access(test_path, os.R_OK):
        print('Importing ' + x)
        
        locals()[x] = pd.read_csv(test_path)




test_date = 20180720

stops['region'] = None
# Get calendar Exceptions operating on a specific date
calendar_dates = calendar_dates[calendar_dates['date'] == test_date]

# Get Schedules operating on a specific date
calendar = calendar[(calendar['start_date'] <= test_date) & 
                   (calendar['end_date'] >= test_date) &
                   (calendar['friday'] == 1)]

# Get the current service IDs running on a given date
calendar_merge = calendar.merge(calendar_dates, left_on='service_id', right_on='service_id', how='outer')
service_id = calendar_merge['service_id']
calendar_merge = None



shpfile = path + 'lga\\LGA_2016_AUST.shp'
ds = ogr.Open(shpfile)
lyr = ds.GetLayer()
lyr[0].items()

bool = [(x.items()['LGA_NAME16'] in lga_to_check) & (x.items()['STE_NAME16'] == 'Victoria') & (x.GetGeometryRef() != None) for x in lyr]
ds = ogr.Open(shpfile)
lyr = ds.GetLayer()

test = list(compress(lyr, bool))

for feat in test:
    attributes = feat.items()
    geom=feat.GetGeometryRef()

    n = 0

    lga = attributes['LGA_NAME16']

    print(lga)


    stop_copy = stops

    for index, data in stop_copy[stop_copy['region'].isnull()].iterrows():
        point = [data['stop_lon'], data['stop_lat']]
        if point_in_polygon(point, geom):
            stops.loc[index, 'region'] = lga
            n += 1

    print(lga + ' found ' + str(n))


# Get trips that are operating on the given date
trips = trips[(trips['service_id'].isin(service_id))]
trips

# Get First and Last Times
trips = trips.merge(stop_times, on='trip_id', how='inner')
trips = trips.merge(stops, on='stop_id', how='inner')
trips = trips.merge(routes, on='route_id', how='inner')
trip_stats = trips.groupby(['trip_id', 'region'], as_index=False).agg({"arrival_time":["min", "max"], "route_short_name":["first"]})



#trip_stats['duration'] = [datetime.strptime(x, '%H:%M:%S') for x in trip_stats['arrival_time']['max']] - [datetime.strptime(x, '%H:%M:%S') for x in trip_stats['arrival_time']['min']]

#trip_stats.to_csv(path + 'outputs.txt')



#trip_stats_2 = trip_stats.groupby('region').agg({'trip_id': 'count'})


