#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import googlemaps
import logging
import sqlite3

import config
import daft_types

logging.basicConfig(level=logging.INFO)

# This is the max number of origins or destinations allowed by the GMaps API.
_BATCH_SIZE = 25

def get_existing_distance_origins(mode, destination):
    with daft_types.get_db_connection(for_type=daft_types.Distance) as conn:
        conn.execute(daft_types.Distance.create_table_statement())
        cur = conn.execute('SELECT origin FROM distance WHERE mode=? AND destination=?;', (mode, destination))
        return [d.origin for d in cur.fetchall()]

def load_distances_to_destination(client, mode, destination):
    existing_origins = set(get_existing_distance_origins(mode, destination))

    now = datetime.now()
    # Get a datetime for Monday two weeks from now (to make sure the queries
    # don't use live traffic information)
    arrival_time=(
        now + timedelta(days=14 - now.weekday())).replace(
            hour=9, minute=0, second=0, microsecond=0)

    with daft_types.get_db_connection(for_type=daft_types.Listing) as conn:
        cur = conn.execute('SELECT * from listing;')
        while True:
            listings = cur.fetchmany(size=_BATCH_SIZE)
            if len(listings) == 0:
                return

            new_listings = [l for l in listings
                            if l.location is not None and
                            l.location not in existing_origins]
            origins = [l.location.astuple() for l in new_listings]
            logging.info(
                    'Querying routes for listings without data: %d out of %d',
                    len(origins), len(listings))
            if len(origins) == 0:
                continue
            matrix = client.distance_matrix(
                origins,
                destination.astuple(),
                mode=mode,
                language='en',
                avoid=None,
                units='metric',
                departure_time=None,
                arrival_time=arrival_time,
                transit_mode=['bus', 'rail'],
                transit_routing_preference='fewer_transfers',
                traffic_model=None,
                region='ie')
            if matrix['status'] != 'OK':
                raise Exception(
                    'distance_matrix returned invalid status: %s' % (
                        matrix['status']))
            for listing, distance_dict in zip(new_listings, matrix['rows']):
                if distance_dict['elements'][0]['status'] != 'OK':
                    logging.warning('Invalid status for url \'%s\': %s' % (
                        listing.url, distance_dict['elements'][0]['status']))
                    continue
                distance = daft_types.Distance(
                    listing_url=listing.url,
                    mode=mode,
                    origin=listing.location,
                    destination=destination,
                    duration_secs=distance_dict['elements'][0]['duration']['value'],
                    duration_text=distance_dict['elements'][0]['duration']['text'],
                    distance_m=distance_dict['elements'][0]['distance']['value'],
                    distance_text=distance_dict['elements'][0]['distance']['text'])
                conn.execute(distance.insert_stmt(), distance.astuple())

def main():
    mode='transit'
    client = googlemaps.Client(config.GMAPS_API_KEY)
    x, y = config.COMMUTE_DESTINATION.split(',')
    destination = daft_types.Location(x=float(x), y=float(y))
    load_distances_to_destination(client, mode, destination)

if __name__ == '__main__':
    main()
