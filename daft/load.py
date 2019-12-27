#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import bs4
import dataclasses
import functools
import logging
import re
import requests
import sqlite3
import sys

import daft_types

logging.basicConfig(level=logging.INFO)

UA = 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0'
BASE_URL = 'https://www.daft.ie'

# Dublin City, detached house, min 3 bed, min 3 bathroom
LIST_URL = '/dublin-city/houses-for-sale/?s%5Bmnb%5D=3&s%5Bmnbt%5D=2&s%5Badvanced%5D=1&s%5Bhouse_type%5D%5B0%5D=detached&searchSource=sale'
OFFSET_URL_TMPL = '&offset=%d'
LOCATION_RE = re.compile('"longitude":(-?[0-9.]+),"latitude":(-?[0-9.]+)', re.UNICODE)
AREA_RE = re.compile(r'([0-9.]+).*(ft|m)?', re.I)
BEDS_RE = re.compile('Number of beds is ([0-9]+)')
BATHS_RE = re.compile('Number of bathroom is ([0-9]+)')

def run_in_executor(f):
    @functools.wraps(f)
    async def wrapper(*args, **kwargs):
        call = functools.partial(f, *args, **kwargs)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, call)
    return wrapper

def parsePrice(price_str):
    if not price_str:
        return None
    try:
        return int(price_str.translate({'$': None, 0x20ac: None, '.': None, 44: None}))
    except ValueError:
        return None

def parseArea(area_str):
    m = AREA_RE.search(area_str)
    if not m:
        return None
    a = float(m.group(1))
    if m.group(2) == 'ft':
        a /= 10.764
    return a

class ListingParser:
    def __init__(self, url, content):
        self.url = url
        self.content = content
        self.doc = bs4.BeautifulSoup(content, 'lxml')

    @property
    def price(self):
        price_str = self.doc.find('strong', {'class': 'PropertyInformationCommonStyles__costAmountCopy'}).string
        return parsePrice(price_str)

    @property
    def address(self):
        return self.doc.find('h1', {'class': 'PropertyMainInformation__address'}).text

    @property
    def location(self):
        scripts = self.doc.find_all('script')
        for s in scripts:
            if s.string is None:
                continue
            if 'var trackingParam' in s.string:
                m = LOCATION_RE.search(s.string)
                if m:
                    return daft_types.Location(float(m.group(2)), float(m.group(1)))
        return None

    @property
    def beds(self):
        t = self.doc.find(alt=BEDS_RE)
        if not t:
            return None
        m = BEDS_RE.match(t['alt'])
        return int(m.group(1))

    @property
    def baths(self):
        t = self.doc.find(alt=BATHS_RE)
        if not t:
            return None
        m = BATHS_RE.match(t['alt'])
        return int(m.group(1))

    @property
    def area(self):
        span = self.doc.find('span', {'class': 'PropertyOverview__floorArea'})
        if not span:
            return None
        return parseArea(span.next_sibling.string)

    def MakeListing(self):
        return daft_types.Listing(url=self.url, doc=self.content, price=self.price,
                       address=self.address, location=self.location, beds=self.beds,
                       bathrooms=self.baths, area=self.area)

async def parse_listing(url):
    logging.info('Started parsing %s', url)
    ret = ListingParser(url, await MakeRequest(url)).MakeListing()
    logging.info('Finished parsing %s', url)
    return ret


@run_in_executor
def MakeRequest(url: str) -> bs4.BeautifulSoup:
    resp = requests.get(BASE_URL + url, headers={'User-Agent': UA})
    if resp.status_code != 200:
        raise ValueError('Got error code %d' % resp.status_code)
    return resp.content

def GetLinks(content):
    links = bs4.BeautifulSoup(content, 'lxml').find_all(
        'a', {'class': 'PropertyInformationCommonStyles__addressCopy--link'},
        href=True)
    return [a['href'] for a in links]

async def load_into_table(conn, listing):
    with conn:
        conn.execute(listing.insert_stmt(), listing.astuple())

async def process_listing_url(url, conn):
    l = await parse_listing(url)
    await load_into_table(conn, l)

async def main():
    conn = daft_types.get_db_connection(for_type=daft_types.Listing)
    conn.execute(daft_types.Listing.create_table_statement())
    existing_listings = {l.url for l in
                         conn.execute('SELECT * FROM listing;').fetchall()}
    tasks = []
    offset = 0
    while True:
        soup = await MakeRequest(LIST_URL + OFFSET_URL_TMPL % offset)
        links = GetLinks(soup)
        if not links:
            break
        offset += len(links)
        links = [l for l in links if l not in existing_listings]
        print('Got %d new links' % len(links))
        tasks.extend([asyncio.create_task(process_listing_url(link, conn)) for link in links])
    if tasks:
        await asyncio.wait(tasks)
    conn.close()

if __name__ == '__main__':
    asyncio.run(main())
    
