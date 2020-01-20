#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import bs4
import dataclasses
import datetime
import functools
import logging
import re
import requests
import sqlite3
import sys

import config
import daft_types

logging.basicConfig(level=logging.INFO)

UA = 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0'
BASE_URL = 'https://www.daft.ie'

OFFSET_URL_TMPL = '&offset=%d'
LOCATION_RE = re.compile(
    '"longitude":(-?[0-9.]+),"latitude":(-?[0-9.]+)', re.UNICODE)
AREA_RE = re.compile(r'([0-9.]+).*(ft|m)?', re.I)
BEDS_RE = re.compile('Number of beds is ([0-9]+)')
BATHS_RE = re.compile('Number of bathroom is ([0-9]+)')
ADDED_RE = re.compile(
    '"published_date":"([0-9]{4})-([0-9]{2})-([0-9]{2})"', re.UNICODE)

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
    return int(price_str.translate(
        {'$': None, 0x20ac: None, '.': None, 44: None}))
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
  def __init__(self, url: str, content: str, first_seen: datetime.datetime,
               scraped_on: datetime.datetime):
    self.url = url
    self.content = content
    self.first_seen = first_seen
    self.scraped_on = scraped_on
    self.doc = bs4.BeautifulSoup(content, 'lxml')

  @property
  def price(self):
    price_str = self.doc.find(
        'strong',
        {'class': 'PropertyInformationCommonStyles__costAmountCopy'})
    return parsePrice(price_str.string)

  @property
  def address(self):
    return self.doc.find(
        'h1', {'class': 'PropertyMainInformation__address'}).text

  @property
  def tracking_params(self):
    scripts = self.doc.find_all('script')
    for s in scripts:
      if s.string is None:
        continue
      if 'var trackingParam' in s.string:
        return s.string

  @property
  def location(self):
      params = self.tracking_params
      if params:
          m = LOCATION_RE.search(params)
          if m:
              return daft_types.Location(
                  float(m.group(2)), float(m.group(1)))

  @property
  def added(self):
    params = self.tracking_params
    if params:
      m = ADDED_RE.search(params)
      if m:
        return datetime.date(
            int(m.group(1)), int(m.group(2)), int(m.group(3)))

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

  def make_listing(self):
    return daft_types.Listing(
      url=self.url, doc=self.content, price=self.price,
      address=self.address, location=self.location, beds=self.beds,
      bathrooms=self.baths, area=self.area, added=self.added,
      scraped_on=datetime.datetime.now(datetime.timezone.utc),
      first_seen=self.first_seen)

@run_in_executor
def make_request(url: str) -> bs4.BeautifulSoup:
  resp = requests.get(BASE_URL + url, headers={'User-Agent': UA})
  if resp.status_code != 200:
    raise ValueError('Got error code %d' % resp.status_code)
  return resp.content

def get_links_from_search_results(content):
  links = bs4.BeautifulSoup(content, 'lxml').find_all(
      'a', {'class': 'PropertyInformationCommonStyles__addressCopy--link'},
      href=True)
  return [a['href'] for a in links]

async def load_into_table(conn, listing):
  with conn:
    conn.execute(listing.insert_stmt(), listing.astuple())

async def process_listing_url(url, existing_listings, now, conn):
  logging.info('Started parsing %s', url)
  first_seen = now
  if url in existing_listings:
    stored_listing = existing_listings[url]
    content = stored_listing.doc
    if stored_listing.first_seen:
      first_seen = stored_listing.first_seen
  else:
    content = await make_request(url)
  l = ListingParser(url, content, first_seen, now).make_listing()
  await load_into_table(conn, l)
  logging.info('Finished parsing %s', url)
  return url

async def main():
  now = datetime.datetime.now(datetime.timezone.utc)
  with daft_types.get_db_connection() as conn:
    daft_types.Listing.create_or_alter_table(conn)
  conn = daft_types.get_db_connection(for_type=daft_types.Listing)
  existing_listings = {l.url: l for l in
                       conn.execute('SELECT * FROM listing;').fetchall()}
  tasks = []
  offset = 0
  while True:
    soup = await make_request(
      config.DAFT_SEARCH + OFFSET_URL_TMPL % offset)
    links = get_links_from_search_results(soup)
    if not links:
      break
    offset += len(links)
    print('Got %d new links' % len(links))
    for link in links:
      tasks.append(asyncio.create_task(
          process_listing_url(link, existing_listings, now, conn)))
  if tasks:
    await asyncio.wait(tasks)

  conn.close()

if __name__ == '__main__':
  asyncio.run(main())
    
