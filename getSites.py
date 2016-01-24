#!/usr/bin/env python
from bs4 import BeautifulSoup
import requests
from collections import namedtuple
import sqlite3

page = requests.get('http://www.craigslist.org/about/sites')

soup = BeautifulSoup(page.text)

us = soup.find('section', class_ = 'body')
us = us.find('div', class_ = 'colmask')



clCity = namedtuple('clCity', 'state city url')
cities = []
for col in us.findAll('div', class_ = 'box'):
	for item in col.findAll(['h4', 'ul']):
		if item.name == 'h4':
			state = item.text
		if item.name == 'ul':
			for city in item.findAll('li'):
				cities.append(
						clCity(
							state = state, 
							city = city.a.text,
							url  = city.a['href']
							)
							)


db = 'craigslist.db'
conn = sqlite3.connect(db)
cursor = conn.cursor()
cursor.executemany('insert into sites values(?,?,?)', cities)
conn.commit()

