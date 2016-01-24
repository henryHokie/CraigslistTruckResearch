#!/usr/bin/env python
from bs4 import BeautifulSoup
import requests
from collections import namedtuple, defaultdict
from selenium import webdriver
import re
import time
import sqlite3
import os
from os.path import expanduser, exists
from datetime import datetime
import sqlite3
from pyvirtualdisplay import Display
import platform
from multiprocessing import Pool
import sys

db = './craigslist.db'
parsedURLs = set()

def Get_City_URLS():
	MainSite = 'http://www.craigslist.org/about/sites'

	r = requests.get(MainSite)
	soup = BeautifulSoup(r.text, 'lxml')

	body = soup.find('section', class_ = 'body')

	items = body.findAll(['h1', 'div'])

	h1s = body.findAll('h1')

	USItems = [item for n, item in enumerate(items) if 
	n > items.index(h1s[0]) and 
	n < items.index(h1s[1]) ]

	URL = namedtuple('URL', 'city url')

	urls = []
	for item in USItems:
		for location in item.findAll('a'):
			urls.append(URL(city = location.text, 
				url = location['href']))
	return urls 

def getListings(base = 'http://denver.craigslist.org'):
	display = Display(visible = 0, size = (800, 600))
	display.start()


	base =  base[:base.index('t.org') + 5]
	url = '%s/search/cta?query=trd' % base
	profile = r'30idwlam.default'
	fp = webdriver.FirefoxProfile(profile)

	browser = webdriver.Firefox(firefox_profile = fp)
	urls = set()

	keepGoing = True
	while len(urls) % 100 == 0 and keepGoing:
		error = False
		attempts = 0
		while not error and attempts < 3:
			attempts += 1
			browser.get('%s&s=%d' % (url, len(urls)))
			time.sleep(1)
			soup = BeautifulSoup(browser.page_source, 'lxml')
			if soup.find('h1', {'id': 'errorTitleText'}):
				error = True
				if attempts == 3:
					print 'could not find server after three attempts for :%s&s=%d' % (url, len(urls))
					browser.quit()
					display.stop()
					return
		try:

			divs = soup.find('div', class_ = 'content')
			for p in divs.findAll('p', class_ = 'row'):
				if 'craigslist' not in p.a['href']:
					urls.add(base + p.a['href'])
			if soup.find('span', class_ = 'displaycount').text == '0':
				keepGoing = False
		except AttributeError:
			keepGoing = False

	browser.quit()
	display.stop()
	return urls


def convertDateToInt(dte):
	dte = dte[: -5]
	dte = datetime.strptime(dte, '%Y-%m-%dT%H:%M:%S')
	return dte.strftime('%Y%m%d%H%M')



def parseAds(url):

	try:
		page = requests.get(url)
	except Exception:
		print 'Could not connect to:%' % url
		return

	soup = BeautifulSoup(page.text, 'lxml')
	try:
		location = soup.find('h2', class_ = 'postingtitle').find('small')
	except AttributeError:
		return

	if location:
			city = location.text.strip('( )')

	accessDate = datetime.now().strftime('%Y%m%d')

	adDetails = defaultdict(lambda: None)
	for attributeGroup in soup.findAll('p', class_ = 'attrgroup'):
		for attribute in attributeGroup.findAll('span'):
			text = attribute.text
			if ':' not in text:
				adDetails['title'] = text
			else:
				key, value = text.split(': ')
				adDetails[key.strip()] = value 
	lat = lon = city = None
	price = soup.find('span', class_ = 'price')
	if price:
		adDetails['price'] = price.text.strip('$')
	location = soup.find('h2', class_ = 'postingtitle').find('small')
	if location:
			city = location.text.strip('( )')

	mapAddress = soup.find('p', class_ = 'mapaddress')
	if mapAddress:
		mapAddress = mapAddress.small.a['href']
		try:
			lat, lon = re.search('@(\d+\.\d+,-\d+\.\d+)',mapAddress).group(1).split(',')
		except AttributeError:
			pass
	try:
		body = soup.find('section', {'id': 'postingbody'}).text
	except AttributeError:
		body = None
		print url, 'Has no Body'

	title = soup.find('span', class_ = 'postingtitletext')
	for tag in title.findAll(['span', 'small']):
		tag.replaceWith('')
	adDetails['title'] = title.text.replace(' - ', '')

	postdate  = soup.find('p', class_='postinginfo').time['datetime']
	
	adDetails['postDate'] = convertDateToInt(postdate)
	adDetails['accessDate'] = accessDate

	adDetails['lat'] = lat
	adDetails['lon'] = lon
	adDetails['location'] = city
	adDetails['mapAddress'] = mapAddress
	adDetails['body'] = body

	adDetails['url'] = url
	time.sleep(.5)

	parsedURLs.add(url)

	return adDetails




def createGetTableInfo():
	#print db
	if not exists(db): create = True
	else: create = False
	conn = sqlite3.connect(db)
	c = conn.cursor()
	if create:
		with open('CreateTable.sql', 'r') as infile:
			sql = infile.read()
		c.execute(sql)
		conn.commit()
	
	c.execute('pragma table_info(trdvalues);')
	cols = [row[1] for row in c.fetchall()]
	conn.close()
	return cols

def loopCityFile(cityURL):
	urls = getListings(cityURL)
	output = []
	if urls:
		for url in [u for u in urls if u not in parsedURLs]:
			data = parseAds(url)
			if data:
				output.append(data)
		keys = [d.keys() for d in output]
		#[parsedKeys.add(k) for d in keys for k in d]
		upload = [[row[k] for k in cols] for row in output]

		conn = sqlite3.connect(db)
		c = conn.cursor()
		inputs = ['?'] * len(cols)
		string = 'insert into trdvalues values (%s);' % ','.join(inputs)
		c.executemany(string, upload )
		conn.commit()
		conn.close()
	with open('processedURLs.txt','a+') as outfile:
		outfile.write(cityURL + '\n')



def resumeURL(cityURLs):

	with open ('processedURLs.txt', 'r') as infile:
		processedURLs = [line.strip() for line in infile]
	print len(cityURLs), len(processedURLs)
	newSet = [url for url in cityURLs if url not in processedURLs]
	print len(newSet)
	return newSet			


cols = createGetTableInfo()



if __name__ == '__main__':
	
	t0 = time.time()
	start = datetime.now().strftime('%c')
	cityURLs = ['http:' + u[1][:-1] for u in Get_City_URLS()]		
	if len(sys.argv) <> 1:
		cityURLs = resumeURL(cityURLs)
	else:
		if exists('processedURLs.txt'):os.system('rm processedURLs.txt')
 

	#parsedKeys = set()
	#pool = Pool(1)
	map(loopCityFile, cityURLs)
	with open('Scrapper.log', 'a+') as outfile:
		outfile.write('Start Time: %s|Total Mins:%d|End Time: %s\n' % (start, (time.time() - t0)/60, datetime.now().strftime('%c')))






