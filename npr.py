import urllib
import dateutil.parser as dateparser
import re
import urllib2
import xmltodict
import json
import BeautifulSoup as bs
from pprint import pprint
from xml.dom import minidom
from HTMLParser import HTMLParser
from pymongo import MongoClient
import csv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
import datetime

def add_crimes(id, incidentDate, category, stat, statDesc, addressStreet, city, zip, xCoor, yCoor, incidentId, reportDistrict, seq, unitId, unitName, deleted):
    exists = db.crimes.find(
        {
            "id" : id
         }
    )

    if exists.count() == 0:
        result = db.crimes.insert_one(
            {
                "id": id,
                "incidentDate" : incidentDate,
                "category" : category,
                "stat" : stat,
                "statDesc" : statDesc,
                "addressStreet" : addressStreet,
                "city" : city,
                "zip" : zip,
                "xCoor" : xCoor,
                "yCoor" : yCoor,
                "incidentId" : incidentId,
                "reportDistrict" : reportDistrict,
                "seq" : seq,
                "unitId" : unitId,
                "unitName" : unitName,
                "deleted " : deleted
            }
        )
        print city

def add_news(news_source, news_title, news_link, news_body):
    exists = db.news.find({"source": news_source, "link": news_link})

    if exists.count() == 0:
        result = db.news.insert_one(
            {
                "source": news_source,
                "title": news_title,
                "link": news_link,
                "body": news_body
            }
        )

        print news_link

def pretty(d, indent=0):
    for key, value in d.iteritems():
        print '\t' * indent + str(key)
        if isinstance(value, dict):
            pretty(value, indent + 1)
        else:
            print '\t' * (indent + 1) + str(value)


def get_xml_as_dictionary(url):
    """
    :rtype : dict
    """
    file = urllib.urlopen(url)
    kpccdata = file.read()
    file.close()
    xmldoc = xmltodict.parse(kpccdata)
    return xmldoc


def get_html(url):
    file = urllib.urlopen(url)
    kpccdata = file.read()
    file.close()

    soup = bs.BeautifulSoup(kpccdata)
    return soup


from HTMLParser import HTMLParser


class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
        self.containstags = False

    def handle_starttag(self, tag, attrs):
        self.containstags = True

    def handle_data(self, d):
        self.fed.append(d)

    def has_tags(self):
        return self.containstags

    def get_data(self):
        return ''.join(self.fed)


def strip_tags(html):
    must_filtered = True
    while ( must_filtered ):
        s = MLStripper()
        s.feed(str(html))
        html = s.get_data()
        must_filtered = s.has_tags()
    return html

def loadSheriffsDatabase():
    # Sheriff's Department Data
    # http://shq.lasdnews.net/CrimeStats/CAASS/PART_I_AND_II_CRIMES.csv

    print "Loading Sheriff's crimes database..."
    print datetime.datetime.utcnow()
    url = 'http://shq.lasdnews.net/CrimeStats/CAASS/PART_I_AND_II_CRIMES.csv'
    response = urllib2.urlopen(url)
    cr = csv.reader(response)

    for row in cr:
        add_crimes(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15])

    print "Sheriff's crimes database load complete."

def loadSources():
    # Loop through news sources
    print "Traversing sources..."
    print datetime.datetime.utcnow()
    dbSources = db.sources.find()
    for s in dbSources:
        source = s["name"]
        print "Pulling from ", source,"..."

        if s["format"] == "json":
            url = s["url"]
            response = urllib.urlopen(url)
            data = json.loads(response.read())
            response.close();

            story = data["list"]["story"]
            storycount = len(story)
            if debug : print ("# of stories: {}".format(storycount))

            for i in range(len(story)):
                item = story[i]

                # title
                title = item["title"]["$text"]

                # link
                itemlink = item["link"][1]["$text"] + "&output=json"

                # body
                itemresponse = urllib.urlopen(itemlink)
                item = json.loads(itemresponse.read())
                body = item["list"]["story"][0]["text"]["paragraph"]


                if debug : print title
                if debug : print itemlink
                for j in range(len(body)):
                    try:
                        bodyText = body[j]["$text"]
                        if debug : print bodyText
                    except:
                        pass

                add_news(source, title, itemlink, bodyText)
        elif s["format"] == "rss" or s["format"] == "xml":
            xmldoc = get_xml_as_dictionary(s["url"])

            if debug : print s["divContainer"]
            items = xmldoc["rss"]["channel"]["item"]
            if debug: print len(items), "items"
            for i in range(len(items)):
                try:
                    item = items[i]
                    title = item["title"]
                    link = item["link"]
                    if debug : print title
                    if debug : print link
                    soup = get_html(link)
                    body = soup.findAll("div", s["divContainer"])[0]
                    if debug : print ""

                    if debug : print body
                    add_news(source, title, link, strip_tags(body))
                except:
                    pass
    print "Sources traversal complete."

###############################################################
## Data Acquisition Main()

debug = 0

# DB client
mongo = MongoClient()
db = mongo.bigDataTest

#loadSources()

# Define scheduler
scheduler = BlockingScheduler()
#scheduler = BackgroundScheduler()

print "Adding Sheriffs database scheduled job executing every 12  hours..."
scheduler.add_job(loadSheriffsDatabase, 'interval', hours=12, id='load_sheriff')

print "Adding Load Sources scheduled job executing every 2 hours..."
scheduler.add_job(loadSources, 'interval', hours=2, id='load_sources')

loadSheriffsDatabase()
loadSources()

print "Starting scheduler..."
print datetime.datetime.utcnow()
scheduler.start()


#print "Removing Load Sources job..."
#scheduler.remove_job('load_sources')

#print "Removing Sheriff's database scheduled job..."
#scheduler.remove_job('load_sheriff')
