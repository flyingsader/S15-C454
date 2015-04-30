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

        print result.inserted_id

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

###############################################################
## Data Acquisition Main()

# DB client
mongo = MongoClient()
db = mongo.bigDataTest

# Loop through sources
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
        # print "# of stories: {}".format(storycount)

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


            #print title
            #print itemlink
            # pprint(body)
            for j in range(len(body)):
                try:
                    bodyText = body[j]["$text"]
                    #print bodyText
                except:
                    pass

            add_news(source, title, itemlink, bodyText)
    elif s["format"] == "rss" or s["format"] == "xml":
        xmldoc = get_xml_as_dictionary(s["url"])

        items = xmldoc["rss"]["channel"]["item"]
        for i in range(len(items)):
            item = items[i]
            title = item["title"]
            link = item["link"]
            #print title
            #print link
            soup = get_html(link)
            body = soup.findAll("div", "prose-body")[0]
            #pprint(strip_tags(body))
            # print strip_tags(body)
            #print ""

            #print body
            add_news(source, title, link, strip_tags(body))


# NPR
# url = "http://api.npr.org/query?id=1001&apiKey=MDE3NTQ2MTM2MDE0MTcwNTkyMjRlMjc5NA001&output=json"
# response = urllib.urlopen(url)
# data = json.loads(response.read())
# response.close();
# source = "NPR"
#
# print "Pulling from ", source, "..."
#
# # print contents
# # pprint(jsondata)
#
# story = data["list"]["story"]
# storycount = len(story)
# # print "# of stories: {}".format(storycount)
#
# for i in range(len(story)):
#     item = story[i]
#
#     # title
#     title = item["title"]["$text"]
#
#     # link
#     itemlink = item["link"][1]["$text"] + "&output=json"
#
#     # body
#     itemresponse = urllib.urlopen(itemlink)
#     item = json.loads(itemresponse.read())
#     body = item["list"]["story"][0]["text"]["paragraph"]
#
#
#     #print title
#     #print itemlink
#     # pprint(body)
#     for j in range(len(body)):
#         try:
#             bodyText = body[j]["$text"]
#             #print bodyText
#         except:
#             pass
#
#     add_news(source, title, itemlink, bodyText)

# xmldoc = get_xml_as_dictionary("http://feeds.scpr.org/893KpccSouthernCaliforniaNews?format=xml")
# source = "KPCC"
#
# print "Pulling from ", source, "..."
#
# items = xmldoc["rss"]["channel"]["item"]
# for i in range(len(items)):
#     item = items[i]
#     title = item["title"]
#     link = item["link"]
#     #print title
#     #print link
#     soup = get_html(link)
#     body = soup.findAll("div", "prose-body")[0]
#     #pprint(strip_tags(body))
#     # print strip_tags(body)
#     #print ""
#
#     #print body
#     add_news(source, title, link, strip_tags(body))

# pretty(items,0)
