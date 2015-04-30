__author__ = 'Michael'

# Install pymongo first!!
# pip install pymongo

# Start mongod before running
from pymongo import MongoClient

mongo = MongoClient()
db = mongo.bigDataTest

allNews = db.news.find()
nprNews = db.news.find({"source": "NPR"})
kpccNews = db.news.find({"source":"KPCC"})

#for doc in allNews:
#    print (doc)

def add_source(name, url, format):
    exists = db.sources.find({"name": name})

    if exists.count() == 0:
        result = db.sources.insert_one(
            {
                "name": name,
                "url": url,
                "format": format
            }
        )
        print "Added ", name, "to sources..."
    else:
        print name, "already exists in sources."

add_source("NPR","http://api.npr.org/query?id=1001&apiKey=MDE3NTQ2MTM2MDE0MTcwNTkyMjRlMjc5NA001&output=json", "json")
add_source("KPCC", "http://feeds.scpr.org/893KpccSouthernCaliforniaNews?format=xml", "rss")

dbSources = db.sources.find()

print "Sources:"
for s in dbSources:
    sourceNews = db.news.find({"source": s["name"]}).count()
    print "\t", s["name"], ":", sourceNews, "articles"