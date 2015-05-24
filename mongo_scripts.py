__author__ = 'Michael'
import re

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

def add_crime_alias(crime, aliases):
    # Check for existing alias, delete if found
    exists = db.crime_alias.find({"crime": crime})
    if exists.count() > 0:
        db.crime_alias.remove({"crime": crime})
        # print "Crime", crime, "already exists.  Updating"

    # Insert updated alias info
    data = {}
    data['crime'] = crime
    data['alias'] = aliases
    db.crime_alias.insert_one(data)
    # print "Crime", crime, "added"

def get_cities():
    text_file = open("neighborhoods.txt", "r")
    lines = text_file.readlines()
    # print lines
    # print len(lines)
    text_file.close()
    return lines

def extract_city(text, city_list):
    for city in city_list:
        REGEX = re.compile(city)
        m = re.search(city, text, re.M|re.I)
        if m:
            return city

def add_source(name, url, format, divContainer):
    exists = db.sources.find({"name": name})

    if exists.count() == 0:
        result = db.sources.insert_one(
            {
                "name": name,
                "url": url,
                "format": format,
                "divContainer": divContainer
            }
        )
        print "Added ", name, "to sources..."
    else:
        print name, "already exists in sources."

#db.sources.remove({})

# Add News Sources
add_source("NPR","http://api.npr.org/query?id=1001&apiKey=MDE3NTQ2MTM2MDE0MTcwNTkyMjRlMjc5NA001&output=json", "json", "")
add_source("KPCC", "http://feeds.scpr.org/893KpccSouthernCaliforniaNews?format=xml", "rss", "prose-body")
add_source("NBC-LA", "http://www.nbclosangeles.com/news/local/?rss=y&embedThumb=y&summary=y", "rss", "articleText")

# Add Crime Aliases: used to initialize data in mongodb
add_crime_alias("CRIMINAL HOMICIDE", ["murder", "homicide", "criminal homicide"])
add_crime_alias("FORCIBLE RAPE", ["rape"])
add_crime_alias("ROBBERY", ["robbery", "theft"])
add_crime_alias("AGGRAVATED ASSAULT", ["assault"])
add_crime_alias("BURGLARY", ["burglary", "theft"])
add_crime_alias("LARCENY THEFT", ["larceny", "theft"])
add_crime_alias("GRAND THEFT AUTO", ["grand theft auto", "auto theft", "car stolen", "stolen car", "car jack", "car jacking", "car-jacking"])
add_crime_alias("ARSON", ["arson", "fire"])
add_crime_alias("FORGERY", ["forgery"])
add_crime_alias("FRAUD AND NSF CHECKS", ["fraud"])
add_crime_alias("SEX OFFENSES FELONIES", ["sex offense", "sex offender"])
add_crime_alias("SEX OFFENSES MISDEMEANORS", [])
add_crime_alias("NON-AGGRAVATED ASSAULTS", ["assault"])
add_crime_alias("WEAPON LAWS", ["weapon"])
add_crime_alias("OFFENSES AGAINST FAMILY", ["abuse"])
add_crime_alias("NARCOTICS", ["drugs", "narcotics"])
add_crime_alias("LIQUOR LAWS", [])
add_crime_alias("DRUNK / ALCOHOL / DRUGS", ["drunk", "alcohol", "alcoholic", "intoxicated"])
add_crime_alias("DISORDERLY CONDUCT", ["disorderly conduct", "fight", "fighting", "brawl"])
add_crime_alias("VAGRANCY", ["vagrant", "vagrancy", "loitering"])
add_crime_alias("GAMBLING", ["gambling"])
add_crime_alias("DRUNK DRIVING VEHICLE / BOAT", ["drunk driving", "drunk driver", "DUI", "driving while intoxicated"])
add_crime_alias("VEHICLE / BOATING LAWS", ["pulled over", "pulled-over", "speeding"])
add_crime_alias("VANDALISM", ["graffiti", "vandalism", "destruction of property"])
add_crime_alias("WARRANTS", ["warrant"])
add_crime_alias("RECEIVING STOLEN PROPERTY", [])
add_crime_alias("FEDERAL OFFENSES W/O MONEY", [])
add_crime_alias("FEDERAL OFFENSES WITH MONEY", [])
add_crime_alias("FELONIES MISCELLANEOUS", [])
add_crime_alias("MISDEMEANORS MISCELLANEOUS", ["arrest"])


dbSources = db.sources.find()

print "Sources:"
for s in dbSources:
    sourceNews = db.news.find({"source": s["name"]}).count()
    print "\t", s["name"], ":", sourceNews, "articles"

print ""
crimes = db.crimes.find().count()
print "Crimes:", crimes

crime_aliases = db.crime_alias.find().count()
print "Crime aliases:", crime_aliases

print ""
city_list = get_cities()
print "number of cities loaded", len(city_list)
print ""

#########################################################################
# Get list of crimes
dbCrimes = db.crime_alias.find()

for c in dbCrimes:
    # Get list of aliases for each crime
    aliasList = c["alias"]
    for a in aliasList:
        # Search each article for crime
        REGEX = re.compile(a)
        crimeStories = db.news.find({ "body": {"$regex" : REGEX } })
        if crimeStories.count() > 0 :
            print "Crime" , a, "has", crimeStories.count(), "matches"
            #for story in crimeStories:
                #print " >> ", story["title"]


