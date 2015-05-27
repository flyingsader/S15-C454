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
import statistics


###############################################################
## Data Acquisition Main()

debug = 0

# DB client
mongo = MongoClient()
db = mongo.bigDataTest

pipeline = [ {"$group" : {"_id":"$zip", "count":{"$sum":1}} }, { "$group": { "_id": "$_id", "avgCount": { "$avg": "$count" } } }, { "$sort": { "avgCount": -1  }  } ];
sumByZip = db.crimes.aggregate( pipeline );

averages = []

for zipEntry in sumByZip:
    print zipEntry["_id"], "=", zipEntry["avgCount"];
    averages.append( zipEntry["avgCount"] );

medianValue = statistics.median(averages);
avg = statistics.mean(averages);
mode = statistics.mode(averages);
stdev = statistics.stdev(averages, medianValue);

print "==============================";
print "mean/avg crimes per zipcode", avg;
print "median crimes per zipcode", medianValue;
print "mode crimes per zipcode", mode;
print "stdev crimes per zipcode", stdev;



# db.Listing.find().forEach(function(item){
#     db.Listing.update({_id: item._id}, {$set: { LowerCaseAddress: item.Address.toLowerCase() }})
# })

def read_file(filename):
    with open(filename, 'r') as f:
        data = [row for row in csv.reader(f.read().splitlines())]
    return data


csv_path = "2010+Census+Population+By+Zipcode+(ZCTA).csv"
cr = read_file(csv_path);

for row in cr:
    zipcode = row[0];
    population = row[1];
    print "searching for zip", zipcode, "..."
    matchingzip = db.crimes.find( { "zip": zipcode }  ); #     sourceNews = db.news.find({"source": s["name"]}).count()
    print " => found ", matchingzip.count(), "matches"
    for match in matchingzip:
        db.crimes.update(  { "_id": match["_id"] }, { "$set": { "population" : population }  } );


# # get_neighborhood_for_crimes()
# # exit(0)
#
# # Sheriff's Department Data
# # http://shq.lasdnews.net/CrimeStats/CAASS/PART_I_AND_II_CRIMES.csv
#
# print "Loading Sheriff's crimes database..."
#
# csv_path = "2014-PART_I_AND_II_CRIMES.csv"
# with open(csv_path, "rb") as csvfile:
#     cr = csv.reader(csvfile)
#
#     for row in cr:
#         # add_crimes(row[11], row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[15], row[16])
#         # (          id      , incidentDate, category, stat  , statDesc, addressStreet, city  , zip   , xCoor , yCoor  , incidentId, reportDistrict, seq    , unitId , unitName, deleted):
#         add_crimes(row[11] , row[0]      , row[2]  , row[3], row[4]  , row[5]       , row[7], row[8], row[9], row[10], row[11]   , row[12]       , row[13], row[15], row[16] , row[17]);
#
#         # print "incidentDate", row[0]
#         # print "incidentReportedDate", row[1]
#         # print "category", row[2]
#         # print "state", row[3]
#         # print "statDesc", row[4]
#         # print "address", row[5]
#         # print "street", row[6]
#         # print "city", row[7]
#         # print "zip", row[8]
#         # print "xCoor", row[9]
#         # print "yCoor", row[10]
#         # print "incidentId", row[11]
#         # print "reportingDistrict", row[12]
#         # print "seq", row[13]
#         # print "gangRelated", row[14]
#         # print "unitId", row[15]
#         # print "unitName", row[16]
#         # print "deleted", row[17]
#         #
#         # break


