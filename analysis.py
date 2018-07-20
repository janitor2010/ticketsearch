from pymongo import MongoClient
import pprint
from functools import reduce
from collections import Counter

client = MongoClient()
db = client.mydb
hashtags = db.hashtags
posts = db.posts

# hashtags.delete_many({})
# posts.delete_many({})

def concatList(el_prev, el):
    if 'hashtags' in el:
        el_prev.extend(el['hashtags']['items'])
    return el_prev

def worldCupHashes():
    allHashes = hashtags.find().sort("count")

def makeConfirmed(confirmedTickets):
    uniques = set(confirmedTickets)
    for u in uniques:
        filterParam = { 'shortcode': u }
        post = {
            'isTicket': True # manual confirmation
        }
        posts.update_one(filterParam, {'$set': post}, upsert=True)

def confirmedHashes():
    confirmed = posts.find({ 'isTicket': True })
    return list(reduce(concatList, confirmed, []))

def falseHashes():
    falses = posts.find({ 'isTicket': False })
    return list(reduce(concatList, falses, []))

def topTicketHashes():
    c = Counter(confirmedHashes())
    return c.most_common(20)

# manual confirmation correct posts to find new hashtags
confirmedTickets = ['BjJbi0igVgb','BhzbHBEFm-2','BiCYk54ACXt',
'Bj4ePvmnqZC','Bj9oIvXgNVT','Bj9oIvXgNVT','BjJbi0igVgb','Bjjp-FkAtPW','BjRp1-7FaFw','Bk3Q5XSjI0a','BkcxpzPHsXN',
'BkkjpYBl1-h','BkPOZvdF-zV','Bkv7yTynSPk','Bkv8x2FH3Ww',
'BlT25BnhJNB',
'BlTrmC6gY7u',
'BlUszgzAcxZ',
'BlTSIhzHuD',
'BlT25BnhJNB',
'BlSSkJ8BxL7',
'BlSqm6OlLeM',
'BlRBToxlWWr',
'BlQddJjH2GR',
'BlQ-92Lj9Sy',
'BlQ-BlQ7vWDn9fn',
'BlBOuy4lFZe']

makeConfirmed(confirmedTickets)
print(topTicketHashes())
