from pymongo import MongoClient
import requests
import json
import re
import os
import sys
from urllib.parse import urlparse
from threading import Thread
import queue
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import random
from functools import reduce

def beginSearch(tag, filterHashes, current_end_cursor=None):
    global allPosts
    global proxyIp

    if current_end_cursor: link = 'https://www.instagram.com/explore/tags/'+tag+'/?__a=1&max_id='+current_end_cursor
    else: link = 'https://www.instagram.com/explore/tags/'+tag+'/?__a=1'

    print(link)

    data = requestJson(link)

    if not data:
        print("Couldnt connect to page", link)
    else:
        # save next page link
        current_end_cursor = data['graphql']['hashtag']['edge_hashtag_to_media']['page_info']['end_cursor']
        has_next_page = data['graphql']['hashtag']['edge_hashtag_to_media']['page_info']['has_next_page']
        countAll = data['graphql']['hashtag']['edge_hashtag_to_media']['count']

        posts = data['graphql']['hashtag']['edge_hashtag_to_media']['edges']

        # start processing, add to queue
        try:
            for post in posts:
                p = { 'post': post, 'filterHashes': filterHashes  }
                q.put(p)
            q.join()
        except KeyboardInterrupt:
            sys.exit(1)

        allPosts = allPosts+len(posts)
        print("---------------- Processed:", allPosts, " / ", countAll)

        # recursion if has next page
        if has_next_page:
            beginSearch(tag, filterHashes, current_end_cursor)
        else:
            print('All pages were processed', countAll)



def requestJson(link):
    global proxyIp

    ua = UserAgent() # From here we generate a random user agent
    headers = {
        'User-Agent': ua.random
    }

    try:
        response = requests.get(link, headers=headers, timeout=(10,10), proxies = proxyIp)
        if response.status_code == 200:
            try:
                return response.json()
            except:
                print('bad json: ', response)
        else:
            # try proxy
            updateProxy()
            return requestJson(link)

    except requests.exceptions.ConnectTimeout:
        print('Oops. Connection timeout occured!')
    except requests.exceptions.ReadTimeout:
        print('Oops. Read timeout occured')
    except requests.exceptions.ConnectionError:
        print('Seems like dns lookup failed..')
    except requests.exceptions.HTTPError as err:
        print('Oops. HTTP Error occured')
        print('Response is: {content}'.format(content=err.response.content))


def findProxy():
    global proxies

    ua = UserAgent() # From here we generate a random user agent

    headers = {
        'User-Agent': ua.random
    }

    response = requests.get('https://www.sslproxies.org/', headers=headers, timeout=(5,5))
    proxies_doc = response.content

    soup = BeautifulSoup(proxies_doc, 'html.parser')
    proxies_table = soup.find(id='proxylisttable')

    for row in proxies_table.tbody.find_all('tr'):
        proxies.append({
          'ip':   row.find_all('td')[0].string,
          'port': row.find_all('td')[1].string,
          'isHttps': row.find_all('td')[6].string,
        })

def updateProxy():
    global proxies
    global proxyIp

    proxy_index = random_proxy()
    proxy = proxies[proxy_index]

    prot = 'https' if  proxy['isHttps'] == 'yes' else 'http'

    proxyIp = { prot: proxy['ip']+':'+proxy['port'] }
    print("Proxy updated", proxyIp)

def random_proxy():
    global proxies
    return random.randint(0, len(proxies) - 1)

def saveHash(name):
    hashtags = db.hashtags
    hashtag = {
        'name': name.lower()
    }
    # save hashtag and count its frequency
    hashtags.update_one(hashtag, {'$set': hashtag, '$inc': { 'count': 1 } }, upsert=True)


def process(q):
    while True:
        data = q.get()
        post = data['post']
        filterHashes = data['filterHashes']

        description = post['node']['edge_media_to_caption']['edges']

        # shortcode
        shortcode = post['node']['shortcode']
        postFullInfo = postInfo(shortcode)

        ticketIsFound = False
        if len(description):
            descriptionText = description[0]['node']['text']
            result = findHashtags(descriptionText)

            # save hashtags if they related to wc
            words = ['world','cup','fifa','football','чм','футбол','goal','чемпионат','мир','wc2018']
            for hash in result:
                for word in words:
                    search = re.search(r'{0}'.format(word), hash, re.IGNORECASE)
                    if search:
                        saveHash(hash)
                        break


            # check description
            for key in filterHashes:
                searchInDesc = re.search(r'{0}'.format(key), descriptionText, re.IGNORECASE)
                if searchInDesc:
                    ticketIsFound = True
                    savePost(postFullInfo)
                    break

        # check comments
        if postFullInfo and not ticketIsFound:
            cmnts = comments(postFullInfo)
            if cmnts:
                for comment in cmnts:
                    if ticketIsFound: break
                    commentText = comment['node']['text']
                    for key in filterHashes:
                        searchInComment = re.search(r'{0}'.format(key), commentText, re.IGNORECASE)
                        if searchInComment:
                            ticketIsFound = True
                            savePost(postFullInfo)
                            break

        q.task_done()

def findHashtags(text):
    return re.findall(r'#(\w+)', text)

def postInfo(shortcode):
    link = 'https://www.instagram.com/p/'+shortcode+'/?__a=1'
    data = requestJson(link)

    return data

def comments(postInfo):
    comments = postInfo['graphql']['shortcode_media']['edge_media_to_comment']['edges']
    if len(comments):
        return comments

def onlyImages(el_prev, el):
    if 'node' in el:
        item = el['node']
    else: item = el

    # if photo
    if not item['is_video']:
        el_prev.append(item['display_url'])
    return el_prev


def savePost(fullInfo):
    global mainSearchHashtag

    descr = ''
    if 'edges' in fullInfo['graphql']['shortcode_media']['edge_media_to_caption']:
        if len(fullInfo['graphql']['shortcode_media']['edge_media_to_caption']['edges']):
            descr = fullInfo['graphql']['shortcode_media']['edge_media_to_caption']['edges'][0]['node']['text']

    shortcode = fullInfo['graphql']['shortcode_media']['shortcode']
    description = descr
    link = 'https://www.instagram.com/p/'+shortcode+'/'
    hashtags = findHashtags(description) # list
    cmnts = comments(fullInfo) # list
    likes = fullInfo['graphql']['shortcode_media']['edge_media_preview_like']['count'] # int
    loc = fullInfo['graphql']['shortcode_media']['location'] # str

    # several photos
    isCarousel = 'edge_sidecar_to_children' in fullInfo['graphql']['shortcode_media']
    imgs = fullInfo['graphql']['shortcode_media']['edge_sidecar_to_children']['edges'] if isCarousel else [fullInfo['graphql']['shortcode_media']]

    imagesSrc = reduce(onlyImages, imgs, [])

    posts = db.posts
    filterParam = { 'shortcode': shortcode }
    post = {
        'shortcode': shortcode,
        'description': description,
        'link': link,
        'hashtags': {
            'count': len(hashtags),
            'items': hashtags,
        },
        'comments': {
            'count': fullInfo['graphql']['shortcode_media']['edge_media_to_comment']['count'],
            'items': cmnts,
        },
        'likes': likes,
        'images': {
            'count': len(imagesSrc),
            'items': imagesSrc,
        },
        'loc': loc,
        'isTicket': False # manual confirmation
    }
    posts.update_one(filterParam, {'$set': post}, upsert=True)

    # save photo to folder
    for src in imagesSrc:

        result = requests.get(src)
        if result.status_code == 200:
            a = urlparse(src)
            name = shortcode+'---'+os.path.basename(a.path)

            directory = 'imgs/'+mainSearchHashtag
            if not os.path.exists(directory):
                os.makedirs(directory)

            try:
                with open(directory+'/'+name, 'wb') as handler:
                    handler.write(result.content)
            except IOError as e:
                print("I/O error({0}): {1}".format(e.errno, e.strerror))
            except ValueError:
                print("Could not convert data to an integer.")
            except:
                print ("Unexpected error:", sys.exc_info()[0])
                raise

client = MongoClient()
db = client.mydb

allPosts = 0
proxyIp = {}
proxies = []

findProxy()

concurrent = 50
q = queue.Queue(concurrent * 2)

for i in range(concurrent):
    t = Thread(target=process, args=(q,))
    t.daemon = True
    t.start()

# 2 ways of searching - 1: from worldcup -> tickets  |  2: from footballtickets -> worldcup

# hashtag for search
# mainSearchHashtag = 'fifa2018' # way 1
mainSearchHashtag = 'footballtickets' # way 2

# folder for searched images
directory = 'imgs/'+mainSearchHashtag
if not os.path.exists(directory):
    os.makedirs(directory)

# hashtags for filtering results
# filterHashes = ['ticket','билет']  # way 1
filterHashes = ['world','fifa','чм','чемпионат','мир']  # way 2
beginSearch(mainSearchHashtag, filterHashes)
