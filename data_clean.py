import re
import sqlite3
import csv
#tweets = "saya retweet @inipatutdibuang"
# tweets = "test https://www.sajejee.com/testing/adesub bukan url"
#tweets = tweets.replace('http\S+|www.\S+', '')
#tweets = tweets.replace('@\s+', '')
#tweets = tweets.replace(r'@.*?(?=\s)', '')

# m = re.search('http(.+?)$',tweets)
# url = 'http'+m.group(1)
# #kalau url bukan kat hujung
# if ' ' in url:
#     m =re.search('http(.+?) ',tweets)
#
# url = 'http'+m.group(1)
# p = tweets.replace(url,'')
# #p = p.replace('http.com','')
# print(m.group(1))
# print(url)
# print(p)

def cleanUrl(tweet):
    filter = re.search('http(.+?)$', tweet)
    if filter != None:
        while filter != None:
            url = 'http' + filter.group(1)
            if ' ' in url:
                filter = re.search('http(.+?) ', tweet)
                url = 'http' + filter.group(1)+' '
            tweet = tweet.replace(url,' ')
            #print("Delete : " + url + "\nCleaned : " + tweet)
            filter = re.search('http(.+?)$', tweet)

        return tweet
    else:
        return tweet

def remove_unicode(tweet):
    filter = re.search(r'\\u(.+?)$', tweet)
    if filter != None:
        while filter != None:
            url = '\\u' + filter.group(1)
            if ' ' in url:
                filter = re.search(r'\\u(.+?) ', tweet)
                url = '\\u' + filter.group(1) + ' '
            tweet = tweet.replace(url, ' ')
            # print("Delete : " + url + "\nCleaned : " + tweet)
            filter = re.search(r'\\u(.+?)$', tweet)

        return tweet
    else:
        return tweet

#by HuseinZol
def filter_emoji(in_str):
    emoji_pattern = re.compile("["
    u"\U0001F600-\U0001F64F"  # emoticons
    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
    u"\U0001F680-\U0001F6FF"  # transport & map symbols
    u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
    u"\U00010000-\U0001FFFF" # segala jenis unicode dibuang
    u"\u2705"
                       "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', in_str)

conn = sqlite3.connect('streamingscrap.sqlite')
curs = conn.cursor()
query = 'SELECT * FROM Tweets;'
#rows = curs.execute(query)
rows = curs.execute(query)
rows = rows.fetchall()



with open('twittercleaned.csv', 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['id', 'twitter']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for row in rows:
        tweets = str(row[1])
        tweets = tweets.replace('\n', ' ')
        id = str(row[0])
        row1 = id
        row2 = filter_emoji(cleanUrl(tweets))
        # row2 = row2.replace(r'\u2705','')
        writer.writerow({'id': row1, 'twitter': row2})

    #print(id + " "+ filter_emoji(cleanUrl(tweets)))
    # m = re.search('http(.+?)$', tweets)
    # if m != None:
    #     url = 'http' + m.group(1)
    #     if ' ' in url:
    #         m = re.search('http(.+?) ', tweets)
    #         url = 'http' + m.group(1)
    #     cleaned = tweets.replace(url, '')
    #     print("Deleted : " + url)
    #     print("Cleaned : " + cleaned)


