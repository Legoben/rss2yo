from tornado import ioloop, web, escape, gen, httpclient #install
import torndb #install
import feedparser #install
import sys
import time
import datetime
import dateutil.parser as parser #install
from multiprocessing.pool import ThreadPool

_workers = ThreadPool(15)


print "restarted"

def checkRSS(entry):
    #print entry

    #def parseRSS(resp):
        try:
            client = httpclient.HTTPClient()
            req = httpclient.HTTPRequest(entry['url'], connect_timeout=5, request_timeout=3, user_agent="rssyo.com")
            resp = client.fetch(req)

            feed = feedparser.parse(resp.body)

            if len(feed['items']) == 0:
                #print("no items")
                return

            if entry['datetime'] != '':

                if parser.parse(entry['datetime']) < parser.parse(feed['items'][0]['published']):
                    print("new")

                    #Send the Yo
                    client = httpclient.HTTPClient()
                    req = httpclient.HTTPRequest("http://api.justyo.co/yoall/", method='POST', body="api_token="+entry['apikey']+"&link="+feed['items'][0]['link'], user_agent="rssyo.com")
                    try:
                        resp = client.fetch(req)
                    except Exception:
                        print(req.body)
                        return

                    if 'id' in feed['items'][0]:
                        id = feed['items'][0]['id']
                    elif 'title' in feed['items'][0]:
                        id = feed['items'][0]['title']

                    date = feed['items'][0]['published']

                    print(date, id, entry['id'])
                    mysql.execute("UPDATE feeds SET datetime=%s, lastid=%s WHERE id=%s", date, id, entry['id'])

                    print("yo")
            else:
                if 'id' in feed['items'][0]:
                    id = feed['items'][0]['id']
                elif 'title' in feed['items'][0]:
                    id = feed['items'][0]['title']
                else:
                    return

                if entry['lastid'] != id:
                    print("new")

                    #Send the Yo
                    client = httpclient.HTTPClient()
                    req = httpclient.HTTPRequest("http://api.justyo.co/yoall/", method='POST', body="api_token="+entry['apikey']+"&link="+feed['items'][0]['link'], user_agent="rssyo.com")
                    client.fetch(req)
                    mysql.execute("UPDATE feeds SET datetime=%s, lastid=%s WHERE id=%s", "", id, entry['id'])
        except Exception as e:
            print(e, entry['url'])
            pass








@gen.engine
def crawlRSS():
    res = mysql.query("SELECT * FROM feeds")

    for entry in res:
        #print(entry['url'])
        try:
            _workers.apply_async(checkRSS, (entry, ))
            #checkRSS(entry)
        except Exception as e:
            print(e)
            pass

    print "done"
    yield gen.Task(ioloop.IOLoop.instance().add_timeout, time.time() + 300)

    #time.sleep(30)

    crawlRSS()



class IndexHandler(web.RequestHandler):
    def get(self):
        self.render("pages/index.html")

    def post(self):
        self.add_header("Content-Type:", "application/json")

        url = self.get_argument("url", strip=True)
        apikey = self.get_argument("apikey", strip=True)

        q = mysql.query("SELECT * FROM feeds WHERE apikey=%s AND url=%s", apikey,url)


        if len(q) != 0:
            self.write('{"error":"Already exists"}')
            return

        query = mysql.query("SELECT * FROM feeds WHERE apikey=%s", apikey)
        if len(query) >= 10:
            self.write('{"error":"Too many broadcasts."}')
            return

        #print(q)


        #print(url, apikey)

        if apikey == '' or url == '':
            self.write('{"error":"Pleae don\'t leave blank fields"}')
            return

        try:
            f =feedparser.parse(url)
        except Exception :
            self.write('{"error":"Could not connect to RSS Feed"}')
            return


        if 'bozo_exception' in f:
            self.write('{"error":"Error: Could not parse RSS Feed"}')
            return

        if len(f['items']) != 0:

            if 'published' in f['items'][0]:
                published = f['items'][0]['published']
            else:
                published = ''


            if 'id' in f['items'][0]:
                id = f['items'][0]['id']
            elif 'title' in f['items'][0]:
                id = f['items'][0]['title']
            else:
                self.write('{"error":"Could not parse RSS Feed - Required title tag not found."}')
                return

            row = mysql.execute("INSERT INTO feeds VALUES (0, %s, %s, %s, %s)", url, apikey, published, id)

            self.write('{"success":true}')





        else:
            self.write('{"error":"RSS Feed did not appear to have any items in it."}')
            return




class DeleteFeeds(web.RequestHandler):

    def post(self):
        print("DELETING")
        apikey = self.get_argument("apikey", None, True)
        if apikey == None or apikey == '':
            self.write('{"error":"API Key must not be blank"}')
            return

        row = mysql.execute("DELETE FROM feeds WHERE apikey=%s", apikey)
        #print(row)
        self.write('{}')


try:
    #Connect to SQL
    mysql = torndb.Connection("localhost", "yo2rss", user="root", password="")
    q = open("feeds.sql").read()
    try:
        pass
        #mysql.execute(q)
    except Exception:
        print "table already exists"

except Exception as e:
    print(e)
    sys.exit("Error: Could not connect to MySQL.")

app = web.Application([
     (r'/', IndexHandler),
    (r'', IndexHandler),
    (r'/delete', DeleteFeeds),
    (r'/delete/(.*)', DeleteFeeds),
    (r'/static/(.*)', web.StaticFileHandler, {'path': "static"}),
], debug=True)

if __name__ == '__main__':
    app.listen(80)
    crawlRSS()
    #_workers.apply_async(crawlRSS)
    ioloop.IOLoop.instance().start()


