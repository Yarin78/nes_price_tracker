# -*- coding: utf-8 -*-

import datetime
import os
import urllib, urlparse
import logging
import nes_db
import re
import tradera_nes_crawler
from google.appengine.api import taskqueue
from google.appengine.ext.webapp import template
import webapp2
import MySQLdb

class MainPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('Nothing to see here!')

class ClassifyPage(webapp2.RequestHandler):
    def get(self):
        db = nes_db.connect()
        cursor = db.cursor()
        cursor.execute("""
            SELECT t.id, t.title, url, image_url, content, coalesce(g.title, '') as game_title,
                t.multi = 1, coalesce(t.cartridge, 1) = 1, t.manual = 1, t.box = 1, coalesce(t.comment, '')
            FROM tradera_items AS t
            LEFT JOIN games AS g ON t.game_id=g.id
            WHERE t.verified IS NULL OR t.verified=0
            ORDER BY id LIMIT 10""")
        items = []
        for (id, title, url, image_url, content, game_title, multi, cartridge, manual, box, comment) in cursor.fetchall():
            items.append({
                'id': id,
                'title': title,
                'url': url,
                'image_url': image_url,
                'content': content,
                'game_title' : game_title,
                'multi' : multi,
                'cartridge' : cartridge,
                'manual' : manual,
                'box' : box,
                'comment' : comment })

        cursor.execute("SELECT title FROM games")
        games = [str(row[0]) for row in cursor.fetchall()]

        template_values = {
            'items' : items,
            'all_games' : games
        }

        path = os.path.join(os.path.dirname(__file__), 'classify.html')
        self.response.out.write(template.render(path, template_values))

    def post(self):
        db = nes_db.connect()
        cursor = db.cursor()

        cursor.execute("SELECT id, title FROM games")
        game_title_map = {} # title -> id
        for row in cursor.fetchall():
            game_title_map[row[1]] = int(row[0])

        param_pattern = re.compile("item\[([0-9]*)\]\[([a-z0-9_]*)\]")
        items={}
        for k,v in self.request.params.iteritems():
            m=param_pattern.match(k)
            item_id = int(m.groups(0)[0])
            item_prop = m.groups(0)[1]
            if not item_id in items:
                items[item_id] = {}
            items[item_id][item_prop] = v

        item_cnt = 0
        for (item_id, props) in items.iteritems():
            if props.get('comment').lower() == 'skip':
                continue

            data = []
            query = "UPDATE tradera_items SET "
            stmts = []
            if 'game_title' in props and props['game_title'] in game_title_map:
                stmts.append("game_id = %s")
                data.append(game_title_map[props['game_title']])
            else:
                stmts.append("game_id = null")
            stmts.append("multi = %s")
            data.append(props.get('multi', 'off') == 'on')
            stmts.append("cartridge = %s")
            data.append(props.get('cartridge', 'off') == 'on')
            stmts.append("manual = %s")
            data.append(props.get('manual', 'off') == 'on')
            stmts.append("box = %s")
            data.append(props.get('box', 'off') == 'on')
            stmts.append("comment = %s")
            data.append(props.get('comment'))
            stmts.append("verified = 1")

            data.append(item_id)
            cursor.execute(query + ",".join(stmts) + " WHERE id=%s", data)
            item_cnt += 1

        db.commit()

        self.response.headers['Content-Type'] = 'text/html'
        self.response.out.write('<html><body><p>Updated %d items!<p><a href="/classify">Classify more</a></body></html>' % item_cnt)

class AutoClassifyDuplicates(webapp2.RequestHandler):
    def get(self):
        db = nes_db.connect()
        cursor = db.cursor()

        result = cursor.execute("""update tradera_items as t1
inner join tradera_items as t2 on t1.title=t2.title and t1.seller=t2.seller
set t1.game_id=t2.game_id,
	t1.multi=t2.multi,
    t1.cartridge=t2.cartridge,
    t1.box=t2.box,
    t1.manual=t2.manual,
    t1.verified=t2.verified,
    t1.comment=t2.comment
where coalesce(t1.verified,0)=0 and t2.verified=1;""")
        db.commit()
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('%d duplicates classified' % result)


class TraderaSearchCrawler(webapp2.RequestHandler):
    def get(self):
        task = taskqueue.add(
            url='/crawl_task',
            target='default',
            params=self.request.params)

        msg = 'Crawl search task enqueued, ETA {}.'.format(task.eta)
        logging.info(msg)

        self.response.headers['Content-Type'] = 'text/plain'
        self.response.write(msg)

    def post(self):
        url = self.request.get('url', None)
        params = self.request.params

        if url:
            params = dict(urlparse.parse_qsl(urlparse.urlparse(url).query))
            logging.info('Searching Tradera (cont.) at %s' % url)
            (next_url, items) = tradera_nes_crawler.search_tradera_next(url)
        else:
            logging.info('Starting new search at Tradera with params %s' % urllib.urlencode(self.request.params))
            (next_url, items) = tradera_nes_crawler.search_tradera(self.request.params)

        logging.info('Search result contained %d items' % len(items))

        finished_items = params.get('itemStatus', '').lower() == 'ended'
        #logging.info('Finished items: %s' % finished_items)

        data = [int(item_id) for (item_id, item_url) in items]
        query = "SELECT id FROM tradera_items WHERE id IN (" + ",".join(['%s'] * len(items)) + ")"
        if finished_items:
            # Never recrawl finished auctions that we already have in the DB
            query += " AND finished IS NOT NULL"
        else:
            # Recrawl ongoing auctions if older than 24 hours since last crawl
            query += " AND last_crawled > %s"
            yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).isoformat()
            data.append(yesterday)

        db = nes_db.connect()
        cursor = db.cursor()
        cursor.execute(query, data)
        skip_ids = set()
        for row in cursor.fetchall():
            skip_ids.add(row[0])

        no_items = 0
        for (item_id, item_url) in items:
            if not int(item_id) in skip_ids:
                taskqueue.add(
                    url='/crawl_item_task',
                    target='default',
                    params={'url': item_url, 'id': item_id})
                no_items += 1
                #break

        logging.info('%d crawl item tasks enqueued' % no_items)

        if next_url:
            task = taskqueue.add(
                url='/crawl_task',
                target='default',
                params={'url': next_url})
            logging.info('Crawl search task enqueued for next page %s' % next_url)
        else:
            logging.info('Last search result page reached')

class TraderaItemCrawler(webapp2.RequestHandler):
    def post(self):
        url = self.request.get('url', None)
        id = self.request.get('id', None)
        if url and id:
            logging.info('Crawling Tradera item %d at %s...' % (int(id), url))
            tradera_nes_crawler.crawl_item(url)

application = webapp2.WSGIApplication([
    ('/', MainPage),
    ('/classify', ClassifyPage),
    ('/auto_classify_duplicates', AutoClassifyDuplicates),
    ('/crawl', TraderaSearchCrawler),
    ('/crawl_task', TraderaSearchCrawler),
    ('/crawl_item_task', TraderaItemCrawler)
], debug=True)
