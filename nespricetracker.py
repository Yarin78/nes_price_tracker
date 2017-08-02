# -*- coding: utf-8 -*-

import datetime
import os
import urllib, urlparse
import logging
import nes_db
from google.appengine.api import taskqueue


import webapp2
import MySQLdb

import tradera_nes_crawler


class MainPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('Nothing to see here!')

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
    ('/crawl', TraderaSearchCrawler),
    ('/crawl_task', TraderaSearchCrawler),
    ('/crawl_item_task', TraderaItemCrawler)
], debug=True)
