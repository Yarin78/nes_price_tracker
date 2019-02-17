# -*- coding: utf-8 -*-

import datetime
import os
from urllib.parse import urlparse, parse_qsl
import logging
import nes_db
import re
import json
import tradera_nes_crawler
from flask import Flask, render_template, request, abort
import pymysql
from google.cloud import tasks_v2beta3

app = Flask(__name__)

GCP_PROJECT = 'yarin-nes-sales'
QUEUE_NAME = 'tradera-nes-crawler-queue'
LOCATION = 'europe-west1'

@app.route('/')
def main():
    return 'Nothing to see here!'


def classify_duplicates():
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
        t1.verified_at=t2.verified_at,
        t1.comment=t2.comment
        where coalesce(t1.verified,0)=0 and t2.verified=1;""")
    db.commit()
    return result


@app.route('/classify', methods=['GET'])
def classify_get():
    db = nes_db.connect()
    cursor = db.cursor()
    cursor.execute("SELECT count(*) FROM tradera_items AS t WHERE t.verified IS NULL OR t.verified=0;")
    total_items = cursor.fetchone()[0]
    cursor.execute("SELECT count(DISTINCT title, seller) FROM tradera_items AS t WHERE t.verified IS NULL OR t.verified=0;")
    unique_items = cursor.fetchone()[0]

    cursor.execute("""
        SELECT t.id, t.title, t.seller, url, image_url, content, coalesce(g.title, '') as game_title,
            t.multi = 1, coalesce(t.cartridge, 1) = 1, t.manual = 1, t.box = 1, coalesce(t.comment, '')
        FROM tradera_items AS t
        LEFT JOIN games AS g ON t.game_id=g.id
        WHERE t.verified IS NULL OR t.verified=0
        ORDER BY id LIMIT 10""")
    items = []
    seen=set()
    for (id, title, seller, url, image_url, content, game_title, multi, cartridge, manual, box, comment) in cursor.fetchall():
        ts = "%s: %s" % (seller, title)
        if ts in seen:
            continue
        seen.add(ts)
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

    return render_template('classify.html',
        items=items,
        all_games=games,
        total_items=total_items,
        unique_total_items=unique_items)


@app.route('/classify', methods=['POST'])
def classify_post():
    db = nes_db.connect()
    cursor = db.cursor()

    cursor.execute("SELECT id, title FROM games")
    game_title_map = {} # title -> id
    for row in cursor.fetchall():
        game_title_map[row[1]] = int(row[0])

    param_pattern = re.compile("item\[([0-9]*)\]\[([a-z0-9_]*)\]")
    items={}
    for k,v in request.form.items():
        m=param_pattern.match(k)
        item_id = int(m.groups(0)[0])
        item_prop = m.groups(0)[1]
        if not item_id in items:
            items[item_id] = {}
        items[item_id][item_prop] = v

    item_cnt = 0
    for (item_id, props) in items.items():
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
        stmts.append("verified_at = now()")

        data.append(item_id)
        cursor.execute(query + ",".join(stmts) + " WHERE id=%s", data)
        item_cnt += 1

    db.commit()
    duplicates = classify_duplicates()

    return render_template('classify_response.html', item_cnt=item_cnt, duplicates=duplicates)


@app.route('/auto_classify_duplicates')
def auto_classify_duplicates():
    result = classify_duplicates()
    return '%d duplicates classified' % result


@app.route('/crawl')
def crawl():
    client = tasks_v2beta3.CloudTasksClient()
    parent = client.queue_path(GCP_PROJECT, LOCATION, QUEUE_NAME)

    task = {
        'app_engine_http_request': {
            'http_method': 'POST',
            'relative_uri': '/crawl_task',
            'body': json.dumps({'params': request.args}).encode(),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
    }

    response = client.create_task(parent, task)
    logging.info('Created crawl search task {}'.format(response.name))
    return str(response)


@app.route('/crawl_task', methods=['POST'])
def crawl_task():
    payload=request.get_json()
    if not payload:
        return abort(400, 'Payload missing')

    url = payload.get('url')
    params = payload.get('params')

    if url:
        params = dict(parse_qsl(urlparse(url).query))
        logging.info('Searching Tradera (cont.) at %s' % url)
        (next_url, items) = tradera_nes_crawler.search_tradera_next(url)
    else:
        logging.info('Starting new search at Tradera with params %s' % params)
        (next_url, items) = tradera_nes_crawler.search_tradera(params)

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

    client = tasks_v2beta3.CloudTasksClient()
    parent = client.queue_path(GCP_PROJECT, LOCATION, QUEUE_NAME)

    no_items = 0
    for (item_id, item_url) in items:
        if not int(item_id) in skip_ids:
            task = {
                'app_engine_http_request': {  # Specify the type of request.
                    'http_method': 'POST',
                    'relative_uri': '/crawl_item_task',
                    'body': json.dumps({'url': item_url, 'id': int(item_id)}).encode(),
                    'headers': {
                        'Content-Type': 'application/json'
                    }
                }
            }
            client.create_task(parent, task)
            no_items += 1

    logging.info('%d crawl item tasks enqueued' % no_items)

    if next_url:
        task = {
            'app_engine_http_request': {  # Specify the type of request.
                'http_method': 'POST',
                'relative_uri': '/crawl_task',
                'body': json.dumps({'url': next_url}).encode(),
                'headers': {
                    'Content-Type': 'application/json'
                }
            }
        }
        client.create_task(parent, task)
        logging.info('Crawl search task enqueued for next page %s' % next_url)
    else:
        logging.info('Last search result page reached')

    return 'Search done'


@app.route('/crawl_item_task', methods=['POST'])
def crawl_item_task():
    logging.info('is at crawl_item_task')
    payload=request.get_json()
    if not payload:
        return abort(400, 'Payload missing')
    logging.info('has payload')
    try:
        url = payload['url']
        id = int(str(payload['id']))
    except:
        return abort(400, 'Both url and id must be set in payload')

    logging.info('Crawling Tradera item %d at %s...' % (id, url))
    tradera_nes_crawler.crawl_item(url)
    return 'Crawled item %d successfully' % id


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    app.run(host='127.0.0.1', port=8080, debug=True)
