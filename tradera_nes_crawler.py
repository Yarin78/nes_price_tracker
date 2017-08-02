# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import urllib, urllib2, urlparse
import os, os.path
import re
#import locale
from datetime import datetime
import logging
import nes_db

TRADERA_URL = 'http://www.tradera.com'
QUERY_STRING = 'NES SCN'

def extract_search_result(soup):
    # Returns a tuple containing the next page in the search
    # and a list of (item-id, item-url) tuples

    items = []

    for item in soup.find_all('li', 'item-card'):
        item_id = item['data-item-id']
        item_url = urlparse.urljoin(TRADERA_URL, item['data-item-url'])
        items.append((item_id, item_url))

    next_tag = soup.find('li','search-pagination-next')
    next_page_url = None
    if next_tag:
        next_page_url = urlparse.urljoin(TRADERA_URL, next_tag.a['href'])

    return (next_page_url, items)

def search_tradera(query_params):
    query_string = urllib.urlencode(query_params)
    url = '%s/search?%s' % (TRADERA_URL, query_string)

    logging.info('Fetching url %s' % url)

    try:
        result = urllib2.urlopen(url)
        content = result.read()
    except urllib2.URLError:
        logging.exception('Caught exception fetching url')
        content = ''

    soup = BeautifulSoup(content, 'html.parser')
    return extract_search_result(soup)

def search_tradera_next(url):
    try:
        result = urllib2.urlopen(url)
        content = result.read()
    except urllib2.URLError:
        logging.exception('Caught exception fetching url')
        content = ''

    soup = BeautifulSoup(content, 'html.parser')
    return extract_search_result(soup)



def extract_item(soup):
    if soup.find('article','view-item-ended-summary'):
        return extract_finished_auction_item(soup)
    else:
        return extract_ongoing_auction_item(soup)

def fix_short_date(dt):
    # Parses a date in the format "5 maj 17:34"
    # Assumes the year is the current year
    dt = dt.replace('maj', 'may').replace('oct', 'okt') # Ugly hack
    #locale.setlocale(locale.LC_ALL, "sv_SE") # Doesn't work in dev AppEngine environment
    dt = datetime.strptime(dt, '%d %b %H:%M')
    dt = dt.replace(year=datetime.now().year)
    return dt

def extract_price(price_str):
    stripped = price_str.replace('kr', '')
    pattern = re.compile(r'[\s\xa0]+') # Thousand separators may become \xa0
    stripped = re.sub(pattern, '', stripped)
    #print 'Extracting price from string "%s" to "%s"' % (price_str, stripped)
    return int(stripped)


def extract_ongoing_auction_item(soup):
    title = soup.find('header', 'view-item-details-header').h1.string

    image_url = urlparse.urljoin(TRADERA_URL, soup.find('article','image-gallery').img['src'])

    article=soup.find('article', 'view-item-details-wrapper')

    fixed_price_tag = article.find('h2', 'view-item-fixed-price')
    if fixed_price_tag:
        price = extract_price(fixed_price_tag.string)
        end_date = None
        bids = 0
    else:
        end_date = article.find('span', 'view-item-bidding-details-enddate').string
        end_date = fix_short_date(end_date)
        if end_date < datetime.now():
            # Year wrapping, end is in january and we're currently in december
            end_date = end_date.replace(year=end_date.year + 1)
        end_date = end_date.isoformat()
        bids = int(article.find(attrs={"data-bid-count":True}).string)
        if bids > 0:
            price = extract_price(article.find('span', 'view-item-bidding-details-amount').span.string)
        else:
            price = extract_price(article.find('span', 'view-item-bidding-details-heading').next_sibling.string)

    shipping = ' '.join([x.strip() for x in list(soup.find('ul', 'view-item-details-shipping-details-options-list').strings)]).strip()

    # The rest is same as for finished auctions items
    seller = soup.find('a', 'view-item-details-list-seller-name').span.string

    description_tag = soup.find('section', 'view-item-description').find('div', 'content-text')
    description = ' '.join([x.strip() for x in description_tag.strings])

    published_time = soup.find('li','view-item-footer-information-details-published').strong.next_sibling.strip()
    item_id = int(soup.find('li','view-item-footer-information-details-itemid').strong.next_sibling.strip())

    res = {
        'id'          : item_id,
        'title'       : title,
        'image_url'   : image_url,
        'price'       : price,
        'bids'        : bids,
        'seller'      : seller,
        'shipping'    : shipping,
        'description' : description,
        'published'   : published_time,   # 2017-06-17 09:41
        'ending'      : end_date  # None if fixed price
    }

    return res

def extract_finished_auction_item(soup):
    article=soup.find('article','view-item-ended-summary')

    title = article.h2.string
    image_url = urlparse.urljoin(TRADERA_URL, article.img['src'])

    finished_tag = article.find('span', string='Avslutad')
    if finished_tag:
        finished_time = finished_tag.next_sibling.next_sibling.string

    end_bid_amount_tag = article.find('span', 'view-item-ended-summary-bid-amount')
    if end_bid_amount_tag:
        price = extract_price(end_bid_amount_tag.string)
        buyer = end_bid_amount_tag.next_sibling.next_sibling.string
    else:
        # No winner, different tag
        price_tag = article.find('span', string='Utropspris')
        if price_tag:
            price = extract_price(price_tag.next_sibling.next_sibling.string)
        else:
            # Secret price!? "Reservationspris ej uppnått"
            # http://www.tradera.com/item/300806/284644656/nintendo-entertainment-system-nes-scn
            price = -1
        buyer = None

    bid_count_tag = article.find('span','view-item-ended-summary-bid-count')
    if bid_count_tag:
        bids = int(bid_count_tag.a.string.split(' ')[0])
    else:
        bids = 0

    shipping = ' '.join([x.string for x in article.find_all('span', 'view-item-ended-summary-shipping-option')])

    # The rest is same as for ongoing auctions items
    seller = soup.find('a', 'view-item-details-list-seller-name').span.string

    description_tag = soup.find('section', 'view-item-description').find('div', 'content-text')
    description = ' '.join([x.strip() for x in description_tag.strings])

    published_time = soup.find('li','view-item-footer-information-details-published').strong.next_sibling.strip()
    item_id = int(soup.find('li','view-item-footer-information-details-itemid').strong.next_sibling.strip())

    finished_time = fix_short_date(finished_time)
    if finished_time > datetime.now():
        # We're currently in january and auction finished in december
        finished_time = finished_time.replace(year=end_date.year - 1)

    finished_time = finished_time.isoformat()

    res = {
        'id'          : item_id,
        'title'       : title,
        'image_url'   : image_url,
        'price'       : price,
        'bids'        : bids,
        'seller'      : seller,
        'buyer'       : buyer,
        'shipping'    : shipping,
        'description' : description,
        'published'   : published_time,   # 2017-06-17 09:41
        'finished'    : finished_time
    }

    return res



def save_item(data):
    db = nes_db.connect()
    cursor = db.cursor()

    query = """INSERT INTO tradera_items(id, title, url, image_url, price, bids, fixed_price, seller, published, finished, ending, buyer, shipping, content, first_crawled, last_crawled)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                title=VALUES(title),
                price=VALUES(price),
                bids=VALUES(bids),
                finished=VALUES(finished),
                ending=VALUES(ending),
                buyer=VALUES(buyer),
                shipping=VALUES(shipping),
                content=VALUES(content),
                image_url=VALUES(image_url),
                last_crawled=VALUES(last_crawled);"""

    now = datetime.now().isoformat()
    finished = data.get('finished', None)
    ending = data.get('ending', None)
    buyer = data.get('buyer', None)
    if buyer:
        buyer = buyer.encode('utf-8')
    fixed_price = 0
    if not ending and not finished:
        fixed_price = 1

    #print data
    cursor.execute(query, [
        data['id'],
        data['title'].encode('utf-8'),
        data['url'],
        data['image_url'],
        data['price'],
        data['bids'],
        fixed_price,
        data['seller'].encode('utf-8'),
        data['published'],
        finished,
        ending,
        buyer,
        data['shipping'].encode('utf-8'),
        data['description'],
        now,
        now]);

    db.commit()

def crawl_item(url):
    # Crawls a Tradera item given it's URL and updates the database
    try:
        # Remove any non-ASCII characters in the URI. Seems to work well enough.
        clean_url = re.sub(r'[^\x00-\x7F]+','', url)
        result = urllib2.urlopen(clean_url)
        content = result.read().decode('utf-8')
    except urllib2.URLError:
        logging.exception('Caught exception fetching URL %s' % url)
        return

    soup = BeautifulSoup(content, 'html.parser')
    data = extract_item(soup)
    data['url'] = url
    save_item(data)

def test_crawl_items():
    crawl_item('http://www.tradera.com/item/300801/284299979/megaman-3-nes-scn-')
    crawl_item('http://www.tradera.com/item/300810/286817599/chip-and-dale-scn-cib-nes')
    crawl_item('http://www.tradera.com/item/300801/240958184/the-simpsons-bart-vs-the-space-mutants-scn-cib-nes')
    crawl_item('http://www.tradera.com/item/300808/286707375/double-dribble-scn-nes')

def test_extract_item():
    for x in ['finished_auction', 'finished_no_winner', 'ongoing_auction', 'ongoing_auction2', 'ongoing_fixed_price']:
        f=open('examples/%s.html' % x)
        print extract_item(BeautifulSoup(f.read(), 'html.parser'))


#crawl_item('http://www.tradera.com/item/300813/286835828/super-mario-bros-nes-scn')
#crawl_item('http://www.tradera.com/item/300810/284744630/yoshis-cookie-nes-scn-cib')
#crawl_item('http://www.tradera.com/item/300810/286878928/robowarrior-scn-svensksalt-nes')
#crawl_item('http://www.tradera.com/item/300806/284644656/nintendo-entertainment-system-nes-scn')
#crawl_item('http://www.tradera.com/item/300810/283107722/déjá-vu-nes-scn-mycket-fint-skick')

#test_extract_item()

#print search_tradera()
#print search_tradera_next('http://www.tradera.com/search?itemType=All&itemCondition=All&sellerType=All&sortBy=Relevance&priceRange=All&itemStatus=Ended&county=HelaSverige&q=NES+SCN&queryScope=AllWordsAnyOrder&paging=MjpTaG9wSXRlbXw0OHw2NTM.&spage=2')
