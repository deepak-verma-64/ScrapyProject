# -*- coding: utf-8 -*-

import re
from urllib.parse import urlparse, urljoin
import decimal
import traceback
import dateparser
import w3lib.url
from tables import *
import scrapy.spiders
from scrapy.http.cookies import CookieJar
from datetime import datetime
from pytz import timezone
from mmdash.settings import *
from helpers import find_between
from mmdash.items import Listing, Lister
from scrapy.http.request import Request
from datetime import datetime, timedelta
import calendar
from mmdash import settings
import random
from random import shuffle
import json
import helpers


class DubiCarsSpider(scrapy.Spider):
    name = 'dubicars'
    lmt_enabled = True
    pure_nnl = False
    custom_settings = {
        'LMT_USERNAME': 'lum-customer-hl_65606ef5-zone-fixed',
        'RETRY_HTTP_CODES': [403, 500, 501, 502, 503]
    }

    fields_to_validate = {"root_fields": {"category_1": str, "category_2": str, "listing_id_on_site": str, "url": str, "title": str,
                     "description": str, "created_at": str, "price": float, "currency": str, "featured": bool,
                     "promo_value": list, "region": str, "country": str, "item_condition": str, "car_brand": str,
                     "car_model": str, "car_year": str, "car_fuel": str, "car_transmission": str, "user_id": str,
                     "user_first_name": str, "user_type": str, "user_subtype": str, "business_url": str,
                     "business_name": str, "business_phone": str, "image_main": str, "images": list,
                     "image_count": int}, "references": {}, "databag": {}}

    check_for_other_processes = False

    def __init__(self, mode='serp_only', country='AE', days=3, limit=500000):
        self.isProduction = True
        self.days = int(days)
        self.country = country
        self.site_id = 3
        self.region = 'MEA'
        self.site = 'dubicars.com'
        self.args = locals()
        self.tz = timezone('Asia/Dubai')
        self.base_url = 'https://www.dubicars.com/'
        self.mode = mode

        self.categories = [
            {'name': 'new', 'category_id': 2, 'subcategory_id': 1,
             'path': 'https://www.dubicars.com/search?view=&o=&fs=0&fss=0&ma=&mo=0&eo=&pf=&pt=&emif=100&emit=20000&fb=&dp=0&lp=60&l=&yf=&yt=&kf=&kt=&c=new&st=&b=&f=&g=&cy=&co=&gi={}&s=&st={}'},
            {'name': 'used', 'category_id': 2, 'subcategory_id': 1,
             'path': '/search?view=&o=&fs=0&fss=0&ma=&mo=0&eo=&pf=&pt=&emif=100&emit=20000&fb=&dp=0&lp=60&l=&yf=&yt=&kf=&kt=&c=used&st=&b=&f=&g=&cy=&co=&gi={}&s=&st={}'}
        ]
        self.seller_type = ['dealer', 'cpo', 'private']
        self.import_gcc = ['gcc', 'import']
        self.map_sub_type = {'dealer': 'dealer', 'cpo': 'Certified pre-owned', 'private': 'owner'}
        self.map_type = {'dealer': 'business', 'cpo': 'Certified pre-owned', 'private': 'private'}

    def start_requests(self):
        if self.mode == 'serp_only':
            for st in self.seller_type:
                for gcc in self.import_gcc:
                    for category_data in self.categories:
                        req_url = urljoin(self.base_url, category_data['path'].format(gcc, st))
                        yield scrapy.Request(req_url, meta=category_data)


    @staticmethod
    def get_img_url(script_data):
        try:
            return str([img_url.strip().rstrip()
                    for img_url in script_data.split("',")
                    if 'images' in img_url][0])[1:]
        except Exception:
            return None

    def parse(self, response):
        ads = response.css('section[id="serp-list-new"] ul li')
        for i, item in enumerate(ads):
            try:

                if 'hidden' in item.css('li::attr(class)').extract_first(''):
                    continue

                listing = Listing()
                listing['category_1'] = 'motors'
                listing['category_2'] = 'cars'

                try:
                    li_data = json.loads(item.css('li::attr(data-sp-item)').extract_first(''))
                except json.decoder.JSONDecodeError:
                    continue

                listing['title'] = item.css('li::attr(data-item-title)').extract_first('')
                listing['url'] = item.css('div.img span.img a::attr(href)').extract_first('')

                try:
                    price_str = float(item.css('li::attr(data-item-price)').extract_first('0').strip())
                except ValueError:
                    price_str = float('0')

                listing['price'] = price_str
                listing['currency'] = 'AED'

                listing['item_condition'] = response.meta.get('name')
                image_main = self.get_img_url(item.css('span.aspect-16-9 a script::text').extract_first(''))
                listing['image_main'] = urljoin(self.base_url, image_main)

                created_at = datetime.now(timezone('Asia/Dubai'))
                listing['created_at'] = created_at.strftime("%Y-%m-%d %H:%M:%S")

                labels = list()

                if li_data.get('f'):
                    listing['featured'] = True
                    labels.append(item.css('div.tags-container div.featured::text').extract_first(''))

                is_imp = item.css('div.tags-container span.exclusive::text').extract_first()
                if is_imp:
                    labels.append(is_imp)

                user_type = w3lib.url.url_query_parameter(response.url, 'st')
                labels.append(self.map_type.get(user_type))
                listing['promo_value'] = labels

                listing['user_type'] = self.map_type.get(user_type)

                listing['user_subtype'] = self.map_sub_type.get(user_type)

                user_id = item.css('div.cta a::attr(href)').extract_first('unknown').replace('tel:', '').strip()

                if user_type == 'private':
                    listing['user_first_name'] = item.css('a[href="#dealer-contact"]::attr(data-dealer-name)').extract_first('unknown')
                    try:
                        user_id = hashlib.sha256(user_id.encode('utf-8')).hexdigest()
                    except TypeError:
                        continue
                else:
                    is_biz_url = item.css('div.cta a div.logo img').extract_first()
                    if is_biz_url:
                        listing['business_url'] = [href for href in item.css('div.cta a::attr(href)').extract() if 'com' in href][0]
                    listing['business_name'] = item.css('a[href="#dealer-contact"]::attr(data-dealer-name)').extract_first('unknown')
                    listing['business_phone'] = item.css('div.cta a::attr(href)').extract_first('unknown').replace('tel:', '').strip()

                listing['user_id'] = user_id

                listing['listing_id_on_site'] = str(li_data.get('id'))

                yield scrapy.Request(listing['url'], meta={'listing': listing, 'is_filter': False}, callback=self.parse_adp)

            except Exception as e:
                self.crawler.stats.inc_value('num_exceptions')
                self.crawl_log.exceptions += u"{0} on crawling: {1} \n {2} \n Ad content: \n {3} \n--------\n".format(
                    type(e).__name__, response.url, traceback.format_exc(), '')

        next_page = response.xpath(".//a[@class='next']/@href").extract_first()
        if next_page and len(ads) > 0:
            yield Request(next_page, meta=response.meta)

    def parse_adp(self, response):
        try:
            listing = response.meta.get('listing')

            listing['adp_crawled'] = 1
            listing['adp_status'] = response.status

            listing['description'] = ' '.join(response.css('p[id="description-content"] *::text').extract())

            listing['country'] = 'UAE'
            listing['region'] = response.css('meta[name="locality"]::attr(content)').extract_first('').split(',')[0].strip()

            listing['image_count'] = str(len(response.css('div.slides img')))
            image_list = list()
            for script_data in response.css('div.slides li script::text').extract():
                image_list.append(urljoin(self.base_url, self.get_img_url(script_data)))
            listing['images'] = image_list

            databag = dict()
            for tr in response.css('section[id="item-details"] table tr'):
                key = tr.css('tr th::text').extract_first('')[:-1]
                value = tr.css('tr td::text').extract_first('')
                databag[key] = value

            try:
                car_model = response.css('nav.breadcrumbs span a')[-1].css('a::attr(href)').extract_first('').split('/')[-1].title()
            except Exception:
                try:
                    car_model = json.loads(response.css('div.item-before-info::attr(data-targeting)').extract_first('')).get('model').split('-')[-1].title()
                except Exception:
                    car_model = databag.get('Model', '')

            listing['car_brand'] = databag.get('Make', '')
            listing['car_model'] = car_model
            listing['car_year'] = databag.get('Year', '')
            listing['car_fuel'] = databag.get('Fuel', '')
            listing['car_transmission'] = databag.get('Gearbox', '')

            yield listing
        except Exception as e:
            self.crawler.stats.inc_value('num_exceptions')
            self.crawl_log.exceptions += u"{0} on crawling: {1} \n {2} \n Ad content: \n {3} \n--------\n".format(
                type(e).__name__, response.url, traceback.format_exc(), '')