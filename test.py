import json
import re
from pprint import pprint

import requests
from lxml import html
from lxml.etree import _ElementUnicodeResult


def parse(self):
    return html.fromstring(response.content, parser=html.HTMLParser(encoding='utf-8'))

requests.Response.parse = parse


def save(self):
    name = self.url.split('/')[-1] + '.html'
    with open(name, 'w', encoding=response.encoding) as f:
        f.write(response.text)
    return self

requests.Response.save = save

def strip(s):
    return s.strip()



while True:
    response = requests.get('https://www.avito.ru/sankt-peterburg/kvartiry?cd=1')
    #response = requests.get('https://www.avito.ru/sankt-peterburg/kvartiry/sdam-ASgBAgICAUSSA8gQ?s=1')
    response.raise_for_status()
    response.save('sdam.html')
    tree = response.parse()
    urls = tree.xpath('//a[@class="snippet-link js-snippet-link"]/@href')
    for url in urls:
        response = requests.get(url)
        response.raise_for_status()
        tree = response.save().parse()

        data = dict(
            url=url,
            title=tree.xpath('//span[@class="title-info-title-text"]/text()')[0].strip(),
            posted_datetm=tree.xpath('//*[@class="title-info-metadata-item-redesign"]/text()')[0].strip(),
            sub_price=tree.xpath('//*[@class="item-price-sub-price"]')[0].text_content().strip(),
            old_prise=tree.xpath('//*[@class="item-price-old"]')[0].text_content().strip(),
            # params=[i.text_content().split(': ') for i in tree.xpath('//*[@class="item-params-list-item"]')]),
            address=tree.xpath('//span[@class="item-address__string"]/text()')[0].strip(),
            text=tree.xpath('//div[@itemprop="description"]')[0].text_content().strip(),
            views=tree.xpath('//*[@class="title-info-metadata-item title-info-metadata-views"]')[0].text_content().strip(),
            seller_name=tree.xpath('//div[@class="seller-info-name js-seller-info-name"]')[0].text_content().strip(),
            seller_id=tree.xpath('//div[@class="seller-info-name js-seller-info-name"]')[0].text_content().strip().split('/')[1],
            seller_url=tree.xpath('//div[@class="seller-info-name js-seller-info-name"]/a/@href')[0].strip(),
        )
        data.update(json.loads(re.search('(\[.+\])', str(tree.xpath('//script')[0].text_content())).groups()[0]))

