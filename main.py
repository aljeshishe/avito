import email
import json
import logging
import logging.handlers
import os
import re
import string
import time
from pathlib import Path
from contextlib import contextmanager, closing
from datetime import datetime
from lxml.etree import tostring
from lxml import html
import prequests as requests
from lxml.html import Element, HtmlElement

log = logging.getLogger(__name__)


headers = '''Host: www.avito.ru
User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:74.0) Gecko/20100101 Firefox/74.0
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
Accept-Language: en-US,en;q=0.5
Accept-Encoding: gzip, deflate, br
Connection: keep-alive
Cookie: u=2k0y1l0n.1eo4fm.gd1ui33z9z; v=1584795471; buyer_location_id=653240; luri=sankt-peterburg; sx=H4sIAAAAAAACA52V0ZKiMBBF%2F8XnfWgwQGf%2FRqJEaCVIA%2B24Nf%2B%2BN1vl7DiPllVSlslJp2%2Ffy59dW59pnHzDloIzdppi4JiMdr%2F%2F7Lbd790UzpdySV6r4JhYoolLMZE5JxJp92t32v0uKnbMhaf9569dS8dxEGXZRAMBSS5wYOYn8kK1mjZdLJRwWiJOLIEtRGbseEGyr4E8%2BVq78jRSIRYloAh2KaCQt6osGcgqzueibJjaGMEKIZKZw%2B4n0j9kkRTn6QoYOYqqOJwZK0N09op0%2BeKH4bAdDoJTLZqLSjg%2BctL3kDUBWc5La3zRsS0ZXRQLliiFGJ9IuXd9v9IwnYMFl9tpQRmHGtou%2FANZ5F665NPhUcZJJZil6MSpULQn0s2X%2BxL7SW8SU6KQFAvAU8wHk7wimyxPmORSNuNjf1cN2AK1Xcw9%2BFL8%2BhH6wa9jzZiyQJnqkuQClJ17RbLLF%2B9v%2Fq7Dw8U6oLxkSUg0mr6BLCufL34cpzT3i5Xoe8JiDEku1vitKr3PQ9T6cNrWbr2nCCVjJIay%2Bn8uo9x0uBbTx11wVgoKjSh7IYbstO%2FI2pcNkI2%2FJT9cVrvHrDfnOpIJf8lTLJNBrdu8sCSMNzRCf5giwUXxRfGy%2BVdle7VbuwmVGqOCqAoPYZO%2BgdzvqzzqlbU2L6GbO4YNnRkldNO9U2RFdRa8WJchbYzcUMiDPIApWFj1PWSVZ6hdt86Xug4bZE5ZcQww0uGJPNp47VN7Pw8BMWG4CQYNjk24kIj7oU4JZN0us936rlcRgnHxME3o5hPZ3ZN%2F6HZePpK6nCzCGCJGwHAk%2FU5EK7PDm3I6rvvtOCN8kJlJKUCj%2BD%2BA%2B2PXNsMUP85G%2BAQsiwH9hoIKn31HNlTnaPPz4dHe5tBWYoKZxKwhM7H1iWyMbsiMw%2BGAMLGEEGBCFGEdfqr%2BQGbvVKe1q05TPXjN1oZ%2F2Am%2Bv1p5nWyty7P1R1PYBQkVXEryb85goxdksc9VtkUl13PT%2BhEJLYSXhKIWPL5aSW0ry%2BLtxnAUxhb%2F4tXkkEPu58WLKldZ7pu70PLYPjDq2YaIrZyDX70ch%2FGk50t%2FxH3BcqT5bQEyQluie5Wn2H9%2B%2FgXVy9QKIQcAAA%3D%3D; dfp_group=37; sessid=406371494d15aa1f66766d2e978c7cff.1584795474; __cfduid=d224628d008c0077fb8ca09f612d672961584795476; buyer_tooltip_location=653240; no-ssr=1; f=5.0c4f4b6d233fb90636b4dd61b04726f147e1eada7172e06c47e1eada7172e06c47e1eada7172e06c47e1eada7172e06cb59320d6eb6303c1b59320d6eb6303c1b59320d6eb6303c147e1eada7172e06c8a38e2c5b3e08b898a38e2c5b3e08b890df103df0c26013a7b0d53c7afc06d0b2ebf3cb6fd35a0ac7b0d53c7afc06d0b8b1472fe2f9ba6b9c7f279391b0a3959c7cea19ce9ef44010f7bd04ea141548c71e7cb57bbcb8e0f2da10fb74cac1eab2da10fb74cac1eab2da10fb74cac1eab2da10fb74cac1eab2da10fb74cac1eab2da10fb74cac1eab2da10fb74cac1eab2da10fb74cac1eab2da10fb74cac1eab2da10fb74cac1eab2da10fb74cac1eab2da10fb74cac1eab2da10fb74cac1eab868aff1d7654931c9d8e6ff57b051a58d53a34211e148d88b0a8b49ec157ab29938bf52c98d70e5c6d2582a4fb5550e930b016c035941205d21ab7cd585086e0b86f1b8aaa5acad607ce30aedaa83a5d343076c04c14cfd09d9b2ff8011cc827cbf1a5019b899285ad09145d3e31a5690839cf02ea7744fc8db57d0f7c7638d471e7cb57bbcb8e0fd1d953d27484fd81666d5156b5a01ea6; _nfh=66f342aeb8b39d20d1a7234c825e4599; so=1584801930
Upgrade-Insecure-Requests: 1
Pragma: no-cache
Cache-Control: no-cache
TE: Trailers'''
headers = email.message_from_string(headers)


@contextmanager
def context(verbose=True, **kwargs):
    kwargs_str = ' '.join(map(lambda i: f'{i[0]}={i[1]}', kwargs.items()))
    if verbose:
        log.info(f'Processing {kwargs_str}')
    try:
        yield None
        if verbose:
            log.info(f'Finished processing {kwargs_str}')
    except Exception as e:
        log.exception(f'Exception while processing {kwargs_str}')


def get(url, headers):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response


def parse(self):
    return html.fromstring(self.content, parser=html.HTMLParser(encoding='utf-8'))


requests.Response.parse = parse

requests.Response._old_raise_for_status = requests.Response.raise_for_status


def raise_for_status(self):
    self._old_raise_for_status()
    return self


requests.Response.raise_for_status = raise_for_status


def save(self):
    name = self.url.split('/')[-1] + '.html'
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    name = ''.join(c for c in name if c in valid_chars)
    with open(name, 'w', encoding=self.encoding) as f:
        f.write(self.text)
    return self


requests.Response.save = save


def strip(s):
    return s.strip()


def parse(self, _path, method='text_content'):
    t = self.xpath(_path)
    if not t:
        return None
    t = t[0]
    if method == 'text_content':
        t = t.text_content()
    elif method==None:
        t = t
    return t.strip()


HtmlElement.parse = parse


def main(on_result):
    host = 'https://www.avito.ru'
    for page in range(1, 100):
        # все квартиры 'sankt-peterburg/kvartiry?cd=1'
        response = requests.get(f'{host}/sankt-peterburg/kvartiry/sdam-ASgBAgICAUSSA8gQ?cd=1&p={page}',
                                headers=headers,
                                retry_on=403)
        tree = response.raise_for_status().parse()
        urls = tree.xpath('//a[@itemprop="url"]/@href')
        for url in urls:
            url = '{}/{}'.format(host, url)
            with context(url=url):
                tree = requests.get(url, headers=headers, retry_on=403).raise_for_status().parse()
                data = dict(
                    url=url,
                    title=tree.parse('//span[@class="title-info-title-text"]'),
                    posted_datetm=tree.parse('//*[@class="title-info-metadata-item-redesign"]'),
                    sub_price=tree.parse('//*[@class="item-price-sub-price"]'),
                    old_prise=tree.parse('//*[@class="item-price-old"]'),
                    address=tree.parse('//span[@class="item-address__string"]'),
                    text=tree.parse('//div[@itemprop="description"]'),
                    views=tree.parse('//*[@class="title-info-metadata-item title-info-metadata-views"]'),
                    seller_name=tree.parse('//div[@class="seller-info-name js-seller-info-name"]'),
                    seller_url=tree.parse('//div[@class="seller-info-name js-seller-info-name"]/a/@href', method=None),
                )
                found = re.search('(\[.+\])', tree.parse('//script'))
                if found:
                    dicts = json.loads(found.group(0))
                    data.update(dicts[0])
                    data.update(dicts[1])
                on_result.send(data)


def result_writer(file_name):
    temp_file_name = file_name.with_suffix('.tmp')
    log.info(f'Started writing results to {temp_file_name}')
    try:
        with open(temp_file_name, 'w', encoding='utf-8') as fp:
            while True:
                data = yield
                # log.debug(' '.join(map(lambda i: f'{i[0]}:{i[1]}', data.items())))
                data['datetm'] = str(datetime.now())
                fp.write('{}\n'.format(json.dumps(data, ensure_ascii=False)))
                fp.flush()
    except GeneratorExit:
        log.info(f'Finished writing results. Renaming {temp_file_name} to {file_name}')
        os.rename(temp_file_name, file_name)


def now_str():
    return datetime.now().strftime('%y_%m_%d__%H_%M_%S')


if __name__ == '__main__':
    # requests.Proxies.instance(proxies=['68.183.180.179:8080'])
    jsons_path = Path('jsons')
    jsons_path.mkdir(parents=True, exist_ok=True)
    Path('logs').mkdir(parents=True, exist_ok=True)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s|%(levelname)-4.4s|%(thread)-6.6s|%(filename)-10.10s|%(funcName)-10.10s|%(message)s',
                        handlers=[logging.StreamHandler(),
                                  logging.handlers.RotatingFileHandler('logs/avito_{}.log'.format(now_str()), maxBytes=200 * 1024 * 1024, backupCount=5)
                                  ])

    logging.getLogger('requests').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    log.info('Started')
    with closing(result_writer(file_name=jsons_path / '{}.json'.format(now_str()))) as on_result:
        on_result.send(None)
        main(on_result=on_result)
    log.info('Finished')
