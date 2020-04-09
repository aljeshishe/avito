import email
import json
import logging
import logging.handlers
import os
import re
import string
import time
from functools import partial
from pathlib import Path
from contextlib import contextmanager, closing
from datetime import datetime
from lxml.etree import tostring
from lxml import html
import prequests as requests
from lxml.html import Element, HtmlElement
from prequests import content_has

from processor import Processor

log = logging.getLogger(__name__)

headers = '''Host: www.avito.ru
User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:74.0) Gecko/20100101 Firefox/74.0
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
Accept-Language: en-US,en;q=0.5
Accept-Encoding: gzip, deflate, br
Referer: https://www.avito.ru/sankt-peterburg
Connection: keep-alive
Cookie: buyer_from_page=main; u=2k13fk6s.1eo4fm.gdchsw9u7l; v=1585591180
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


def as_tree(content):
    return html.fromstring(content, parser=html.HTMLParser(encoding='utf-8'))


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
    elif method == None:
        t = t
    return t.strip()


HtmlElement.parse = parse
processor = Processor(100)
retry_on = (content_has('временно ограничен'), content_has('Доступ временно заблокирован'))


def main(on_result):
    host = 'https://www.avito.ru'
    for page in range(1, 50):
        # все квартиры 'sankt-peterburg/kvartiry?cd=1'
        processor.add(partial(on_page, host, on_result, page))


def on_page(host, on_result, page):
    response = requests.get(f'{host}/sankt-peterburg/kvartiry/sdam-ASgBAgICAUSSA8gQ?cd=1&p={page}',
                            headers=headers,
                            retry_on=retry_on)
    response.raise_for_status()
    tree = as_tree(response.content)
    urls = tree.xpath('//a[@itemprop="url"]/@href')
    for url in urls:
        url = '{}/{}'.format(host, url)
        processor.add(partial(on_url, on_result, url))


def on_url(on_result, url):
    with context(url=url):
        response = requests.get(url, headers=headers, retry_on=retry_on)
        response.raise_for_status()
        data = on_content(response.content)
        data['url'] = url
        on_result.send(data)


def on_content(content):
    tree = as_tree(content=content)
    data = dict(
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
    return data


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
    # requests.Proxies.instance(throttling_interval)  # TODO prequests should request proxies, and all threads should wait till proxies received
    # requests.Proxies.instance(proxies=['68.183.180.179:8080'])
    jsons_path = Path('jsons')
    jsons_path.mkdir(parents=True, exist_ok=True)
    Path('logs').mkdir(parents=True, exist_ok=True)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s|%(levelname)-4.4s|%(thread)-6.6s|%(filename)-10.10s|%(funcName)-10.10s|%(message)s',
                        handlers=[logging.StreamHandler(),
                                  logging.handlers.RotatingFileHandler('logs/avito_{}.log'.format(now_str()),
                                                                       maxBytes=200 * 1024 * 1024, backupCount=5)
                                  ])

    logging.getLogger('requests').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    log.info('Started')
    with closing(result_writer(file_name=jsons_path / '{}.json'.format(now_str()))) as on_result:
        on_result.send(None)
        main(on_result=on_result)
        try:
            processor.wait_done()
        except KeyboardInterrupt:
            log.info('Ctrl+C pressed')
        finally:
            processor.stop()
    log.info('Finished')
