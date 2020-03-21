import email
import json
import logging
import os
from pathlib import Path
from contextlib import contextmanager, closing
from datetime import datetime

from lxml import html
import requests


log = logging.getLogger(__name__)

shops = dict(metro='''Host: igooods.ru
User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:73.0) Gecko/20100101 Firefox/73.0
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
Accept-Language: en-US,en;q=0.5
Accept-Encoding: gzip, deflate
Connection: keep-alive
Cookie: _igooods_session_cross_domain=K3JsY2RFTWFIbE5CdUkwZU92dzVscE9HZzJNRGp5Lzh6NXFPRENTckpsQlMvRUJ3d3o2MlZWYVFlVVFMNFdyNVJ0ZjdhbzFCUElKTVVadzZpV3JWeHpObllCSHJualB2bjdHVEtINlNFRzVvbEx0Y3ZxNW9RbVlYV1FvOG4vLzNPdjIrTmJXTVFFaEVNWmRvVXJQckZQczdHMWRGUUp5UFh6UHNiZUtoYWo2UkI4d2EraTdiM2h3aWJUblo3a1U1VVZPa2E4OU1rSVBmSlZTTVA3QXNLV004Zm5OaXhuT1d4OVI0cVNsaWNHZ2lQUjFaZXBCK0FkSW85MGlvNGlpVHNYMlYrTUFuZ3BZOVBpcllLZjZ5Snc9PS0tOHBpNC9hTktrUDhEVlFZRklRcVRHQT09--bc63f30bb0e38ac3094766d78cf249a65806eb4d; ref=products_main; sa_current_city_cross_domain=%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3
Upgrade-Insecure-Requests: 1
Pragma: no-cache
Cache-Control: no-cache''',
             prisma='''Host: igooods.ru
User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:73.0) Gecko/20100101 Firefox/73.0
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
Accept-Language: en-US,en;q=0.5
Accept-Encoding: gzip, deflate, br
Connection: keep-alive
Cookie: _igooods_session_cross_domain=cFFoVDlMSllsNmlkQmdEWit1VWV5N2pISkdGT3NkcVZqeDhiOEZNZW10UVhPaGpxOUMwblhEcnZHbldEL2tYaURuNmNUcFhuOXcrWCt0T2NGQXF2TEc4UkhkWjdIME1IOHpmT0ppNTNXNzgyQkpyUHRZWTdVYmdtMFh6NHlhMXNrY2gzczQvM3JhZVpCcXpyZHFXTlhSbjQwNGZKNVU5N1FPNUJsUU1SVytEODNlQ29Oa2xoUEJwcURoSzh2ZEc0cVlMVkZOQ0lLQ1JlWjdVbjRJK29VaDBxV0JEOTVVdFdCVGdldnV4aXNrZzB3bVFRb2dVZU9jRUZZb3hQNDhhUVpIMVpPS2FwQnVmemhkM1ZWaVhSZ0E9PS0tVnk4TzRkUzBQREJiTGtpUmd0MksyQT09--6606b9d7bf839d9e124cb14c03f635ff2cde7f70; ref=products_main; sa_current_city_cross_domain=%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3; ya_metrica_view_select_address=true; ga_view_select_address=true
Upgrade-Insecure-Requests: 1
Pragma: no-cache
Cache-Control: no-cache''',
             lenta='''Host: igooods.ru
User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:73.0) Gecko/20100101 Firefox/73.0
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
Accept-Language: en-US,en;q=0.5
Accept-Encoding: gzip, deflate
Connection: keep-alive
Cookie: _igooods_session_cross_domain=ZVZCTmtla3BmcGFBWW9keEoweThRdUVYOU5SalI2VjNaS1RCKzB4SkRDZy9uVnJVaWRJYzZDM3ZrcWJtamFsT1ZieHJKNUJaU2t5K1hjcEdDeG9MMm9xd1pKY3pHS2hhaTdqRFVrbEhyNm50SGRIZFN0dTR3c0RWQlNOUWFEWFlMTkhqU3pKbUNFK3lyNThaMEJ1OEZOaG9WVFpJN0VPeWorV2crNGpXdlBHTUlPWFFsYUFZNEx3N0pPYTdMNzhjWlhrUnhxSTZvZDlOSnJxcEVqVno4VHo0Z0VlSnhFT0E0aXFReVp6eWlNYU5rU2ZWaTRFQ01KQzN3My91VHoyR1F6YjhHam1SNnFmQndGak1aUHltdWc9PS0tN0dqMDJINm1IbDVWckg4TEtVMC9wZz09--da3c4e170d10f06bab0dc4d823f881a6010b9bdf; ref=products_main; sa_current_city_cross_domain=%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3; ga_view_select_address=true
Upgrade-Insecure-Requests: 1
Pragma: no-cache
Cache-Control: no-cache''',
             karusel='''Host: igooods.ru
User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:73.0) Gecko/20100101 Firefox/73.0
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
Accept-Language: en-US,en;q=0.5
Accept-Encoding: gzip, deflate, br
Connection: keep-alive
Cookie: _igooods_session_cross_domain=d3pwUUdadG1UVmtjN3drdHk1NklLbGt4WENMUU1iSkEvT3BFWERSY3dKSW5Oa0hNWFJaK2ltU2c1c3NmdEZUamJaWDZmR2xLTDFrWUpFSlp3ZFNZaDQvckJidFVOSUFLYXZjYlVJRDFPZ2FDc05oWVgzYVVCWmhxZzJTNlJiejBYalduTEZSVXU0ZVhaNzYzaGdyQjRTYXFFcTJxdmlJRlZZTzMzVVNmdFBZcGpzd2JUSHhEb1VqcUZyM3hWR2FsbDArWndxNzJHNk9uWUtJOFpOdklnZE1jRVd3aFhPc1lVazd3N29mT1pFZFh5eWdVOUtQVDNrZzBQWmRCOWd5ejRma1kwODhmZklEbVVxRjhmZmZ5b2c9PS0tRFpvbUt0c1EyRkVUdHkwYzVrNkRZUT09--cbeee7ed1db38e41522ff261b1e55a6e50569c1f; ref=products_main; sa_current_city_cross_domain=%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3; ya_metrica_view_select_address=true; ga_view_select_address=true; last_seen_products=[]
Upgrade-Insecure-Requests: 1
Pragma: no-cache
Cache-Control: no-cache''')


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


def save(name, response):
    with open(name, 'w', encoding=response.encoding) as f:
        f.write(response.text)


def get(url, headers, shop=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response


def parse(response):
    return html.fromstring(response.content, parser=html.HTMLParser(encoding='utf-8'))


def process(on_result, shop, headers):
    response = get('https://igooods.ru/products', headers)
    assert f'{shop}-selected' in response.text
    categories = parse(response).xpath('//div[@class="b-side-menu__item small with-children"]/a[@data-category-id]/@data-category-id')
    log.info(f'Processing categories {" ".join(categories)}')
    for cat in sorted(categories):
        with context(shop=shop, cat=cat):
            url = 'https://igooods.ru/products?category_id={cat}&from_category=true&page={page}&q[by]=letter&q[order]=asc'
            tree = parse(get(url.format(cat=cat, page=1), headers=headers, shop=shop))
            pages = int(tree.xpath('//div[@data-total-pages]/@data-total-pages')[0])
            for page in range(1, pages):
                # if page == 2: return
                tree = parse(get(url=url.format(cat=cat, page=page), headers=headers))
                products = tree.xpath('//div[@class="b-product-small-card"]')

                for product in products:
                    with context(verbose=False, shop=shop, cat=cat, page=page, product=product):
                        attrib = product.xpath('.//div[@class="g-cart-action small"]')[0].attrib
                        del attrib['class']
                        data = dict(attrib)
                        a_tag = product.xpath('.//a[@class="with-ellipsis name js-link-to-popup"]')[0]
                        data['shop'] = shop
                        data['cat'] = cat
                        data['name'] = a_tag.text
                        data['url'] = a_tag.attrib['href']
                        on_result.send(data)


def result_writer(file_name):
    temp_file_name = file_name.with_suffix('.tmp')
    log.info(f'Started writing results to {temp_file_name}')
    try:
        with open(temp_file_name, 'w', encoding='utf-8') as fp:
            while True:
                data = yield
                log.debug(' '.join(map(lambda i: f'{i[0]}:{i[1]}', data.items())))
                data['datetm'] = str(datetime.now())
                fp.write('{}\n'.format(json.dumps(data, ensure_ascii=False)))
                fp.flush()
    except GeneratorExit:
        log.info(f'Finished writing results. Renaming {temp_file_name} to {file_name}')
        os.rename(temp_file_name, file_name)


def now_str():
    return datetime.now().strftime('%y_%m_%d__%H_%M_%S')


if __name__ == '__main__':
    jsons_path = Path('jsons')
    jsons_path.mkdir(parents=True, exist_ok=True)
    Path('logs').mkdir(parents=True, exist_ok=True)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s.%(msecs)03d|%(levelname)-4.4s|%(thread)-6.6s|%(funcName)-10.10s|%(message)s',
                        handlers=[logging.FileHandler('logs/food_{}.log'.format(now_str())),
                                  handler])
    logging.getLogger('requests').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    log.info('Started')
    with closing(result_writer(file_name=jsons_path / '{}.json'.format(now_str()))) as on_result:
        on_result.send(None)
        for shop, headers in shops.items():
            with context(shop=shop):
                process(on_result, shop, email.message_from_string(headers))
    log.info('Finished')
