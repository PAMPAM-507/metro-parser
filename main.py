# Hard. Написать парсер для сайта Метро (https://online.metro-cc.ru/)

# Для каждой торговой площадки требования одни и те же.
# Спарсить любую категорию, где более 100 товаров,
# для городов Москва и Санкт-Петербург и выгрузить в
# любой удобный формат(csv, json, xlsx). Важно,
# чтобы товары были в наличии.

# Необходимые данные:
# id товара из сайта/приложения,
# наименование,
# ссылка на товар,
# регулярная цена,
# промо цена,
# бренд.

import multiprocessing
import os
import threading
from typing import Dict, Iterable, List, NoReturn
from dataclasses import dataclass, field
import traceback

import requests
from bs4 import BeautifulSoup

from dto import ParserReportDTO, PriceDTO, ProductCardReportDTO
from save_result import JoinAllXlsxFiles, XlsxResultWriter


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.5) Gecko/20091102 Firefox/3.5.5T(.NET CLR 3.5.30729)',
}


COOKIES_LIST_ST_PT = [
    {  # Санкт-Петербург, Ул. Пулковское шоссе, д. 23, лит. A (м. Звездная)
        'metroStoreId': '20',
        'pickupStore': '20',
    },
    {  # Санкт-Петербург, Ул. Пр-т Косыгина, д. 4, лит. А (м. Ладожская)
        'metroStoreId': '16',
        'pickupStore': '16',
    },
    {  # Санкт-Петербург, Ул. Комендантский пр-т, д. 3, лит. А (м. Комендантский пр-т)
        'metroStoreId': '15',
        'pickupStore': '15',
    },

]


COOKIES_LIST_MOSCOW = [
    {  # Ул. Ленинградское шоссе, д. 71Г (м. Речной вокзал)
        'metroStoreId': '10',
        'pickupStore': '10',
    },
    {  # Ул. Пр-т Мира, д. 211, стр. 1 (м. Ростокино)
        'metroStoreId': '11',
        'pickupStore': '11',
    },
    {  # Ул. Дорожная, д. 1, корп. 1 (м. Южная)
        'metroStoreId': '12',
        'pickupStore': '12',
    },
    {  # Ул. Рябиновая, д. 59 (м. Аминьевская)
        'metroStoreId': '13',
        'pickupStore': '13',
    },
    {  # Ул. Дмитровское шоссе, д. 165Б (м. Физтех)
        'metroStoreId': '14',
        'pickupStore': '14',
    },
    {  # Ул. Маршала Прошлякова, д. 14 (м. Строгино)
        'metroStoreId': '17',
        'pickupStore': '17',
    },
    {  # Ул. 104 км МКАД, д. 6 (м. Щелковская)
        'metroStoreId': '18',
        'pickupStore': '18',
    },
    {  # Ул. Шоссейная, д. 2Б (м. Печатники)
        'metroStoreId': '19',
        'pickupStore': '19',
    },
    {  # П. Московский, кв-л 34, д. 3, стр. 1
        'metroStoreId': '49',
        'pickupStore': '49',
    },
    {  # Ул. Складочная, д. 1, стр. 1 (м. Савеловская)
        'metroStoreId': '308',
        'pickupStore': '308',
    },
    {  # Ул. 1-я Дубровская, д. 13А, стр. 1 (м. Дубровка)
        'metroStoreId': '356',
        'pickupStore': '356',
    },
    {  # Ул. Боровское шоссе, д. 10А (м. Боровское шоссе)
        'metroStoreId': '363',
        'pickupStore': '363',
    },

]


class MetroParser():

    def __init__(self, url: str,) -> NoReturn:
        self.url = url

    @staticmethod
    def __get_request(product_link: str, cookies: Dict[str, str], headers: Dict[str, str] = headers) -> str:
        response = requests.get(product_link, 
                                cookies=cookies, 
                                headers=headers,)
        if response.status_code == 200:
            return response.text

    @staticmethod
    def __get_brand_index(soup: BeautifulSoup):
        product_attrs = soup.find_all('span', 
                                      class_='product-attributes__list-item-name-text')
        product_attrs_list = [attr.text.replace('\n', '').strip() for attr in product_attrs]
        index = product_attrs_list.index('Бренд')
        return index

    def __get_data_from_card(self, parameter: ProductCardReportDTO) -> ParserReportDTO:

        product_link = parameter.product_link
        regular_price = parameter.regular_price
        promo_price = parameter.promo_price
        cookies = parameter.cookies

        try:
            response = self.__get_request(product_link, 
                                          cookies=cookies, 
                                          headers=headers)
        except Exception as e:
            print(f"Ошибка при получении данных для {product_link}: {e}")
            traceback.print_exc()
            # return None

        soup = BeautifulSoup(response, 'lxml')

        # Начальные значения по умолчанию
        article_number = None
        name = None
        brand = None

        try:
            # Ищем блок с атрибутами
            data = soup.find(
                'ul', class_='product-attributes__list style--product-page-short-list')

            # Проверяем наличие данных для бренда
            if data:
                brand_elements = data.find_all('a', 
                                               class_='product-attributes__list-item-link reset-link active-blue-text')
                if brand_elements:
                    brand_index = self.__get_brand_index(soup=soup)
                    if 0 <= brand_index < len(brand_elements):
                        brand = brand_elements[brand_index].text.strip().replace(' ', '')

            # Получаем артикул
            article_elem = soup.find(
                'p', class_='product-page-content__article')
            if article_elem:
                article_number = article_elem.text.replace('\n', '').replace(' ', '').split(':')[-1]

            # Получаем название
            name_elem = soup.find('h1', 
                                  class_='product-page-content__product-name catalog-heading heading__h2')
            if name_elem:
                name_span = name_elem.find('span')
                if name_span:
                    name = name_span.text.replace('\n', '').strip()

        except Exception as e:
            print(f"Ошибка при получении данных для {product_link}: {e}")
            traceback.print_exc()

        return ParserReportDTO(
            article_number=article_number,
            name=name,
            product_link=product_link,
            regular_price=regular_price,
            promo_price=promo_price,
            brand=brand
        )

    @staticmethod
    def __get_page_numbers(soup: BeautifulSoup) -> List[str]:
        page_numbers = soup.find_all('a', 
                                     class_='v-pagination__item catalog-paginate__item nuxt-link-active')
        page_numbers_list = [page_number.text for page_number in page_numbers]
        active_page_number = soup.find('a', 
                                       class_='v-pagination__item catalog-paginate__item nuxt-link-exact-active nuxt-link-active v-pagination__item--active')
        page_numbers_list.insert(0, '1')
        if active_page_number:
            page_numbers_list.append(active_page_number.text)
        print(page_numbers_list)

        return page_numbers_list

    @staticmethod
    def __get_prices(card: BeautifulSoup,) -> List[str]:
        promo_price, regular_price = 0, 0

        prices = card.find_all('span', class_='product-price__sum-rubles')
        penny = card.find_all('span', class_='product-price__sum-penny')

        prices_amount = len(prices)
        penny_amount = len(penny)

        if prices_amount == 1:
            regular_price = prices[0]

            if penny_amount == 1:
                regular_price = regular_price.text + penny[0].text
            else:
                regular_price = regular_price.text

        else:
            if penny_amount == 2:
                penny_promo, penny_regular = penny[0:2]
                promo_price, regular_price = prices[0:2]
                promo_price, regular_price = promo_price.text + \
                    penny_promo.text, regular_price.text + penny_regular.text
            else:
                promo_price, regular_price = prices[0:2]
                promo_price, regular_price = promo_price.text, regular_price.text

        # print(regular_price, promo_price)

        regular_price = ''.join(str(regular_price).split())
        promo_price = ''.join(str(promo_price).split())

        return PriceDTO(regular_price=regular_price,
                        promo_price=promo_price)

    def run(self, cookies_list: List[Dict[str, str]]) -> Iterable[ProductCardReportDTO]:
        for cookies in cookies_list:
            page_number = 1

            while True:
                url = self.url + f'{page_number}'
                response = self.__get_request(product_link=url, 
                                              cookies=cookies, 
                                              headers=headers)
                
                soup = BeautifulSoup(response, 'lxml')
                address = soup.find('div', 
                                    class_='header-address header-main__address').find('button').text.replace('\n', '').strip()

                page_numbers_list = self.__get_page_numbers(soup)

                if str(page_number) not in page_numbers_list:
                    print(f'Вы пытались выбрать {page_number}', 'Страница не найдена')
                    break

                product_cards = soup.find_all('div', 
                                              class_='product-card__content')

                print('products amount', 
                      len(product_cards))
                
                print('page number:', page_number, 
                      '| address:', 
                      address, 
                      '| products found:', 
                      len(product_cards), 
                      'actual pages: ', 
                      page_numbers_list)

                for card in product_cards:
                    card_url = 'https://online.metro-cc.ru' + \
                        card.find('a').get('href')

                    if card.find('span', class_='simple-button__text').text == 'Только в торговом центре':
                        continue

                    price = self.__get_prices(card)

                    information = self.__get_data_from_card(ProductCardReportDTO(
                        product_link=card_url,
                        regular_price=price.regular_price,
                        promo_price=price.promo_price
                    ))

                    yield information

                page_number += 1


def main(cookies_list: List[Dict[str, str]], number: int=1) -> NoReturn:

    result_file = f'./result_file{number}.xlsx'

    # Если экземпляр кофе есть хотя бы в одном из филиалов Metro (Москва и Санкт-Петербург)
    # и доступен для покупки онлайн, то
    # товар будет добавлен в итоговый xlsx-файл

    # Ссылка на растворимый кофе, который есть в наличии
    url = f'https://online.metro-cc.ru/category/chaj-kofe-kakao/kofe?from=under_search&in_stock=1&attributes=1710000248%3Arastvorimyy&page='
    print('process number: ', number)

    xlsx_result_writer = XlsxResultWriter(result_file=result_file)
    xlsx_result_writer.write(product_generator=MetroParser(url=url,).run(cookies_list),
                             worksheet=f'Кофе')


if __name__ == '__main__':

    process1 = multiprocessing.Process(
        target=main, args=(COOKIES_LIST_ST_PT, 1))
    process2 = multiprocessing.Process(
        target=main, args=(COOKIES_LIST_MOSCOW[:4], 2))
    process3 = multiprocessing.Process(
        target=main, args=(COOKIES_LIST_MOSCOW[4:7], 3))
    process4 = multiprocessing.Process(
        target=main, args=(COOKIES_LIST_MOSCOW[7:9], 4))
    process5 = multiprocessing.Process(
        target=main, args=(COOKIES_LIST_MOSCOW[9:], 5))

    process1.start()
    process2.start()
    process3.start()
    process4.start()
    process5.start()

    process1.join()
    process2.join()
    process3.join()
    process4.join()
    process5.join()

    file_paths = ['result_file1.xlsx',
                  'result_file2.xlsx',
                  'result_file3.xlsx',
                  'result_file4.xlsx',
                  'result_file5.xlsx']

    JoinAllXlsxFiles().join(file_paths)
