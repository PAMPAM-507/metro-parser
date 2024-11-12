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

import asyncio
from typing import Dict, List
from dataclasses import dataclass, field
import traceback

import aiohttp
from bs4 import BeautifulSoup

from dto import ParserReportDTO, PriceDTO, ProductCardReportDTO
from save_result import CleanXlsxFiles, XlsxResultWriter, ResultWriter


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
ALL_COOKIES = COOKIES_LIST_ST_PT + COOKIES_LIST_MOSCOW

class MetroParser():

    def __init__(self, url: str, result_writer: ResultWriter) -> None:
        self.url = url
        self.result_writer = result_writer

    @staticmethod
    async def __get_request(product_link: str, cookies: Dict[str, str], headers: Dict[str, str] = headers) -> str:
        async with aiohttp.ClientSession() as session:
            async with session.get(product_link, 
                                   cookies=cookies, 
                                   headers=headers) as resp:
                if resp.status == 200:
                    return await resp.text()

    @staticmethod
    def __get_brand_index(soup: BeautifulSoup):
        product_attrs = soup.find_all('span', 
                                      class_='product-attributes__list-item-name-text')
        product_attrs_list = [attr.text.replace('\n', '').strip() for attr in product_attrs]
        index = product_attrs_list.index('Бренд')
        return index

    async def __get_data_from_card(self, parameter: ProductCardReportDTO) -> ParserReportDTO:

        product_link = parameter.product_link
        regular_price = parameter.regular_price
        promo_price = parameter.promo_price
        cookies = parameter.cookies

        try:
            response = await self.__get_request(product_link, cookies=cookies, headers=headers)
        except Exception as e:
            print(f"Ошибка при получении данных для {product_link}: {e}")
            traceback.print_exc()

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

        regular_price = ''.join(str(regular_price).split())
        promo_price = ''.join(str(promo_price).split())

        return PriceDTO(regular_price=regular_price,
                        promo_price=promo_price)

    async def run(self, cookies_list: List[Dict[str, str]]) -> ProductCardReportDTO:
        for cookies in cookies_list:
            page_number = 1

            while True:
                url = self.url + f'{page_number}'
                response = await self.__get_request(product_link=url, 
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

                    information = await self.__get_data_from_card(ProductCardReportDTO(
                        product_link=card_url,
                        regular_price=price.regular_price,
                        promo_price=price.promo_price
                    ))
                
                    self.result_writer.write(product=information,)
                
                page_number += 1


async def main(cookies_list: List[Dict[str, str]]) -> None:
    result_file = f'./result.xlsx'
    
    # Если экземпляр кофе есть хотя бы в одном из филиалов Metro (Москва и Санкт-Петербург)
    # и доступен для покупки онлайн, то
    # товар будет добавлен в итоговый xlsx-файл
    
    # Ссылка на растворимый кофе, который есть в наличии
    url = f'https://online.metro-cc.ru/category/chaj-kofe-kakao/kofe?from=under_search&in_stock=1&attributes=1710000248%3Arastvorimyy&page='

    xlsx_result_writer = XlsxResultWriter(result_file=result_file)
    
    tasks = [asyncio.create_task(MetroParser(url=url, 
                                             result_writer=xlsx_result_writer).run([cookies])) for cookies in cookies_list]
    
    await asyncio.gather(*tasks)

    xlsx_result_writer.close()
    
if __name__ == '__main__':
    
    asyncio.run(main(cookies_list=ALL_COOKIES))

    CleanXlsxFiles.clean(file_path='result.xlsx')



