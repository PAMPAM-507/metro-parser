from dataclasses import dataclass, field
from typing import NoReturn


@dataclass
class ParserReportDTO:
    article_number: str
    name: str
    product_link: str
    regular_price: str
    promo_price: str
    brand: str

    def __len__(self) -> int:
        return 6
    
    def __post_init__(self) -> NoReturn:
        self.promo_price = self.promo_price if int(self.promo_price) > 0 else None


@dataclass
class ProductCardReportDTO:
    product_link: str
    regular_price: str
    promo_price: str
    cookies: dict = field(default_factory=dict)


@dataclass
class PriceDTO:
    regular_price: str
    promo_price: str


# headers = {
#     'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.5) Gecko/20091102 Firefox/3.5.5T(.NET CLR 3.5.30729)',
# }



# import requests
# from bs4 import BeautifulSoup


# cookies = {  # Ул. Складочная, д. 1, стр. 1 (м. Савеловская)
#         'metroStoreId': '308',
#         'pickupStore': '308',
#     }

# url = 'https://online.metro-cc.ru/products/255g-nap-kofeynyy-maccoffee-cappuccino-di-torino-3v1-rastv-254710'
# response = requests.get(url, cookies=cookies, headers=headers,).text
# soup = BeautifulSoup(response, 'lxml')





# product_attrs = soup.find_all('span', class_='product-attributes__list-item-name-text')
# product_attrs_list = [attr.text.replace('\n', '').strip() for attr in product_attrs]
# index = product_attrs_list.index('Бренд')
# print(index)



