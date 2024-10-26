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




