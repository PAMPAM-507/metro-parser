from abc import ABC, abstractmethod
from typing import NoReturn

import xlsxwriter
import pandas as pd
from dto import ProductCardReportDTO


class ResultWriter(ABC):
    
    @abstractmethod
    def write():
        pass


class XlsxResultWriter(ResultWriter):
    
    
    def __init__(self, result_file: str,) -> NoReturn:
        self.file_path = result_file
        self.result_file = xlsxwriter.Workbook(result_file)
        self.current_row = 0
        
    def close(self) -> NoReturn:
        self.result_file.close()
        
    def write(self, product: ProductCardReportDTO, worksheet: str='Кофе') -> NoReturn:
        if self.current_row == 0:
            # Если это первая запись, создаём новый лист и добавляем заголовки
            self.page = self.result_file.add_worksheet(worksheet)
            headers = ['Id', 'Название', 'Ссылка на продукт', 'Цена', 'Акционная цена', 'Бренд']
            for col, header in enumerate(headers):
                self.page.write(self.current_row, col, header)
            self.current_row += 1
        
        print(product)

        self.page.write(self.current_row, 0, product.article_number)
        self.page.write(self.current_row, 1, product.name)
        self.page.write(self.current_row, 2, product.product_link)
        self.page.write(self.current_row, 3, product.regular_price)
        self.page.write(self.current_row, 4, product.promo_price)
        self.page.write(self.current_row, 5, product.brand)

        self.current_row += 1
        
    
class CleanFiles(ABC):
    
    @staticmethod
    @abstractmethod
    def clean(file_path: str) -> NoReturn:
        pass


class CleanXlsxFiles(CleanFiles):
    
    @staticmethod
    def clean(file_path: str) -> NoReturn:
        df = pd.read_excel(file_path)

        df = df.dropna(subset=['Id'])

        df = df.drop_duplicates(subset=['Id'])

        df.to_excel(file_path, index=False)
        
    