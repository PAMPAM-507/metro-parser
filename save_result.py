from abc import ABC, abstractmethod
import os
from typing import Iterable, List, NoReturn

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
        
        
    
    def write(self, product_generator: Iterable[ProductCardReportDTO], worksheet: str) -> NoReturn:
        page = self.result_file.add_worksheet(worksheet)
        
        row = 0
        column = 0
        
        page.write('A1', 'Id')
        page.write('B1', 'Название')
        page.write('C1', 'Ссылка на продукт')
        page.write('D1', 'Цена')
        page.write('E1', 'Акционная цена')
        page.write('F1', 'Бренд')
        
        page.set_column('A:A', 20)
        page.set_column('B:B', 20)
        page.set_column('C:C', 50)
        page.set_column('D:D', 50)
        page.set_column('E:E', 50)
        page.set_column('F:F', 50)
        
        for parameter in product_generator:
            print(parameter)
            
            if parameter.article_number:
                page.write(row+1, 0, parameter.article_number)
                
            if parameter.name:
                page.write(row+1, 1, parameter.name)
                
            if parameter.product_link:
                page.write(row+1, 2, parameter.product_link)
                
            if parameter.regular_price:
                page.write(row+1, 3, parameter.regular_price)
                
            if parameter.promo_price:
                page.write(row+1, 4, parameter.promo_price)
            
            if parameter.brand:
                page.write(row+1, 5, parameter.brand)
            
            row += 1
        
        self.__close()
        
    def __close(self) -> NoReturn:
        self.result_file.close()
        

    
class JoinAllFiles(ABC):
    
    @abstractmethod
    def join():
        pass

class JoinAllXlsxFiles(JoinAllFiles):
    
    @staticmethod
    def __check_file_paths(file_paths: List[str]) -> List[str]:
        checked_file_paths = []
        for file_path in file_paths:
            if os.path.exists(file_path):
                checked_file_paths.append(file_path)
                print(f"Файл '{file_path}' существует.")
            else:
                print(f"Файл '{file_path}' не найден.")
        
        return checked_file_paths
    
    @staticmethod
    def __remove_all_helping_xlsx(checked_file_paths: str, final_file: str) -> NoReturn:
        for file_path in checked_file_paths:
            if file_path != final_file and os.path.exists(file_path):
                os.remove(file_path)
                print(f"Удален файл '{file_path}'.")
    
    def join(self, file_paths: List[str], sheet_name: str='Кофе', subset_: str='Id') -> NoReturn:
        
        checked_file_paths = self.__check_file_paths(file_paths)
        
        data_frames = []

        for file_path in checked_file_paths:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            data_frames.append(df)

        combined_df = pd.concat(data_frames, 
                                ignore_index=True).drop_duplicates(subset=subset_)

        combined_df = combined_df.dropna(subset=[subset_])
        
        combined_df.to_excel('result.xlsx', 
                             sheet_name=sheet_name, 
                             index=False,)
        
        self.__remove_all_helping_xlsx(checked_file_paths=checked_file_paths, 
                                       final_file='result.xlsx',)
        
    