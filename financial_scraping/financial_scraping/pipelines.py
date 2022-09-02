# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymysql


class FinancialScrapingPipeline:
    

    def open_spider(self, spider):
        self.connection = pymysql.connect(host='localhost',
                                          user='root',
                                          password='',
                                          database='Industries_info')
        self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        self.connection.close()


    def process_item(self, item, spider):
        self.cursor.execute(
"""
INSERT INTO Companies_info
VALUE (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
""", (item.get('Symbol'),
item.get('Name'),
item.get('Industry'),
item.get('Module'),
item.get('Market_Cap'),
item.get('Equity_MRQ'),
item.get('Total_Assets_MRQ'),
item.get('P_E'),
item.get('P_S'),
item.get('P_B'),
item.get('Sales_TTM'),
item.get('Sales_LFY'),
item.get('Profit_after_Tax_TTM'),
item.get('Profit_after_Tax_LFY'),
item.get('url'))
        )
        self.connection.commit()
        return item
