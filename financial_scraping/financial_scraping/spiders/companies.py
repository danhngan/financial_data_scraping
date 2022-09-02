import scrapy
import pandas as pd
from requests_html import HTML
import pyppeteer
import asyncio

class CompaniesSpider(scrapy.Spider):
    name = 'companies'
    allowed_domains = ['en.stockbiz.vn']
    start_urls = ['http://en.stockbiz.vn/']

    script = '''
() => {
var button = document.getElementById("ctl00_webPartManager_wp572523149_wp829393896_view1_lbtnNext");
button.click()
}'''

    key_xpath = '//table[@id="ctl00_webPartManager_wp572523149_wp829393896_view1_gvCompanies"]/tbody/tr[2]/td[1]/span/span/a'
    
    def __init__(self):
        self.industries_df = pd.read_csv('./data/industries.csv')
        print(self.industries_df.shape)
        self.start_idx = int(input('Start index: '))
        self.end_idx = int(input('End index: '))
        
        self.industries = []
        self.modules = []
        self.urls = []
        for idx in range(self.start_idx, self.end_idx):
            self.industries.append(self.industries_df.loc[idx,'industry name'])
            self.modules.append(self.industries_df.loc[idx,'module name'])
            self.urls.append(self.industries_df.loc[idx,'url'])

    async def _get_url(self, url, browser, sleep=10):
        page = await browser.newPage()
        await page.goto(url)
        await asyncio.sleep(sleep)
        return page

    async def render_page(self, page, current, script, sleep = 5, n_tries=50):
        key = current.xpath(self.key_xpath)
        key = key[0].text
        await page.evaluate(script)
        while n_tries > 0:
            n_tries -= 1
            await asyncio.sleep(sleep)
            content = await self.get_page_content(page)
            content = HTML(html=content)
            new_key = content.xpath(self.key_xpath)[0].text
            if key != new_key:
                return content

    async def get_page_content(self, page, sleep = 1):
        content = await page.content()
        await asyncio.sleep(sleep)
        return content

    async def parse(self, response):
        # launch chrome browser
        browser = await pyppeteer.launch()
        idx = 0
        while idx < len(self.industries):
            
            # get websites
            tasks = [asyncio.create_task(self._get_url(url, browser=browser, sleep=30)) for url in self.urls[idx: idx+5]]
            pages = [await task for task in tasks]
            
            # get contents
            tasks = [asyncio.create_task(self.get_page_content(page)) for page in pages]
            responses = [[res_idx, HTML(html = await task)] for res_idx, task in enumerate(tasks)]
            
            industries = self.industries[idx:idx + 5]
            modules = self.modules[idx:idx + 5]

            while len(responses) > 0:
                temp = []
                tasks = []
                for res_idx, response in responses:
                    tables = {}
                    for i in range(4):
                        tables[i] = response.xpath(f'//table[@id="ctl00_webPartManager_wp572523149_wp829393896_view{i+1}_gvCompanies"]/tbody/tr')
                    
                    for i in range(1, len(tables[0])):
                        comp_page = {}
                        for j in range(4):
                            comp_page[j] = tables[j][i].text.split('\n')
                        print('Symbol', comp_page[0][0])
                        yield { 'Symbol': comp_page[0][0],
                                'Name': comp_page[0][1],
                                'Industry': industries[res_idx],
                                'Module': modules[res_idx],
                                'Market_Cap': comp_page[0][2],
                                'Equity_MRQ': comp_page[0][3],
                                'Total_Assets_MRQ': comp_page[0][4],
                                'P_E': comp_page[1][2],
                                'P_S': comp_page[1][3],
                                'P_B': comp_page[1][4],
                                'Sales_TTM': comp_page[2][2],
                                'Sales_LFY': comp_page[2][3],
                                'Profit_after_Tax_TTM': comp_page[3][2],
                                'Profit_after_Tax_LFY': comp_page[3][3],
                                'url':tables[0][i].xpath('.//a/@href')[0]}
                    
                    # generate another page
                    if len(response.find('#ctl00_webPartManager_wp572523149_wp829393896_view1_lbtnNext')) > 0:
                        tasks.append(asyncio.create_task(self.render_page(pages[res_idx], response, self.script, sleep=5, n_tries=15)))
                        temp.append([res_idx])
                    else:
                        pages[res_idx].close()
                if len(temp) > 0:
                    for i in range (len(tasks)):
                        temp[i].append(await tasks[i])
                    # get content takes a short time
                responses = temp
            idx += 5
        await browser.close()