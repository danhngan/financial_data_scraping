import scrapy


class IndustriesSpider(scrapy.Spider):
    name = 'industries'
    allowed_domains = ['en.stockbiz.vn']
    start_urls = ['http://en.stockbiz.vn']

    def start_requests(self):
        
        yield scrapy.Request('http://en.stockbiz.vn/Industries.aspx', callback=self.parse_industries)
    
    def parse_industries(self, response):
        industries = response.xpath('//div[@class="module"]')
        for industry in industries:
            industry_name = industry.xpath('.//div[@class="moduleHeaderInline"]/a/text()').get()
            subins = industry.xpath('.//div[@class="headlineMed"]')
            for ins in subins:
                yield {
                    'industry name': industry_name,
                    'module name': ins.xpath('./a/text()').get(),
                    'url': ins.xpath('./a/@href').get()
                }