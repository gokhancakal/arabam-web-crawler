import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
import urllib
from urllib.parse import urlparse
from urllib.parse import urljoin
from scrapy.crawler import CrawlerProcess
from twisted.internet import reactor, defer
from ArabamWebCrawler.items import ArabamWebCrawlerItem

#Vars

class ArabamSpider(scrapy.Spider):
    name = 'ArabamSpider'
    allowed_domains = ['arabam.com']
    #brand_urls = ["volvo-s60"]
    #brand_urls = ["volkswagen-polo-1-0"]
    brand_urls = ["anadol","aston-martin"]
    start_urls = ["https://www.arabam.com/ikinci-el/otomobil/" + str(i) + "?take=50" for i in brand_urls ]
 
    def parse(self, response):
        pagination = response.xpath('//*[@id="js-hook-missing-space-content"]/div[@class="listing-new-pagination cb tac mt16 pt16"]')
        rows = response.xpath('//*[@id="main-listing"]/tbody/tr')
        for row in rows:       
            urls = rows.xpath('./td/h3/a/@href').extract()
            for i in urls:
                yield scrapy.Request(urllib.parse.urljoin('https://www.arabam.com', i[1:]),callback=self.parse_url)
        next_page = pagination.xpath('//*[@id="pagingNext"]/@href').extract_first() 
        if next_page is not None:
            next_page = urllib.parse.urljoin('https://www.arabam.com', next_page)
            yield scrapy.Request(next_page, callback=self.parse)    

    #Getting data with xpath       
    def parse_url(self, response): 
        table = response.xpath('*//div[@class="detail-column-detail pr"]')
        for rows in table:
                item = ArabamWebCrawlerItem()
                item['col1'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "advert-detail-title", " " ))]/text()').extract()
                item['col1'] = "title: " +  str(item['col1'])
                item['col2'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "w66", " " ))]/text()').get()
                if item['col2'] == "" or item['col2'] == " " or item['col2'] == "  ":
                    item['col2'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bold", " " )) and contains(concat( " ", @class, " " ), concat( " ", "fl", " " ))]/text()').get()    
                item['col2'] = "price: " +  str(item['col2'])
                item['col3'] = rows.xpath('//*[(@id = "js-hook-for-observer-detail")]//*[contains(concat( " ", @class, " " ), concat( " ", "color-black2018", " " ))]/text()').get()
                item['col3'] = "address: " +  str(item['col3'])
                item['col4'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 1) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()
                item['col5'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 2) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()
                item['col6'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 3) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()
                item['col7'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 4) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()
                item['col8'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 5) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()
                item['col9'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 6) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()
                item['col10'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 7) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()
                item['col11'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 8) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()
                item['col12'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 9) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()
                item['col13'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 10) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()
                item['col14'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 11) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()
                item['col15'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 13) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()
                item['col16'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 14) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()
                item['col17'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 15) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()  
                item['col18'] = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "bcd-list-item", " " )) and (((count(preceding-sibling::*) + 1) = 16) and parent::*)]//*[contains(concat( " ", @class, " " ), concat( " ", "bli-particle", " " ))]/text()').extract()                
                yield item

process = CrawlerProcess({
'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
'FEED_FORMAT': 'csv',
'FEED_URI': '/home/gokhan/data/arabam_raw.csv'})
process.crawl(ArabamSpider)
process.start()