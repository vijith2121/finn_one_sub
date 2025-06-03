import scrapy
# from finn_one_sub.items import Product
from lxml import html

class Finn_one_subSpider(scrapy.Spider):
    name = "finn_one_sub"
    start_urls = ["https://example.com"]

    def parse(self, response):
        parser = html.fromstring(response.text)
        print("Visited:", response.url)
