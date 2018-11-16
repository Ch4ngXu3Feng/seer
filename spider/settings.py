# coding=utf-8

BOT_NAME = 'seer'
SPIDER_MODULES = ['spider.spiders']
NEWSPIDER_MODULE = 'spider.spiders'
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'spider.middlewares.random_user_agent.Middleware': 300,
}
ROBOTSTXT_OBEY = False
RANDOMIZE_DOWNLOAD_DELAY = True
COOKIES_ENABLED = True
CONCURRENT_REQUESTS_PER_DOMAIN = 1
CONCURRENT_REQUESTS_PER_IP = 1
DOWNLOAD_DELAY = 2
