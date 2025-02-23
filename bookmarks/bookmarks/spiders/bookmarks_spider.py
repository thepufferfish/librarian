import uuid
from scrapy.spiders import SitemapSpider
from datetime import datetime

class BookmarksSpider(SitemapSpider):
    name = 'bookmarks'
    sitemap_urls = ['https://bookmarks.reviews/wp-sitemap.xml']
    sitemap_follow = ['posts-bookmark']
    sitemap_rules = [('/bookmark/', 'parse_bookmark')]

    def sitemap_filter(self, entries):
        for entry in entries:
            try:
                date_time = datetime.strptime(entry["lastmod"], '%Y-%m-%dT%H:%M:%S%z')
            except:
                date_time = datetime.now().astimezone()
            if date_time > datetime.strptime('2015-05-13T00:00:00-05:00', '%Y-%m-%dT%H:%M:%S%z'):
                yield entry

    def parse_bookmark(self, response):
        title = response.xpath('//h1[contains(@class, "book_detail_title")][@itemprop="name"]/text()').get(default='').strip()
        author = response.xpath('//div[@itemprop="author"]/span[@itemprop="name"]/text()').get(default='').strip()
        publisher = response.xpath('//div[@itemprop="publisher"]/span[@itemprop="name"]/text()').get(default='').strip()
        publish_date = response.xpath('//div[@itemprop="datePublished"]/text()').get(default='').strip()
        description = ''.join(response.xpath('//div[@class="book_manual_description"]//text()').getall()).strip()
        genres = [genre.strip() for genre in response.xpath('//span[@itemprop="genre"]/text()').getall()]

        book_data = {
            'book_id': str(uuid.uuid5(uuid.NAMESPACE_DNS, title + author)),
            'title': title,
            'author': author,
            'publisher': publisher,
            'publish_date': publish_date,
            'description': description,
            'genres': genres,
            'link': response.url,
            'last_scraped': datetime.now().isoformat(),
            'reviews': []
        }

        see_all_reviews_link = response.xpath('//a[contains(text(), "See All Reviews")]/@href').get()
        see_all_reviews_link = see_all_reviews_link.replace('//all', '/all')
        if see_all_reviews_link:
            request = response.follow(see_all_reviews_link, self.parse_reviews)
            request.meta['book_data'] = book_data
            yield request
        else:
            yield book_data

    def parse_reviews(self, response):
        book_data = response.meta['book_data']

        reviews = response.xpath('//span[@itemprop="review"]')
        for review in reviews:
            review_author = review.xpath('.//span[@itemprop="name"]/text()').get(default='').strip()
            review_publisher = review.xpath('.//a[@class="bookmarks_source_link"]/text()').get(default='').strip()
            review_rating = review.xpath('.//span[contains(@class, "review_rating")]/text()').get(default='').strip()
            review_text = ''.join(review.xpath('.//div[@class="bookmarks_a_review_pullquote"]/text()').getall()).strip()
            review_link = review.xpath('.//a[@class="see_more_link"]/@href').get(default='')

            book_data['reviews'].append({
                'review_author': review_author,
                'review_publisher': review_publisher,
                'review_rating': review_rating,
                'review_text': review_text,
                'review_link': review_link
            })

        yield book_data
