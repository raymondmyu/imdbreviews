# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
from bs4 import BeautifulSoup


class ReviewsSpider(scrapy.Spider):
    name = 'reviews'
    allowed_domains = ['imdb.com']
    start_urls = ['http://www.imdb.com/list/ls050490282/']

    def parse(self, response):

        # Start with list of best animations
        title_list = response.css('.lister-item div.lister-item-image::attr(data-tconst)').extract()
        title_names = response.css('.lister-item .lister-item-header>a::text').extract()

        # Get the links
        title_links = ['http://www.imdb.com/title/{}/'.format(x) for x in title_list]
        review_links = ['http://www.imdb.com/title/{}/reviews/'.format(x) for x in title_list]

        # Go to each animation title's link to find related titles
        for title_link, name in zip(title_links, title_names):
            # Get related titles
            yield scrapy.Request(title_link, callback=self.parse_related, meta={'title': name})

        # Get the reviews
        for review_link, name, title in zip(review_links, title_names, title_list):
            # Get reviews
            yield scrapy.Request(review_link, callback=self.parse_user_reviews, meta={'title': title, 'name': name, 'pageindex': 0, 'df': None})


    def parse_user_reviews(self, response):
        def get_page_reviews(pageindex, title, name, df, selector):

            # Returns dataframe with reviews appended
            if pageindex == 0:
                df_reviews = pd.DataFrame(columns=['title', 'stars', 'date', 'r_title', 'text'])
            else:
                df_reviews = df

            for review in selector:
                stars = review.css('.rating-other-user-rating span:nth-child(2)::text').extract_first()
                date = review.css('.display-name-date span:nth-child(2)::text').extract_first()
                r_title = review.css('.title::text').extract_first()
                text = BeautifulSoup(review.css('.content .text').extract_first(), 'lxml').text.strip()
                df_reviews = df_reviews.append(
                    pd.Series([name, stars, date, r_title, text],
                              index=['title', 'stars', 'date', 'r_title', 'text']),
                    ignore_index=True)
            return df_reviews

        pageindex = response.meta['pageindex']
        title = response.meta['title']
        name = response.meta['name']
        df = response.meta['df']
        selector = response.css('.lister-item-content')

        # Get the current page's reviews
        df_reviews = get_page_reviews(pageindex, title, name, df, selector)

        # Check if there are more pages
        load_more = response.css('.load-more-data::attr(data-key)')
        if load_more:
            link = 'http://www.imdb.com/title/{}/reviews/_ajax?ref_=undefined&paginationKey={}'.format(title, load_more.extract_first())
            yield scrapy.Request(link, callback=self.parse_user_reviews, meta={'title': title, 'name': name, 'pageindex': pageindex+1, 'df': df_reviews})
        else:
            df_reviews.to_csv('{}.csv'.format(name))


    def parse_related(self, response):
        print('Getting related titles of ', response.meta['title'])

        # Get related titles and names
        related_titles = response.css('div.rec_item::attr(data-tconst)').extract()
        related_names = response.css('div.rec_item img::attr(title)').extract()
        related_links = ['http://www.imdb.com/title/{}/reviews/'.format(x) for x in related_titles]

        for title, name, link in zip(related_titles, related_names, related_links):
            # Get reviews
            yield scrapy.Request(link, callback=self.parse_user_reviews,
                                 meta={'title': title, 'name': name, 'pageindex': 0, 'df': None})