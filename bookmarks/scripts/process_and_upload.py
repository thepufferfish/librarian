import sys
import os
import json
import re
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.database import Database

def format_publish_date(publish_date):
    publish_date = re.sub(r' ([0-9]),', r' 0\1,', publish_date)
    publish_date = datetime.strptime(publish_date, '%B %d, %Y')
    return(publish_date)

def process_and_upload_scrapy_data(json_file, db_file):
    db = Database(db_file)
    db.create_tables()
    
    with open(json_file, 'r') as file:
        data = json.load(file)
        
        for book in data:

            book['publish_date'] = format_publish_date(book['publish_date'])
            book_values = (book['title'], book['author'], book['publisher'], book['publish_date'], book['description'])
            book_id = db.insert_book(book_values)

            for genre in book['genres']:
                genre_values = (book_id, genre)
                db.insert_genre(genre_values)

            for review in book['reviews']:
                review['review_author'] = re.sub(r',$', '', review['review_author'])
                review_values = (book_id, review['review_author'], review['review_publisher'], review['review_rating'], review['review_text'], review['review_link'])
                db.insert_review(review_values)

    if __name__ == '__main__':
        process_and_upload_scrapy_data('data/raw/output.json', 'data/processed/books_reviews.db')