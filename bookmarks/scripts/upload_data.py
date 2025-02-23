import json
import sqlite3

def create_tables(conn):
    with conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS books (
                            id INTEGER PRIMARY KEY,
                            title TEXT,
                            author TEXT,
                            publisher TEXT,
                            publish_date TEXT,
                            description TEXT,
                            genres TEXT
                        );''')

        conn.execute('''CREATE TABLE IF NOT EXISTS reviews (
                            id INTEGER PRIMARY KEY,
                            book_id INTEGER,
                            review_author TEXT,
                            review_publisher TEXT,
                            review_rating TEXT,
                            review_text TEXT,
                            review_link TEXT,
                            FOREIGN KEY(book_id) REFERENCES books(id)
                        );''')

def upload_data(json_file, db_file):
    conn = sqlite3.connect(db_file)
    create_tables(conn)
    
    with open(json_file, 'r') as file:
        data = json.load(file)
        
        for book in data:
            genres = ','.join(book['genres'])
            book_values = (book['title'], book['author'], book['publisher'], book['publish_date'], book['description'], genres)
            
            with conn:
                cursor = conn.execute('''INSERT INTO books (title, author, publisher, publish_date, description, genres)
                                         VALUES (?, ?, ?, ?, ?, ?);''', book_values)
                book_id = cursor.lastrowid

                for review in book['reviews']:
                    review_values = (book_id, review['review_author'], review['review_publisher'], review['review_rating'], review['review_text'], review['review_link'])
                    conn.execute('''INSERT INTO reviews (book_id, review_author, review_publisher, review_rating, review_text, review_link)
                                    VALUES (?, ?, ?, ?, ?, ?);''', review_values)

    conn.close()

if __name__ == '__main__':
    upload_data('data/raw/output.json', 'data/processed/books_reviews.db')
