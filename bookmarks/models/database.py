import sqlite3

class Database:

    def __init__(self, db_name):
        self.db_name = db_name
        self.connection = sqlite3.connect(self.db_name)
        self.cursor = self.connection.cursor()

    def create_tables(self):
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS books (
                            id INTEGER PRIMARY KEY,
                            title TEXT,
                            author TEXT,
                            publisher TEXT,
                            publish_date TEXT,
                            description TEXT
                        );''')
        
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS genres (
                            id INTEGER PRIMARY KEY,
                            book_id INTEGER,
                            genre TEXT,
                            FOREIGN KEY(book_id) REFERENCES books(id)
                        );''')

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS reviews (
                            id INTEGER PRIMARY KEY,
                            book_id INTEGER,
                            review_author TEXT,
                            review_publisher TEXT,
                            review_rating TEXT,
                            review_text TEXT,
                            review_link TEXT,
                            FOREIGN KEY(book_id) REFERENCES books(id)
                        );''')
    
    def insert_book(self, book_values):
        self.cursor.execute('''INSERT OR IGNORE INTO books (title, author, publisher, publish_date, description)
                               VALUES (?, ?, ?, ?, ?);''', book_values)
        book_id = self.cursor.lastrowid
        return book_id
    
    def insert_genre(self, genre_values):
        self.cursor.execute('''INSERT OR IGNORE INTO genres (book_id, genre)
                               VALUES (?, ?);''', genre_values)
        
    def insert_review(self, review_values):
        try:
            self.cursor.execute('''INSERT INTO reviews (book_id, review_author, review_publisher, review_rating, review_text, review_link)
                                   VALUES (?, ?, ?, ?, ?, ?);''', review_values)
        except sqlite3.IntegrityError:
            print(f"Skipping duplicate review for {review_values[0]}")