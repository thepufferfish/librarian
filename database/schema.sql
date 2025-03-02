CREATE SCHEMA bookmarks;

CREATE TABLE bookmarks.raw_data(
	id SERIAL PRIMARY KEY,
	book_id UUID,
	title TEXT,
	author TEXT,
	publisher TEXT,
	publish_date TEXT,
	description TEXT,
	genres TEXT,
	reviews TEXT,
	raw_json JSONB,
	timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bookmarks.books (
	id UUID PRIMARY KEY,
	title TEXT,
	author TEXT,
	publisher TEXT,
	publish_date TEXT,
	description TEXT,
	link TEXT,
	last_scraped TIMESTAMP
);

CREATE TABLE bookmarks.book_genres (
	book_id UUID REFERENCES bookmarks.books(id),
	genre TEXT
);


