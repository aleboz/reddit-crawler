import sqlite3
import os.path

class Store:
    dirname = None
    snapshot = None
    author = None
    is_open = False
    def __init__(self, dirname):
        conn = sqlite3.connect(os.path.join(dirname, 'snapshot.db'))
        c = conn.cursor()
        c.execute('''CREATE TABLE comments (subreddit_id text, subreddit text, id text, parent_id text, link_id text, author text, body text, body_html text, edited real, created real, ups real, downs real)''')
        c.execute('''CREATE TABLE posts (subreddit_id text, subreddit text, id text, author text, title text, selftext text, selftext_html text, edited real, created real, ups real, downs real)''')
        conn.commit()
        conn.close()
        conn = sqlite3.connect(os.path.join(dirname, 'author.db'))
        c = conn.cursor()
        c.execute('''CREATE TABLE comments (subreddit_id text, subreddit text, id text, parent_id text, link_id text, author text, body text, body_html text, edited real, created real, ups real, downs real)''')
        c.execute('''CREATE TABLE posts (subreddit_id text, subreddit text, id text, author text, title text, selftext text, selftext_html text, edited real, created real, ups real, downs real)''')
        conn.commit()
        conn.close()
        self.dirname = dirname

    def open(self):
        if self.is_open:
            return
        self.snapshot = sqlite3.connect(os.path.join(self.dirname, 'snapshot.db'))
        self.author = sqlite3.connect(os.path.join(self.dirname, 'author.db'))
        self.is_open = True

    def close(self):
        if not self.is_open:
            return
        self.snapshot.commit()
        self.snapshot.close()
        self.author.commit()
        self.author.close()
        self.is_open = False

    def store_snapshot(self, post, comments):
        comments_list = []
        posts_list = []

        for entry in comments:
            comments_list.append( (entry['subreddit_id'], entry['subreddit'], entry['id'], entry['parent_id'], entry['link_id'], entry['author'], entry['body'], entry['body_html'], entry['edited'], entry['created'], entry['ups'], entry['downs']) )

        posts_list.append( (post['subreddit_id'], post['subreddit'], post['id'], post['author'], post['title'], post['selftext'], post['selftext_html'], post['edited'], post['created'], post['ups'], post['downs']) )

        c = self.snapshot.cursor()
        c.executemany('INSERT INTO posts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', posts_list)
        c.executemany('INSERT INTO comments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', comments_list)
        self.snapshot.commit()
        print 'Committed '+str(len(comments_list)+len(posts_list))+' entries'

    def store_author(self, blob):
        comments_list = []
        posts_list = []
        for entry in blob:
            if entry['kind'] == 't3':
                posts_list.append( (entry['subreddit_id'], entry['subreddit'], entry['id'], entry['author'], entry['title'], entry['selftext'], entry['selftext_html'], entry['edited'], entry['created'], entry['ups'], entry['downs']) )
            elif entry['kind'] == 't1':
                comments_list.append( (entry['subreddit_id'], entry['subreddit'], entry['id'], entry['parent_id'], entry['link_id'], entry['author'], entry['body'], entry['body_html'], entry['edited'], entry['created'], entry['ups'], entry['downs']) )
        c = self.author.cursor()
        c.executemany('INSERT INTO posts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', posts_list)
        c.executemany('INSERT INTO comments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', comments_list)
        self.author.commit()
        print 'Committed '+str(len(comments_list)+len(posts_list))+' entries'
