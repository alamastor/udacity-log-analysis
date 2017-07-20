#!/usr/bin/env python3
'''Script to print out various statistics from news database.
'''

import psycopg2


def main():
    '''Run functions to log database statistics.
    '''
    conn = psycopg2.connect(dbname='news')
    create_article_views(conn)
    print_popular_articles(conn)
    print_author_popularity(conn)
    print_high_error_days(conn)
    conn.close()


def print_popular_articles(conn):
    '''Print three most viewed articles in database.
    '''
    cur = conn.cursor()
    cur.execute("""
        SELECT title, COUNT(title)
        FROM article_views
        GROUP BY title
        ORDER BY COUNT(title) DESC
        """)
    print('\nMost Popular Articles:')
    for i in range(3):
        res = cur.fetchone()
        print('{0}. {1} -- {2:,} views'.format(i + 1, res[0], res[1]))
    cur.close()


def print_author_popularity(conn):
    '''Print authors in database in order of popularity.
    '''
    cur = conn.cursor()
    cur.execute("""
        SELECT author, COUNT(author)
        FROM article_views
        GROUP BY author
        ORDER BY COUNT(author) DESC
        """)
    print('\nMost Popular Authors:')
    for i, res in enumerate(cur.fetchall()):
        print('{0}. {1} -- {2:,} views'.format(i + 1, res[0], res[1]))
    cur.close()


def print_high_error_days(conn):
    '''Print all days in database which had HTTP error rates higher that 1%.
    '''
    cur = conn.cursor()
    cur.execute("""
        SELECT
            (daily_errors.count / daily_queries.count::float) * 100
            AS error_percent,
            daily_queries.day
        FROM (
            SELECT
                DATE_TRUNC('day', time) AS day, COUNT(DATE_TRUNC('day', time))
            FROM log
            WHERE method = 'GET'
            GROUP BY date_trunc('day', time)
        ) AS daily_queries
        JOIN
        (
            SELECT
                DATE_TRUNC('day', time) AS day, COUNT(DATE_TRUNC('day', time))
            FROM log
            WHERE
                method = 'GET' AND status != '200 OK'
            GROUP BY DATE_TRUNC('day', time)
        ) AS daily_errors
        ON daily_queries.day = daily_errors.day
        WHERE daily_errors.count / daily_queries.count::float >= 0.01
        ORDER BY error_percent DESC
        """)
    print('\nHigh Request Error Days:')
    for res in cur.fetchall():
        print('{0:%B %d, %Y} -- {1:0.1f}%'.format(res[1], res[0]))
    cur.close()


def create_article_views(conn):
    '''Create temporary view called 'article_views' to display all requests
    with associated article title and author name.
    '''
    cur = conn.cursor()
    cur.execute('''
        CREATE TEMP VIEW article_views AS
        SELECT au.name AS author, a.title AS title, l.time AS access_time
        FROM articles AS a JOIN log AS l ON l.path LIKE '/article/'||a.slug
        JOIN authors AS au ON a.author = au.id
        WHERE l.method = 'GET'AND l.status = '200 OK'
    ''')
    conn.commit()
    cur.close()


if __name__ == '__main__':
    main()
