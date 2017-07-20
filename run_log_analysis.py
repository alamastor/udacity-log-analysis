#!/usr/bin/env python3
import psycopg2


def main():
    conn = psycopg2.connect(dbname='news')
    create_article_views(conn)
    print_popular_articles(conn)
    print_popular_authors(conn)
    print_high_error_days(conn)
    conn.close()


def print_popular_articles(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT title, count(title)
        FROM article_views
        GROUP BY title
        ORDER BY count(title) DESC
        """)
    print('\nMost Popular Articles:')
    for i in range(3):
        res = cur.fetchone()
        print('{0}. {1} -- {2:,} views'.format(i + 1, res[0], res[1]))
    cur.close()


def print_popular_authors(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT author, count(author)
        FROM article_views
        GROUP BY author
        ORDER BY count(author) DESC
        """)
    print('\nMost Popular Authors:')
    for i, res in enumerate(cur.fetchall()):
        print('{0}. {1} -- {2:,} views'.format(i + 1, res[0], res[1]))
    cur.close()


def print_high_error_days(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            (daily_errors.count / daily_queries.count::float) * 100
            AS error_percent,
            daily_queries.day
        FROM (
            SELECT date_trunc('day', time) as day, count(date_trunc('day', time))
            FROM log
            WHERE method = 'GET'
            GROUP BY date_trunc('day', time)
        ) AS daily_queries
        JOIN
        (
            SELECT date_trunc('day', time) as day, count(date_trunc('day', time))
            FROM log
            WHERE
            method = 'GET' AND status != '200 OK'
            GROUP BY date_trunc('day', time)
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
    cur = conn.cursor()
    cur.execute('''
        CREATE TEMP VIEW article_views AS
        SELECT au.name as author, a.title as title, l.time as access_time
        FROM
        articles as a JOIN log as l ON l.path LIKE '/article/'||a.slug
        JOIN authors as au ON a.author = au.id
        WHERE
        l.method = 'GET'AND l.status = '200 OK'
    ''')
    conn.commit()
    cur.close()


if __name__ == '__main__':
    main()