import courses
import sqlite3
from contextlib import contextmanager
import re
from multiprocessing import Pool
import requests
import os

import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')
steam_handler = logging.StreamHandler()
steam_handler.setLevel(logging.INFO)
steam_handler.setFormatter(formatter)
logger.addHandler(steam_handler)


@contextmanager
def db(fname):
    try:
        conn = sqlite3.connect(fname)
        yield conn.cursor()
    finally:
        conn.commit()
        conn.close()


def _slugify(string):
    match = re.match(r'(\w{3,4})-(\w)(\d{3,4})', string.lower())
    return "%s-%s-%s" % match.groups()


def refresh_courses():
    s = courses.login()
    course_list = courses.list_courses(s)

    with db("db.sqlite") as cursor:
        db_courses = [
            (c['id'], _slugify(c['key_code']), c['name']) for c in course_list.values()
        ]
        cursor.executemany("INSERT INTO course VALUES (?, ?, ?)", db_courses)


def _document_id_from_url(url):
    domain = courses.DOMAIN.replace('.', r'\.')
    match = re.match(r'http://' + domain + r'/documents/\d+/publication/(\d+)', url)
    return match.groups()[-1]


def _download_id_from_url(url):
    domain = courses.DOMAIN.replace('.', r'\.')
    match = re.match(r'http://' + domain + r'/files/download/(\d+)/document', url)
    return match.groups()[-1]


def refresh_documents():
    s = courses.login()
    with db("db.sqlite") as cursor:
        for course in list(cursor.execute("SELECT id FROM course")):
            course_id = int(course[0])
            documents = courses.list_course_files(s, course_id)
            db_documents = [
                (d['name'], course_id, _document_id_from_url(d['pageurl'])) for d in documents
            ]
            cursor.executemany("INSERT INTO document (name, course_id, document_id) VALUES (?, ?, ?)", db_documents)


def get_download_ids():
    s = courses.login()
    with db("db.sqlite") as cursor:
        documents = list(cursor.execute("SELECT course_id, document_id FROM document WHERE download_id IS NULL"))
    for doc in documents:
        course_id = int(doc[0])
        document_id = int(doc[1])

        u = "http://" + courses.DOMAIN + "/documents/%i/publication/%i" % (course_id, document_id)
        url = courses.get_doc_url(s, u)

        if url.strip():
            params = (_download_id_from_url(url), course_id, document_id)
            logger.info(params)
            with db("db.sqlite") as cursor:
                cursor.execute("UPDATE document SET download_id=? WHERE course_id=? AND document_id=?", params)
        else:
            logger.warning("Document failed: %s, %s" % (course_id, document_id))


def _download_file(s, url, fname):
    r = s.get(url, stream=True)
    with open(fname, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)


def _dl_from_id(download_id):
    try:
        url = "http://" + courses.DOMAIN + "/files/download/%i/document" % download_id
        fname = "./data/" + str(download_id)
        if not os.path.exists(fname):
            _download_file(s, url, fname)
        return True
    except Exception as e:
        logger.warning(str(download_id) + " " + str(e))
        return False


def _init_pool():
    global s
    s = requests.Session()


def download_documents():
    with open("slugs") as sl:
        slugs = list(map(lambda x: x.strip(), sl.readlines()))

    slugs_str = ", ".join(["'"+s+"'" for s in slugs])
    with db("db.sqlite") as cursor:
        documents = list(cursor.execute("""
            SELECT download_id
            FROM document
            JOIN course ON
                course.id = document.course_id
            WHERE
                was_downloaded=0
                AND download_id IS NOT NULL
                AND course.slug IN ("""+slugs_str+""")
        """))

    p = Pool(10, _init_pool)
    download_ids = [int(d[0]) for d in documents]
    try:
        res = p.map(_dl_from_id, download_ids)
    except Exception as e:
        logger.warning("Error while mapping documents" + str(e))

    z = list(zip([1 if r else 0 for r in res], download_ids))

    with db("db.sqlite") as cursor:
        cursor.executemany("UPDATE document SET was_downloaded=? WHERE download_id=?", z)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Mouhahaha')
    parser.add_argument('--courses', '-c', dest='refresh_courses', action='store_true')
    parser.add_argument('--docs', '-d', dest='refesh_docs', action='store_true')
    parser.add_argument('--urls', '-u', dest='get_urls', action='store_true')
    parser.add_argument('--get', '-g', dest='dl', action='store_true')

    parser.add_argument('--all', '-a', dest='all', action='store_true')

    args = parser.parse_args()

    if args.all or args.refresh_courses:
        logger.info("Refreshing course list...")
        refresh_courses()

    if args.all or args.refesh_docs:
        logger.info("Refreshing document list...")
        refresh_documents()

    if args.all or args.get_urls:
        with db("db.sqlite") as cursor:
            res = cursor.execute("SELECT COUNT(*) FROM document WHERE download_id IS NULL")
            n_docs = int(list(res)[0][0])
        logger.info("Retrieving document urls. %i to go..." % n_docs)
        get_download_ids()

    if args.all or args.dl:
        with db("db.sqlite") as cursor:
            res = cursor.execute("SELECT COUNT(*) FROM document WHERE was_downloaded=0 AND download_id IS NOT NULL")
            n_docs = int(list(res)[0][0])
        logger.info("Retrieving document files. %i to go..." % n_docs)
        download_documents()

    logger.info("Done.")
