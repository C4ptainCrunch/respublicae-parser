import courses
import sqlite3
from contextlib import contextmanager
import re

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


if __name__ == '__main__':
    # logger.info("Refreshing course list...")
    # refresh_courses()

    # logger.info("Refreshing document list...")
    # refresh_documents()

    with db("db.sqlite") as cursor:
        res = cursor.execute("SELECT COUNT(*) FROM document WHERE download_id IS NULL")
        n_docs = int(list(res)[0][0])
    logger.info("Retrieving document urls. %i to go..." % n_docs)
    get_download_ids()

    logger.info("Done.")
