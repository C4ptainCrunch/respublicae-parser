import requests
from bs4 import BeautifulSoup
import config


DOMAIN = 'beta.respublicae.be'

HEADERS = {
    'Host': DOMAIN,
    'User-Agent': 'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:37.0) Gecko/20100101 Firefox/42.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
}


def login():
    session = requests.Session()
    session.get('http://%s/users/login' % DOMAIN)

    data = {
        'email': config.email,
        'password': config.password,
    }

    headers = {
        "Referer": "http://%s/users/login" % DOMAIN
    }
    headers.update(HEADERS)

    session.post(
        'http://%s/users/login' % DOMAIN,
        headers=HEADERS, data=data
    )

    return session


def list_courses(session):
    headers = {
        'Accept': '*/*',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'http://%s/subscriptions/courses' % DOMAIN,
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }
    headers.update(HEADERS)

    courses = []

    for i in range(0, 5000, 30):
        data = 'terms=&start=%i' % i
        resp = session.post(
            'http://%s/ajax/get_courses' % DOMAIN,
            headers=headers, data=data
        )
        courses += resp.json()['data']

    courses_dict = {course['key_code']: course for course in courses}

    return courses_dict


def list_course_files(session, course_id):
    resp = session.get(
        'http://%s/documents/%s' % (DOMAIN, course_id),
        headers=HEADERS
    )
    resp.encoding = 'utf-8'

    soup = BeautifulSoup(resp.text, "html.parser")
    docs = soup.findAll("div", class_="big-list-item-infos")

    files = []

    for doc in docs:
        title = doc.find("p", class_="big-list-item-infos-title")
        page_url = title.a['href']
        files.append({
            'name': title.a.getText(),
            'pageurl': page_url
        })

    return files


def get_doc_url(session, page_url):
    resp = session.get(page_url, headers=HEADERS)
    soup = BeautifulSoup(resp.text, "html.parser")
    u = soup.find("p", class_="download-button-wrapper").a['href']
    return u
