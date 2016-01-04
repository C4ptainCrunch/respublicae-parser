from celery import Celery
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

app = Celery('hello')
app.config_from_object('celeryconfig')

from courses import list_courses, list_course_files, get_doc_url


@app.task
def main():
    courses = list(list_courses().values())[:50]
    for course in courses:
        get_course.delay(course)
    logger.info("%i courses were enqued", len(courses))


@app.task
def get_course(course):
    files = list_course_files(course['id'])
    for file in files:
        get_file_data.delay(file)
    logger.info("%i files were enqued in course %s", len(files), course["key_code"])


@app.task
def get_file_data(file):
    name = file['name']
    page_url = file['page_url']

    url = get_doc_url(page_url)

    return name, url
