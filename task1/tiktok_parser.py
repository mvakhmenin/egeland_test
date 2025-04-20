import requests
import re
import psycopg2
import time
import os
import sys
import structlog
from structlog.dev import ConsoleRenderer
from structlog.processors import JSONRenderer
import logging
from dotenv import load_dotenv
load_dotenv()

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def get_app_logger(app_name):
    pod_name = os.uname()[1]
    processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso")
        ]
    if os.environ.get("LOGGING_TYPE") == 'console':
        processors += [ConsoleRenderer()]
    else:
        processors += [JSONRenderer()]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(sys.stdout)  # Вывод в stdout
    )
    logger = structlog.get_logger()
    logger = logger.bind(pod_name=pod_name, app_name=app_name)
    return logger

def get_pg_conn():
    """
    Функция возвращает соединение к БД PostgreSQL
    """
    host = os.environ['HOST']
    port = os.environ['PORT']
    user = os.environ['DB_USER']
    password = os.environ['PASS']
    db_name = os.environ['DBNAME']
    return psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=host,
        port=port
        )

def get_platform_id(platform_name):
    """
    Функция для получения platform_id по platform_name
    """
    sql_platform_id = f"""
        SELECT platform_id
        FROM platforms
        WHERE platform_name = '{platform_name}'
    """
    pg_conn = get_pg_conn()
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(sql_platform_id)
            platform_id = cursor.fetchone()
    except Exception as e:
        logger.error('Ошибка при получении platform_id')
        logger.error(e)
    finally:
        if pg_conn:
            pg_conn.close()
    return platform_id[0]


def get_user_names(limit=None, offset=None):
    """
    Функция для получения ID и имен пользователей для парсинга
    Поддерживает параметры для пагинации (limit, offset)
    """
    sql_user_names = """
        SELECT user_id, user_name
        FROM users
        ORDER BY user_id
    """
    if limit is not None:
        sql_user_names += f" LIMIT {limit}"
        if offset is not None:
            sql_user_names += f" OFFSET {offset}"
    pg_conn = get_pg_conn()
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(sql_user_names)
            user_names = cursor.fetchall()
    except Exception as e:
        logger.error('Ошибка при получении user_id')
        logger.error(e)
    finally:
        if pg_conn:
            pg_conn.close()
    return {user_name[0]:user_name[1] for user_name in user_names}

def process_too_many_r(timeout = 5):
    """
    Функция для обработки ошибки 429 (Too Many Requests)
    В настоящий момент реализована как заглушка
    Возможны более сложные алгоритмы обработки
    """
    time.sleep(timeout)

def process_serv_unavail(timeout = 5):
    """
    Функция для обработки ошибки 503 (Service Unavailable)
    В настоящий момент реализована как заглушка
    Возможны более сложные алгоритмы обработки
    """
    time.sleep(timeout)

def process_other_err(timeout = 5):
    """
    Функция для обработки прочих ошибок
    В настоящий момент реализована как заглушка
    Возможны более сложные алгоритмы обработки
    """
    time.sleep(timeout)

def get_page_text(headers, username):
    """
    Функция для получения текста страницы для дальнейшего парсинга
    Поддерживает обработку ошибко 429, 503 и пр.
    """
    link = f"https://www.tiktok.com/@{username}?lang=en"
    with requests.Session() as session:
        adapter = requests.adapters.HTTPAdapter(max_retries=20)
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        while True:
            r = session.get(link, headers=headers)
            if r.status_code == 200:
                return r.content.decode('utf-8', 'ignore')
            elif r.status_code == 429:
                process_too_many_r()
            elif r.status_code == 503:
                process_serv_unavail()
            else: process_other_err()

def get_description(page_text):
    """
    Функция для получения описания профиля пользователя
    """
    return page_text.split('"desc":')[1].split('"}')[0]

def get_real_num(stat):
    """
    Функция для преобразования чисел в размерности тысяч и миллионов
    в целые числа
    """
    if stat[-1] == 'k': stat = int(round(float(stat[:-1]) * 1_000))
    elif stat[-1] == 'm': stat = int(round(float(stat[:-1]) * 1_000_000))
    else: stat = int(round(float(stat)))
    return stat

def get_stats(description_text):
    """
    Функция для получения значений пользовательской статистики
    значения статистики получаются регулярным выражением, пробразуются в числа
    и возвращаются в виде словаря
    """
    numbers = re.findall(r'\d+\.?\d*[km]?|\d+', description_text)
    numbers = [get_real_num(num) for num in numbers]
    return {k:v for k,v in zip(['followers', 'subscriptions', 'likes'], numbers)}

def stats_2_db(stats, user_id, user_name, platform_id):
    pg_conn = get_pg_conn()
    sql_insert = f"""
        INSERT INTO user_stats (user_id, platform_id, followers, subscriptions, likes)
        VALUES ({user_id}, {platform_id}, {stats['followers']}, {stats['subscriptions']}, {stats['likes']})
        ON CONFLICT ON CONSTRAINT user_id_uniq_record_hour
            DO UPDATE SET
                followers = EXCLUDED.followers,
                subscriptions = EXCLUDED.subscriptions,
                likes = EXCLUDED.likes,
                record_ts = NOW();
    """
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(sql_insert)
            pg_conn.commit()
            logger.info(f'Статистика по пользователю {user_name} добавлена в БД')
    except Exception as e:
        logger.error(f'Ошибка при получении добавлении статистик по пользователю {user_name}')
        logger.error(e)
    finally:
        if pg_conn: pg_conn.close()

logger = get_app_logger('tiktok_parser')

def main(limit=None, offset=None):
    PLATFORM_NAME = 'TikTok'
    platform_id = get_platform_id(PLATFORM_NAME)
    user_names = get_user_names(limit, offset)
    for user_id, user_name in user_names.items():
        page_text = get_page_text(headers, user_name)
        description = get_description(page_text)
        stats = get_stats(description)
        logger.info(f'{user_id}, {user_name}, {stats}')
        stats_2_db(stats, user_id, user_name, platform_id)

if __name__ == "__main__":
    parsing_message = ''
    if len(sys.argv) == 1:
        parsing_message = 'Запускаю парсинг полного списка пользователей'
        logger.info(parsing_message)
        main()
    elif len(sys.argv) == 2:
        limit = sys.argv[1]
        parsing_message += f'Запускю парсинг {limit} пользователей'
        logger.info(parsing_message)
        main(limit)
    elif  len(sys.argv) == 3:
        limit = sys.argv[1]
        offset = sys.argv[2]
        parsing_message += f'Запускю парсинг {limit} пользователей, с отступом {offset}'
        logger.info(parsing_message)
        main(limit, offset)
    else:
        logger.error('Скрипт принимает максимум 2 аргумента: LIMIT и OFFSET')