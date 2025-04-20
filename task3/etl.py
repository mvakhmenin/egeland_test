import asyncio
import asyncpg
from io import BytesIO
import csv
import os
import sys
import structlog
from structlog.dev import ConsoleRenderer
from structlog.processors import JSONRenderer
import logging
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

dict_table_names = ['user', 'source', 'subject', 'course', 'package']

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

async def get_pg_conn():
    """
    Функция возвращает соединение к БД PostgreSQL
    """
    host = os.environ['HOST']
    port = os.environ['PORT']
    user = os.environ['DB_USER']
    password = os.environ['PASS']
    database = os.environ['DBNAME']
    return await asyncpg.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )

async def get_pg_conn_pool():
    """
    Функция возвращает пул соединений к БД PostgreSQL
    """
    host = os.environ['HOST']
    port = os.environ['PORT']
    user = os.environ['DB_USER']
    password = os.environ['PASS']
    database = os.environ['DBNAME']
    return await asyncpg.create_pool(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port,
            min_size=5,
            max_size=20
        )

async def create_tables():
    """
    Функция создает таблицы для загрузки данных
    """
    pg_conn = await get_pg_conn()
    try:
        await pg_conn.execute("DROP TABLE IF EXISTS orders")
        for dict_table_name in dict_table_names:
            await pg_conn.execute(f"DROP TABLE IF EXISTS {dict_table_name}s")
            await pg_conn.execute(f"""
                    CREATE TABLE {dict_table_name}s (
                        {dict_table_name}_id SERIAL PRIMARY KEY,
                        {dict_table_name}_name TEXT NOT NULL UNIQUE,
                        create_ts TIMESTAMP DEFAULT now()
                    )
                """)
        await pg_conn.execute("""
                CREATE TABLE orders (
                    order_id SERIAL PRIMARY KEY,
                    user_id INT NOT NULL,
                    source_id INT NOT NULL,
                    order_ts TIMESTAMP NOT NULL,
                    order_amount NUMERIC(10,2) NOT NULL,
                    subject_id INT NOT NULL,
                    course_id INT NOT NULL,
                    package_id INT NOT NULL
                )
            """)
    except Exception as e:
        logger.error(f'Ошибка при создании таблиц. \n {e}')
    finally:
        await pg_conn.close()

async def create_fkeys():
    """
    Функция создает внешние ключи для таблицы orders
    для контроля целостности данных
    """
    pg_conn = await get_pg_conn()
    try:
        for dict_table_name in dict_table_names:
            await pg_conn.execute(f"""
            ALTER TABLE orders
            ADD CONSTRAINT fk_{dict_table_name}
            FOREIGN KEY ({dict_table_name}_id) 
            REFERENCES {dict_table_name}s({dict_table_name}_id);
            """)
    except Exception as e:
        logger.error(f'Ошибка при создании внешний ключей для таблиц. \n {e}')
    finally:
        await pg_conn.close()

async def process_csv_batch(batch,
                    users_cache,
                    sources_cache,
                    subjects_cache,
                    courses_cache,
                    packages_cache,
                    pool):
    """
    Функция нормализует данные, добавляет данные в справочники
    и записывает нормализованные данные в таблицу с заказами
    через COPY, а не INSERT
    """
    async with pool.acquire() as conn:
        orders_data = BytesIO()
        for row in batch:
            user_name = row['user']
            if user_name not in users_cache:
                user_id = await conn.fetchval(
                    "INSERT INTO users (user_name) VALUES ($1) RETURNING user_id",
                    user_name
                )
                users_cache[user_name] = user_id
            source_name = row['source']
            if source_name not in sources_cache:
                source_id = await conn.fetchval(
                    "INSERT INTO sources (source_name) VALUES ($1) RETURNING source_id",
                    source_name
                )
                sources_cache[source_name] = source_id
            subject_name = row['subject']
            if subject_name not in subjects_cache:
                subject_id = await conn.fetchval(
                    "INSERT INTO subjects (subject_name) VALUES ($1) RETURNING subject_id",
                    subject_name
                )
                subjects_cache[subject_name] = subject_id
            course_name = row['course']
            if course_name not in courses_cache:
                course_id = await conn.fetchval(
                    "INSERT INTO courses (course_name) VALUES ($1) RETURNING course_id",
                    course_name
                )
                courses_cache[course_name] = course_id
            package_name = row['package']
            if package_name not in packages_cache:
                package_id = await conn.fetchval(
                    "INSERT INTO packages (package_name) VALUES ($1) RETURNING package_id",
                    package_name
                )
                packages_cache[package_name] = package_id
            order_datetime = row['datetime'] if row['datetime']!='' else '1970-01-01 00:00:00'
            order_amount = row['amount'] if row['amount']!='' else 0
            try:
                order_amount = order_amount.replace(',','.')
                order_amount = order_amount.replace(' ','')
                order_amount = order_amount.replace(' ','')
            except:
                pass

            orders_data.write(
                f"{users_cache[user_name]}\t{sources_cache[source_name]}\t{order_datetime}\t{order_amount}\t{subjects_cache[subject_name]}\t{courses_cache[course_name]}\t{packages_cache[package_name]}\n".encode('utf-8')
            )
        orders_data.seek(0)
        await conn.copy_to_table(
            'orders',
            source=orders_data,
            format='csv',
            delimiter='\t',
            columns=('user_id', 'source_id', 'order_ts','order_amount','subject_id','course_id','package_id')
        )

logger = get_app_logger('tiktok_parser')

async def main(data_file):
    await create_tables()
    BATCH_SIZE = 1_000
    users_cache = {}
    sources_cache = {}
    subjects_cache = {}
    courses_cache = {}
    packages_cache = {}
    pool = await get_pg_conn_pool()
    with open(data_file, 'r') as f:
        reader = csv.DictReader(f)
        batch = []
        for i, row in enumerate(reader, 1):
            batch.append(row)
            if i % BATCH_SIZE == 0:
                logger.info(f"Обработка батча {i // BATCH_SIZE} ({i} строк)...")
                await process_csv_batch(batch, users_cache, sources_cache, subjects_cache, courses_cache, packages_cache, pool)
                batch = []
        if batch:
                logger.info("Обработка последнего батча...")
                await process_csv_batch(batch, users_cache, sources_cache, subjects_cache, courses_cache, packages_cache, pool)
    logger.info("Создание внешних клиючей")
    await create_fkeys() # Создаем внешнике ключи только после окончания загрузки

if __name__ == "__main__":
    if len(sys.argv)==1:
        logger.error('Необходимо указать имя файла для загрузки')
    elif len(sys.argv)==2:
        data_file = sys.argv[1]
        logger.info(f'Начинаю загрузку файла {data_file}')
        asyncio.run(main(data_file))
    else:
        logger.error('Скрипт принимает только один параметр - имя файла для загрузки')