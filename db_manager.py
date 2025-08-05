# db_manager.py
import psycopg2
# Удален неправильный импорт: from pip._internal import commands
from psycopg2 import sql
from psycopg2.extras import execute_values
import logging
import config  # Импортируем наш модуль config
import traceback

logger = logging.getLogger(__name__)


def get_db_connection():
    """Устанавливает соединение с базой данных PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            client_encoding='UTF8'  # <--- ДОБАВЬТЕ ЭТУ СТРОКУ
        )
        logger.info("Successfully connected to the PostgreSQL database with client_encoding=UTF8.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Error connecting to PostgreSQL database: {e}")
        raise


def create_tables_if_not_exist():
    """Создает таблицы в БД, если они еще не существуют."""
    # Вот недостающий кортеж с командами SQL
    commands_sql = (
        """
        CREATE TABLE IF NOT EXISTS metrika_traffic_sources (
            id SERIAL PRIMARY KEY,
            fetch_date DATE NOT NULL DEFAULT CURRENT_DATE,
            report_date DATE NOT NULL,
            source_group VARCHAR(255), -- e.g., 'Переходы из поисковых систем', 'Прямые заходы'
            source_engine VARCHAR(255), -- e.g., 'Яндекс', 'Google', 'Переходы по ссылкам на сайтах'
            source_detail VARCHAR(512), -- ym:s:lastSignSource
            visits INTEGER,
            users INTEGER,
            UNIQUE (report_date, source_group, source_engine, source_detail) -- уникальность для предотвращения дублей за день
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS metrika_conversions (
            id SERIAL PRIMARY KEY,
            fetch_date DATE NOT NULL DEFAULT CURRENT_DATE,
            report_date DATE NOT NULL,
            goal_id VARCHAR(255) NOT NULL,
            goal_name VARCHAR(255),
            source_engine VARCHAR(255), -- 'organic' for search engines, or specific source
            source_detail VARCHAR(512), -- ym:s:lastSignSource для детализации
            reaches INTEGER,
            conversion_rate REAL, -- FLOAT в SQL это REAL или DOUBLE PRECISION
            UNIQUE (report_date, goal_id, source_engine, source_detail)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS metrika_behavior (
            id SERIAL PRIMARY KEY,
            fetch_date DATE NOT NULL DEFAULT CURRENT_DATE,
            report_date DATE NOT NULL,
            bounces INTEGER,
            bounce_rate REAL,
            page_depth REAL,
            avg_visit_duration_seconds INTEGER,
            UNIQUE (report_date)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS topvisor_positions (
            id SERIAL PRIMARY KEY,
            fetch_date DATE NOT NULL DEFAULT CURRENT_DATE,
            report_date DATE NOT NULL,
            keyword TEXT NOT NULL,
            search_engine_name VARCHAR(100), -- "Yandex", "Google"
            search_engine_id INTEGER,
            region_name VARCHAR(255), -- Название региона, если сможем получить
            region_id INTEGER,
            position INTEGER,
            url TEXT,
            UNIQUE (report_date, keyword, search_engine_id, region_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS topvisor_visibility (
            id SERIAL PRIMARY KEY,
            fetch_date DATE NOT NULL DEFAULT CURRENT_DATE,
            report_date DATE NOT NULL,
            search_engine_name VARCHAR(100),
            search_engine_id INTEGER,
            region_name VARCHAR(255),
            region_id INTEGER,
            visibility_score REAL,
            UNIQUE (report_date, search_engine_id, region_id)
        );
        """
        # TODO: Добавить таблицы для Yandex Webmaster (ИКС, индексация), если будем использовать
    )
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Используем commands_sql, который мы определили выше
        for command_idx, command in enumerate(commands_sql):
            try:
                logger.debug(f"Executing SQL command #{command_idx + 1}:\n{command}")
                cur.execute(command)
            except (Exception, psycopg2.Error) as sql_error:  # Catch error per command
                logger.error(f"SQL Error occurred with command #{command_idx + 1}:")
                logger.error(f"Failed SQL command:\n{command}")

                logger.error(f"Raw Python error object: {repr(sql_error)}")

                if hasattr(sql_error, 'pgcode'):
                    logger.error(f"PostgreSQL error code (pgcode): {sql_error.pgcode}")
                if hasattr(sql_error, 'pgerror') and sql_error.pgerror:
                    raw_pg_message = sql_error.pgerror
                    decoded_message = ""
                    try:
                        # psycopg2.Error.pgerror can be bytes or str depending on version and context
                        if isinstance(raw_pg_message, bytes):
                            decoded_message = raw_pg_message.decode('utf-8')
                        else:
                            decoded_message = raw_pg_message  # Assume already str
                        logger.error(f"PostgreSQL error message (pgerror, decoded as UTF-8): {decoded_message}")
                    except UnicodeDecodeError:
                        try:
                            if isinstance(raw_pg_message, bytes):
                                decoded_message = raw_pg_message.decode('cp1251', errors='replace')
                                logger.error(
                                    f"PostgreSQL error message (pgerror, decoded as CP1251 with replace): {decoded_message}")
                            else:  # If it was already a string but not UTF-8, this path is unlikely to be hit right
                                logger.error(
                                    f"PostgreSQL error message (pgerror, was string, attempt CP1251 interpretation - unusual): {raw_pg_message.encode('latin1').decode('cp1251', errors='replace')}")

                        except Exception as decode_e:
                            logger.error(
                                f"PostgreSQL error message (pgerror, could not decode, raw value): {raw_pg_message!r}. Decode error: {decode_e}")
                    except AttributeError:
                        logger.error(f"PostgreSQL error message (pgerror, likely already string): {sql_error.pgerror}")

                logger.error(f"Full traceback for the SQL error:\n{traceback.format_exc()}")
                if conn:
                    conn.rollback()
                return

        cur.close()
        conn.commit()
        logger.info("Tables checked/created successfully.")
    except (Exception, psycopg2.Error) as error:
        logger.error(
            f"An unexpected error occurred during table creation process (not specific SQL command): {repr(error)}")
        logger.error(f"Full traceback for unexpected error:\n{traceback.format_exc()}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def bulk_insert_data(table_name, columns, data_tuples):
    """
    Выполняет массовую вставку данных в указанную таблицу.
    :param table_name: Имя таблицы.
    :param columns: Список названий колонок.
    :param data_tuples: Список кортежей с данными для вставки.
    """
    if not data_tuples:
        logger.info(f"No data to insert into {table_name}.")
        return

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        conflict_columns_map = {
            "metrika_traffic_sources": "(report_date, source_group, source_engine, source_detail)",
            "metrika_conversions": "(report_date, goal_id, source_engine, source_detail)",
            "metrika_behavior": "(report_date)",
            "topvisor_positions": "(report_date, keyword, search_engine_id, region_id)",
            "topvisor_visibility": "(report_date, search_engine_id, region_id)"
        }

        conflict_clause = ""
        if table_name in conflict_columns_map:
            # Создаем список полей для DO UPDATE SET, исключая сами поля конфликта, если они есть в columns
            # Это более продвинутый вариант, пока оставим DO NOTHING для простоты
            # update_columns = [col for col in columns if col not in conflict_columns_map[table_name]]
            # if update_columns:
            #    set_clause = ", ".join([f"{sql.Identifier(col).string} = EXCLUDED.{sql.Identifier(col).string}" for col in update_columns])
            #    conflict_clause = f"ON CONFLICT {conflict_columns_map[table_name]} DO UPDATE SET {set_clause}"
            # else:
            conflict_clause = f"ON CONFLICT {conflict_columns_map[table_name]} DO NOTHING"

        # Формируем SQL-запрос с использованием sql.SQL для безопасной вставки имен таблиц и колонок
        cols_sql = sql.SQL(', ').join(map(sql.Identifier, columns))
        query_template_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s {}").format(
            sql.Identifier(table_name),
            cols_sql,
            sql.SQL(conflict_clause)  # Вставляем ON CONFLICT ... как есть (уже строка)
        )

        # psycopg2.extras.execute_values ожидает строку запроса
        execute_values(cur, query_template_sql.as_string(cur), data_tuples, page_size=100)

        conn.commit()
        logger.info(f"Successfully inserted {len(data_tuples)} rows into {table_name}.")
    except (Exception, psycopg2.Error) as error:
        logger.error(
            f"Error during bulk insert into {table_name}: {repr(error)}")  # Используем repr(error) для безопасности
        logger.error(f"Full traceback for bulk insert error:\n{traceback.format_exc()}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            # cur.close() должен быть перед conn.close() и только если cur был успешно создан
            if 'cur' in locals() and cur:
                cur.close()
            conn.close()


if __name__ == '__main__':
    logging.basicConfig(
        level=config.LOG_LEVEL.upper(),  # Используем уровень из конфига
        format='%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Вывод в консоль
            # logging.FileHandler(config.LOG_FILE, encoding='utf-8') # Вывод в файл, если нужно
        ]
    )
    # Создаем папку для логов, если ее нет и если LOG_FILE указан
    # if config.LOG_FILE:
    #    log_dir = os.path.dirname(config.LOG_FILE)
    #    if log_dir and not os.path.exists(log_dir):
    #        os.makedirs(log_dir)
    #    # Перенастраиваем FileHandler с путем из конфига
    #    file_handler = logging.FileHandler(config.LOG_FILE, encoding='utf-8')
    #    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s'))
    #    logging.getLogger().addHandler(file_handler)

    try:
        config.check_config()
    except EnvironmentError as e:
        logger.error(f"Failed to load configuration for db_manager test: {e}")
        exit(1)

    create_tables_if_not_exist()

    # Пример тестовой вставки (раскомментируйте и адаптируйте для проверки)
    # logger.info("Starting test bulk insert...")
    # test_traffic_data = [
    #    ('2023-10-26', 'Поисковые системы', 'Яндекс', 'yandex.ru/search', 100, 80),
    #    ('2023-10-26', 'Поисковые системы', 'Google', 'google.com/organic', 150, 120),
    # ]
    # traffic_columns = ['report_date', 'source_group', 'source_engine', 'source_detail', 'visits', 'users']
    # bulk_insert_data('metrika_traffic_sources', traffic_columns, test_traffic_data)

    # test_behavior_data = [
    #    ('2023-10-26', 10, 0.15, 3.5, 180),
    # ]
    # behavior_columns = ['report_date', 'bounces', 'bounce_rate', 'page_depth', 'avg_visit_duration_seconds']
    # bulk_insert_data('metrika_behavior', behavior_columns, test_behavior_data)
    # logger.info("Test bulk insert finished.")

    logger.info("db_manager.py test script finished.")