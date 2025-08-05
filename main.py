# main.py (ФИНАЛЬНАЯ ВЕРСИЯ С ПЛАНИРОВЩИКОМ)
import logging
import time
from datetime import date, timedelta
import schedule  # Импортируем библиотеку для планирования

import config
import db_manager
import metrika_api
import topvisor_api

# (Настройка логирования остается без изменений)
log_level = config.LOG_LEVEL.upper() if config.LOG_LEVEL else 'INFO'
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Все функции fetch_and_store_* остаются без изменений

def fetch_and_store_all_traffic_sources(date_from, date_to):
    """Получает данные по всем источникам трафика из Яндекс.Метрики и сохраняет их в БД."""
    logger.info(f"Starting to fetch all traffic sources data from {date_from} to {date_to}.")
    sources_data_list_of_dicts = metrika_api.get_traffic_sources_summary(date_from, date_to)

    if not sources_data_list_of_dicts:
        logger.warning(f"No data for all traffic sources received from Metrika API for period {date_from} - {date_to}.")
        return

    columns_for_db = ['report_date', 'source_group', 'source_engine', 'source_detail', 'visits', 'users']
    data_to_insert_tuples = [tuple(d.get(col) for col in columns_for_db) for d in sources_data_list_of_dicts]

    if not data_to_insert_tuples:
        logger.info("No data (all_traffic) to insert into database after transformation.")
        return

    logger.info(
        f"Attempting to insert {len(data_to_insert_tuples)} records (all_traffic) into metrika_traffic_sources.")
    db_manager.bulk_insert_data('metrika_traffic_sources', columns_for_db, data_to_insert_tuples)
    logger.info(f"Finished fetching and storing all traffic sources data for {date_from} - {date_to}.")


def fetch_and_store_behavior_data(date_from, date_to):
    """Получает сводные поведенческие данные из Яндекс.Метрики и сохраняет их в БД."""
    logger.info(f"Starting to fetch behavior summary data from {date_from} to {date_to}.")
    behavior_data_list_of_dicts = metrika_api.get_behavior_summary(date_from, date_to)

    if not behavior_data_list_of_dicts:
        logger.warning(f"No behavior summary data received from Metrika API for period {date_from} - {date_to}.")
        return

    columns_for_db = ['report_date', 'bounces', 'bounce_rate', 'page_depth', 'avg_visit_duration_seconds']
    data_to_insert_tuples = [tuple(d.get(col) for col in columns_for_db) for d in behavior_data_list_of_dicts]

    if not data_to_insert_tuples:
        logger.info("No data (behavior) to insert into database after transformation.")
        return

    logger.info(f"Attempting to insert {len(data_to_insert_tuples)} records (behavior) into metrika_behavior.")
    db_manager.bulk_insert_data('metrika_behavior', columns_for_db, data_to_insert_tuples)
    logger.info(f"Finished fetching and storing behavior summary data for {date_from} - {date_to}.")


def fetch_and_store_conversions_data(date_from, date_to):
    """Получает данные по конверсиям из Яндекс.Метрики и сохраняет их в БД."""
    logger.info(f"Starting to fetch conversions data from {date_from} to {date_to}.")
    conversions_data_list_of_dicts = metrika_api.get_conversions_data(date_from, date_to)

    if not conversions_data_list_of_dicts:
        logger.warning(f"No conversions data received from Metrika API for period {date_from} - {date_to}.")
        return

    columns_for_db = ['report_date', 'goal_id', 'goal_name', 'source_engine', 'source_detail', 'reaches',
                      'conversion_rate']
    data_to_insert_tuples = [tuple(d.get(col) for col in columns_for_db) for d in conversions_data_list_of_dicts]

    if not data_to_insert_tuples:
        logger.info("No data (conversions) to insert into database after transformation.")
        return

    logger.info(f"Attempting to insert {len(data_to_insert_tuples)} records (conversions) into metrika_conversions.")
    db_manager.bulk_insert_data('metrika_conversions', columns_for_db, data_to_insert_tuples)
    logger.info(f"Finished fetching and storing conversions data for {date_from} - {date_to}.")


def fetch_and_store_topvisor_positions(date_from, date_to):
    """Получает историю позиций из Топвизора и сохраняет их в БД."""
    logger.info(f"Starting to fetch Topvisor positions from {date_from} to {date_to}.")
    positions_data_list_of_dicts = topvisor_api.get_positions_history(
        date_from_str=date_from,
        date_to_str=date_to,
        project_id=config.TOPVISOR_PROJECT_ID,
        region_indexes=config.TOPVISOR_REGION_INDEXES,
        searcher_ids=config.TOPVISOR_SEARCHERS
    )

    if not positions_data_list_of_dicts:
        logger.warning(f"No positions data received from Topvisor API for period {date_from} - {date_to}.")
        return

    columns_for_db = ['report_date', 'keyword', 'search_engine_name', 'search_engine_id', 'region_id', 'position', 'url']
    data_to_insert_tuples = [tuple(d.get(col) for col in columns_for_db) for d in positions_data_list_of_dicts]

    if not data_to_insert_tuples:
        logger.info("No data (positions) to insert into database after transformation.")
        return

    logger.info(f"Attempting to insert {len(data_to_insert_tuples)} records (positions) into topvisor_positions.")
    db_manager.bulk_insert_data('topvisor_positions', columns_for_db, data_to_insert_tuples)
    logger.info(f"Finished fetching and storing Topvisor positions data for {date_from} - {date_to}.")


def fetch_and_store_topvisor_visibility(date_from, date_to):
    """Получает историю видимости из Топвизора и сохраняет ее в БД."""
    logger.info(f"Starting to fetch Topvisor visibility from {date_from} to {date_to}.")
    visibility_data_list_of_dicts = topvisor_api.get_visibility_summary(
        date_from_str=date_from,
        date_to_str=date_to,
        project_id=config.TOPVISOR_PROJECT_ID,
        region_indexes=config.TOPVISOR_REGION_INDEXES,
        searcher_ids=config.TOPVISOR_SEARCHERS
    )

    if not visibility_data_list_of_dicts:
        logger.warning(f"No visibility data received from Topvisor API for period {date_from} - {date_to}.")
        return

    # Важно! В get_visibility_summary нет region_name, поэтому исключаем его
    columns_for_db = ['report_date', 'search_engine_name', 'search_engine_id', 'region_id', 'visibility_score']
    data_to_insert_tuples = [tuple(d.get(col) for col in columns_for_db) for d in visibility_data_list_of_dicts]

    if not data_to_insert_tuples:
        logger.info("No data (visibility) to insert into database after transformation.")
        return

    logger.info(f"Attempting to insert {len(data_to_insert_tuples)} records (visibility) into topvisor_visibility.")
    db_manager.bulk_insert_data('topvisor_visibility', columns_for_db, data_to_insert_tuples)
    logger.info(f"Finished fetching and storing Topvisor visibility data for {date_from} - {date_to}.")


# ================== НОВЫЙ БЛОК: ФУНКЦИЯ-ЗАДАЧА ДЛЯ ПЛАНИРОВЩИКА ==================
def run_daily_job():
    """
    Основная задача, которая запускается планировщиком.
    Собирает данные за "вчера".
    """
    logger.info("================== Starting scheduled daily job ==================")
    try:
        # Устанавливаем даты для сбора данных за "вчера"
        yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        date_from = yesterday
        date_to = yesterday

        logger.info(f"Data will be fetched for the date: {date_from}")

        # --- Секция Метрики ---
        if config.METRIKA_TOKEN and config.METRIKA_COUNTER_ID:
            fetch_and_store_all_traffic_sources(date_from, date_to)
            fetch_and_store_behavior_data(date_from, date_to)
            fetch_and_store_conversions_data(date_from, date_to)
            logger.info("Metrika data fetching section finished.")
        else:
            logger.warning("Metrika API token or counter ID not configured. Skipping Metrika data.")

        # --- Секция Топвизора ---
        if config.TOPVISOR_API_KEY and config.TOPVISOR_PROJECT_ID:
            fetch_and_store_topvisor_positions(date_from, date_to)
            fetch_and_store_topvisor_visibility(date_from, date_to)
            logger.info("Topvisor data fetching section finished.")
        else:
            logger.warning("Topvisor configuration is incomplete. Skipping Topvisor data.")

    except Exception as e:
        logger.error(f"An error occurred during the daily job: {e}", exc_info=True)

    logger.info("================== Scheduled daily job finished ==================")



def run_historical_load(days_to_load):
    """
    Выполняет разовую загрузку данных за указанное количество прошедших дней.
    """
    logger.info(f"================== Starting HISTORICAL data load for the last {days_to_load} days ==================")
    try:
        # Устанавливаем даты для сбора исторических данных
        today = date.today()
        # Данные всегда доступны до "вчера" включительно
        date_to = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        date_from = (today - timedelta(days=days_to_load)).strftime('%Y-%m-%d')

        logger.info(f"Historical data will be fetched for the period: {date_from} to {date_to}")

        # --- Секция Метрики ---
        if config.METRIKA_TOKEN and config.METRIKA_COUNTER_ID:
            fetch_and_store_all_traffic_sources(date_from, date_to)
            fetch_and_store_behavior_data(date_from, date_to)
            fetch_and_store_conversions_data(date_from, date_to)
        else:
            logger.warning("Metrika configuration is incomplete. Skipping Metrika historical load.")

        # --- Секция Топвизора ---
        if config.TOPVISOR_API_KEY and config.TOPVISOR_PROJECT_ID:
            fetch_and_store_topvisor_positions(date_from, date_to)
            fetch_and_store_topvisor_visibility(date_from, date_to)
        else:
            logger.warning("Topvisor configuration is incomplete. Skipping Topvisor historical load.")

    except Exception as e:
        logger.error(f"An error occurred during the historical data load: {e}", exc_info=True)

    logger.info("================== HISTORICAL data load finished ==================")

# ================== ОБНОВЛЕННЫЙ БЛОК: ОСНОВНАЯ ЛОГИКА ЗАПУСКА И ПЛАНИРОВАНИЯ ==================
if __name__ == '__main__':
    logger.info("Script started as a service.")

    try:
        config.check_config()
    except EnvironmentError as e:
        logger.error(f"Configuration check failed: {e}. Aborting.")
        exit(1)

    logger.info("Checking and creating database tables if they don't exist...")
    db_manager.create_tables_if_not_exist()

    # --- ШАГ 1: ИСТОРИЧЕСКАЯ ЗАГРУЗКА ---
    # Выполняем загрузку данных за последние 30 дней.
    run_historical_load(days_to_load=180)

    # --- ШАГ 2: ЗАГРУЗКА ЗА ВЧЕРА И ЗАПУСК ПЛАНИРОВЩИКА ---
    # Запускаем ежедневную задачу сразу, чтобы гарантировать наличие самых свежих (вчерашних) данных
    logger.info("Running daily job for yesterday to ensure the latest data is present...")
    run_daily_job()

    # Настраиваем расписание на будущее
    schedule.every().day.at("03:00").do(run_daily_job)
    logger.info(f"Job scheduled to run every day at 03:00. Next run is at: {schedule.next_run}")

    # Основной цикл, который поддерживает работу скрипта
    while True:
        schedule.run_pending()
        time.sleep(60)