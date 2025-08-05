# metrika_api.py
import requests
import logging
from datetime import date, timedelta, datetime
import time
import config  # Наш модуль конфигурации

logger = logging.getLogger(__name__)

# Константы для API Метрики
METRIKA_API_URL = config.METRIKA_API_URL
TOKEN = config.METRIKA_TOKEN
COUNTER_ID = config.METRIKA_COUNTER_ID


def get_metrika_data(metrics, dimensions, date1, date2, filters=None, sort=None, limit=10000, offset=1):
    """
    Универсальная функция для запроса данных из API Яндекс.Метрики.
    """
    if not TOKEN or not COUNTER_ID:
        logger.error("Metrika API Token or Counter ID is not configured.")
        return None

    headers = {
        'Authorization': f'OAuth {TOKEN}',
        'Content-Type': 'application/json'
    }
    params = {
        'ids': COUNTER_ID,
        'metrics': metrics,
        'dimensions': dimensions,
        'date1': date1,  # YYYY-MM-DD
        'date2': date2,  # YYYY-MM-DD
        'limit': limit,
        'offset': offset,
        'accuracy': 'full'  # для получения несемплированных данных, если возможно
    }
    if filters:
        params['filters'] = filters
    if sort:
        params['sort'] = sort

    all_data = []
    current_offset = offset

    while True:
        params['offset'] = current_offset
        logger.debug(f"Requesting Metrika API with params: {params}")
        try:
            response = requests.get(METRIKA_API_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()  # Вызовет исключение для HTTP-ошибок 4xx/5xx

            response_data = response.json()
            logger.debug(f"Metrika API response (sample): {str(response_data)[:500]}")

            if 'data' in response_data and response_data['data']:
                all_data.extend(response_data['data'])

                total_rows = response_data.get('total_rows', 0)

                if not all_data or total_rows == 0:  # Если total_rows 0, или данных нет
                    break

                if len(all_data) >= total_rows:
                    break

                if len(response_data['data']) < limit:
                    break
                current_offset += limit

            else:  # Нет данных или data пустой
                logger.info(f"No data found in Metrika response for current page (offset {current_offset}).")
                break

            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error requesting Metrika API: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_details = e.response.json()
                    logger.error(f"Metrika API error details: {error_details}")
                except ValueError:
                    logger.error(f"Metrika API error response content: {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred during Metrika API request: {e}")
            return None

    logger.info(
        f"Successfully fetched {len(all_data)} rows from Metrika API for metrics='{metrics}', dimensions='{dimensions}' between {date1} and {date2}.")
    return all_data


def get_search_engine_traffic(date_from, date_to):
    """
    Получает данные о трафике из поисковых систем.
    Разделяет на Яндекс, Google и "Другие поисковые системы".
    """
    metrics = 'ym:s:visits,ym:s:users'
    dimensions = 'ym:s:date,ym:s:lastSearchEngine'
    filters = "ym:s:lastSignTrafficSource=='organic'"

    raw_data = get_metrika_data(metrics, dimensions, date_from, date_to, filters=filters, sort='ym:s:date')

    if raw_data is None:
        return []

    processed_data = []
    for item_idx, item in enumerate(raw_data):
        try:
            record_date_str = item['dimensions'][0].get('name') if item['dimensions'] and item['dimensions'][
                0] else None

            search_engine_name = "Не определено"
            if len(item['dimensions']) > 1 and item['dimensions'][1] and item['dimensions'][1].get('name'):
                search_engine_name = item['dimensions'][1]['name']
            elif len(item['dimensions']) > 1 and item['dimensions'][1] and item['dimensions'][1].get('name') is None:
                logger.warning(
                    f"Item #{item_idx} (search_engine) has explicit None for search_engine_name for date {record_date_str}. Using default. Item: {item}")

            if not record_date_str:
                logger.warning(f"Skipping item #{item_idx} (search_engine) due to missing date: {item}")
                continue

            visits = int(item['metrics'][0])
            users = int(item['metrics'][1])

            source_group = "Переходы из поисковых систем"
            specific_engine = search_engine_name
            general_engine_category = "Другие поисковые системы"

            normalized_search_engine_name = search_engine_name.lower()
            if 'яндекс' in normalized_search_engine_name or 'yandex' in normalized_search_engine_name:
                general_engine_category = "Яндекс"
            elif 'google' in normalized_search_engine_name:
                general_engine_category = "Google"
            elif 'mail.ru' in normalized_search_engine_name or 'go.mail.ru' in normalized_search_engine_name:
                general_engine_category = "Mail.ru"
            elif 'bing' in normalized_search_engine_name:
                general_engine_category = "Bing"
            elif 'duckduckgo' in normalized_search_engine_name:
                general_engine_category = "DuckDuckGo"

            processed_data.append({
                'report_date': record_date_str,
                'source_group': source_group,
                'source_engine': general_engine_category,
                'source_detail': specific_engine,
                'visits': visits,
                'users': users
            })
        except (IndexError, KeyError, TypeError) as e:
            logger.error(f"Error processing item #{item_idx} (search_engine): {item}. Error: {e}. Skipping.")
            continue

    logger.info(f"Processed {len(processed_data)} records for search engine traffic.")
    return processed_data


def get_traffic_sources_summary(date_from, date_to):
    """
    Получает данные о трафике по всем источникам.
    ym:s:lastTrafficSource - тип источника (рекламные системы, поисковые системы, социальные сети и т.д.)
    ym:s:lastSourceEngine - детализация источника (например, имя ПС, домен сайта, название соцсети)
    """
    metrics = 'ym:s:visits,ym:s:users'
    dimensions = 'ym:s:date,ym:s:lastTrafficSource,ym:s:lastSourceEngine'
    sort_by = 'ym:s:date,ym:s:lastTrafficSource,ym:s:lastSourceEngine'

    raw_data = get_metrika_data(
        metrics=metrics,
        dimensions=dimensions,
        date1=date_from,
        date2=date_to,
        filters=None,
        sort=sort_by
    )

    if raw_data is None:
        return []

    processed_data = []
    for item_idx, item in enumerate(raw_data):
        try:
            record_date_str = item['dimensions'][0].get('name') if item['dimensions'] and item['dimensions'][
                0] else None

            traffic_source_type = "Не определено"  # ym:s:lastTrafficSource
            if len(item['dimensions']) > 1 and item['dimensions'][1] and item['dimensions'][1].get('name'):
                traffic_source_type = item['dimensions'][1]['name']

            source_engine_detail_name = "Не определено"
            if len(item['dimensions']) > 2 and item['dimensions'][2] and item['dimensions'][2].get('name'):
                source_engine_detail_name = item['dimensions'][2]['name']
            elif len(item['dimensions']) > 2 and item['dimensions'][2] and item['dimensions'][2].get('name') is None:
                pass

            if not record_date_str:
                logger.warning(f"Skipping item #{item_idx} (all_sources) due to missing date: {item}")
                continue

            visits = int(item['metrics'][0])
            users = int(item['metrics'][1])

            current_source_group = "Прочие источники"
            current_source_engine = traffic_source_type
            current_source_detail = source_engine_detail_name

            normalized_api_traffic_type = traffic_source_type.lower()

            if normalized_api_traffic_type == 'organic traffic' or normalized_api_traffic_type == 'search engine traffic':  # ### ИЗМЕНЕНО ###
                current_source_group = "Переходы из поисковых систем"
                normalized_engine_detail = source_engine_detail_name.lower()
                if 'яндекс' in normalized_engine_detail or 'yandex' in normalized_engine_detail:
                    current_source_engine = "Яндекс"
                elif 'google' in normalized_engine_detail:
                    current_source_engine = "Google"
                elif 'mail.ru' in normalized_engine_detail or 'go.mail.ru' in normalized_engine_detail:
                    current_source_engine = "Mail.ru"
                elif 'bing' in normalized_engine_detail:
                    current_source_engine = "Bing"
                elif 'duckduckgo' in normalized_engine_detail:
                    current_source_engine = "DuckDuckGo"
                else:
                    current_source_engine = source_engine_detail_name if source_engine_detail_name != "Не определено" else "Другие поисковые системы"

            elif normalized_api_traffic_type == 'direct traffic':
                current_source_group = "Прямые заходы"
                current_source_engine = "Прямые заходы"
                if current_source_detail == "Не определено" or current_source_detail.lower() == "(none)":
                    current_source_detail = "Прямой заход"

            elif normalized_api_traffic_type == 'link traffic':
                current_source_group = "Переходы по ссылкам на сайтах"
                current_source_engine = "Сайты-источники"

            elif normalized_api_traffic_type == 'social network traffic':
                current_source_group = "Переходы из социальных сетей"
                current_source_engine = "Социальные сети"

            elif normalized_api_traffic_type == 'ad traffic':
                current_source_group = "Переходы по рекламе"
                current_source_engine = "Рекламные системы"

            elif normalized_api_traffic_type == 'internal traffic':
                current_source_group = "Внутренние переходы"
                current_source_engine = "Внутренние переходы"

            elif normalized_api_traffic_type == 'recommendation system traffic':
                current_source_group = "Переходы из рекомендательных систем"
                current_source_engine = "Рекомендательные системы"

            elif normalized_api_traffic_type == 'messenger traffic':
                current_source_group = "Переходы из мессенджеров"
                current_source_engine = "Мессенджеры"

            elif normalized_api_traffic_type == 'saved page traffic':
                current_source_group = "Переходы с сохраненных страниц"
                current_source_engine = "Сохраненные страницы"

            elif traffic_source_type == "Не определено":
                current_source_group = "Прочие источники (тип не определен)"
                current_source_engine = "Не определено"
            elif current_source_group == "Прочие источники":  # Если не подошло ни под одно правило выше
                current_source_engine = traffic_source_type if traffic_source_type != "Не определено" else "Прочие источники"

            processed_data.append({
                'report_date': record_date_str,
                'source_group': current_source_group,
                'source_engine': current_source_engine,
                'source_detail': current_source_detail,
                'visits': visits,
                'users': users
            })
        except (IndexError, KeyError, TypeError) as e:
            logger.error(f"Error processing item #{item_idx} (all_sources): {item}. Error: {e}. Skipping.")
            continue

    logger.info(f"Processed {len(processed_data)} records for all traffic sources.")
    return processed_data


def get_behavior_summary(date_from, date_to):
    """
    Получает сводные данные по поведению пользователей на сайте:
    - Отказы (визиты и показатель)
    - Глубина просмотра
    - Среднее время на сайте
    Данные агрегируются по дням для всего сайта.
    """
    metrics = 'ym:s:bounces,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds'
    dimensions = 'ym:s:date'  # Агрегируем по дням
    sort_by = 'ym:s:date'

    raw_data = get_metrika_data(
        metrics=metrics,
        dimensions=dimensions,
        date1=date_from,
        date2=date_to,
        filters=None,
        sort=sort_by
    )

    if raw_data is None:
        return []

    processed_data = []
    for item_idx, item in enumerate(raw_data):
        try:
            record_date_str = item['dimensions'][0].get('name') if item['dimensions'] and item['dimensions'][
                0] else None

            if not record_date_str:
                logger.warning(f"Skipping item #{item_idx} (behavior) due to missing date: {item}")
                continue

            bounces = int(item['metrics'][0]) if item['metrics'][0] is not None else 0
            bounce_rate = float(item['metrics'][1]) if item['metrics'][1] is not None else 0.0
            page_depth = float(item['metrics'][2]) if item['metrics'][2] is not None else 0.0
            avg_visit_duration_seconds = int(item['metrics'][3]) if item['metrics'][3] is not None else 0

            processed_data.append({
                'report_date': record_date_str,
                'bounces': bounces,
                'bounce_rate': bounce_rate,
                'page_depth': page_depth,
                'avg_visit_duration_seconds': avg_visit_duration_seconds
            })
        except (IndexError, KeyError, TypeError, ValueError) as e:
            logger.error(f"Error processing item #{item_idx} (behavior): {item}. Error: {e}. Skipping.")
            continue

    logger.info(f"Processed {len(processed_data)} records for behavior summary.")
    return processed_data


if __name__ == '__main__':
    logging.basicConfig(
        level=config.LOG_LEVEL.upper(),
        format='%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
        handlers=[logging.StreamHandler()]
    )

    try:
        config.check_config()
        if not config.METRIKA_TOKEN or not config.METRIKA_COUNTER_ID:
            logger.error("Metrika Token or Counter ID is not set in .env file. Aborting test.")
            exit()
    except EnvironmentError as e:
        logger.error(f"Failed to load configuration for metrika_api test: {e}")
        exit(1)

    test_date_to = date.today().strftime('%Y-%m-%d')
    test_date_from = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')  # За вчера

    logger.info(f"--- Testing get_search_engine_traffic from {test_date_from} to {test_date_to} ---")
    search_traffic = get_search_engine_traffic(test_date_from, test_date_to)
    if search_traffic:
        logger.info(
            f"Received {len(search_traffic)} search engine traffic entries. Sample: {search_traffic[0] if search_traffic else 'N/A'}")

    logger.info(f"--- Testing get_traffic_sources_summary from {test_date_from} to {test_date_to} ---")
    all_sources_traffic = get_traffic_sources_summary(test_date_from, test_date_to)
    if all_sources_traffic:
        logger.info(f"Received {len(all_sources_traffic)} all sources traffic entries. Samples:")
        for i, entry in enumerate(all_sources_traffic):
            if i < 2:
                logger.info(entry)
            else:
                logger.info(f"... and {len(all_sources_traffic) - 2} more entries.")
                break

    logger.info(f"--- Testing get_behavior_summary from {test_date_from} to {test_date_to} ---")
    behavior_data = get_behavior_summary(test_date_from, test_date_to)
    if behavior_data:
        logger.info(f"Received {len(behavior_data)} behavior summary entries. Samples:")
        for i, entry in enumerate(behavior_data):
            if i < 2:
                logger.info(entry)
            else:
                logger.info(f"... and {len(behavior_data) - 2} more entries.")
                break
    else:
        logger.warning("No behavior data received or an error occurred.")

    logger.info(f"--- Testing get_conversions_data from {test_date_from} to {test_date_to} ---")
    conversions_data = get_conversions_data(test_date_from, test_date_to)
    if conversions_data:
        logger.info(f"Received {len(conversions_data)} conversions data entries. Samples:")
        for i, entry in enumerate(conversions_data):
            if i < 2:
                logger.info(entry)
            else:
                logger.info(f"... and {len(conversions_data) - 2} more entries.")
                break
    else:
        logger.warning("No conversions data received or an error occurred.")