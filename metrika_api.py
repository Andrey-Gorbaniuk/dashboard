# metrika_api.py (ФИНАЛЬНАЯ ИСПРАВЛЕННАЯ ВЕРСИЯ)
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
        'date1': date1,
        'date2': date2,
        'limit': limit,
        'offset': offset,
        'accuracy': 'full'
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
            response.raise_for_status()

            response_data = response.json()
            if 'data' in response_data and response_data['data']:
                all_data.extend(response_data['data'])
                total_rows = response_data.get('total_rows', 0)
                if len(all_data) >= total_rows or len(response_data['data']) < limit:
                    break
                current_offset += limit
            else:
                break
            time.sleep(0.5)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error requesting Metrika API: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    logger.error(f"Metrika API error details: {e.response.json()}")
                except ValueError:
                    logger.error(f"Metrika API error response content: {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred during Metrika API request: {e}")
            return None

    logger.info(
        f"Successfully fetched {len(all_data)} rows from Metrika API for metrics='{metrics}', dimensions='{dimensions}' between {date1} and {date2}.")
    return all_data


def get_traffic_sources_summary(date_from, date_to):
    """
    Получает данные о трафике по всем источникам.
    """
    metrics = 'ym:s:visits,ym:s:users'
    dimensions = 'ym:s:date,ym:s:lastTrafficSource,ym:s:lastSourceEngine'
    raw_data = get_metrika_data(metrics=metrics, dimensions=dimensions, date1=date_from, date2=date_to,
                                sort='ym:s:date')

    if raw_data is None:
        return []

    processed_data = []
    for item in raw_data:
        try:
            record_date_str = item['dimensions'][0]['name']
            traffic_source_type = item['dimensions'][1]['name'] or "Не определено"
            source_engine_detail = item['dimensions'][2]['name'] or "Не определено"
            visits = int(item['metrics'][0])
            users = int(item['metrics'][1])

            source_group = "Прочие источники"
            source_engine = traffic_source_type

            # --- Логика категоризации ---
            norm_type = traffic_source_type.lower()
            if 'organic' in norm_type or 'search' in norm_type:
                source_group = "Переходы из поисковых систем"
                norm_engine = source_engine_detail.lower()
                if 'yandex' in norm_engine or 'яндекс' in norm_engine:
                    source_engine = "Яндекс"
                elif 'google' in norm_engine:
                    source_engine = "Google"
                else:
                    source_engine = "Другие поисковые системы"
            elif 'direct' in norm_type:
                source_group = "Прямые заходы"
                source_engine = "Прямые заходы"
            elif 'social' in norm_type:
                source_group = "Переходы из социальных сетей"
                source_engine = "Социальные сети"
            # ... можно добавить другие elif для рекламного, ссылочного и т.д. ...

            processed_data.append({
                'report_date': record_date_str, 'source_group': source_group,
                'source_engine': source_engine, 'source_detail': source_engine_detail,
                'visits': visits, 'users': users
            })
        except (IndexError, KeyError, TypeError) as e:
            logger.error(f"Error processing traffic source item: {item}. Error: {e}. Skipping.")
            continue
    logger.info(f"Processed {len(processed_data)} records for all traffic sources.")
    return processed_data


def get_behavior_summary(date_from, date_to):
    """
    Получает сводные данные по поведению пользователей на сайте.
    """
    metrics = 'ym:s:bounces,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds'
    dimensions = 'ym:s:date'
    raw_data = get_metrika_data(metrics=metrics, dimensions=dimensions, date1=date_from, date2=date_to,
                                sort='ym:s:date')

    if raw_data is None:
        return []

    processed_data = []
    for item in raw_data:
        try:
            processed_data.append({
                'report_date': item['dimensions'][0]['name'],
                'bounces': int(item['metrics'][0] or 0),
                'bounce_rate': float(item['metrics'][1] or 0.0),
                'page_depth': float(item['metrics'][2] or 0.0),
                'avg_visit_duration_seconds': int(item['metrics'][3] or 0)
            })
        except (IndexError, KeyError, TypeError, ValueError) as e:
            logger.error(f"Error processing behavior item: {item}. Error: {e}. Skipping.")
            continue
    logger.info(f"Processed {len(processed_data)} records for behavior summary.")
    return processed_data


# ====================================================================================
# ФИНАЛЬНАЯ ИСПРАВЛЕННАЯ ФУНКЦИЯ ДЛЯ КОНВЕРСИЙ
# ====================================================================================
def get_conversions_data(date_from, date_to):
    """
    Получает данные по всем настроенным целям Яндекс.Метрики в разрезе источников.
    ФИНАЛЬНАЯ ВЕРСИЯ: Корректно обрабатывает ответ API, пропуская цели не из текущего чанка.
    """
    if not config.METRIKA_GOAL_IDS_FOR_REQUEST:
        logger.warning("No goal IDs configured. Skipping conversion data.")
        return []

    all_processed_data = []
    goals_map = config.METRIKA_GOALS_MAP

    max_goals_per_request = 10
    goal_ids_chunks = [
        config.METRIKA_GOAL_IDS_FOR_REQUEST[i:i + max_goals_per_request]
        for i in range(0, len(config.METRIKA_GOAL_IDS_FOR_REQUEST), max_goals_per_request)
    ]

    for chunk_idx, goal_ids_chunk in enumerate(goal_ids_chunks):
        if not goal_ids_chunk: continue
        logger.info(f"Processing conversions chunk {chunk_idx + 1}/{len(goal_ids_chunks)} with goals: {goal_ids_chunk}")

        metrics_list_chunk = [m for goal_id in goal_ids_chunk for m in
                              (f"ym:s:goal{goal_id}reaches", f"ym:s:goal{goal_id}conversionRate")]
        metrics_str_chunk = ",".join(metrics_list_chunk)
        dimensions = 'ym:s:date,ym:s:goalID,ym:s:lastTrafficSource,ym:s:lastSourceEngine'

        raw_data_chunk = get_metrika_data(metrics=metrics_str_chunk, dimensions=dimensions, date1=date_from,
                                          date2=date_to)

        if not raw_data_chunk:
            logger.warning(f"No data received for conversions chunk {chunk_idx + 1}.")
            continue

        for item_idx, item in enumerate(raw_data_chunk):
            try:
                record_date_str = item['dimensions'][0].get('name')
                goal_id_from_api = item['dimensions'][1].get('name')

                # Проверяем, есть ли goal_id из ответа API в ТЕКУЩЕМ запрошенном чанке.
                try:
                    index_in_chunk = goal_ids_chunk.index(str(goal_id_from_api))
                    metric_offset = index_in_chunk * 2
                except ValueError:
                    # Нормальная ситуация: API вернул цель не из этого чанка. Молча пропускаем.
                    continue

                traffic_source_type = item['dimensions'][2].get('name', "Не определено")
                source_engine_detail_name = item['dimensions'][3].get('name', "Не определено")

                if not record_date_str: continue

                goal_name = goals_map.get(goal_id_from_api, "Неизвестная цель")
                reaches = int(item['metrics'][metric_offset]) if item['metrics'][metric_offset] is not None else 0
                conversion_rate = float(item['metrics'][metric_offset + 1]) if item['metrics'][
                                                                                   metric_offset + 1] is not None else 0.0

                # (Блок категоризации источников)
                current_source_engine_category = "Прочие источники"
                normalized_api_traffic_type = traffic_source_type.lower()
                if 'organic' in normalized_api_traffic_type or 'search' in normalized_api_traffic_type:
                    current_source_engine_category = "Другие поисковые системы"
                    normalized_engine_detail = source_engine_detail_name.lower()
                    if 'яндекс' in normalized_engine_detail or 'yandex' in normalized_engine_detail:
                        current_source_engine_category = "Яндекс"
                    elif 'google' in normalized_engine_detail:
                        current_source_engine_category = "Google"
                elif 'direct' in normalized_api_traffic_type:
                    current_source_engine_category = "Прямые заходы"
                elif 'social' in normalized_api_traffic_type:
                    current_source_engine_category = "Социальные сети"
                elif 'link' in normalized_api_traffic_type:
                    current_source_engine_category = "Переходы по ссылкам на сайтах"
                elif 'ad' in normalized_api_traffic_type:
                    current_source_engine_category = "Переходы по рекламе"
                # ... и т.д.

                all_processed_data.append({
                    'report_date': record_date_str, 'goal_id': goal_id_from_api,
                    'goal_name': goal_name, 'source_engine': current_source_engine_category,
                    'source_detail': source_engine_detail_name, 'reaches': reaches,
                    'conversion_rate': conversion_rate
                })
            except Exception as e:
                logger.error(f"Error processing conversion item #{item_idx}: {item}. Error: {e}. Skipping.")
                continue

        time.sleep(0.5)

    logger.info(f"Processed {len(all_processed_data)} records for conversions data.")
    return all_processed_data