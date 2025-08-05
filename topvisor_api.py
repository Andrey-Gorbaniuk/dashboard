# topvisor_api.py (ФИНАЛЬНАЯ ВЕРСИЯ)
import requests
import logging
import time
import json
from datetime import datetime, timedelta
import config

logger = logging.getLogger(__name__)

BASE_TOPVISOR_API_URL = config.TOPVISOR_API_URL
API_KEY = config.TOPVISOR_API_KEY
USER_ID = config.TOPVISOR_USER_ID

SEARCHER_MAP = {
    1: "Yandex XML", 2: "Yandex", 3: "Google", 4: "Go.Mail.ru", 5: "Rambler",
    6: "Bing", 7: "Yahoo", 8: "ASK", 9: "Sputnik", 10: "Youtube",
}


def call_public_api(method_path, params_data):
    if not API_KEY or not USER_ID:
        logger.error("Topvisor API Key or User ID is not configured.")
        return None

    full_url = f"{BASE_TOPVISOR_API_URL.strip('/')}/v2/json/get/{method_path.strip('/')}"
    headers = {'User-Id': str(USER_ID), 'Authorization': f'Bearer {API_KEY}', 'Content-Type': 'application/json',
               'Accept': 'application/json'}
    payload = params_data

    # Убираем лишний лог, чтобы не засорять вывод
    # logger.info(f"Attempting Topvisor Public API Call. URL: {full_url}")
    logger.debug(f"Payload (body): {json.dumps(payload, ensure_ascii=False)}")

    try:
        response = requests.post(full_url, headers=headers, json=payload, timeout=60)
        response_content = response.json()

        if response.status_code != 200:
            logger.error(f"Response Status Code: {response.status_code}")
            logger.error(f"Raw Response Content: {json.dumps(response_content, indent=2, ensure_ascii=False)}")

        response.raise_for_status()

        if "errors" in response_content and response_content["errors"]:
            logger.error(f"Topvisor API returned an error: {response_content['errors']}")
            return None

        return response_content.get("result")

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}", exc_info=True)
        return None


def get_positions_history(date_from_str, date_to_str, project_id, region_indexes, searcher_ids):
    """
    НАДЕЖНАЯ ВЕРСИЯ.
    Итерирует по поисковым системам, чтобы делать более простые и надежные запросы.
    """
    all_positions_data = []

    for searcher_id in searcher_ids:
        logger.info(f"Fetching positions for searcher ID: {searcher_id}")
        params = {
            "project_id": project_id,
            "regions_indexes": region_indexes,
            "searchers": [searcher_id],
            "dates": [date_from_str, date_to_str],
            "positions_fields": ["position", "relevant_url"],
            "fields": ["name", "id"],
            "limit": 2000,
            "offset": 0,
            "show_all_positions_data_from_date": 1
        }

        result = call_public_api(method_path="positions_2/history", params_data=params)
        time.sleep(1)

        if result and "keywords" in result and isinstance(result["keywords"], list):
            searcher_name = SEARCHER_MAP.get(searcher_id, f"SearcherID {searcher_id}")

            for keyword_data in result["keywords"]:
                keyword_name = keyword_data.get("name")
                positions_data = keyword_data.get("positionsData", {})

                if not isinstance(positions_data, dict):
                    continue

                for composite_key, pos_data in positions_data.items():
                    try:
                        parts = composite_key.split(':')
                        if len(parts) < 3: continue

                        report_date = parts[0]
                        region_id = int(parts[2])
                        position_val = pos_data.get("position")

                        if not isinstance(position_val, (int, str)) or not str(position_val).isdigit():
                            continue

                        position = int(position_val)

                        all_positions_data.append({
                            "report_date": report_date, "keyword": keyword_name,
                            "search_engine_name": searcher_name, "search_engine_id": searcher_id,
                            "region_id": region_id, "position": position,
                            "url": pos_data.get("relevant_url")
                        })
                    except (ValueError, TypeError, IndexError) as e:
                        logger.warning(f"Error parsing position data for '{keyword_name}': {e}. Skipping.")
                        continue

    logger.info(f"Processed {len(all_positions_data)} position records for project {project_id}.")
    return all_positions_data


def get_visibility_summary(date_from_str, date_to_str, project_id, region_indexes, searcher_ids):
    """
    НАДЕЖНАЯ ВЕРСИЯ.
    Получает историю видимости, итерируя по дням.
    """
    from datetime import datetime, timedelta
    all_visibility_data = []

    try:
        start_date = datetime.strptime(date_from_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(date_to_str, '%Y-%m-%d').date()
    except ValueError:
        logger.error(f"Invalid date format for visibility. Expected YYYY-MM-DD.")
        return []

    current_date = start_date
    while current_date <= end_date:
        day_str = current_date.strftime('%Y-%m-%d')

        for region_id in region_indexes:
            for searcher_id in searcher_ids:
                # Убираем лишний лог, чтобы не засорять вывод
                # logger.info(f"Requesting visibility for project {project_id}, region {region_id}, searcher {searcher_id}, date {day_str}")

                params = {
                    "project_id": project_id, "region_index": region_id,
                    "searcher": searcher_id, "dates": [day_str, day_str],
                    "show_visibility": True,
                }
                time.sleep(1)
                result = call_public_api(method_path="positions_2/summary", params_data=params)

                if result and "visibilities" in result:
                    visibility_list = result.get("visibilities", [])
                    if visibility_list and visibility_list[0] is not None:
                        try:
                            visibility_score = float(visibility_list[0])
                            searcher_name = SEARCHER_MAP.get(searcher_id, f"SearcherID {searcher_id}")
                            all_visibility_data.append({
                                "report_date": day_str, "search_engine_name": searcher_name,
                                "search_engine_id": searcher_id, "region_id": region_id,
                                "visibility_score": visibility_score
                            })
                        except (ValueError, TypeError, IndexError) as e:
                            logger.warning(f"Could not parse visibility '{visibility_list}' for {day_str}. Error: {e}")

        current_date += timedelta(days=1)

    logger.info(f"Processed {len(all_visibility_data)} visibility records for the entire period.")
    return all_visibility_data