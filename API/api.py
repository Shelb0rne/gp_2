import pandas as pd
import requests
from tqdm import tqdm
from logs.logger import new_logger
import time
import os
import re
from difflib import get_close_matches
from dotenv import load_dotenv
load_dotenv()


def normalize_model_name(value):
    if pd.isna(value):
        return ''

    value = str(value).lower().strip()
    value = value.replace('_', ' ').replace('-', ' ').replace('/', ' ')
    value = re.sub(r'\([^)]*\)', ' ', value)
    value = re.sub(r'[^a-z0-9\s]', ' ', value)

    stop_words = {
        'sedan', 'wagon', 'hatchback', 'coupe', 'suv', 'van', 'truck',
        'awd', 'fwd', 'rwd', '4wd', '2wd', 'hybrid', 'phev', 'ev'
    }
    tokens = [token for token in value.split() if token not in stop_words]
    return ' '.join(tokens)


def find_best_model_match(model, model_names):
    normalized_target = normalize_model_name(model)
    normalized_map = {}

    for original_name in model_names:
        normalized_name = normalize_model_name(original_name)
        if normalized_name:
            normalized_map[normalized_name] = original_name

    if not normalized_target:
        return None

    if normalized_target in normalized_map:
        return normalized_map[normalized_target]

    compact_target = normalized_target.replace(' ', '')
    for normalized_name, original_name in normalized_map.items():
        compact_name = normalized_name.replace(' ', '')
        if compact_target == compact_name:
            return original_name
        if compact_target in compact_name or compact_name in compact_target:
            return original_name

    close_matches = get_close_matches(normalized_target, list(normalized_map.keys()), n=1, cutoff=0.8)
    if close_matches:
        return normalized_map[close_matches[0]]

    return None


def safe_get(url, *, params=None, timeout=(5, 25), retries=3, delay=1, retry_statuses=None):
    if retry_statuses is None:
        retry_statuses = {429, 500, 502, 503, 504}

    last_exception = None

    for attempt in range(retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            if response.status_code not in retry_statuses:
                return response

            if attempt < retries:
                wait_time = delay * (attempt + 1)
                log.warning(f'Повтор запроса через {wait_time} сек. URL: {url} Status: {response.status_code}')
                time.sleep(wait_time)
                continue

            return response

        except Exception as e:
            last_exception = e
            if attempt < retries:
                wait_time = delay * (attempt + 1)
                log.warning(f'Ошибка запроса, повтор через {wait_time} сек. URL: {url} Type: {type(e)} : {e}')
                time.sleep(wait_time)
                continue
            raise

    if last_exception is not None:
        raise last_exception

    raise RuntimeError(f'Не удалось выполнить запрос: {url}')

API_KEY = os.getenv('API_KEY')

def get_marketcheck_metrics(make, model, year):
    if not API_KEY:
        return None, None, None

    try:
        r = safe_get(
            'https://api.marketcheck.com/v2/search/car/active',
            params={
                'api_key': API_KEY,
                'year': year,
                'make': make,
                'model': model,
                'stats': 'price,dom',
                'rows': 0
            },
            timeout=(5, 30)
        )

        if r.status_code != 200:
            log.error(f'Ошибка при получении агрегатов MarketCheck для {make} {model} {year}. Response {r.status_code}')
            return None, None, None

        data = r.json()
        stats = data.get('stats', {})
        avg_price = stats.get('price', {}).get('mean')
        avg_days_on_market = stats.get('dom', {}).get('mean')
        popularity_score = data.get('num_found')
        log.info(f'Получены агрегаты MarketCheck для {make} {model} {year}')
        return avg_price, avg_days_on_market, popularity_score

    except Exception as e:
        log.error(f'Ошибка при получении агрегатов MarketCheck для {make} {model} {year} Type: {type(e)} : {e}')
        return None, None, None


def getData(make, model, year, car_url):
    info = {
        'url' : car_url,
        'make': make,
        'model': model,
        'year': year,
        'recalls_count': 0,
        'complaints_count': 0,
        'injuries': 0,
        'deaths': 0,
        'has_fire': 0,
        'has_crash': 0,
        'overall_rating': None,
        'front_crash_rating': None,
        'side_crash_rating': None,
        'rollover_rating': None,
        'manufacturer_country': None,
        'model_exists': False,
        'marketcheck_avg_price': None,
        'marketcheck_avg_days_on_market': None,
        'marketcheck_popularity_score': None,
        'matched_model_name': None,
        'model_match_status': 'not_checked',

    }

    try:
        url = f'https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMakeYear/make/{make}/modelyear/{year}?format=json'
        r = safe_get(url)

        if r.status_code == 200:
            models_list = r.json().get('Results', [])
            model_names = [m.get('Model_Name', '').lower() for m in models_list]
            matched_model = find_best_model_match(model, model_names)
            info['model_exists'] = matched_model is not None
            info['matched_model_name'] = matched_model
            info['model_match_status'] = 'matched' if matched_model else 'not_found'

            if not info['model_exists']:
                log.warning(f'Модель {make} {model} {year} не найдена, запись будет сохранена без части API-данных')
                info['marketcheck_avg_price'], info['marketcheck_avg_days_on_market'], info['marketcheck_popularity_score'] = get_marketcheck_metrics(make, model, year)
                return info
            log.info(f'Объект {make} {model} {year} найден в базе АПИ как {matched_model}')
        else:
            log.error(f'Статус запроса {r.status_code} при поиске {make} {model} {year}')
            info['marketcheck_avg_price'], info['marketcheck_avg_days_on_market'], info['marketcheck_popularity_score'] = get_marketcheck_metrics(make, model, year)
            return info
        
    except Exception as e:
        log.error(f'Ошибка при проверки наличия информации о {make} {model} {year} года Type: {type(e)}: {e}')
        info['marketcheck_avg_price'], info['marketcheck_avg_days_on_market'], info['marketcheck_popularity_score'] = get_marketcheck_metrics(make, model, year)
        return info

    try:
        lookup_model = info['matched_model_name'] or model
        url = f'https://api.nhtsa.gov/recalls/recallsByVehicle?make={make}&model={lookup_model}&modelYear={year}'
        r = safe_get(url)
        if r.status_code == 200:
            recalls = r.json().get('results', [])
            info['recalls_count'] = len(recalls)
            
            log.info(f'Получены данные об отзывных компаниях для {make} {model} {year} года')
        else:
            log.error(f'Ошибка при поиске информации об отзывных компаниях для {make} {model} {year} года. Response {r.status_code}')


    except Exception as e:
        log.error(f'Ошибка при поиске информации об отзывных компаниях для {make} {model} {year} года Type: {type(e)} : {e}')

    
    try:
        url = f'https://api.nhtsa.gov/complaints/complaintsByVehicle?make={make}&model={lookup_model}&modelYear={year}'
        r = safe_get(url)
        if r.status_code == 200:
            complaints = r.json().get('results', [])
            info['complaints_count'] = len(complaints)
            
            for c in complaints:
                info['injuries'] += c.get('numberOfInjuries', 0) or 0
                info['deaths'] += c.get('numberOfDeaths', 0) or 0
                if c.get('fire'):
                    info['has_fire'] += 1
                if c.get('crash'):
                    info['has_crash'] += 1
            
            log.info(f'Получены данные о жалобах и проиcшествий для {make} {model} {year} года')
        
        else:
            log.error(f'Ошибка при получении информации о жалобах и проишествиях для {make} {model} {year} года. Response {r.status_code}')

    except Exception as e:
        log.error(f'Ошибка при получении информации о жалобах и проишествиях для {make} {model} {year} года Type: {type(e)} : {e}')

    
    try:
        url = f'https://api.nhtsa.gov/SafetyRatings/modelyear/{year}/make/{make}/model/{lookup_model}'
        r = safe_get(url)
        if r.status_code == 200 :
            results = r.json().get('Results', [])
            
            if results:
                vehicle_id = results[0].get('VehicleId')
                
                r2 = safe_get(f'https://api.nhtsa.gov/SafetyRatings/VehicleId/{vehicle_id}')
                if r2.status_code == 200:
                    safety = r2.json().get('Results', [{}])[0]
                    
                    info['overall_rating'] = safety.get('OverallRating')
                    info['front_crash_rating'] = safety.get('OverallFrontCrashRating')
                    info['side_crash_rating'] = safety.get('OverallSideCrashRating')
                    info['rollover_rating'] = safety.get('RolloverRating')
                
                    log.info(f'Получена информация про оценку безопасности для {make} {model} {year} года')
                else:
                    log.error(f'Ошибка при получении данных о безопасности для {make} {model} {year} года. Response {r2.status_code}')
            
            else:
                log.warning(f'Данные о безопасности для {make} {model} {year} не найдены')
        
        else:
            log.error(f'Ошибка при получении данных о безопасности для {make} {model} {year} года. Response {r.status_code}')

    except Exception as e:
        log.error(f'Ошибка при получении данных о безопасности для {make} {model} {year} года Type: {type(e)} : {e}')
    
    
    
    try:
        url = f'https://vpic.nhtsa.dot.gov/api/vehicles/GetManufacturerDetails/{make}?format=json'
        r = safe_get(url)
        if r.status_code == 200:
            mfr = r.json().get('Results', [])
            if mfr:
                info['manufacturer_country'] = mfr[0].get('Country')
                log.info(f'Получены данные о стране производителе для {make} {model} {year} года')
            else:
                log.warning(f'Данные о стране производителе {make} {model} {year} не найдены')
        
        else:
            log.error(f'Ошибка при получении данных о стране производителе для {make} {model} {year} года. Response {r.status_code}')
    except Exception as e:
        log.error(f'Ошибка при получении данных о стране производителе для {make} {model} {year} года Type: {type(e)} : {e}')
    
    marketcheck_model = info['matched_model_name'] or model
    info['marketcheck_avg_price'], info['marketcheck_avg_days_on_market'], info['marketcheck_popularity_score'] = get_marketcheck_metrics(make, marketcheck_model, year)
    return info

log = new_logger()

data = pd.read_csv('url_final.csv')
data = data.dropna(subset=['year', 'make', 'model', 'url'])
year_make_model = data[['year', 'make', 'model', 'url']]
new_data = []


for i in tqdm(range(len(year_make_model))):
    year = year_make_model.iloc[i]['year']
    make = str(year_make_model.iloc[i]['make'])
    model = str(year_make_model.iloc[i]['model'])
    url = year_make_model.iloc[i]['url']
    info = getData(make, model, int(year), url)

    new_data.append(info)
    time.sleep(0.3)
    if i % 50 == 0:
        pd.DataFrame(new_data).to_csv('checkpoint.csv', index= False)

df = pd.DataFrame(new_data)
df.to_csv('final_data.csv', index=False)
