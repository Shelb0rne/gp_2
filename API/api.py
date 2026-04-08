import pandas as pd
import requests
from tqdm import tqdm
from logs.logger import new_logger
import time


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
    }

    try:
        url = f'https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMakeYear/make/{make}/modelyear/{year}?format=json'
        r = requests.get(url, timeout=(5,25))

        if r.status_code == 200:
            models_list = r.json().get('Results', [])
            model_names = [m.get('Model_Name', '').lower() for m in models_list]
            info['model_exists'] = model.lower() in model_names

            if not info['model_exists']:
                log.warning(f'Модель {make} {model} {year} не найдена')
                return None
            log.info(f'Объект {make} {model} {year} надена в базе АПИ')
        else:
            log.error(f'Статус запроса {r.status_code} при поиске {make} {model} {year}')
            return None
        
    except Exception as e:
        log.error(f'Ошибка при проверки наличия информации о {make} {model} {year} года Type: {type(e)}: {e}')
        return None

    try:
        url = f'https://api.nhtsa.gov/recalls/recallsByVehicle?make={make}&model={model}&modelYear={year}'
        r = requests.get(url, timeout=(5,25))
        if r.status_code == 200:
            recalls = r.json().get('results', [])
            info['recalls_count'] = len(recalls)
            
            log.info(f'Получены данные об отзывных компаниях для {make} {model} {year} года')
        else:
            log.error(f'Ошибка при поиске информации об отзывных компаниях для {make} {model} {year} года. Response {r.status_code}')

    except Exception as e:
        log.error(f'Ошибка при поиске информации об отзывных компаниях для {make} {model} {year} года Type: {type(e)} : {e}')
    
    try:
        url = f'https://api.nhtsa.gov/complaints/complaintsByVehicle?make={make}&model={model}&modelYear={year}'
        r = requests.get(url, timeout=(5,25))
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
        url = f'https://api.nhtsa.gov/SafetyRatings/modelyear/{year}/make/{make}/model/{model}'
        r = requests.get(url, timeout=(5,25))
        if r.status_code == 200 :
            results = r.json().get('Results', [])
            
            if results:
                vehicle_id = results[0].get('VehicleId')
                
                r2 = requests.get(f'https://api.nhtsa.gov/SafetyRatings/VehicleId/{vehicle_id}', timeout=(5,25))
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
        r = requests.get(url, timeout=(5,25))
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
    
    return info

log = new_logger()

data = pd.read_csv('checkpoint_4400.csv')
data = data.dropna(subset=['year', 'make', 'model', 'url'])
year_make_model = data[['year', 'make', 'model', 'url']]
new_data = []


for i in tqdm(range(len(year_make_model))):
    year = year_make_model.iloc[i]['year']
    make = str(year_make_model.iloc[i]['make'])
    model = str(year_make_model.iloc[i]['model'])
    url = year_make_model.iloc[i]['url']
    info = getData(make, model, int(year), url)
    if info is None:
        time.sleep(0.2)
        continue

    new_data.append(info)
    time.sleep(0.2)

df = pd.DataFrame(new_data)

df.to_csv('new_data.csv', index=False)

