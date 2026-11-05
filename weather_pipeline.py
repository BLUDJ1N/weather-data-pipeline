"""Загрузка почасовой погоды трёх городов из Open-Meteo в PostgreSQL.

Требуются requests и psycopg[binary], Python 3.10+.
База weather_analytics и таблица public.weather_hourly уже должны существовать.
Даты для ручного запуска меняются внизу файла. Обе даты включены.
"""

from datetime import datetime, timedelta, timezone
from getpass import getpass

import psycopg
import requests


CITIES = [
    {"city_name": "Воронеж", "latitude": 51.67, "longitude": 39.18},
    {"city_name": "Москва", "latitude": 55.75, "longitude": 37.62},
    {"city_name": "Санкт-Петербург", "latitude": 59.94, "longitude": 30.31},
]


def fetch_weather(latitude, longitude, start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "timezone": "Europe/Moscow",
        "temperature_unit": "celsius",
        "precipitation_unit": "mm",
        "wind_speed_unit": "kmh",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


def transform_hourly_weather(hourly, city_name, utc_offset_seconds):
    source_timezone = timezone(timedelta(seconds=utc_offset_seconds))
    weather_records = []

    for weather_time, temperature, precipitation, wind_speed in zip(
        hourly["time"],
        hourly["temperature_2m"],
        hourly["precipitation"],
        hourly["wind_speed_10m"],
        strict=True,
    ):
        record = {
            "city_name": city_name,
            "weather_time": datetime.fromisoformat(weather_time).replace(
                tzinfo=source_timezone
            ),
            "temperature_c": temperature,
            "precipitation_mm": precipitation,
            "wind_speed_kmh": wind_speed,
        }
        weather_records.append(record)

    return weather_records


def validate_weather_records(weather_records):
    if not weather_records:
        raise ValueError("Получен пустой список погодных записей")

    for record in weather_records:
        if record["temperature_c"] is None:
            raise ValueError("Отсутствует температура")

        if record["precipitation_mm"] is None:
            raise ValueError("Отсутствует значение осадков")

        if record["wind_speed_kmh"] is None:
            raise ValueError("Отсутствует скорость ветра")

        if record["precipitation_mm"] < 0:
            raise ValueError("Отрицательные осадки недопустимы")

        if record["wind_speed_kmh"] < 0:
            raise ValueError("Отрицательная скорость ветра недопустима")


def load_weather_records(connection, weather_records):
    query = """
        INSERT INTO public.weather_hourly (
            city_name,
            weather_time,
            temperature_c,
            precipitation_mm,
            wind_speed_kmh
        )
        VALUES (
            %(city_name)s,
            %(weather_time)s,
            %(temperature_c)s,
            %(precipitation_mm)s,
            %(wind_speed_kmh)s
        )
        ON CONFLICT (city_name, weather_time)
        DO UPDATE SET
            temperature_c = EXCLUDED.temperature_c,
            precipitation_mm = EXCLUDED.precipitation_mm,
            wind_speed_kmh = EXCLUDED.wind_speed_kmh,
            loaded_at = CURRENT_TIMESTAMP;
    """

    with connection.cursor() as cursor:
        cursor.executemany(query, weather_records)
        processed_count = cursor.rowcount

    return processed_count


def run_pipeline(start_date, end_date, connection):
    all_weather_records = []

    for city in CITIES:
        data = fetch_weather(
            latitude=city["latitude"],
            longitude=city["longitude"],
            start_date=start_date,
            end_date=end_date,
        )

        city_records = transform_hourly_weather(
            hourly=data["hourly"],
            city_name=city["city_name"],
            utc_offset_seconds=data["utc_offset_seconds"],
        )

        validate_weather_records(city_records)
        all_weather_records.extend(city_records)

        print(f"{city['city_name']}: подготовлено {len(city_records)} записей")

    # Загружаем общий список, когда проверены все города.
    processed_count = load_weather_records(connection, all_weather_records)

    return processed_count


# Этот блок выполняется при ручном запуске файла.
# При импорте из DAG функции доступны без запроса пароля и запуска загрузки.
if __name__ == "__main__":
    start_date = "2026-10-01"
    end_date = "2026-10-03"

    password = getpass("Пароль PostgreSQL: ")

    with psycopg.connect(
        host="localhost",
        port=5432,
        dbname="weather_analytics",
        user="postgres",
        password=password,
        connect_timeout=10,
    ) as connection:
        processed_count = run_pipeline(start_date, end_date, connection)

    print("Вставлено или обновлено записей:", processed_count)
