import asyncpg
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import asyncio
from datetime import datetime
import io

DB_CONFIG = {
    'user': 'ваш_пользователь',
    'password': 'ваш_пароль',
    'database': 'ваша_база',
    'host': 'localhost'
}

# Объявляем функции
async def create_table():
    """Создание таблицы при необходимости."""
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS spimex_trading_results (
                id SERIAL PRIMARY KEY,
                exchange_product_id TEXT,
                exchange_product_name TEXT,
                oil_id TEXT,
                delivery_basis_id TEXT,
                delivery_basis_name TEXT,
                delivery_type_id TEXT,
                volume REAL,
                total REAL,
                count INTEGER,
                date DATE,
                created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.close()
    except Exception as e:
        print(f"Ошибка при создании таблицы: {e}")

async def parse_link(session, url, year=2023):
    """Асинхронное получение и парсинг страницы."""
    try:
        async with session.get(url) as response:
            text = await response.text()
            soup = BeautifulSoup(text, "lxml")
            data = soup.find_all("div", class_="accordeon-inner__item")
            url_list = []
            for i in data:
                times_str = str(i.find("span"))[12:-7]
                if len(url_list) < 10 and int(times_str) >= year:
                    slice_url = "https://spimex.com/" + i.find("a").get("href")
                    url_list.append(slice_url)
                else:
                    break
            return url_list
    except Exception as e:
        print(f"Ошибка при получении парсинга {url}: {e}")
        return []

async def table_cleaner(url):
    """Парсинг Excel файла и подготовка DataFrame."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.read()
        excel_bytes = io.BytesIO(data)
        df = pd.read_excel(excel_bytes)
        # Остальной ваш код обработки DataFrame...
        # Примерный фильтр и обработка:
        mask = df.astype(str).apply(lambda col: col.str.contains('Единица измерения: Метрическая тонна'))
        row_idx = mask.any(axis=1).idxmax()
        df = df.loc[row_idx + 1:]
        headers = df.iloc[0]
        df.columns = headers
        df = df[1:]
        df.columns = [str(col).replace('\n', ' ').strip() for col in df.columns]
        # Передача DataFrame дальше...
        col = [
            'Код Инструмента',
            'Наименование Инструмента',
            'Базис поставки',
            'Объем Договоров в единицах измерения',
            'Обьем Договоров, руб.',
            'Количество Договоров, шт.'
        ]
        df = df[col]
        df.dropna(inplace=True)
        return df
    except Exception as e:
        print(f"Ошибка при чтении Excel файла с {url}: {e}")
        return pd.DataFrame()

async def migrate_df_db(df):
    """Обязательно используем транзакцию и батч-вставку."""
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        async with conn.transaction():
            # Батч-вставка
            values = [
                (
                    row['Код Инструмента'],
                    row['Наименование Инструмента'],
                    row['Код Инструмента'][:4],
                    row['Код Инструмента'][4:7],
                    row['Базис поставки'],
                    row['Наименование Инструмента'][-1],
                    float(row['Объем Договоров в единицах измерения']) if pd.notnull(row['Объем Договоров в единиц измерения']) else None,
                    float(row['Обьем Договоров, руб.']) if pd.notnull(row['Обьем Договоров, руб.']) else None,
                    int(row['Количество Договоров, шт.']) if pd.notnull(row['Количество Договоров, шт.']) else None,
                    datetime.now().date()
                ) for _, row in df.iterrows()
            ]
            # Группа значений
            await conn.executemany('''
                INSERT INTO spimex_trading_results (
                    exchange_product_id,
                    exchange_product_name,
                    oil_id,
                    delivery_basis_id,
                    delivery_basis_name,
                    delivery_type_id,
                    volume,
                    total,
                    count,
                    date
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ''', values)
        await conn.close()
    except Exception as e:
        print(f"Ошибка при вставке данных в базу: {e}")

# Основная функция
async def main():
    await create_table()
    page = 1
    start_time = datetime.now()
    async with aiohttp.ClientSession() as session:
        while True:
            url = f"https://spimex.com/markets/oil_products/trades/results/?page=page-{page}"
            links = await parse_link(session, url)
            if not links:
                break
            for link in links:
                print(f'Обработка: {link}')
                df = await table_cleaner(link)
                if not df.empty:
                    await migrate_df_db(df)
            page += 1
    print(f"Время выполнения: {(datetime.now() - start_time).total_seconds():.2f} сек.")

if __name__ == "__main__":
    asyncio.run(main())