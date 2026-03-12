import aiohttp
import aiosqlite
from bs4 import BeautifulSoup
import pandas as pd
import asyncio
from datetime import datetime

# Создаем таблицу один раз
async def create_table():
    async with aiosqlite.connect('spimex_trading_results.db') as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS spimex_trading_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        await db.commit()


async def parse_link(session, url, year=2023):
    '''Асинхронное получение и парсинг страницы'''
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


async def table_cleaner(url):
    '''Асинхронное чтение и очистка Excel файла'''
    # Используем pandas через aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.read()

    # Сохраняем во временный файл, чтобы pd.read_excel мог его открыть
    import io
    excel_bytes = io.BytesIO(data)
    df = pd.read_excel(excel_bytes)

    mask = df.astype(str).apply(lambda col: col.str.contains('Единица измерения: Метрическая тонна'))
    row_idx = mask.any(axis=1).idxmax()
    df = df.loc[row_idx + 1:]

    headers = df.iloc[0]
    df.columns = headers
    df.columns = [str(col).replace('\n', ' ').strip() for col in df.columns]

    df['Количество Договоров, шт.'] = pd.to_numeric(df['Количество Договоров, шт.'], errors='coerce')
    df = df[(df['Количество Договоров, шт.'] != 0) & (df['Количество Договоров, шт.'].notnull())]
    col = [
        'Код Инструмента',
        'Наименование Инструмента',
        'Базис поставки',
        'Объем Договоров в единицах измерения',
        'Обьем Договоров, руб.',
        'Количество Договоров, шт.'
    ]
    df = df[col]
    return df[:-2]


async def migrate_df_db(df):
    '''Асинхронное добавление данных в базу'''
    async with aiosqlite.connect('spimex_trading_results.db') as db:
        for _, row in df.iterrows():
            await db.execute('''
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (
                    row['Код Инструмента'],
                    row['Наименование Инструмента'],
                    row['Код Инструмента'][:4],
                    row['Код Инструмента'][4:7],
                    row['Базис поставки'],
                    row['Наименование Инструмента'][-1],
                    int(row['Объем Договоров в единицах измерения']) if pd.notnull(
                        row['Объем Договоров в единицах измерения']) else None,
                    int(row['Обьем Договоров, руб.']) if pd.notnull(row['Обьем Договоров, руб.']) else None,
                    int(row['Количество Договоров, шт.']) if pd.notnull(row['Количество Договоров, шт.']) else None,
                    datetime.now().date()
                )
            )
        await db.commit()


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
            tasks = []
            for link in links:
                print(f'ссылка: {link} /n страница: {page}')
                async def process_link(link):
                    try:
                        df = await table_cleaner(link)
                        await migrate_df_db(df)
                    except Exception as e:
                        print(f"Error processing {link}: {e}")
                tasks.append(process_link(link))
            await asyncio.gather(*tasks)
            page += 1
    print(f"Время выполнения: {(datetime.now() - start_time).total_seconds():.2f} секунд")


if __name__ == "__main__":
    asyncio.run(main())

# times 894.1573 sec синхронно
# elements 150693 синхронно

# times 155.19 sec асинхронно
# elements 150693 асинхронно