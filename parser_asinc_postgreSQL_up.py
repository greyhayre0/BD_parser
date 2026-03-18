import asyncpg
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import asyncio
from datetime import datetime
import io
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создаем таблицу один раз в PostgreSQL
async def create_table():
    conn = await asyncpg.connect(user='postgresql', password='postgres',
                                 database='db', host='localhost')
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
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.read()

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
    '''Асинхронное добавление данных в PostgreSQL с транзакциями и батч-вставкой'''
    conn = None
    try:
        conn = await asyncpg.connect(user='postgresql', password='postgres',
                                     database='db', host='localhost')
        
        # Начинаем транзакцию
        async with conn.transaction():
            records = []
            for _, row in df.iterrows():
                records.append((
                    row['Код Инструмента'],
                    row['Наименование Инструмента'],
                    row['Код Инструмента'][:4],
                    row['Код Инструмента'][4:7],
                    row['Базис поставки'],
                    row['Наименование Инструмента'][-1],
                    float(row['Объем Договоров в единицах измерения']) if pd.notnull(row['Объем Договоров в единицах измерения']) else None,
                    float(row['Обьем Договоров, руб.']) if pd.notnull(row['Обьем Договоров, руб.']) else None,
                    int(row['Количество Договоров, шт.']) if pd.notnull(row['Количество Договоров, шт.']) else None,
                    datetime.now().date()
                ))
            
            if records:
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
                ''', records)
                
                logger.info(f"Successfully inserted {len(records)} records")
        
    except asyncpg.PostgresError as e:
        logger.error(f"Database error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in migrate_df_db: {e}")
        raise
    finally:
        if conn:
            await conn.close()

async def process_link_with_retry(link, page, max_retries=3):
    """Обработка ссылки с повторными попытками при ошибках"""
    for attempt in range(max_retries):
        try:
            logger.info(f'Обработка ссылки: {link} (страница: {page}, попытка: {attempt+1})')
            df = await table_cleaner(link)
            await migrate_df_db(df)
            return
        except aiohttp.ClientError as e:
            logger.error(f"Network error processing {link}: {e}")
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)
        except asyncpg.PostgresError as e:
            logger.error(f"Database error processing {link}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error processing {link}: {e}")
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)

async def main():
    await create_table()
    page = 1
    start_time = datetime.now()
    
    async with aiohttp.ClientSession() as session:
        while True:
            url = f"https://spimex.com/markets/oil_products/trades/results/?page=page-{page}"
            try:
                links = await parse_link(session, url)
                if not links:
                    break
                    
                tasks = []
                for link in links:
                    task = asyncio.create_task(
                        process_link_with_retry(link, page)
                    )
                    tasks.append(task)
                
                # Собираем результаты и обрабатываем возможные ошибки
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Анализируем результаты
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Task failed for link {links[i]}: {result}")
                
                page += 1
                
            except Exception as e:
                logger.error(f"Error processing page {page}: {e}")
                break
    
    elapsed_time = (datetime.now() - start_time).total_seconds()
    logger.info(f"Время выполнения: {elapsed_time:.2f} секунд")

if __name__ == "__main__":
    asyncio.run(main())