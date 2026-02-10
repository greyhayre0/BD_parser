import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime
import time

conn = sqlite3.connect('spimex_trading_results.db')
cursor = conn.cursor()

cursor.execute('''
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

conn.close()

def parse_link(url,year = 2023):
    '''Набор ссылок с страницы'''
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "lxml")
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

def table_clener(url):
    '''Чистим и подготавливаем таблицу'''
    df = pd.read_excel(url)
    mask = df.astype(str).apply(lambda col: col.str.contains('Единица измерения: Метрическая тонна'))
    row_idx = mask.any(axis=1).idxmax()
    df = df.loc[row_idx+1:]

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

def migrate_df_db(df, db='spimex_trading_results.db'):
    conn = sqlite3.connect('spimex_trading_results.db')
    cursor = conn.cursor()
    for index, row in df.iterrows():
        cursor.execute('''
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['Код Инструмента'],
            row['Наименование Инструмента'],
            row['Код Инструмента'][:4],
            row['Код Инструмента'][4:7],
            row['Базис поставки'],
            row['Наименование Инструмента'][-1],
            int(row['Объем Договоров в единицах измерения']) if pd.notnull(row['Объем Договоров в единицах измерения']) else None,
            int(row['Обьем Договоров, руб.']) if pd.notnull(row['Обьем Договоров, руб.']) else None,
            int(row['Количество Договоров, шт.']) if pd.notnull(row['Количество Договоров, шт.']) else None,
            datetime.now().date()
        ))
        
    conn.commit()
    conn.close()

page = 1

if __name__ == "__main__":
    start_time = time.time()
    while True:
        url = f"https://spimex.com/markets/oil_products/trades/results/?page=page-{page}"
        links = parse_link(url)
        for link in links:
            print(f'ссылка{link} /n {page} /n {url}')
            try:
                df = table_clener(link)
                migrate_df_db(df)
            except:
                print("bad xls")
        page += 1
        if not links:
            break
    print(f"{start_time - time.time():.4f}")

# times 894.1573 sec
# elements 150693