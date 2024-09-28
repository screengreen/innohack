# Проект innohack: Связывание записей (Record Linkage)

## 1. Описание
Проект "Связывание записей" предназначен для идентификации и объединения записей из разных источников данных, которые относятся к одним и тем же объектам (в данном случае персональные данные людей). Он позволяет улучшить качество данных и устранить дублирование.

## 2. Особенности
- Поддержка различных алгоритмов связывания записей.
- Возможность работы с неструктурированными и структурированными данными.
- Инструменты для предобработки, очистки и нормализации данных.
- Выбор признаков для улучшения точности связывания.
- Поддержка визуализации и анализа результатов.

## 3. Стэк
- Python 3.X
- Pandas
- Recordlinkage

## 4. Установка библиотек
Для настройки и запуска проекта вам потребуется установить необходимые библиотеки через терминал с помощью `pip`:
- `pip install pandas`
- `pip install recordlinkage`

## 5. Использование
### а) Подготовка данных
1. Подготовьте исходные данные в формате CSV. Пример файлов:
  - `main1.csv`
  - `main2.csv`
  - `main3.csv`
  
### б) Алгоритм работы с данными
#### Hужно провести импорт pandas
    import pandas as pd
#### Загрузка и чтение данных
    df1 = pd.read_csv('main1.csv', index_col='Unnamed: 0')
    df3 = pd.read_csv('main3.csv', index_col='Unnamed: 0')
#### Создаем функции для работы со столбцом имя
    def name_column(df)
    # Убираем имя с индексом
    columns_with_name = [col for col in df.columns if 'name' in col]
    if 'Unnamed: 0' in columns_with_name:
        columns_with_name.remove('Unnamed: 0')
    
    # Переименовываем столбец в 'name'
    if len(columns_with_name)==1:
        df.rename({columns_with_name[0]: 'name'}, axis=1, inplace=True)
    
    # Объединяем ФИО в одну строку
    if len(columns_with_name)>=2:      
        df['name'] = df[columns_with_name].apply(lambda row: ''.join(row.values.astype(str)), axis=1)
#### Функция для очистки датасета (заполняет пустоты и удаляет дубликаты)
    def preprocess_columns(df)
    # Переименование столбца или объединение в одно имя
    if 'full_name' in df.columns:
        df.rename({'full_name': 'name'}, axis=1, inplace=True)
    if 'first_name' in df.columns:
        df['name'] = df['first_name']+' '+df['middle_name']+' '+df['last_name']
        
        # Очиста от значений "нет" и "отсутствует"
        df['name'] = df['name'].str.lower().replace(r'нет', '', regex=True)
        df['name'] = df['name'].str.lower().replace(r'отсутствует', '', regex=True)
    
    # Префикс почты (на случай указания одного аккаунта под разными доменами)
    if 'email' in df.columns: 
        df['email_prefix'] = df['email'].apply(lambda x: str(x).split('@')[0])
    
    # Очитска имени (на случай наличия посторонних символов)
    df['name_clean'] = df['name'].str.lower().replace(r'[^a-zA-Zа-яА-Я ]', '', regex=True)
    df['name_clean'] = df['name_clean'].str.split().str.join(' ')

#### Обработка столбцов в таблицах
    preprocess_columns(df1)
    preprocess_columns(df3)
    # Очистка исходя из префикса почт
    df1_clean = df1.drop_duplicates(subset=['email_prefix'], keep='first')
    df3_clean = df3.drop_duplicates(subset=['email_prefix'], keep='first')

#### Объединяем таблицы
    # Пересечение таблиц по префиксу
    df_union = df1_clean[df1_clean['email_prefix'].isin(df3_clean['email_prefix'])]

    # Добавляем снизу те записи из таблицы 1, которые не вошли в таблицу с объединением
    df_union = pd.concat([df_union, df1_clean[~df1_clean['email_prefix'].isin(df3_clean['email_prefix'])]], axis=0)

    # Добавляем снизу те записи из таблицы 3, которые не содержатся в таблице пересечений
    df_union = pd.concat([df_union, df3_clean[~df3_clean['email_prefix'].isin(df1_clean['email_prefix'])]], axis=0)
#### Выводим новую таблицу
    df_union

    







