import argparse
import pandas as pd
import re
import recordlinkage
from recordlinkage.compare import Exact, String

def normalize_phone(phone):
    # Удаляем все символы, кроме цифр
    phone = re.sub(r'\D', '', phone)
    
    # Проверяем длину и формат номера
    if len(phone) == 11 and phone.startswith('7'):
        phone = '8' + phone[1:]  # Заменяем '7' на '8'
    elif len(phone) == 10:
        phone = '8' + phone  # Добавляем '8' перед номером
    
    # Если номер слишком короткий, возвращаем то же самое
    elif len(phone) < 10:
        return phone

    return phone
    
def name_column(df):
    '''
    Функция для работы со столбцом имя
    '''

    # Убираем имя с индексом
    columns_with_name = [col for col in df.columns if 'name' in col]
    if 'Unnamed: 0' in columns_with_name:
        columns_with_name.remove('Unnamed: 0')
    
    # Переименовываем столбец в 'name'
    if len(columns_with_name)==1:
        df.rename({columns_with_name[0]: 'name'}, axis=1, inplace=True)
    
    # Объединяем ФИО в одну строку
    if len(columns_with_name)>=2:      
        df['name'] = df[columns_with_name].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
        df = df.drop([*columns_with_name], axis=1)
    return df

def name_clean_column(df):
    '''
    Функция для обработки столбца clean_name
    '''
    # Очитска имени (на случай наличия посторонних символов)
    df['name'] = df['name'].str.lower().replace(r'[^a-zA-Zа-яА-Я ]', '', regex=True)

    # Очиста от значений "нет" и "отсутствует"
    df['name'] = df['name'].str.lower().replace(r'нет', '', regex=True)
    df['name'] = df['name'].str.lower().replace(r'отсутствует', '', regex=True)
    #df['name'] = df['name'].str.split().str.join(' ')

    # Очистка от [оглы, углы, аглы, огли, угли, агли]
    #df['name'] = df['name'].str.lower().replace(r'оглы', '', regex=True)
    #df['name'] = df['name'].str.lower().replace(r'углы', '', regex=True)
    #df['name'] = df['name'].str.lower().replace(r'аглы', '', regex=True)
    #df['name'] = df['name'].str.lower().replace(r'огли', '', regex=True)
    #f['name'] = df['name'].str.lower().replace(r'угли', '', regex=True)
    #df['name'] = df['name'].str.lower().replace(r'агли', '', regex=True)

    # сортировка
    df['name'] = df['name'].apply(lambda x: ' '.join(sorted(x.split())))
    return df

def preprocess_columns(df):
    '''
    Функция для очистки датасета, заполняет пустоты и удаляет дубликаты

    param:
    df: pd.dataframe - таблица с данными

    return:
    None
    '''

    df = name_column(df)
    df = name_clean_column(df)
    
    # Префикс почты (на случай указания одного аккаунта под разными доменами)
    if 'email' in df.columns: 
        df['email'] = df['email'].apply(lambda x: str(x).split('@')[0])
    
    

    # Приведение номера телефона к единому виду
    if 'phone' in df.columns:
        df['phone'] = df['phone'].apply(normalize_phone)
    
    # даты рождения
    return df


def main():
    # Create the parser
    parser = argparse.ArgumentParser(description='Process three paths.')

    # Add arguments for the three paths
    parser.add_argument('path1', type=str, help='The first path')
    parser.add_argument('path2', type=str, help='The second path')
    parser.add_argument('path3', type=str, help='The third path')

    # Parse the arguments
    args = parser.parse_args()

    # Access the paths
    path1 = args.path1
    path2 = args.path2
    path3 = args.path3
    
    df1 = pd.read_csv(path1)
    df2 = pd.read_csv(path2)
    df3 = pd.read_csv(path3)

    df1 = preprocess_columns(df1)
    df2 = preprocess_columns(df2)
    df3 = preprocess_columns(df3)   


    #df1 and df3
    compare = recordlinkage.Compare()
    # compare.string('name', 'name', method='levenshtein', threshold=0.2, label='name')
    compare.string('phone', 'phone', method='levenshtein', threshold=0.2, label='phone')
    # compare.string('email', 'email', method='levenshtein', threshold=0.2, label='email')

    compare.string('birthdate', 'birthdate', method='jarowinkler', threshold=0.8, label='birthdate')

    from recordlinkage.index import Block

    # Create indexer
    indexer = Block('name')
    new_candidate_links = indexer.index(df1, df2)

    # Compare records
    features = compare.compute(new_candidate_links, df1, df2)

    features.index.to_frame().reset_index().drop(['level_0', 'level_1'], axis=1).rename(columns={0:'df1', 1:'df2'}).to_csv('df1df2.csv', index=False)



    #df1 and df2
    compare = recordlinkage.Compare()
    compare.exact('sex', 'sex', label='sex')
    compare.string('name', 'name', method='levenshtein', threshold=0.2, label='name')
    # compare.string('email', 'email', method='levenshtein', threshold=0.2, label='email')
    compare.string('birthdate', 'birthdate', method='jarowinkler', threshold=0.8, label='birthdate')

    from recordlinkage.index import Block

    # Create indexer
    indexer = Block('email')
    new_candidate_links = indexer.index(df1, df3)

    # Compare records
    features = compare.compute(new_candidate_links, df1, df3)

    features.index.to_frame().reset_index().drop(['level_0', 'level_1'], axis=1).rename(columns={0:'df1', 1:'df3'}).to_csv('df1df3.csv', index=False)


if __name__ == '__main__':
    main()
