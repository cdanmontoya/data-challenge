import difflib

import numpy as np
import pandas as pd




def _get_country_fuzzy(df: pd.DataFrame) -> pd.DataFrame:
    countries = ['Argentina', 'Brasil', 'Chile', 'México', 'Paraguay', 'Uruguay']

    df['Nat'] = df['Nat'].str.title().str.strip()
    df['Nat'] = df['Nat'].fillna('Unknown')

    df['country'] = (df['Nat']
    .str.replace('Arg', 'Argentina')
    .str.replace('Bra', 'Brasil')
    .str.replace('Ch', 'Chile')
    .str.replace('Mex', 'México')
    .str.replace('Par', 'Paraguay')
    .str.replace('Uru', 'Uruguay')
    .apply(lambda x: difflib.get_close_matches(x, countries, cutoff=0.7)).str[0])

    return df


def _get_country(df: pd.DataFrame) -> pd.Series:
    return (df['coding'].str.split('_').str[0]
            .str.replace('A', 'Argentina')
            .str.replace('B', 'Brasil')
            .str.replace('C', 'Chile')
            .str.replace('M', 'México')
            .str.replace('P', 'Paraguay')
            .str.replace('U', 'Uruguay')
            .str.replace('X', 'Unknown'))


def _get_birth_year(df: pd.DataFrame) -> pd.Series:
    return df['coding'].str.split('_').str[1].str.replace('X', '0').astype(int)


def _get_ranking(df: pd.DataFrame) -> pd.Series:
    return df['coding'].str.split('_').str[2].astype(str)


def clean_names_info(df: pd.DataFrame) -> pd.DataFrame:
    df['id_number'] = df.id_number.astype(int)
    df['age'] = df.Edad.astype(int)
    df['country'] = _get_country(df)
    df['birth_year'] = _get_birth_year(df)
    df['ranking'] = _get_ranking(df)

    df = df[['id_number', 'age', 'birth_year', 'country', 'ranking']]

    return df


def get_name_prefix(df: pd.DataFrame) -> pd.Series:
    return df['Full Name'].apply(lambda x: x.split('.')[0] if '.' in x else None)


def get_full_name(df: pd.DataFrame) -> pd.Series:
    return (df['Full Name']
            .apply(lambda x: x.split('.')[1] if '.' in x else x)  # Remove prefix
            .str.replace(r'[^\w\s]', '', regex=True)  # Remove special characters
            .str.replace(r'\s+', ' ', regex=True)  # Remove consecutive whitespaces
            .str.strip()
            )


def clean_names_dataset(df: pd.DataFrame) -> pd.DataFrame:
    df['prefix'] = get_name_prefix(df)
    df['full_name'] = get_full_name(df)

    df['first_name'] = df['full_name'].str.split(' ').str[0]

    df['last_name'] = df['full_name'].apply(
        lambda x: ' '.join(x.split()[-2:]) if len(x.split()) > 2 else x.split()[-1])
    df['first_last_name'] = df['last_name'].str.split(' ').str[0]
    df['second_last_name'] = df['last_name'].str.split(' ').apply(
        lambda x: x[1] if len(x) > 1 else None)

    df['id_number'] = df['ID'].astype(int)

    df = df[
        ['id_number', 'prefix', 'full_name', 'first_name', 'last_name', 'first_last_name', 'second_last_name']]

    return df


def get_cleaned_dataset() -> pd.DataFrame:
    names_info = pd.read_csv('data/names_info.csv')
    names_dataset = pd.read_csv('data/names_dataset.csv')

    names_info = clean_names_info(names_info)
    names_dataset = clean_names_dataset(names_dataset)

    result = pd.merge(names_info, names_dataset, on=['id_number'], how='outer').replace({np.nan: None})

    return result


if __name__ == '__main__':
    result = get_cleaned_dataset()
    result.to_csv('data/cleaned_data.csv', header=True, index=False)