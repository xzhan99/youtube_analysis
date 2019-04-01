import json

import pandas as pd

PATH = '/Users/andrewzhan/Documents/Projects/PycharmProjects/youtube_trending/data/'
COUNTRIES = ('CA', 'DE', 'FR', 'GB', 'IN', 'JP', 'KR', 'MX', 'RU', 'US')
NECESSARY_COLUMNS = ('video_id', 'trending_date', 'category_id', 'views', 'likes', 'dislikes')


def read_from_csv_file(country):
    data_frame = pd.read_csv(PATH + country + 'videos.csv', usecols=NECESSARY_COLUMNS, encoding='utf-8')
    return data_frame


def save_to_csv_file(data_frame):
    data_frame.to_csv(PATH + 'AllVideos_short.csv', encoding='utf-8')


def add_columns(data_frame, country):
    def add_country():
        new_column = pd.DataFrame(data=[country] * len(data_frame))
        data_frame['country'] = new_column

    def add_category():
        with open(PATH + country + '_category_id.json') as file:
            contents = json.load(file).get('items')
            cat_map = {int(item['id']): item['snippet']['title'] for item in contents}
        new_column = []
        invalid_rows = []
        for index, cat_id in enumerate(data_frame['category_id']):
            cat_name = cat_map.get(cat_id, None)
            if cat_name:
                new_column.append(cat_name)
            else:
                invalid_rows.append(index)
        data_frame.drop(index=invalid_rows, inplace=True)
        data_frame['category'] = new_column

    add_country()
    add_category()


def remove_invalid_id(data_frame):
    invalid_rows = []
    for index, row in data_frame.iterrows():
        if row['video_id'].strip() == '#NAME?' or row['video_id'].startswith('-'):
            invalid_rows.append(index)
    data_frame.drop(invalid_rows, axis=0, inplace=True)


if __name__ == '__main__':
    all_videos = None
    for country in COUNTRIES:
        df = read_from_csv_file(country)
        add_columns(df, country)
        if all_videos is not None:
            all_videos = pd.concat([all_videos, df])
        else:
            all_videos = df
    remove_invalid_id(all_videos)
    save_to_csv_file(all_videos)
