import argparse
from datetime import datetime

from pyspark import SparkContext

MAX_DATE = datetime(9999, 1, 1)


def generate_pair(line):
    parts = line.split(',')
    video_id = parts[0].strip().encode('utf-8')
    trending_date = datetime.strptime(parts[1].strip(), "%y.%d.%m")
    category = parts[3].strip().encode('utf-8')
    likes = int(parts[6].strip())
    dislikes = int(parts[7].strip())
    country = parts[11].strip().encode('utf-8')
    return (video_id, country, category), (trending_date, likes, dislikes)


def merge(pair, video):
    value1, value2 = pair
    trending_date, likes, dislikes = video
    if trending_date < value1['date']:
        value2 = value1
        value1 = {'date': trending_date, 'difference': dislikes - likes}
    elif trending_date < value2['date']:
        value2 = {'date': trending_date, 'difference': dislikes - likes}
    return value1, value2


def combine(pair1, pair2):
    combined_list = sorted(list(pair1 + pair2), key=lambda x: x['date'])
    return combined_list[:2]


def calculate_difference(pair):
    key, value = pair
    video1, video2 = value
    if video1['date'] == MAX_DATE or video2['date'] == MAX_DATE:
        return key, 0
    return key, video2['difference'] - video1['difference']


def reformat(pair):
    key, difference = pair
    video_id, country, category = key
    return ','.join(video_id, difference, category, country)


if __name__ == "__main__":
    sc = SparkContext(appName='Fastest growing videos')
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', help='the input path')
    parser.add_argument('--output', help='the output path')
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output

    # remove first line
    video_data = sc.textFile(input_path).filter(lambda x: not x.startswith('video_id'))
    # generate key-value pair
    video_by_id_country = video_data.map(generate_pair)
    # reduce and only keep earliest two data for each key
    init_value = {'date': MAX_DATE, 'difference': 0}
    video_diff = video_by_id_country.aggregateByKey((init_value, init_value), merge, combine, 1)
    # calculate difference by diff = (dislikes2 - likes2) - (dislikes1 - likes1), then select top 10
    result = video_diff.map(calculate_difference).sortBy(lambda x: x[1], ascending=False).take(10)
    # reformat
    result = sc.parallelize(result).map(reformat)
    # save to file
    result.saveAsTextFile(output_path)

    sc.stop()
