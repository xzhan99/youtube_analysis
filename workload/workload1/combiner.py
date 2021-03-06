#!/usr/bin/python3
import sys


def read_map_output(line):
    parts = line.strip().split('\t')
    category = parts[0]
    video_id, country = parts[1].split(',')
    return category, video_id, country


def output(category, country_dict):
    for video_id, country_set in country_dict.items():
        countries_str = ','.join(country_set)
        print('%s\t%s,%s'%(category, video_id, countries_str))


def combiner():
    """
    Input: category    video_id,country
    Output: category    video_id,country_1,country_2,...,country_n
    """
    current_category = ''
    trending_countries = {}
    for line in sys.stdin:
        category, video_id, country = read_map_output(line)
        if not current_category:
            current_category = category
        elif category != current_category:
            # receiving data with new category means last category finished
            output(current_category, trending_countries)
            trending_countries.clear()
            current_category = category
        country_set = trending_countries.get(video_id, set())
        country_set.add(country)
        trending_countries[video_id] = country_set
    output(current_category, trending_countries)


if __name__ == "__main__":
    combiner()
