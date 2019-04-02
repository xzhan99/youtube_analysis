#!/usr/bin/python3
import sys


def read_combiner_output(line):
    category, parts = line.strip().split('\t')
    parts = parts.split(',')
    video_id = parts[0]
    country_set = set(parts[1:])
    return category, video_id, country_set


def output(category, country_dict):
    total = sum(map(lambda x: len(x), country_dict.values()))
    num_of_videos = len(country_dict)
    print('%s: %s' % (category, total / num_of_videos))


def reducer():
    """
    Input: category    video_id,country_1,country_2,...,country_n
    Output: category: 1.4
    """
    current_category = ''
    trending_countries = {}
    for line in sys.stdin:
        category, video_id, country_set = read_combiner_output(line)
        if not current_category:
            current_category = category
        elif category != current_category:
            # receiving data with new category means last category finished
            output(current_category, trending_countries)
            trending_countries.clear()
            current_category = category
        # get previous country list if exists and add new countries in it
        trending_countries[video_id] = trending_countries.get(video_id, set()) | country_set
    output(current_category, trending_countries)


if __name__ == "__main__":
    reducer()
