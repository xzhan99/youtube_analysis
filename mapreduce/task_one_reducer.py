#!/usr/bin/python3
import json
import sys


def read_map_output(line):
    parts = line.strip().split('\t')
    category = parts[0]
    info = json.loads(parts[1])
    video_id, country = info['video_id'], info['country']
    return category, video_id, country


def output(category, country_dict):
    total = sum(map(lambda x: len(x), country_dict.values()))
    num_of_videos = len(country_dict)
    print('%s: %s' % (category, total / num_of_videos))


def tag_reducer():
    """
    Input: category    {"video_id": "nIYrRNklra8", "country": "CA"}
    Output: category: 1.4
    """
    current_category = ''
    trending_countries = {}
    for line in sys.stdin:
        parts = line.strip().split('\t')
        category = parts[0]
        if not current_category:
            current_category = category
        try:
            info = json.loads(parts[1])
        except json.decoder.JSONDecodeError:
            pass
        else:
            video_id, country = info['video_id'], info['country']
            if category != current_category and current_category:
                # receiving data with new category means last category finished
                output(current_category, trending_countries)
                trending_countries = {}
                current_category = category
            country_set = trending_countries.get(video_id, set())
            country_set.add(country)
            trending_countries[video_id] = country_set
    output(current_category, trending_countries)


if __name__ == "__main__":
    tag_reducer()
