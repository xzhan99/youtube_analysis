#!/usr/bin/python3
import json
import sys


class TrendingVideo(object):
    video_id = None
    trending_date = None
    views = 0
    likes = 0
    dislikes = 0
    country = None
    category = None

    def __init__(self, line):
        parts = line.strip().split(',')
        self.video_id = parts[1].strip()
        self.trending_date = parts[2].strip()
        self.views = int(parts[4].strip())
        self.likes = int(parts[5].strip())
        self.dislikes = int(parts[6].strip())
        self.country = parts[7].strip()
        self.category = parts[8].strip()


def tag_mapper():
    """
    Input: i,video_id,trending_date,category_id,views,likes,dislikes,country,category
    Output: category    {"video_id": "nIYrRNklra8", "country": "CA"}
    """
    for index, line in enumerate(sys.stdin):
        if index == 0:
            continue

        video = TrendingVideo(line)
        info = {
            'video_id': video.video_id,
            'country': video.country
        }
        print('%s\t%s' % (video.category, json.dumps(info, ensure_ascii=False)))


if __name__ == "__main__":
    tag_mapper()
