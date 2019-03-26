#!/usr/bin/python3
import sys


class TrendingVideo(object):
    def __init__(self):
        self.video_id = None
        self.trending_date = None
        self.views = 0
        self.likes = 0
        self.dislikes = 0
        self.country = None
        self.category = None


def extract_video_info(line):
    video = TrendingVideo()
    parts = line.strip().split(',')
    video.video_id = parts[1].strip()
    video.trending_date = parts[2].strip()
    video.views = int(parts[4].strip())
    video.likes = int(parts[5].strip())
    video.dislikes = int(parts[6].strip())
    video.country = parts[7].strip()
    video.category = parts[8].strip()
    return video


def tag_mapper():
    """
    Input: i,video_id,trending_date,category_id,views,likes,dislikes,country,category
    Output: category    video_id,country
    """
    for index, line in enumerate(sys.stdin):
        if index == 0:
            continue
        video = extract_video_info(line)
        print('{key}\t{meta}'.format(key=video.category, meta='%s,%s' % (video.video_id, video.country)))


if __name__ == "__main__":
    tag_mapper()
