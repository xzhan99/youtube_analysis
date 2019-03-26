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

    @classmethod
    def extract_video_info(cls, line):
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
    Output: video_id,country    trending_date,likes,dislikes
    """
    for index, line in enumerate(sys.stdin):
        if index == 0:
            continue

        video = TrendingVideo.extract_video_info(line)
        key = '%s,%s' % (video.video_id, video.country)
        meta = ','.join((video.category, video.trending_date, str(video.likes), str(video.dislikes)))
        print('{key}\t{meta}'.format(key=key, meta=meta))


if __name__ == "__main__":
    tag_mapper()
