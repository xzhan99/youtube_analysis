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
        video.video_id = parts[0].strip()
        video.trending_date = parts[1].strip()
        video.category = parts[3].strip()
        video.views = int(parts[5].strip())
        video.likes = int(parts[6].strip())
        video.dislikes = int(parts[7].strip())
        video.country = parts[11].strip()
        return video


def mapper():
    """
    Input: i,video_id,trending_date,category_id,views,likes,dislikes,country,category
    Output: category    video_id,country
    """
    for index, line in enumerate(sys.stdin):
        if index == 0:
            continue
        video = TrendingVideo.extract_video_info(line)
        print('{key}\t{meta}'.format(key=video.category, meta='%s,%s' % (video.video_id, video.country)))


if __name__ == "__main__":
    mapper()
