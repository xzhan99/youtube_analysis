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
    def read_map_output(cls, line):
        video = TrendingVideo()
        key, meta = line.strip().split('\t')
        video.video_id, video.country = key.split(',')
        meta_parts = meta.split(',')
        video.category = meta_parts[0]
        video.trending_date = meta_parts[1]
        video.likes = int(meta_parts[2])
        video.dislikes = int(meta_parts[3])
        return video


def output(video_list):
    if len(video_list) >= 2:
        video_list = sorted(video_list, key=lambda x: x.trending_date)
        gap = (video_list[1].dislikes - video_list[0].dislikes) - (video_list[1].likes - video_list[0].likes)
        if gap > 0:
            print(video_list[0].video_id, gap, video_list[0].category, video_list[0].country)


def tag_reducer():
    """
    Input: video_id,country    trending_date,likes,dislikes
    Output: video_id,gap_between_increase_of_dislikes_and_likes,category,country
    """
    current_key = ''
    video_list = []
    for line in sys.stdin:
        if len(line.strip()) < 1:
            continue
        key = line.strip().split('\t')[0]
        if not current_key:
            current_key = key
        elif current_key != key:
            # meeting a new key means all data with previous key has been processed
            output(video_list)
            video_list.clear()
            current_key = key
        video_list.append(TrendingVideo.read_map_output(line))
    output(video_list)


if __name__ == "__main__":
    tag_reducer()
