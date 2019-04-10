#!/usr/bin/python3
import sys


def extract_video_info(line):
    parts = line.strip().split(',')
    if len(parts) != 12:
        return None
    video = {
        'video_id': parts[0].strip(),
        'category': parts[3].strip(),
        'country': parts[11].strip()
    }
    return video


def mapper():
    """
    Input: video_id,trending_date,category_id,views,likes,dislikes,country,category
    Output: category    video_id,country
    """
    for index, line in enumerate(sys.stdin):
        if index == 0:
            continue
        video = extract_video_info(line)
        if video:
            print('{key}\t{value}'.format(key=video['category'], value='%s,%s' % (video['video_id'], video['country'])))


if __name__ == "__main__":
    mapper()
