# coding:utf-8
__author__ = 'xxj'

from rediscluster import StrictRedisCluster
import os
import datetime
import time


def file_write_redis():
    '''
    将源文件中的内容写入redis中
    :return:
    '''
    gaode_file_path = file_exists()    #
    # fanyule_two_game_file_path = r'/ftp_samba/112/file_4spider/dmn_fanyule2_game/'    #
    startup_nodes = [{'host': 'redis1', 'port': '6379'}]
    r = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)

    # 高德
    gaode_file = open(gaode_file_path, 'r')
    gaode_keyword_length = r.llen('spider:python:gaode:keyword')    # redis中gaode的数据量
    print 'redis中gaode_keyword列表长度：', gaode_keyword_length
    if gaode_keyword_length != 0:
        r.delete('spider:python:gaode:keyword')
        print 'redis中gaode_keyword列表长度不为0, 删除后的列表长度：', r.llen('spider:python:gaode:keyword')
    for line in gaode_file:
        new_line = line.strip()
        if new_line:
            r.rpush('spider:python:gaode:keyword', new_line)
    gaode_keyword_length = r.llen('spider:python:gaode:keyword')
    print '重新写入后redis中gaode_keyword列表长度：', gaode_keyword_length


def file_exists():
    '''
    当文件不存在时，获取已有最新的源文件
    :return:
    '''
    yesterday = datetime.date.today() + datetime.timedelta(-1)
    date = yesterday.strftime('%Y%m%d')
    gaode_file_dir_path = '/ftp_samba/112/file_4spider/gd_location'  # 线上高德源数据文件目录
    gaode_file_path = os.path.join(gaode_file_dir_path, 'gd_{date}_1.txt'.format(date=date))  # 线上高德源数据文件路径
    print '源文件路径：', gaode_file_path
    if not os.path.exists(gaode_file_path):    # 当前文件不存在，获取前一天的最后一个文件
        gaode_file_name = os.listdir(gaode_file_dir_path)[-1]
        gaode_file_path = os.path.join(gaode_file_dir_path, gaode_file_name)
        print '获取最新的来源文件路径：', gaode_file_path
    return gaode_file_path


def main():
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start'
    start_time = time.time()
    file_write_redis()
    # file_exists()
    end_time = time.time()
    print '将文本中的关键词导入redis中花费的时间：', end_time-start_time


if __name__ == '__main__':
    main()
