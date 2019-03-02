# coding:utf-8
__author__ = 'xxj'

import requests
import time
import datetime
import os
import threading
from threading import Lock
from rediscluster import StrictRedisCluster
import json
from Queue import Empty
from requests.exceptions import ReadTimeout, ConnectionError
import Queue
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

KEYWORD_QUEUE = Queue.Queue()  # 源数据队列
DATA_QUEUE = Queue.Queue()  # 生成数据队列
KEYS_QUEUE = Queue.Queue()    # 高德key队列
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
}


class GdException(Exception):
    def __init__(self, message):
        super(GdException, self).__init__(self)
        self.message = message


def gaode(lock, key, r):
    while True:
        try:
            location = r.lpop('spider:python:gaode:keyword')  # 高德来源数据
            if location is None:
                print '数据已抓取完毕，退出'
                return False
            locations = location.split('\t')
            url = 'http://restapi.amap.com/v3/geocode/regeo?output=json&location={location}&key={key}&radius=1000&extensions=all'
            gaode_id = locations[0]
            dushu1 = float(locations[1])
            dushu2 = float(locations[2])
            # 处理（中国的经纬度范围大约为：纬度3.86~53.55，经度73.66~135.05）  大致分为小于65为纬度、大于65为经度
            if dushu1 < 65:
                weidu = '%.6f' % dushu1  # 纬度（接口要求：经纬度小数点后不要超过6位）
                jindu = '%.6f' % dushu2  # 经度
            else:
                weidu = '%.6f' % dushu2  # 纬度
                jindu = '%.6f' % dushu1  # 经度
            location = '{jindu},{weidu}'.format(jindu=jindu, weidu=weidu)  # 经度在前，纬度在后，经纬度间以“,”分割
            index_url = url.format(location=location, key=key)
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), index_url
            response = requests.get(url=index_url, headers=headers, timeout=10)
            content = response.text
            # print content
            content_json = json.loads(content)
            status = content_json.get('status')    # 状态码
            infocode = content_json.get('infocode')    # 错误码
            if status != '1':    # 高德地图接口访问异常（正常为1，错误为0）
                if infocode == '10003':    # 当该key超过日访问量时，切换key
                    print '访问已超出日访问量：', infocode
                    raise GdException('key is error')
                elif infocode == '10020':    # 抓取过快出现的情况
                    print '10020错误：', infocode
                    raise GdException('10020 error')
                elif infocode == '30000':
                    print '30000错误：', infocode
                    raise GdException('30000 error')
                else:    #
                    print '其他错误：', infocode
                    raise GdException('other error')

        except GdException as e:    # 高德接口的相关异常
            with lock:
                if e.message == 'key is error':
                    print 'key is error：', '错误码：', infocode, key
                    # KEYWORD_QUEUE.put(locations)
                    r.rpush('spider:python:gaode:keyword', locations)
                    if KEYS_QUEUE.empty():
                        print 'keys 队列为空，重新获取'
                        key_test()
                        print '重新获取队列中key的数量：', KEYS_QUEUE.qsize()
                    key = KEYS_QUEUE.get(False)
                    print '切换key：', key, '剩余key的数量：', KEYS_QUEUE.qsize()
                elif e.message == '10020 error' or e.message == '30000 error':    # 将值丢回队列中
                    print e.message, locations
                    # KEYWORD_QUEUE.put(locations)
                    r.rpush('spider:python:gaode:keyword', locations)
                elif e.message == 'other error':    # 是否需要将location丢回到队列中
                    print 'other error：', '错误码：', infocode
                    # KEYWORD_QUEUE.put(locations)

        except ReadTimeout as e:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'request_ReadTimeout', e.message, locations
            # KEYWORD_QUEUE.put(locations)
            r.rpush('spider:python:gaode:keyword', locations)

        except ConnectionError as e:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'ConnectionError', e.message, locations
            # KEYWORD_QUEUE.put(locations)
            r.rpush('spider:python:gaode:keyword', locations)

        except BaseException as e:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'BaseException', locations, e.message
            # KEYWORD_QUEUE.put(locations)

        else:
            content_json.update({'gaode_id': gaode_id})
            new_content = json.dumps(content_json, ensure_ascii=False)
            # DATA_QUEUE.put(new_content)
            # file_write(fileout, lock, new_content)    # 文件接口
            print '内容：{}'.format(new_content)
            r.lpush('spider:python:gaode:keyword:dest', new_content)


def key_test():
    '''
    key的筛选
    :return:
    '''
    # keys = ['25383cccf3df39102b7e1744c23b0431', '424e22f9c74be89106c51be72aa94bbc', 'b7874830c4d1e734321b4102f1e82f75',
    #         '6d368e92c8f6745e5712de298c742191', '488f8be228c608c04ae35fb735b0b36b', '5467ccdf46c172d40c4c5b9378a8f1cf',
    #         '6913e421bbf55b3aa6e0cdc03ac7cbab', '4f292524514d3a49bf4eb0e2300f4959', '26cf68d6269a4e6d96b02cdd0cbec3d3',
    #         '6df813df911d9a39ef9e6ae9e4216912', '5cea4decd023d2f7ffcaefc0a6a00a2b', 'dc60e70c4e35cf5b74e4f2836161d516',
    #         '1a8316b77d7d4ec7442f7ef3a7b5115d', '67061f231016336249d98f42dfb703e9', '255324f8a4d256f2a647fb99252dd2d4',
    #         'aeffcbc9e3a8e493e7ab6d59d19b9d88', '3b2c433e11ccb66600f81bd55a206a93', '7120a93907ce387a0e1c6f420dd99f96',
    #         'c0b470a145c16b2cc27bf005dde7617c', 'd7e9764e21519bbfdf4e9e232e1782fa', '5243143851a955ae7ecaf80bc5c3bc55',
    #         'da0ba8d0672420138d5c8cd566844ba3', '40e66158c44eecb3bd599b3f70395f63', '86e6e23ec48a6fd83a94ece7292e3732',
    #         '5d57ba63cfb2bace96e81c764229adb6', 'e7e553c9a43cac6a9bc5a3c9c8fdf565', 'f9b67ab747d9a3868f3b9607c11aeebd',
    #         'd0e29392dfc0d759f72101675696e7c2', '964b53e9880622418fe2996a9f715be2', 'd52d513a0a25c22915ce62e236988788',
    #         '82a1a207cfe3b28e06f8aaf81777cab6', '9ec4d1fb45b2c03fd0197c1b49f30058', '02a487373264e93312089d0477855dc9',
    #         'a101ffa46311b8a94a998e87797dca5b', '6bca591ed4634258b93787280e4279a9', '2de10bb84ef0f6af03f5a35a711ba04a',
    #         '9342aea2a81b8c5e274a91cb627b787b', 'e356246259bc009598dd65e1532a9080', '3924cca7784e605f6ebeec991d1fe182',
    #         '244af1a9452412b24309dd42e915d083', '58dd12fca96ed3924fd459a60c1e009b', '2c6e0767cf00464f6568659db22587b2',
    #         '7d7d31fa61fa1de2b16756d760534632', '8760c40e0d4e44ea1b498e776e50e7af', 'edaaf806744c6fddc7b0a1ad9fc1e44e',
    #         '9f42805bf4dbc1ce6766f9da841da6f5', '9f48c56eaf260dc9cf24d65e2bbdf646', '7d5d2e8caa3b7a3abf19b5f517602088',
    #         'd82d82ad64804c6d76db2d9db55501af', '067ee5b07e7e30ea310c3072f48975ea', '3f8e0fa790bec9b223d44f8a90acf70a',
    #         '791883ed40eeadeb1323ae3beeb67b09', 'dd3d2462f0b7da17ad6b222d51017658', '36fe6a42047590960cef541f47bc2720',
    #         'ac4676d97cdac5cdbc658308363ed286', '4ab209d0caff513d6aa48ada2900a5c5', '84b3222f3b308fc42b713e0e002012f3',
    #         'b7022bef292d038b36056989a2941b53', '21b1eb989d097a604093e877206a77cc', 'a141ff024fb8ac872df60d83424e54e1',
    #         '94327cefd9e2d7891766b13a243a7295', '9227bd8d9f82cfca2c45a36208d74e61', 'c501f537ffb4c4f18aabdbcaf86dcafa',
    #         '044cc0df75f3512413c93cdf0f885e4c', 'ea69e664b522f4208fd8bd912a4969b7', '344ffc3e14f4e7f2d2dd985888af1f64',
    #         'c89da6e0b5cd5dd2dff68a31f9518b63']

    keys = ['adad273010d2636584373d582c4d1854', 'ac1ab9244cc9979b72e353fd7d800ffe', '67c9df44fb9abd13e9f314a5c163c10b',
            '837efddf1701a35b63ef2ffcb00428bb', '133fa4477c061684d54dbacbd5f726ec', 'c6685d7bb9e1d363f3a079c19027221e',
            'dd02c028c2d9597008717a6c12d9ffcf', '59c7639b00cf426d73a67275bc013784', 'e48d01726724a622ecc119f19749ffa7',
            'ebd39e965853b17cfbf03cfa2060079c']

    for key in keys:
        # url = 'http://restapi.amap.com/v3/geocode/regeo?output=json&location=113.414081,23.163472&key={key}&radius=1000&extensions=all'.format(key=key)
        # print time.strftime('[%Y-%m-%d %H:%M:%S]'), url
        # try:
        #     response = requests.get(url=url, headers=headers, timeout=20)
        # except BaseException as e:
        #     print 'KEYS BaseException', e.message
        # response_json = json.loads(response.text)
        # if response_json.get('status') == '1':
        KEYS_QUEUE.put(key)
    print '一共有{key}个key'.format(key=len(keys)), '有效的key有{new_key}个'.format(new_key=KEYS_QUEUE.qsize())


def main():
    lock = Lock()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start'

    startup_nodes = [{'host': 'redis1', 'port': '6379'}]
    r = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    print 'redis中gaode_keyword来源列表长度：', r.llen('spider:python:gaode:keyword')

    key_test()  # 将有效的key放入KEYS_QUEUE
    key = KEYS_QUEUE.get()
    print '获取的第一个key：', key

    threads = []
    for i in xrange(50):
        t = threading.Thread(target=gaode, args=(lock, key, r))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '抓取结束'


if __name__ == '__main__':
    main()


