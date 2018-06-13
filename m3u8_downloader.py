#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
下载m3u8格式视频的小组件，使用python3 asyncio
使用ffmpeg合并ts成mp4
"""
import asyncio
import os
import shutil
import subprocess
from asyncio import Queue
from optparse import OptionParser

import aiohttp
import async_timeout
import requests

ffmpeg_path = "/opt/local/bin/ffmpeg"

headers = {}


def extract_pre(url):
    return "/".join(url.split("/")[:-1])


class M3u8Parser(object):
    """
    解析m3u8文件
    """

    def __init__(self, url):
        self.url = url
        self.url_pre = extract_pre(url)
        self._ts_list = []
        self._parse_m3u8()

    @property
    def ts(self):
        return self._ts_list

    def _parse_m3u8(self):
        body = requests.get(self.url, headers=headers).text
        lines = body.split("\n")
        ts_list = []
        for line in lines:
            if not line or line.startswith("#"):
                continue
            if not line.endswith("ts"):
                raise ValueError("parse error {line}".format(line=line))
            if line.startswith("http"):
                ts_list.append(line)
            else:
                ts_list.append("%s/%s" % (self.url_pre, line.replace("./", "")))
        self._ts_list = ts_list


class M3u8Downloader(object):
    def __init__(self, url, path, worker_num, ts_timeout=120, loop=None):
        self.path = path
        self.ts_timeout = ts_timeout
        self.cache_dir = path + ".cache"
        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)
        self.loop = loop or asyncio.get_event_loop()
        self.ts_list = M3u8Parser(url).ts
        self.worker_num = worker_num
        self.q = Queue()
        self._ts_name_list = []
        self._fill_q()

    def _fill_q(self):
        for index, ts in enumerate(self.ts_list):
            name = str(index) + ".ts"
            self.q.put_nowait((name, ts))
            self._ts_name_list.append(name)

    async def worker(self):
        async with aiohttp.ClientSession() as session:
            while not self.q.empty():
                name, ts = self.q.get_nowait()
                path = os.path.join(self.cache_dir, name)
                if os.path.exists(path):
                    print("{name} already exists".format(name=name))
                else:
                    try:
                        await self.download(session, path, ts)
                    except:
                        print("retry download {name}".format(name=name))
                        self.q.put_nowait((name, ts))
                self.q.task_done()

    async def download(self, session, path, ts):
        print("start download {p} / {total}".format(p=path, total=len(self.ts_list)))
        async with async_timeout.timeout(self.ts_timeout):
            async with session.get(ts, headers=headers) as response:
                with open(path + ".downloading", "wb") as fd:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        fd.write(chunk)
                os.rename(path + ".downloading", path)

    async def run(self):
        # download all .ts
        print("start download...")
        workers = [asyncio.Task(self.worker(), loop=self.loop) for _ in range(self.worker_num)]
        await self.q.join()
        for w in workers:
            w.cancel()
        print("download complete , start merge...")
        self.merge_all_ts()
        print("remove .ts ...")
        shutil.rmtree(self.cache_dir)

    def merge_all_ts(self):
        command = '{ffmpeg_path} -y -i concat:{path_list} -acodec copy -vcodec copy -absf aac_adtstoasc {output}'.format(
            path_list="|".join(
                [os.path.join(self.cache_dir, name) for name in self._ts_name_list]
            ), ffmpeg_path=ffmpeg_path, output=self.path).split(" ")
        p = subprocess.Popen(command)
        p.wait()


def main(url, path, worker_num, ts_timeout):
    print(url)
    global headers
    headers["Referer"] = url
    headers[
        "User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"
    loop = asyncio.get_event_loop()
    downloader = M3u8Downloader(url=url, path=path, worker_num=worker_num, ts_timeout=ts_timeout, loop=loop)
    loop.run_until_complete(downloader.run())


if __name__ == '__main__':
    # m3u8_url = "http://bobo.okokbo.com//ppvod/CF8BB8CFAAF7FEF62C21B6B4B11DAD9A.m3u8"
    # main(m3u8_url, path="/tmp/result1231.mp4", worker_num=32, ts_timeout=120)

    parser = OptionParser()
    parser.add_option('-i', '--input', dest='url',
                      help='m3u8 url')
    parser.add_option('-p', '--path', dest='path',
                      help='path for save video')
    parser.add_option('-w', '--worker', dest='worker_num',
                      type='int',
                      default=24,
                      help='worker_num')
    parser.add_option('-t', '--timeout', dest='timeout',
                      type='int',
                      default=120,
                      help='download ts timeout')
    parser.add_option('-f', '--ffmpeg', dest="ffmpeg_path",
                      default="/opt/local/bin/ffmpeg",
                      help='path of ffmpeg'
                      )
    (options, args) = parser.parse_args()
    if not options.url or not options.path:
        print("need url and path ")
    else:
        ffmpeg_path = options.ffmpeg_path
        main(url=options.url, path=options.path, worker_num=options.worker_num, ts_timeout=options.timeout)
