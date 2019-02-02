# -*- coding: utf-8 -*-
# 此程序用来抓取 的数据
import os

import requests
import time
import random
import re
from multiprocessing.dummy import Pool
import csv
import json
import sys
from fake_useragent import UserAgent, FakeUserAgentError
import hashlib


class Spider(object):
	def __init__(self):
		# self.date = '2018-01-01'
		try:
			self.ua = UserAgent(use_cache_server=False).random
		except FakeUserAgentError:
			pass

	def get_headers(self):
		user_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0',
		               'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/20100101 Firefox/18.0',
		               'IBM WebExplorer /v0.94', 'Galaxy/1.0 [en] (Mac OS X 10.5.6; U; en)',
		               'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
		               'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
		               'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; TheWorld)',
		               'Opera/9.52 (Windows NT 5.0; U; en)',
		               'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.2pre) Gecko/2008071405 GranParadiso/3.0.2pre',
		               'Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.458.0 Safari/534.3',
		               'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.211.4 Safari/532.0',
		               'Opera/9.80 (Windows NT 5.1; U; ru) Presto/2.7.39 Version/11.00']
		user_agent = random.choice(user_agents)
		headers = {'Host': 'www.taptap.com', 'Connection': 'keep-alive',
		           'User-Agent': user_agent,
		           'Referer': 'https://www.taptap.com/app/6514/review?order=default&page=2#review-list',
		           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
		           'Accept-Encoding': 'gzip, deflate, br',
		           'Accept-Language': 'zh-CN,zh;q=0.8',
		           }
		return headers

	def p_time(self, stmp):  # 将时间戳转化为时间
		stmp = float(str(stmp)[:10])
		timeArray = time.localtime(stmp)
		otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
		return otherStyleTime
	
	def replace(self, x):
		x = re.sub(re.compile('<.*?>', re.S), '', x)
		x = re.sub(re.compile('\n'), ' ', x)
		x = re.sub(re.compile('\r'), ' ', x)
		x = re.sub(re.compile('\r\n'), ' ', x)
		x = re.sub(re.compile('[\r\n]'), ' ', x)
		x = re.sub(re.compile('\s{2,}'), ' ', x)
		return x.strip()
	
	def GetProxies(self):
		# 代理服务器
		proxyHost = "http-dyn.abuyun.com"
		proxyPort = "9020"
		# 代理隧道验证信息
		proxyUser = "HI18001I69T86X6D"
		proxyPass = "D74721661025B57D"
		proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
			"host": proxyHost,
			"port": proxyPort,
			"user": proxyUser,
			"pass": proxyPass,
		}
		proxies = {
			"http": proxyMeta,
			"https": proxyMeta,
		}
		return proxies

	def get_comments(self, ss):  # 获取某一页游戏评论
		game_url, product_number, plat_number, page = ss
		print 'page:',page
		p0 = re.compile('https://www\.taptap\.com/app/(\d+?)/review')
		game_id = re.findall(p0, game_url)[0]
		url = 'https://www.taptap.com/app/%s/review?order=default&page=%d' % (game_id, page)
		retry = 5
		while 1:
			try:
				text = requests.get(url, headers=self.get_headers(),proxies=self.GetProxies(),
				            		timeout=10).content.decode('utf-8', 'ignore')
				p = re.compile(
					u'<div class="review-item-text ">.*?rel="nofollow">(.*?)</a>.*?发布于 (.*?)".*?<div class="item-text-body".*?>(.*?)</div>.*?data-value="up".*?<span data-taptap-ajax-vote="count">(.*?)</span>(.*?)<span class="active-text">收起回复</span>',
					re.S)
				items = re.findall(p, text)
				results = []
				for item in items:
					nick_name = item[0]
					cmt_date = item[1].split()[0]
					cmt_time = item[1]
					comments = self.replace(item[2])
					like_cnt = item[3]
					if like_cnt == '':
						like_cnt = '0'
					p = re.compile(u'<span class="normal-text">回复\((\d+?)\)</span>')
					try:
						cmt_reply_cnt = re.findall(p, item[4])[0]
					except:
						cmt_reply_cnt = '0'
					long_comment = '0'
					last_modify_date = self.p_time(time.time())
					src_url = game_url
					tmp = [product_number, plat_number, nick_name, cmt_date, cmt_time, comments, like_cnt,
					       cmt_reply_cnt, long_comment, last_modify_date, src_url]
					print '|'.join(tmp)
					results.append([x.encode('gbk', 'ignore') for x in tmp])
				if len(results) > 0:
					return results
				else:
					return None
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return None
				else:
					continue
	
	def get_comments_topic(self, game_url, product_number, plat_number, page):  # 获取某一页游戏评论
		p0 = re.compile('https://www\.taptap\.com/app/(\d+?)/')
		game_id = re.findall(p0, game_url)[0]
		url = 'https://www.taptap.com/app/%s/topic?type=all&sort=commented&page=%d' % (game_id, page)
		retry = 2
		while 1:
			try:
				text = requests.get(url, headers=self.get_headers(), proxies=self.GetProxies(), timeout=10).content.decode('utf-8', 'ignore')
				p = re.compile(
					u'<div class="topic-item-text">.*?class="taptap-user-name taptap-link" rel="nofollow">(.*?)</a>.*?<p class="item-text-summary">(.*?)</p>.*?<ul class="list-inline pull-right">(.*?)</ul>.*?class="pull-left">(.*?)</span>',
					re.S)
				items = re.findall(p, text)
				results = []
				for item in items:
					nick_name = item[0]
					cmt_date = item[-1].split()[0]
					# if cmt_date < self.date:
					# 	continue
					cmt_time = item[-1] + '00:00:00'
					comments = self.replace(item[1])
					like_cnt = '0'
					p = re.compile('<i></i>.*?<span>(.*?)</span>', re.S)
					try:
						cmt_reply_cnt = re.findall(p, item[2])[0]
					except:
						cmt_reply_cnt = '0'
					long_comment = '0'
					last_modify_date = self.p_time(time.time())
					src_url = game_url
					tmp = [product_number, plat_number, nick_name, cmt_date, cmt_time, comments, like_cnt,
					       cmt_reply_cnt, long_comment, last_modify_date, src_url]
					print '|'.join(tmp)
					results.append([x.encode('gbk', 'ignore') for x in tmp])
				if len(results) > 0:
					return results
				else:
					return None
			except Exception as e:
				retry -= 1
				print e
				if retry == 0:
					print e
					return None
				else:
					continue
	
	def get_total_page(self, game_url):  # 获取网址的总页数
		p0 = re.compile('https://www\.taptap\.com/app/(\d+?)/')
		game_id = re.findall(p0, game_url)[0]
		url = 'https://www.taptap.com/app/%s/review' % game_id
		retry = 5
		while 1:
			try:
				text = requests.get(url, headers=self.get_headers(), proxies=self.GetProxies(),
				                    timeout=10).content.decode('utf-8', 'ignore')
				p0 = re.compile(u'page=(\d+?)#review-list">')
				tmp = re.findall(p0, text)
				if len(tmp) == 0:
					return 1
				else:
					total_page = tmp[-1]
					return int(total_page)
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue
	
	def get_all_review(self, game_url, product_number, plat_number):
		total_page = self.get_total_page(game_url)
		if total_page is None:
			print u'%s 抓取出错' % product_number
			return None
		else:
			print u'%s 共有 %d 页评价' % (product_number, total_page)
			ss = []
			for page in range(1, total_page + 1):
				ss.append([game_url, product_number, plat_number, page])
			pool = Pool(10)
			items = pool.map(self.get_comments, ss)
			pool.close()
			pool.join()
			mm = []
			for item in items:
				if item is not None:
					mm.extend(item)
			with open('data_comments_5.csv', 'a') as f:
				writer = csv.writer(f, lineterminator='\n')
				writer.writerows(mm)


if __name__ == "__main__":
	spider = Spider()
	s1 = []
	with open('data.csv') as f:
		tmp = csv.reader(f)
		for i in tmp:
			if 'http' in i[2]:
				s1.append([i[2], i[0], 'P26'])
	for j in s1:
		print j[1],j[0]
		if j[1] in ['F0000254']:
			spider.get_all_review(j[0], j[1], j[2])
