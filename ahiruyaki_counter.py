# coding: UTF-8

import os
import sys
import json
import re
import urllib2
import datetime
import time
import ConfigParser
import socket
#import simplejson
import struct
import string
import tweepy

# ZABBIX Sender
class ZabbixSender:
    zbx_header = 'ZBXD'
    zbx_version = 1
    zbx_sender_data = {u'request': u'sender data', u'data': []}
    send_data = ''

    def __init__(self, server_host, server_port = 10051):
        self.server_ip = socket.gethostbyname(server_host)
        self.server_port = server_port

    def AddData(self, host, key, value, clock = None):
        add_data = {u'host': host, u'key': key, u'value': value}
        if clock != None:
            add_data[u'clock'] = clock
        self.zbx_sender_data['data'].append(add_data)

#        print self.zbx_sender_data['data']
        return self.zbx_sender_data

    def ClearData(self):
        self.zbx_sender_data['data'] = []

        return self.zbx_sender_data

    def __MakeSendData(self):
        zbx_sender_json = json.dumps(self.zbx_sender_data, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
        json_byte = len(zbx_sender_json)
        self.send_data = struct.pack("<4sBq" + str(json_byte) + "s", self.zbx_header, self.zbx_version, json_byte, zbx_sender_json)
#        print self.send_data

    def Send(self):
        self.__MakeSendData()
        so = socket.socket()
        so.connect((self.server_ip, self.server_port))
        wobj = so.makefile(u'wb')
        wobj.write(self.send_data)
        wobj.close()
        robj = so.makefile(u'rb')
        recv_data = robj.read()
        robj.close()
        so.close()
        tmp_data = struct.unpack("<4sBq" + str(len(recv_data) - struct.calcsize("<4sBq")) + "s", recv_data)
        recv_json = json.loads(tmp_data[3])
#        print recv_json

        return recv_data


class ZabbixAPI(object):
    # ZABBIX Server APIのURL
    zbx_url = ""
    # APIを利用するユーザーID
    zbx_userid = ""
    # パスワード
    zbx_passwd = ""
    #認証キー
    zbx_auth = ""
    # HTTPHEADER
    headers = {"Content-Type":"application/json-rpc"}

    # グラフサイズ width:800
    zbx_gwidth = "800"

    # グラフサイズ height:300
    zbx_gheight = "300"

    # グラフ枠線 デフォルト:なし
    zbx_gborder = "0"

    # auth key 発行用関数
    # 戻り値:ZABBIX API auth key
    def auth(self):
        # json 認証用生成
        auth_post = json.dumps({
            'jsonrpc': '2.0',
            'method': 'user.login',
            'params': {
                'user': self.zbx_userid,
                'password': self.zbx_passwd},
            'auth':None,
            'id': 1})
        # urllib2 へのリクエスト生成
        req = urllib2.Request(self.zbx_url, auth_post, self.headers)
        # リクエストを送信
        f = urllib2.urlopen(req)
        # コンテンツを受け取る
        str_value = f.read()
        # セッションを閉じる
        f.close()
        # コンテンツをJSONに整形
        value = json.loads(str_value)
        # 認証成功ならば authkey を返す
        try:
            self.zbx_auth = value["result"]
            return value["result"]
        # だめだったら認証失敗を返す
        except:
            print "Authentication failure"
            return 0
            quit()
        storage.close()

    def send(self, json_data):
        # 認証キーの追記
        #json_data["auth"] = self.zbx_auth
        # json 化
        #req_json = json
        #  urllib2 へのリクエスト生成
        req = urllib2.Request(self.zbx_url, json_data, self.headers)
        # リクエストを送信
        f = urllib2.urlopen(req)
        # コンテンツを受け取る
        str_value = f.read()
        # セッションを閉じる
        f.close()
        # コンテンツをJSONに整形
        dict_value = json.loads(str_value)
        return dict_value

    # cokkie 取得用login関数
    # 戻り値:cokkieに入れる認証トークン
    def login(self, user, passwd):
        json_login = json.dumps({
            "jsonrpc":"2.0",
            "method":"user.login",
            "params":{
                "user":user,
                "password":passwd},
            "id":1})
        sessionid = self.send(json_login)
        cookie = sessionid["result"]
        cookie = 'zbx_sessionid=' + cookie
        return cookie

    # グラフ取得
    def get_graph(self, cookie, graphid, period, stime):
        opener = urllib2.build_opener()
        opener.addheaders.append(("cookie",cookie))
        graph_url = self.zbx_url.replace("api_jsonrpc", "chart2")
        graphi_get_url = "%s?graphid=%s&width=%s&height=%s&border=%s&period=%s&stime=%s" % (
                            graph_url,
                            graphid,
                            self.zbx_gwidth,
                            self.zbx_gheight,
                            self.zbx_gborder,
                            period,
                            stime)
        graph = opener.open(graphi_get_url)
        return graph

def run_zbxapi(reqjson):
    returndata = zbx_api.send(reqjson)
    result =  returndata["result"]
    if len(result) == 1:
        return result
    else:
        print "error", reqjson, result
        #print returndata
        exit()

def authorize(conf):
    """ Authorize using OAuth.
    """
    auth = tweepy.OAuthHandler(conf.get("twitter","consumer_key"), conf.get("twitter","consumer_secret"))
    auth.set_access_token(conf.get("twitter","access_key"), conf.get("twitter","access_secret"))
    return auth

def create_zbx_item(tweetid, zbx_api, zbx_auth_key):
    item_key = "ahiruyaki.hcount." + tweetid
    reqdata = json.dumps({
        "jsonrpc": "2.0",
        "method": "item.get",
        "params": {
            "hostids": "10107",
#            "output": "extend",
            "search": {
                "key_": item_key}
        },
        "auth":zbx_auth_key,
        "id": 1})
    zbx_item_check_result = zbx_api.send(reqdata)

    if len(zbx_item_check_result["result"]) == 0:
        attweetid = "@" + tweetid
        reqdata = json.dumps({
            "jsonrpc": "2.0",
            "method": "item.create",
            "params": {
                "name": attweetid,
                "key_": item_key,
                "hostid": "10107",
                "type": 2,
                "value_type": 3,
            },
        "auth":zbx_auth_key,
        "id": 1})
        zbx_item_create_result = zbx_api.send(reqdata)
#        itemid = zbx_item_create_result["result"]["itemids"][0]
#        print itemid
        #time.sleep(60)
        return zbx_item_create_result
    else:
        return zbx_item_check_result

def put_zbx_sender(zbxsvip, zbx_key, hostip, sendvalue):
    sender = ZabbixSender(zbxsvip)
    sender.AddData(hostip, zbx_key, sendvalue)
    try:
        sender.Send()
    except:
        print "[ERROR] host: %s  value: %s"%(hostip,sendvalue)
    sender.ClearData()

if __name__ == '__main__':
    config_file_path = "config.ini"
    conf = ConfigParser.SafeConfigParser()
    conf.read(config_file_path)

    # zabbix api login
    zbx_api = ZabbixAPI()
    zbx_api.zbx_url = conf.get("zabbix","url")
    zbx_api.zbx_userid = conf.get("zabbix","userid")
    zbx_api.zbx_passwd = conf.get("zabbix","passwd")
    # get zabbxi api cookie
    zbx_auth_key = zbx_api.auth()

    oneoldtime = datetime.datetime.utcnow() - datetime.timedelta(hours = 1)
    start_time = datetime.datetime(
        int(oneoldtime.strftime("%Y")),
        int(oneoldtime.strftime("%m")),
        int(oneoldtime.strftime("%d")),
        int(oneoldtime.strftime("%H")),
        0,0,0)
    end_time = datetime.datetime(
        int(oneoldtime.strftime("%Y")),
        int(oneoldtime.strftime("%m")),
        int(oneoldtime.strftime("%d")),
        int(oneoldtime.strftime("%H")),
        59,59,999999)

    twdate = start_time + datetime.timedelta(hours = 9)
#    print type(twdate.strftime("%Y年%m月%d日%H時"))
#    print end_time
    yakishi_list = {}

    postdata = unicode(twdate.strftime("%Y年%m月%d日%H時台のあひる焼きカウンター\n"),'utf-8', 'ignore')
    auth = authorize(conf)
    api = tweepy.API(auth_handler=auth)
    keywords = [u"あひる焼き", u"-RT"]
    query = ' AND '.join(keywords)
    for tweet in api.search(q=query, count=1000):
#        print type(tweet.created_at)
        textdata = tweet.text.encode('utf-8')
        if textdata.find("あひる焼き") != -1  and textdata.find("あひる焼きカウンター") == -1:
            if start_time < tweet.created_at < end_time :
                if not tweet.user.screen_name in yakishi_list:
                    itemdata = create_zbx_item(tweet.user.screen_name, zbx_api, zbx_auth_key)
                    yakishi_list[tweet.user.screen_name] = 1
                else:
                    yakishi_list[tweet.user.screen_name] += 1
#                print tweet.created_at, tweet.user.screen_name, tweet.text

    for id, count in yakishi_list.items():
        item_key = "ahiruyaki.hcount." + id
        put_zbx_sender(conf.get("zabbix","ip"), item_key, "ahiruyaki", 1)
        postdata = postdata +  u"@" + id + ": " + str(count) + u"焼き\n"

    #post twitter
#    print postdata
    api.update_status(postdata)

