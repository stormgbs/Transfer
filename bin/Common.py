#!/bin/env python
# -*- coding:utf-8 -*-
# Contact: gaobushuang@
# Comment: ����ģ��
#LastUpdate : 2012-06-14


#���ӻ�������·��
import sys
_new_path_appended = ['/usr/local/bin', '/usr/local/lib/python2.7/site-packages/PyYAML-3.09-py2.7-linux-x86_64.egg', '/usr/local/lib/python2.7/site-packages/ipython-0.10.1-py2.7.egg', '/usr/local/lib/python2.7/site-packages/setuptools-0.6c12dev_r88846-py2.7.egg', '/usr/local/lib/python2.7/site-packages/Fabric-1.3.4-py2.7.egg', '/usr/local/lib/python2.7/site-packages/ssh-1.7.11-py2.7.egg', '/usr/local/lib/python2.7/site-packages/pycrypto-2.5-py2.7-linux-x86_64.egg', '/usr/local/lib/python2.7/site-packages/MySQL_python-1.2.3-py2.7-linux-x86_64.egg', '/usr/local/lib/python27.zip', '/usr/local/lib/python2.7', '/usr/local/lib/python2.7/plat-linux2', '/usr/local/lib/python2.7/lib-tk', '/usr/local/lib/python2.7/lib-old', '/usr/local/lib/python2.7/lib-dynload', '/usr/local/lib/python2.7/site-packages', '/usr/local/lib/python2.7/site-packages/ipython-0.10.1-py2.7.egg/IPython/Extensions', '/home/img/.ipython']
sys.path += _new_path_appended

import os, yaml, time, datetime, commands, MySQLdb, shlex, subprocess, logging, re
from UserDict import UserDict
import os.path

import urllib,httplib

##ȫ����Ҫ����

BASE_DIR = os.path.dirname(os.getcwd())
BIN_DIR = BASE_DIR + '/bin'
CONF_DIR =  BASE_DIR + '/conf'
BAK_DIR = BASE_DIR + '/bak'
LOG_DIR = BASE_DIR + '/log'
BAK_DB_FILE = BAK_DIR + '/transbase.sql.' + '_'.join(str(datetime.datetime.now()).split())
AOS_CMD = BIN_DIR + '/aos-lh'
ConfFile_p = open(CONF_DIR + '/transbase.conf.yaml','r')
#ConfFile_p = open(_CONF_DIR + '/2.conf.yaml','r')
Conf_Dict = yaml.load(ConfFile_p)
ConfFile_p.close()

class CommonConfig(object):
    """�������� """
    _inst = None
    def __new__(cls,var={}):
        if cls._inst is None:
            cls._inst = object.__new__(cls)
            cls._inst.data = UserDict(var)
        return cls._inst
    def update(self,data):
        self.data.update(data)
    def get(self,key):
        return self.data.get(key, None)

def g_cwd():
    import os
    _cwd = os.getcwd()
    if not os.path.exists(_cwd + '/bak'):
        os.mkdir(_cwd + '/bak')
    return _cwd

def get_cluster(isVM='', istring=''):
    '''��ȡ�ַ������ڼ�Ⱥ������Ŀǰ���ʺ�'''
    if isVM == 'vm': return 'vm'
    #IdcMapDict = {'ai': 'c1', 'yf':'c1','cq01': 'c1', 'db': 'c2', 'jx': 'c1', 'm1': 'c2', 'tc': 'c2', 'vm':'vm'}
    CMapDict = {'c1':{'c1','ai','cq','jx','yf'},'c2':{'c2','tc','db','m1'},'vm':{'vm'}}
    ClusterName = None
    strtmp = istring.lower()
    for i in CMapDict.keys():
        for j in CMapDict[i]:
            if j in strtmp:
                ClusterName = i
                break
        if ClusterName is not None: break
    return ClusterName

#def get_module_t(istring = ''):
#    '''��ȡģ������'''
#    return 0

#def get_svr_re(istring = ''):
#    '''xxx'''
#    tmp = None
#    tmp = PATT.match(istring)
#    if tmp: tmp = tmp.groups()
#    # type : tuple, format (module, row, layer)
#    return tmp

#def fetch_servers_list(nodepath = ''):
#    '''return list() �ӽڵ㷵�ط������б�aos-lh����ռ��ϵͳ��ʱ�ϳ�'''
#    cmdstr = AOS_CMD + ' -machine ' + nodepath
#    print "cmdstr=%s" % cmdstr
#    (status, output) = commands.getstatusoutput(cmdstr)
#    print "status=%d" % status
#    if status != 0: print 'Error: function fetch_servers_list()  - commands.getstatusouput() failed N1!'
#    return output.split()

def fetch_servers_list(nodepath = ''):
    '''return list() �ӽڵ㷵�ط������б�aos-lh����ռ��ϵͳ��ʱ�ϳ�'''
    conn = httplib.HTTPConnection('xxx.com')
    queryurl = 'http://xxx.com/aos-lh/index.php?r=ListHost/getInformation&target=machine&filter=%s' % nodepath
    conn.request('GET', queryurl)
    response = conn.getresponse()
    datastr = response.read()
    #print datastr
    conn.close()
    datalist = datastr.split()
    retstatus = datalist[0]
    retlist = datalist[1:]
    if retstatus != '0': print 'Error: function fetch_servers_list()  - commands.getstatusouput() failed N1!'
    return retlist

#def fetch_nodes_list(nodepath=''):
#    '''return list(),��������ڵ��б�aos-lhռ��ϵͳ��ʱ�ϳ�'''
#    (status, output) = commands.getstatusoutput('%s -su %s' % (AOS_CMD, nodepath))
#    if status != 0: print 'Error: function - commands.getstatusouput() failed N2!'
#    return output.split()

def fetch_nodes_list(nodepath=''):
    '''return list(),��������ڵ��б�aos-lhռ��ϵͳ��ʱ�ϳ�'''
    (status, output) = commands.getstatusoutput('%s -su %s' % (AOS_CMD, nodepath))
    if status != 0: print 'Error: function - commands.getstatusouput() failed N2!'
    return output.split()



def human_dtuple(ituple=(())):
    '''((xx,),((yy,),((zz,),...((mm,)) --> (xx, yy, zz, ..., mm)'''
    tmp_tuple = ()
    for i in ituple:
        tmp_tuple += i
    return tmp_tuple

def getV(ituple=(())):
    '''((xx,)) --> xx'''
    tmpx = None
    if len(ituple) == 1 and len(ituple[0]) == 1:
        tmpx = ituple[0][0]
    return tmpx

def Color(msg='', fg='green', bg='black', style='reset'):
    '''From shuyu'''
    color_map = dict(black=0, red=1, green=2, yellow=3, blue=4, magenta=5, cyan=6, white=7, reset=9)
    style_map = dict(reset=0, bold=1, dim=2, underscore=4, blink=5, reverse=7, conceal=8, underline=4)
    return '\033[%d;3%d;4%dm%s\033[0m' % (style_map[style], color_map[fg], color_map[bg] , msg)

class Database(UserDict):
    '''���ݿ�������'''
    conn = None
    cursor = None
    def __init__(self, data):
        UserDict.__init__(self, data)
        try:
            self.conn = MySQLdb.connect(host=self.get('host'), user=self.get('user'), passwd=self.get('pwd'), db=self.get('db'), port=self.get('port'))
        except Exception, err:
            print err
            sys.exit()
        self.cursor = self.conn.cursor() 
    def __del__(self):
        self.cursor.close()
        self.conn.close()
    def exe(self, sql_str=''):
        return self.cursor.execute(sql_str)
    def show_all(self):
        return self.cursor.fetchall()
    def sql(self, sql_str=''):
        self.exe(sql_str)
        return self.show_all()
    def dbdump(self):
        '''���ݿⱸ��'''
        (rstatus, routput) = commands.getstatusoutput('/usr/local/bin/mysqldump -h%s -u%s -p%s  -P%d %s > %s' % (self.get('host'), self.get('user'), self.get('pwd'), self.get('port'), self.get('db'), BAK_DB_FILE))
        if rstatus != 0:
            print 'mysql dump failed, trans aborded !'
            sys.exit()
        return 0
    def dbflush(self):
        '''������ݿ���Խ�����ȫ��ʼ��'''
        if self.dbdump() !=0 :
            print 'db dump failed, flush canceled and exit!'
            sys.exit()
        t_list = [ str(i[0]) for i in self.sql('show tables') ]
        for i in t_list:
            self.exe('delete from %s' % i)
            self.exe('alter table %s auto_increment = 1' % i)
    def dbrecovery(self, bak_file):
        '''���ݿ�ָ�'''
        print 'xxx'

##��־�������ɺ���
def initlog(logfile='./tmp.log', level=0):
    import logging
    logger = logging.getLogger()
    loghdlr = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(message)s')
    loghdlr.setFormatter(formatter)
    logger.addHandler(loghdlr)
    logger.setLevel(level)
    return logger
#logobj = initlog()

##ȫ����Ҫ����
CommonDicts = CommonConfig()
dbhdler = Database(Conf_Dict['DATABASE'])
DBHDLER = dbhdler
TRANS_CMD = Conf_Dict['GLOBAL'].get('trans_cmd')
#PARAMS_TO_TRANS = Conf_Dict['GLOBAL'].get('params_to_trans')

# vi: expandtab ts=4
