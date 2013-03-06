#!/bin/env python
# -*- coding:utf-8 -*-
#Contact : gaobushuang@
#Time : 2012-05-21
#Comment : 传库
#LastUpdate : 2012-06-14

from Common import *
import threading

global TASKLIST, DATALIST, THREADLIST
TASKLIST = []
DATALIST = []
THREADLIST = []

global RETDICT
RETDICT = {}

#错误及退出代码
SDICT = { 
      0: 'OK',
     -1: 'Error_Unkown',
    100: 'Error_SSH',
    101: 'Error_No_disk_free',
    102: 'Error_Get_md5file',
    200: 'Exit_Done_with_err',
    201: 'Exit_Pause'
}
 
class Task(object):
    '''每一条的tasklist record作为一个传输任务'''
    global RETDICT
    import os,Common
    def __init__(self, TID=None, dst_server=None, src_path=None, dst_path=None, stamp='xxx', maxretry=0, sln=1, speedlimit=2000, statusdir='tmp'):
        global RETDICT
        import random
        self.process = None
        self.tid = TID
        self.srcdir = src_path 
        self.server = dst_server
        self.dstdir = dst_path
        self.stamp = stamp + '_' + statusdir
        self.stamp = statusdir
        #self.stamp = stamp + '_' + str(random.uniform(1, 10))
        self.maxretry = maxretry
        self.sln = sln
        self.bwlimit = speedlimit
        self.addparms = []
        self.finish = -1
        self.error = -1
        self.failed = -1
        self.returncode = -2
        RETDICT[self.tid] = None
        self.statusdir = _STATUS_DIR + '/' + str(self.stamp) 
        self.transcmd = "cd /home/img/opbin/transbase/bin/ && python transclient.py --srcdir=" + self.srcdir + ' --dstdir=' + self.dstdir + ' --statusdir=' + str(self.stamp) + ' --md5file=' + self.srcdir + '/index.md5 ' + ' --maxretry=' + str(self.maxretry) + ' --bwlimit=' + str(self.bwlimit) + ' --sln=' + str(self.sln) + ' '
    def rexep(self, cmd=':'):
        '''向目标机器执行命:
        cd /home/img/opbin/transbase/bin/ && python thread_transclient.py \ 
            --srcdir=/user/ns-image/build-img/dbuild/merge/result/vip_180/index
            --dstdir=/home/img/opbin/transbase/bin/data  
            --statusdir=2012.05.28_1254  
            --md5file=/user/ns-image/build-img/dbuild/merge/result/vip_180/index/index.md5 
            --maxretry=2 
            --sln=1 
            --bwlimit=10000
        '''
        from subprocess import Popen,PIPE
        fullcmd = 'ssh -n -o ConnectTimeout=10 ' + self.server + ' \"' + cmd + '\" '
        #print fullcmd
        self.process = Popen(shlex.split(fullcmd), stdout=PIPE, stderr=PIPE, close_fds=True, shell=False)  

    #def poll(self):
    #    '''每一分钟检查传输命令是否执行完毕'''
    #    from time import sleep
    #    tmp = None
    #    while True:
    #        tmp = self.process.poll()
    #        if tmp is None:
    #            sleep(60)
    #        elif tmp == 0:
    #            self.returncode = self.process.returncode
    #            self.finish = 1
    #            break
    #        else:
    #            break

    def exit(self):
        from sys import exit
        exit(self.returncode)

    def getstatusoutput(self, cmd=':'):
        from commands import getstatusoutput
        #print 'cmd=\"%s\"' % cmd
        (status, dataout) = getstatusoutput(cmd)
        #status = status >> 8
        return (status, dataout)

    def start(self):
        '''start task'''
        #if not self.check_ssh():
        #    self.db_commit()
        #    return
        fullcmd = self.transcmd + ' '.join(self.addparms)
        self.rexep(fullcmd)
        #self.poll()
        #self.writeLog()
        #self.exit()
        #self.db_commit()

    def writeLog(self):
        if self.returncode == 0:
            self.error = 0
            self.failed = 0
            self.finish = 1
            LOG.info('%s 传库成功' % self.server)
        elif self.returncode == 100:
            self.error = 0
            self.failed = 1
            self.finish = 1
            LOGWF.error('%s ssh登陆失败' % self.server)
        elif self.returncode == 101:
            self.error = 0
            self.failed = 1
            self.finish = 1
            print '%s 没有足够磁盘空间' % self.server
            LOGWF.error('%s 没有足够磁盘空间' % self.server)
        elif self.returncode == 102:
            self.error = 0
            self.failed = 1
            self.finish = 1
            LOGWF.error('%s 获取index.md5失败' % self.server) 
        else:
            LOGWF.error('%s unkown error' % self.server)

    def dryrun(self):
        '''just testing'''
        self.addparms = ['--dryrun']
        cmd = self.transcmd + ' '.join(self.addparms)
        fullcmd = 'ssh -n -o ConnectTimeout=10 ' + self.server + ' \"' + cmd + '\" '
        print fullcmd

    def stop(self):
        '''取消传输'''
        if self.process is not None:
            self.process.kill()
            #update table tasklist
        else:
            ###此错误信息需写入日志文件
            print 'Warning: %s no such process' % self.server

    def check_ssh(self):
        '''check server ssh authorized'''
        (status, output) = self.getstatusoutput('ssh -n -o ConnectTimeout=10 %s ":"' % self.server)
        if status == 0:
            ret = True
        else:
            self.writeLog()
            self.failed = 1
            self.returncode = 100
            ret = False
        return ret

    def check_status(self, single_level_num=0):
        '''STATUS CODE:
            0 EXIT_SUCESS
        '''
        STATUS = {'EXIT_SUCCESS':0, 'EXIT_HELP':24, 'EXIT_CANTSTART':25, 'EXIT_PAUSED':22, 'HADOOP_FAIL':10, 'GET_LIST_FAIL':11, 'MD5_FAIL':12, 'NODIR_FAIL':13, 'GET_MD5_FAIL':14, 'CANT_CONT':16, 'TOO_MANY_RETRY':90}
        return

    def Check(self):
        import commands
        self.addparms = ['--check']
        cmd = self.transcmd + ' '.join(self.addparms)
        (status, dataout) = commands.getstatusoutput('ssh -n -o ConnectTimeout=10 %s "%s"' % (self.server, cmd.replace('"', r'\"')))
        status = status >> 8
        self.returncode = status
        print "%s - %s" %(self.server, SDICT[status])
        if self.returncode != 0:
            LOGWF.error('%s - %s' % (self.server, SDICT[status]))
        else:
            LOG.info('%s - %s' % (self.server, SDICT[status]))

    def db_commit(self):
        '''向数据库提交最新数据'''        
        DBHDLER.exe("update tasklist finished=%d, failed=%d where TID=%ld" % (self.finish, self.failed, self.TID))
        DBHDLER.exe("commit")


class DataTask(object):
    '''每一类数据作为一个DataTask实例,如vip的bs为一类'''
    import Common
    global TASKLIST, DATALIST
    def __init__(self, d_DID=None, d_dlevel=0, d_module_type='', d_cluster='', d_sln=1, d_dcomment='', d_online=None, stamp='xxx'):
        self.TaskList = []
        self.d_DID = d_DID
        self.d_dlevel = d_dlevel
        self.d_module_type = d_module_type
        self.d_cluster = d_cluster
        self.d_sln = d_sln
        self.d_dcomment = d_dcomment
        self.d_online = d_online
        self.d_stamp = stamp
        self.init_Task()
    def init_Task(self):
        TaskTuple = DBHDLER.sql("select TID, src_path, dst_server, data_total, data_uploaded, dst_path, retry, finished, failed, level, layer, speedlimit, statusdir from tasklist where dlevel='%d' and cluster='%s' and module_type='%s'" % (self.d_dlevel, self.d_cluster,  self.d_module_type))
        for i in TaskTuple:
            #print i
            (d_TID, d_src_path, d_dst_server, d_data_total, d_data_uploaded, d_dst_path, d_retry, d_finished, d_failed, d_level, d_layer, d_speedlimit, d_statusdir) = i
            ##TID=None, dst_server=None, src_path=None, dst_path=None, stamp='', maxretry=0, sln=1, speedlimit=2000
            taskobj = Task(d_TID, d_dst_server, d_src_path, d_dst_path, self.d_stamp, d_retry, self.d_sln, d_speedlimit, d_statusdir)
            self.TaskList.append( taskobj )
            TASKLIST.append( taskobj )
            

class TransBase(object):
    '''基类'''
    import re
    global TASKLIST, DATALIST, THREADLIST, RETDICT
    patt = re.compile(r'(\d{1,2}):(\d{1,2})\-(\d{1,2}):(\d{1,2})')
    def __init__(self, command=None, DID=None, TIMEWINDOW=None):
        self.COMMAND = command
        if self.COMMAND not in ['list','test','check','start','stop']: self.usage()
        if DID is not None:
            if DID == 'ALL':
                self.DID = DID
            elif DID.isdigit():
                self.DID = int(DID)
            else:
                self.usage()
        elif DID is None and COMMAND == 'list':
            self.DID = 'ALL'
        else:
            self.usage()
                
        self.TIMEWINDOW = TIMEWINDOW
        if TIMEWINDOW:
            time_tp = self.patt.match(TIMEWINDOW).groups()
            self.TIMEWINDOW = [ int(i) for i in time_tp ]
        #print self.TIMEWINDOW
        self.stamp = STAMP
        self.dataCount = getV(dbhdler.sql("select count(*) from data_info"))
        self.init_dataList()
    def init_dataList(self):
        global TASKLIST, DATALIST
        if self.COMMAND != 'list':
            if self.DID == 'ALL':
                dataList_tuple = DBHDLER.sql('select DID,dlevel,module_type,cluster,single_level_num,dcomment,online from data_info')
            elif self.DID is not None:
                dataList_tuple = DBHDLER.sql('select DID,dlevel,module_type,cluster,single_level_num,dcomment,online from data_info where DID = "%d"' % self.DID)
            else:
                print 'ERROR: DID illagel'
        elif self.COMMAND == 'list':
            if self.DID == 'ALL' or self.DID is None:
                dataList_tuple = DBHDLER.sql('select DID,dlevel,module_type,cluster,single_level_num,dcomment,online from data_info')
            else:
                #print 'select DID,dlevel,module_type,cluster,single_level_num from data_info where DID=%d' % self.DID
                dataList_tuple = DBHDLER.sql('select DID,dlevel,module_type,cluster,single_level_num,dcomment,online from data_info where DID=%d' % self.DID)
            
        for  i in dataList_tuple:
            #print i
            (i_DID, i_dlevel, i_module_type, i_cluster, i_sln, i_dcomment, i_online) = i
            datataskobj = DataTask(i_DID, i_dlevel, i_module_type, i_cluster, int(i_sln), i_dcomment, int(i_online), self.stamp)
            DATALIST.append( datataskobj )
        #print "len(TASKLIST) = %d" % len(TASKLIST)
        #print "len(DATALIST) = %d" % len(DATALIST)
        #print "RETDICT = ",RETDICT
    
    def list(self):
        #dataList_tuple = DBHDLER.sql('select DID,cluster,dcomment,online from data_info')
        print '|Note: c1 ==> JX集群,  c2 ==> TC集群'
        print '|=======================================|'
        print '| DID\t| Cluster| Comment\t| Status|'
        print '|=======================================|'
        for i in DATALIST:
            print '| %d\t| %s\t| %s\t| %s\t|' % (i.d_DID, i.d_cluster, i.d_dcomment, i.d_online)
        print '|=======================================|'

    #def execute(self, cmd=None, did=None, time=None):
    def execute(self):
        if self.COMMAND == 'list':
            self.list()   

        elif self.COMMAND == 'start':
            [ THREADLIST.append(threading.Thread(target=task.start, args=()).start()) for task in TASKLIST ]
            #self.start_poll()

        elif self.COMMAND == 'test':
            [ THREADLIST.append(threading.Thread(target=task.dryrun, args=()).start()) for task in TASKLIST ]
            #print "len(THREADLIST) = %d" % len(THREADLIST)

        elif self.COMMAND == 'stop':
            [ THREADLIST.append(threading.Thread(target=task.stop, args=()).start()) for task in TASKLIST ]

        elif self.COMMAND == 'check':
            [ THREADLIST.append(threading.Thread(target=task.Check, args=()).start()) for task in TASKLIST ]

        else:
            self.usage()

    def start_poll(self):
        '''60秒检查一次是否完成'''
        global RETDICT
        from time import sleep
        while not self.check_RETDICT():
            for task in TASKLIST:
                if task.process and task.process.poll():
                    RETDICT[task.tid] = task.process.returncode
            sleep(60)
        for task in TASKLIST:
            self.end(task)
        
    def end(self, task):
        if task.returncode == 0:
            task.error = 0
            task.failed = 0
            task.finish = 1
            LOG.info('%s success' % task.server)
        elif task.returncode == 100:
            task.error = 0
            task.failed = 1
            task.finish = 1
            LOGWF.error('%s ssh登陆失败' % task.server)
        elif task.returncode == 101:
            task.error = 0
            task.failed = 1
            task.finish = 1
            print '%s 没有足够磁盘空间' % task.server
            LOGWF.error('%s 没有足够磁盘空间' % task.server)
        elif task.returncode == 102:
            task.error = 0
            task.failed = 1
            task.finish = 1
            LOGWF.error('%s 获取index.md5失败' % task.server)
        else:
            LOGWF.error('%s unkown error' % task.server)
        task.db_commit()

    def check_RETDICT(self):
        global RETDICT
        for i in RETDICT.keys:
            if not RETDICT[i]: return False
        return True
        
    def usage(self):
        print """Transbase version 1.0
  Usage:
    ./transControl COMMAND DID [TIME]

    Args:
      COMMAND    [list|test|check|start|stop]
      DID        [DID|ALL],show DID by COMMAND-list
      TIME       example: '23:00-10:00'

Example:
    /bin/limit ./transControl list [ALL|DID]
    /bin/limit ./transControl test [ALL|DID]
    /bin/limit ./transControl start [ALL|DID]

Report bugs to <gaobushuang@>.\n"""
        sys.exit(0)

def Usage():
    import sys
    print """Transbase version 1.1
Usage:
./transControl COMMAND DID [TIME]
 Args:
  COMMAND    [list|test|check|start|stop]
  DID        [DID|ALL],show DID by COMMAND-list
  TIME       example: '23:00-10:00'

Example:
    /bin/limit ./transControl list [ALL|DID]
    /bin/limit ./transControl test [ALL|DID]
    /bin/limit ./transControl start [ALL|DID]

Report bugs to <gaobushuang@>.\n"""
    sys.exit(0)



if __name__ == '__main__':
    from Common import *
    import os, sys, optparse, Common

    __options = optparse.OptionParser()
    OPS, ARG = __options.parse_args()
    LENGTH = len(ARG)

    if LENGTH < 1 or LENGTH > 3:
        Usage()

    while LENGTH < 3:
        ARG.append(None)
        LENGTH += 1
    COMMAND = ARG[0]
    DID = ARG[1]
    TIME = ARG[2]

    _BASE_DIR = os.path.dirname(os.getcwd())
    _BIN_DIR = _BASE_DIR + '/bin'
    _CONF_DIR =  _BASE_DIR + '/conf'
    _BAK_DIR = _BASE_DIR + '/bak'
    _LOG_DIR = _BASE_DIR + '/log'
    _STATUS_DIR = _BASE_DIR + '/status'
    AOS_CMD = BIN_DIR + '/aos-lh'
    ConfFile_p = open(_CONF_DIR + '/transbase.conf.yaml','r')
    Conf_Dict = yaml.load(ConfFile_p)
    ConfFile_p.close()
    CommonDicts = CommonConfig()
    dbhdler = Database(Conf_Dict['DATABASE'])
    DBHDLER = dbhdler
    TRANS_CMD = Conf_Dict['GLOBAL'].get('trans_cmd')

    STAMP = getV(DBHDLER.sql("select statusstamp from global_config limit 1"))
    ctlstatusdir = _STATUS_DIR + '/' + STAMP
    os.system('[ -d %s ] || mkdir -p %s' %  (ctlstatusdir, ctlstatusdir))
    LOG = initlog(ctlstatusdir + '/task.log', 0)
    LOGWF = initlog(ctlstatusdir + '/task.log.wf', 0)

    print '状态目录为%s' % ctlstatusdir
    inst = TransBase(COMMAND, DID, TIME)
    #print inst.__dict__
    #inst.execute(COMMAND, DID, TIME)
    inst.execute()
