#!/bin/env python
# -*- coding:utf-8 -*-
#Contact : gaobushuang@
#Time : 2012-05-29
#Last Update: 2012-08-06 12:00
#Comment :  client

from Queue import Queue
import threading


#错误及退出代码
SDICT = {
      0: 'OK',
     -1: 'Error_Unkown',
    100: 'Error_SSH',
    101: 'Error_No_disk_free',
    102: 'Error_Get_md5file',
    200: 'Exit_Done_with_err',
    201: 'Exit_Pause',
    700: 'Exit_File_Trans_Md5_OK',
    701: 'Error_SourceFile_NotExist',
    702: 'Error_DestDirectory_NotExist',
    703: 'Error_Md5error',
    704: 'Error_MaxRetry'
}

##日志对象生成函数
def initlog(logfile='./tmp.log', level=0):
    import logging
    logger = logging.getLogger()
    loghdlr = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(message)s')
    loghdlr.setFormatter(formatter)
    logger.addHandler(loghdlr)
    logger.setLevel(level)
    return logger

class Task(object):
    '''每一条的tasklist record作为一次传输任务'''
    import os
    from Queue import Queue
    import threading
    _BASE_DIR = os.path.dirname(os.getcwd())
    _BIN_DIR = _BASE_DIR + '/bin'
    _CONF_DIR =  _BASE_DIR + '/conf'
    _BAK_DIR = _BASE_DIR + '/bak'
    _LOG_DIR = _BASE_DIR + '/log'
    _STATUS_DIR = _BASE_DIR + '/status'
    __options = None
    arg = None
    ops = None
    STATUS = {'EXIT_HELP':24, 'EXIT_CANTSTART':25, 'EXIT_PAUSED':22, 'HADOOP_FAIL':10, 'GET_LIST_FAIL':11, 'MD5_FAIL':12, 'NODIR_FAIL':13, 'GET_MD5_FAIL':14, 'CANT_CONT':16, 'TOO_MANY_RETRY':90}
    def __init__(self):
        #self.queue = Queue(maxsize=0)
        self.mutex = threading.Lock()
        self.threads = []
        self.checkqueue = Queue(maxsize=0)
        self.tranqueue = Queue(maxsize=0)
        self.finishqueue = Queue(maxsize=0)
        self.failqueue = Queue(maxsize=0)
        self.filecount = 0
        self.needtrans = []
        self.parseOptions()
        if self.ops.bwlimit is None: self.ops.bwlimit = 1000
        #print self.ops
        self.timestamp = self.ops.statusdir
        self.statusdir = self._STATUS_DIR + '/' + self.ops.statusdir
        if not os.path.isdir(self.statusdir):
            self.mksure_dir(self.statusdir, False)
        self.md5file = self.statusdir + '/index.md5'
        self.logger = initlog(self.statusdir + '/report.log', 0)
        self.files_dict = None
        self.task_failed = None
        self.task_error = None
        self.task_finished = None
        self.process = None
        self.data_total = self.get_data_total()
        self.data_uploaded = None
        self.returncode = -1
        self.letsgo()

    def letsgo(self):
        if self.ops.dryrun:
            self.dryrun()
            self.exit()
        if self.ops.check == True:
            self.Check()
            self.exit()
        if self.ops.pause != True and self.ops.stop != True and self.ops.cont != True and self.ops.dryrun != True:
            self.start()
        self.exit()

    def parseOptions(self):
        """Parse the trans options"""
        import sys, optparse
        self.__options = optparse.OptionParser(version='Transclient 1.1 serials : %prog')
        self.__options.add_option('--srcdir', '-s', action='store', dest='src_path', help='Source path')
        self.__options.add_option('--dstdir', '-d', action='store', dest='dst_path', help='Destination path')
        self.__options.add_option('--statusdir', '-t', action='store', dest='statusdir', help='The Directory of status')
        self.__options.add_option('--md5file', '-f', action='store', dest='md5file', help='The Md5 file of index data')
        self.__options.add_option('--maxretry', '-r', action='store', type='int', dest='maxretry', help='Max retry times allowed')
        self.__options.add_option('--bwlimit', '-l', action='store', type='int', dest='bwlimit', help='Transfer speed limit (KB/s)')
        self.__options.add_option('--sln', '-n', action='store', type='int', dest='sln', help='The number of single server layer')
        self.__options.add_option('--dryrun', '-D', action='store_true', dest='dryrun', help='Jus dry run')
        self.__options.add_option('--start', '-S', action='store_true', dest='start', help='')
        self.__options.add_option('--pause', '-P', action='store_true', dest='pause', help='')
        self.__options.add_option('--continue', '-C', action='store_true', dest='cont', help='')
        self.__options.add_option('--stop', '-K', action='store_true', dest='stop', help='')
        self.__options.add_option('--check', '-X', action='store_true', dest='check', help='check status')
        if len(sys.argv) == 1:
            self.__options.print_help()
            sys.exit(0)
        ops, arg = self.__options.parse_args()
        self.arg = arg
        self.ops = ops

    def save_parm(self):
        return

    def print_help(self):
        if self.__options:
            self.__options.print_help()

    def execute(self, cmd=':'):
        from commands import getstatusoutput
        #p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, shell=False)  
        #dataout, dataerr = p.communicate()
        #status = p.returncode
        (status, dataout) = getstatusoutput(cmd)
        status = status >> 8
        return (status, dataout)

    def __continue_init(self):
        return

    def makedir(self, abspath=''):
        '''递归建立目录'''
        #print 'abspath=%s' % abspath
        if not os.path.isabs(abspath): return False
        dirs = abspath.split('/')
        if '' in dirs: dirs.remove('')
        if '.' in dirs: dirs.remove('.')
        basedir = ''
        for dir in dirs:
            basedir = basedir + '/' + dir
            #print 'basedir = %s' % basedir
            if not os.path.isdir(basedir): os.mkdir(basedir)
        return True

    def mksure_dir(self, dirstr='', backup=True):
        if dirstr == '.' or dirstr == './' or dirstr == '': return
        bakdir = dirstr + self.timestamp
        if backup and os.path.isdir(dirstr):
            os.rename(dirstr, bakdir)
            self.makedir(dirstr)
        else:
            if os.path.isdir(dirstr): os.system('rm -f %s' % dirstr)
            self.makedir(dirstr)

    def __start_init(self):
        '''从hadoop上的index.md5文件中获取文件列表和相应的Md5sum
        format: files_dict[] = {FILENAME:{'md5':MD5, 'success': 0, 'failed':0, 'retry': 0, 'done':0, 'status'},....}
        done  success  failed  retry 
          1     1        0      N/A          trans and md5 both OK
          1     0        1     =MAX          trans failed
          0     0        0     <MAX          retrying ...

        '''
        #print "self.statusdir = %s" %self.statusdir
        if not os.path.isdir(self.statusdir):
            self.mksure_dir(self.statusdir, False)
        from re import compile
        pat = compile('\./')
        tmp_dict = {}
        xxtmp = os.system('rm -f ' + self.ops.md5file)
        xxxtmp = os.system('rm -f %s/*' % self.statusdir)
        fullcmd = '/home/img/hadoop-client/hadoop/bin/hadoop fs -D speed.limit.kb=%d  -copyToLocal  %s  %s' % (self.ops.bwlimit, self.ops.md5file, self.md5file)
        if os.path.exists(self.md5file):
            os.remove(self.md5file)
        #print fullcmd
        (status, output) = self.execute(fullcmd)
        if status != 0:
            self.task_failed = 1
            self.files_dict = None
            self.returncode = 102
            self.exit()
        yytmp = os.system('rm -f %s/index.md5' % self.ops.dst_path)
        cpmd5file = 'cp -f ' + self.md5file + '  ' + self.ops.dst_path + '/'
        os.system(cpmd5file)
        index_p = open(self.md5file, 'r')
        filelist_p = open(self.statusdir + '/filelist', 'a+')
        for line in index_p.readlines():
            ltmp = line.split()
            fn = pat.sub('', ltmp[1])
            tmp_dict[fn] = {'md5':ltmp[0], 'success':0, 'failed':0, 'done':0, 'status':-1}
            self.tranqueue.put([fn, 0])
            filelist_p.write("%s  %s\n" % (ltmp[0], fn))
        self.filecount = self.tranqueue.qsize()
        self.files_dict = tmp_dict
        filelist_p.close()
        index_p.close()
        tmp_dirs = []
        for file in self.files_dict.keys():
            t_dir = os.path.dirname(file)
            if t_dir not in tmp_dirs:
                tmp_dirs.append(t_dir)
        if '' in tmp_dirs: tmp_dirs.remove('')
        #print tmp_dirs
        if not os.path.isdir(self.ops.dst_path):
            self.mksure_dir(self.ops.dst_path, False)
        for dir in tmp_dirs:
            #不备份数据目录
            #print self.ops.dst_path + '/' + dir
            self.mksure_dir(self.ops.dst_path + '/' + dir, False)

    def is_transdone(self):
        tmpnum = 0
        tmpnum = self.finishqueue.qsize() + self.failqueue.qsize()
        if self.checkqueue.empty() and self.tranqueue.empty() and tmpnum == self.filecount:
            return True
        else:
            return False

    def refresh_needtrans(self):
        '''判断是否传输完毕,获取未完成传输的文件list'''
        if self.mutex.acquire(3):
            self.needtrans = [ file for file in  self.files_dict.keys() if self.files_dict[file]['done'] != 1 and self.files_dict[file]['md5checking'] != 1 and self.files_dict[file]['transing'] != 1]
            self.mutex.release()

    #def md5sum(self, file=''):
    #    (status, output) = self.execute("/usr/bin/md5sum %s/%s | awk '{print $1}'" % (self.ops.dst_path, file))
    #    if status == 0 and output == self.files_dict[file]['md5']:
    #        self.finishqueue.put(file)
    #        print '%s 校验正确' % file
    #        (mstatus, moutput) = self.execute('echo  %s 校验正确 >> %s/check_md5.log' % (file,self.statusdir))
    #        return True
    #    elif output != self.files_dict[file]['md5']:
    #        self.tranqueue.put({file:})
    #        self.files_dict[file]['md5checking'] = 0
    #        print '%s 校验失败，删除此文件' % file
    #        (tstatus, toutput) = self.execute('rm -f %s/%s' % (self.ops.dst_path, file))
    #        self.mutex.release()
    #        return False
    #   else:
    #        self.files_dict[file]['md5checking'] = 0
    #        print '%s 校验错误' % file
    #        self.mutex.release()
    #    self.mutex.release()
    #    return False
    #
    def thread_checkmd5sum(self):
        import time
        while not self.is_transdone():
            cnext = None
            if not self.checkqueue.empty():
                if self.ops.sln > 1:
                    time.sleep(1)
                cnext = self.checkqueue.get(block=False)
                (status, output) = self.execute("/usr/bin/md5sum %s/%s | awk '{print $1}'" % (self.ops.dst_path, cnext[0]))
                if output == self.files_dict[cnext[0]]['md5']:
                    self.finishqueue.put(cnext[0])
                    print '%s 校验正确' % cnext[0]
                    (mstatus, moutput) = self.execute('echo  %s >> %s/success_list.log' % (cnext[0], self.statusdir))
                    (mstatus, moutput) = self.execute('echo  %s 校验正确 >> %s/check_md5.log' % (cnext[0], self.statusdir))
                elif output != self.files_dict[cnext[0]]['md5']:
                    print '%s 校验失败' % cnext[0]
                    cnext[1] += 1
                    if cnext[1] >= self.ops.maxretry:
                        self.failqueue.put(cnext[0])
                        print '%s 校验失败,retry=%d 超过最大重试次数' % (cnext[0], cnext[1])
                        (mstatus, moutput) = self.execute('echo %s 校验失败,retry=%d 超过最大重试次数 >> %s/failed_list.log' % (cnext[0], cnext[1], self.statusdir))
                        (mstatus, moutput) = self.execute('echo %s >> %s/failed_list' % (cnext[0], cnext[1], self.statusdir))
                    else:
                        print '%s 校验失败,未超过最大重试，删除此文件retry=%d' % (cnext[0], cnext[1])
                        (mstatus, moutput) = self.execute('echo %s 校验失败,未超过最大重试，删除此文件retry=%d >> %s/check_md5.log' % (cnext[0], cnext[1], self.statusdir))
                        (tstatus, toutput) = self.execute('rm -f %s/%s' % (self.ops.dst_path, cnext[0]))
                        self.tranqueue.put(cnext)

    def start(self):
        '''start'''
        self.__start_init()
        failcount = None
        checkthread = None
        checkthread = threading.Thread(target=self.thread_checkmd5sum)
        checkthread.start()
        while not self.is_transdone():
            if not self.tranqueue.empty():
                ## next is a list
                next = self.tranqueue.get(block=False)
                if(next[1] > self.ops.maxretry):
                    print "%s 传输失败，retry=%d" % (next[0], next[1])
                    ##self.failqueue.put(next[0])
                    ##(mstatus, moutput) = self.execute('echo  %s >> %s/failed_list.log' % (cnext[0], self.statusdir))
                else:
                    print "%s 传输中，retry=%d" % (next[0], next[1])
                    fullcmd = '/home/img/hadoop-client/hadoop/bin/hadoop fs -D speed.limit.kb=' + str(self.ops.bwlimit) + ' -copyToLocal '+ ' ' + self.ops.src_path + '/' + next[0] + ' ' + self.ops.dst_path + '/' + next[0]
                    #print fullcmd
                    (estatus, eoutput) = self.execute(fullcmd)
                    #print 'estatus = %d' % estatus
                    if estatus == 0:
                        self.checkqueue.put(next)
                        print '%s 传输完毕， retry=%d'  % (next[0], next[1])
                    elif estatus == 255:
                        #255 文件已存在或者不存在
                        print '%s 文件已存在，文件校验 retry=%d' %(next[0], next[1])
                        if 'already exists' in eoutput:
                            self.checkqueue.put(next)
                        elif 'No such file or directory' in eoutput:
                            self.failqueue.put(next[0])
                            print "%s 没有此源文件，retry=%d" % (next[0], next[1])
                        elif 'null' in eoutput:
                            self.failqueue.put(next[0])
                            print "%s 传输失败，null error, retry=%d" % (next[0], next[1])
                        else:
                            self.failqueue.put(next[0])
                            print "%s 传输失败，unkown error, retry=%d" % (next[0], next[1])
        checkthread.join()
        #failecount = self.reflush_result()
        if self.failqueue.empty():
            self.returncode = 0
            self.execute('touch %s/success_flag' % self.statusdir)
        else:
            self.execute('touch %s/failed_flag' % self.statusdir)
        self.exit()        

    def exit(self):
        import sys
        sys.exit(self.returncode)

    def reflush_result(self):
        '''把传输结果写入文件中,并返回失败文件数'''
        success_log = self.statusdir + '/success_list.log'
        failed_log = self.statusdir + '/failed_list.log'
        self.execute(': > %s' % success_log)
        self.execute(': > %s' % failed_log)
        fp = open(failed_log, 'a+')
        sp = open(success_log, 'a+')
        failcount = 0
        #print self.files_dict
        for i in self.files_dict.keys():
            if self.files_dict[i]['success'] == 1:
                sp.write("%s\n" % i)
            if self.files_dict[i]['failed'] == 1: 
                self.returncode = 200
                fp.write("%s\n" % i)
                failcount += 1
        fp.close()
        sp.close()
        return failcount

    def dryrun(self):
        '''xxx'''
        from re import compile
        pat = compile('\./')
        tmp_dict = {}
        fullcmd = '/home/img/hadoop-client/hadoop/bin/hadoop fs -D speed.limit.kb=%d -cat %s ' % (self.ops.bwlimit, self.ops.md5file)
        (status, output) = self.execute(fullcmd)
        if status != 0:
            self.task_failed = 1
            self.files_dict = None
            return
        for line in output.split('\n'):
            ltmp = line.split()
            fn = pat.sub('', ltmp[1])
            tmp_dict[fn] = {'md5':ltmp[0], 'finish':0, 'failed':0, 'retry':0}
        self.files_dict = tmp_dict
        for file in self.files_dict.keys():
            fullcmd = '/home/img/hadoop-client/hadoop/bin/hadoop fs -D speed.limit.kb=' + str(self.ops.bwlimit) +' -copyToLocal'+ ' ' + self.ops.src_path + '/' + file + ' ' + self.ops.dst_path + '/' + file
            print fullcmd
        sys.exit(0)

    def stop(self):
        '''停止传输'''
        if self.process is not None:
            self.process.kill()
            #update table tasklist
        else:
            ###此错误信息需写入日志文件
            print 'Warning: %s no such process' % self.t_dst_server

    def get_data_total(self):
        '''The total size(bytes) to trans'''
        fullcmd = "/home/img/hadoop-client/hadoop/bin/hadoop fs -dus %s | awk '{print $NF}'" % self.ops.src_path
        (status, output) = self.execute(fullcmd)
        return int(output)

    def get_data_uploaded(self):
        '''The size(bytes) has been uploaded'''
        return 0

    def check_diskfree(self):
        '''disk free
            space_needed > t_data_total * single_level_num '''
        ret = None
        ptmp = '/'.join(self.ops.dst_path.split('/')[0:3])
        fullcmd = "df -B1 %s | tail -1 | awk '{print $4}'" % ptmp
        (status, output) = self.execute(fullcmd)
        #print self.data_total
        if self.data_total * int(self.ops.sln) > int(output):
            self.returncode = 101
            ret = False
        else:
            ret = True
        return ret

    def Check(self):
        if self.check_diskfree():
            self.returncode = 0
        self.exit()

if __name__ == '__main__':
    import os,sys
    Task()
