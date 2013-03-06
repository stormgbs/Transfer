#!/bin/env python
# -*- coding: utf-8 -*-
#Contact : gaobushuang@
#Time : 2012-05-21
#Comment : 系统初始化模块
#LastUpdate : 2012-06-14

class TransbaseInit(object):
    '''系统初始化--> 配置文件分析与加载-->数据库表初始化-->机器名分析 -->对应关系初始化并载入tasklist表-->生成传说任务-->传输-->检查-->完成'''
    import re
    ConfDict = {}
    dbhdler = None
    #PATT = re.compile('^\D*-([a-zA-Z]+)(\d+)-(\d+)\.([a-zA-Z]+\d*)$')
    #PATT = re.compile('^(\w*-)*([a-zA-Z]+)-([a-zA-Z]+)-([a-zA-Z]+)(\d+)-(\d+)\.([a-zA-Z]+\d*)$')
    #PATT = re.compile('^(\w*-)*([a-zA-Z]+)-([a-zA-Z]+)-([a-zA-Z]+)(\d+)(-(\d+))*\.([a-zA-Z]+\d*)$')
    PATT = re.compile('^(\w*-)*([a-zA-Z]+\d*)-([a-zA-Z]+)-([a-zA-Z]+)(\d+)(-(\d+))*\.([a-zA-Z]+\d*)$')
    def __init__(self):
        print '系统初始化...'
        self.ConfDict = Conf_Dict
        self.dbhdler = Database(self.ConfDict['DATABASE'])
        tmp = self.dbhdler.sql('select initialized from global_config')
        if len(tmp) != 0 and human_dtuple(tmp)[0] == '1':
            print 'Error：系统已初始化，上一次传库未完成，本次操作取消'
            sys.exit()
        self._init_base()
        self._init_database()
        self.dbhdler.sql("update global_config set initialized='0'")
        print '系统初始化完毕'
    def _init_base(self):
        time.sleep(1)
        for key in self.ConfDict['GLOBAL'].keys():
            CommonDicts.update({key:self.ConfDict['GLOBAL'].get(key)})
        print '基础数据初始化完毕'
    def _init_database(self):
        '''数据库初始化
        初始化前备份数据库后清空原信息'''
        import random
        print '数据库初始化... ...'
        self.dbhdler.dbflush()
        ######################
        #Table: global_config
        ######################
        self.dbhdler.exe("""insert `global_config` (`GID`, `hdp_basedir`, `order`, `initialized`) values (NULL, '%s', '%s', '0')""" %(self.ConfDict['GLOBAL']['hdp_basedir'], self.ConfDict['GLOBAL']['order']))
        print '  表global_config装载完毕'
        hdp_basedir = self.ConfDict['GLOBAL'].get('hdp_basedir')
        d_list = self.ConfDict['DATAINFO']
        for i in d_list:
            CommonDicts.update({i['name'].lower():i['level_num']})
        for i in d_list:
            #Table: node_info
            self.dbhdler.exe("""insert `node_info` (`NID`, `npath`, `dlevel`, `name`, `level_num`) values (NULL, '%s', %d, '%s', %d)""" % (i['base_npath'], i['dlevel'], i['name'], i['level_num']))
            d_name = i['name'].lower()
            '''init table : data_info'''
            for j in i['datas']:
                ####################
                #Table：data_info
                ####################
                self.dbhdler.exe("""insert `data_info` (`DID`, `dlevel`, `level_num`, `layer_num`, `single_level_num`, `data_path`, `module_type`, `node_path`, `dcomment`, `speedlimit`) values (NULL, %d, %d, %d, %d, '%s', '%s', '%s', '%s', %d)""" % (i['dlevel'], i['level_num'], i['layer_num'], j['single_level_num'], j['data_path'], j['module_type'], j['node_path'], j['dcomment'], j['speedlimit']))
                ####################
                #Table: tasklist
                ####################
                dlevel = i['dlevel']
                module_type = j['module_type']
                data_path = j['data_path']
                sln = j['single_level_num']
                level_arr_num = i['level_num']
                svr_list = fetch_servers_list(j['node_path'])
                speedlimitkb = j['speedlimit']
                #print type(svr_list)
                for svr in svr_list:
                    #print self.get_svr_re(svr)
                    (var_head, var_cluster, var_prod, var_mod, var_row, var_tmp, var_layer, var_idc) = self.get_svr_re(svr)
                    var_row = int(var_row)
                    ##var_layer = int(var_layer)
                    if i['layer_num'] == 1: var_layer = 0
                    clsr = get_cluster(var_idc, var_cluster)
                    tmpx = 0
                    while tmpx != sln:
                        abs_src_num = self.get_beginNum(d_name) + sln * var_row + tmpx
                        tmp_dict = {'bs':'index', 'di':'info'}
                        src_path = '%s/%s_%d/%s' % (hdp_basedir, d_name, abs_src_num, tmp_dict[module_type])
                        tmpy = str(tmpx) if sln > 1 else ''
                        dst_path = data_path + tmpy + '/data/index.new'
                        statusstr = dst_path.split('/')[3]
                        self.dbhdler.exe("""insert `tasklist` (`TID`,`src_path`,`dst_server`,`dst_path`,`module_type`,`dlevel`,`cluster`,`level`,`layer`,`retry`,`speedlimit`, `statusdir`) values (NULL, '%s', '%s', '%s', '%s', %d, '%s', %d, %d, '4', %d, '%s')""" % (src_path, svr, dst_path, module_type, dlevel, clsr, int(var_row), int(var_layer), speedlimitkb, statusstr))
                        ##self.dbhdler.exe("""insert `tasklist` (`TID`,`src_path`,`dst_server`,`dst_path`,`module_type`,`dlevel`,`cluster`,`level`,`layer`,`retry`,`speedlimit`, `statusdir`) values (NULL, '%s', '%s', '%s', '%s', %d, '%s', %d, %d, '4', %d, '%s')""" % (src_path, svr, dst_path, module_type, dlevel, clsr, int(var_row), int(var_layer), speedlimitkb, str(random.uniform(1, 10))))
                        tmpx += 1
        print '  表node_info装载完毕'
        self.grace_data_info()
        print '  表data_info装载完毕'
        print '  表tasklist初始化完成'
        print '数据库初始化完毕'
        #filep.close()
    def get_svr_re(self, istring = ''):
        '''正则分析server_name'''
        #print "get_svr_re(): istring=%s" % istring
        tmp = None
        tmp = self.PATT.match(istring)
        tmp = tmp.groups() if tmp else None
        #type : tuple, format (head, cluster, product, module, row, layer_, layer, idc)
        return tmp
    def get_beginNum(self, iname=''):
        '''获取大项数据的hadoop起始数'''
        order_list = self.ConfDict['GLOBAL']['order'].split()
        dict = {}
        tmp_num = 0
        for olist in order_list:
            if olist == iname:
                break
            else:
                #print CommonDicts.data
                tmp_num += CommonDicts.data.get(olist)
        return tmp_num
    def get_clusters(self, i_dlevel=0, i_moudule_type=''):
        '''获取特定大库特定模块的集群list'''
        tmp = ()
        tmp = self.dbhdler.sql('''select cluster from tasklist where dlevel=%d and module_type='%s' group by cluster''' % (i_dlevel, i_moudule_type))
        return human_dtuple(tmp)
    def grace_data_info(self):
        '''xxxx'''
        data_info_r = self.dbhdler.sql('select DID,dlevel,level_num,layer_num,single_level_num,data_path,module_type,node_path,dcomment,speedlimit from data_info')
        for (i_DID, i_dlevel, i_level_num, i_layer_num, i_single_level_num, i_data_path, i_module_type, i_node_path, i_dcomment, i_speedlimit) in data_info_r:
            num = 0
            cluster_tuple = self.get_clusters(i_dlevel, i_module_type)
            #print cluster_tuple
            self.dbhdler.sql("update data_info set cluster='%s' where dlevel=%d and module_type='%s'" % (cluster_tuple[num], i_dlevel, i_module_type))
            num += 1
            while num < len(cluster_tuple):
                self.dbhdler.sql("insert `data_info` (`DID`, `dlevel`, `level_num`, `layer_num`, `single_level_num`, `data_path`, `module_type`, `node_path`, `dcomment`, `cluster`, `speedlimit`) values (NULL, %d, %d, %d, %d, '%s', '%s', '%s', '%s', '%s', %d)" % (i_dlevel, i_level_num, i_layer_num, i_single_level_num, i_data_path, i_module_type, i_node_path, i_dcomment, cluster_tuple[num], i_speedlimit))
                num += 1
    def record_check_bool(self):
        '''检查每类数据的数目是否正确'''
        rstatus = True
        data_info_r = self.dbhdler.sql('select dlevel, level_num, layer_num, single_level_num, module_type from data_info')
        for (i_dlevel, i_level_num, i_layer_num, i_single_level_num, i_module_type) in data_info_r:
            for i in human_dtuple(self.dbhdler.sql('select cluster from tasklist group by cluster')):
                #print 'i=',i
                #print '''select layer from tasklist where dlevel=%d and module_type='%s' and cluster='%s' group by layer''' % (i_dlevel, i_module_type, i)
                if i_layer_num > 1:
                    layers_tuple = human_dtuple(self.dbhdler.sql('''select layer from tasklist where dlevel=%d and module_type='%s' and cluster='%s' group by layer''' % (i_dlevel, i_module_type, i)))
                    if int(i_layer_num) > 1:
                        if len(layers_tuple) != i_layer_num:
                            rstatus = False
                            break
                        for j in layers_tuple:
                            #print 'j=',j
                            #print self.dbhdler.sql('''select count(*) from tasklist where dlevel=%d and module_type='%s' and cluster='%s' and layer=%d''' % (i_dlevel, i_module_type, i, j))
                            aa = human_dtuple(self.dbhdler.sql('''select count(*) from tasklist where dlevel=%d and module_type='%s' and cluster='%s' and layer=%d''' % (i_dlevel, i_module_type, i, j)))[0]
                            if aa != i_level_num:
                                rstatus = False
                                break
                    else:
                        aa = human_dtuple(self.dbhdler.sql('''select count(*) from tasklist where dlevel=%d and module_type='%s' and cluster='%s' '''  % (i_dlevel, i_module_type, i)))[0]
                        if aa != i_level_num:
                            rstatus = False
                            break
                if rstatus == False: break
            if rstatus == False: break
        return rstatus
        

if __name__ == '__main__':
    from Common import *
    import datetime
    ins = TransbaseInit()
    if not ins.record_check_bool():
        print 'Error : record_check_bool()'
    STAMP = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    DBHDLER.exe("update global_config set statusstamp='%s' where initialized='0'" % STAMP)


# vi: expandtab ts=4
