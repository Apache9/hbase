使用前先安装python库kazoo
pip install kazoo

运行说明： 
1. kinit hdfs_admin@XIAOMI.HADOOP
或者对应的hdfs admin权限，这样才可以读到hdfs日志长度;

2. 执行命令例如: 
./check.py --zk_quorums 10.2.201.163:11000,10.2.201.164:11000,10.2.201.165:11000 --cluster lgtst-xiaomi --peer_id 11 --deploy_cmd /home/chengqiming/minos/client/deploy --retries 10
如果需要做log roll，则需要加上参数--log_roll：
./check.py --zk_quorums 10.2.201.163:11000,10.2.201.164:11000,10.2.201.165:11000 --cluster lgtst-xiaomi --peer_id 11 --deploy_cmd /home/chengqiming/minos/client/deploy --retries 10 --log_roll


3. 运行的流程与中间结果说明 
(1) list出所有的region server，生成hlog_roll.txt, 执行hlog_roll;
(2) list出所有zk节点与偏移量,写到文件zk_nodes.txt中;
(3) list出所有的.logs/*/*与.oldlogs/*到hdfs_log.txt中;
(4) 对比zk_nodes.txt与hdfs_log.txt，将每条对比的结果写到compare_result.txt中;
(5) 根据对比结果，统计出还有多少个字节没有被推送;

5. 提示信息说明：
(1) Error_ZKBiggerThanHLog 表示有多少个zk节点的position大于hlog长度，出现这个问题是hbase复制有bug;
(2) Warn_HLogNotExist 表示有多少个zk节点对应的hlog不存在了，出现这个提示是hbase有bug;
(3) Warn_ZKLessThanHLog 表示多少条复制没有完成; 
(4) OK_NotCount 表示有多少条hlog长度为134，而zk上的节点长度为0, 程序会认为已经完成处理;
(5) OK_ZKEqualsHLog 表示有多少个zk节点已经完成复制;

当没有被复制的字节长度是0，且没有Error和Warn提醒，则可以切换!


