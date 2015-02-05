使用前先安装python库kazoo
pip install kazoo

运行说明： 
1. kinit hdfs_admin@XIAOMI.HADOOP
或者对应的hdfs admin权限，这样才可以读到hdfs日志长度;

2 ./check.sh, 参数可以直接在check.sh中修改:
./check.py --zk_quorums 10.2.201.163:11000,10.2.201.164:11000,10.2.201.165:11000 --cluster lgtst-xiaomi --peer_id 11 --deploy_cmd /home/chengqiming/minos/client/deploy --retries 10

中间结果说明：
1. hlog_roll.txt 程序生成的用于hlog_roll的命令脚本;
2. zk_nodes.txt 程序生成的zk节点列表与读的偏移量;
3. hdfs_logs.txt 程序生成的hlog列表;
4. compare_result.txt 程序生成的比较中间结果; 

