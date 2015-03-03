#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import datetime
from kazoo.client import KazooClient
import operator
import struct
import sys
import commands
import re
from optparse import OptionParser
import time

def initZkClient(zkServers):
  zkClient = KazooClient(hosts=zkServers, read_only=True)
  zkClient.start()
  return zkClient

def logRoll(zkClient, hbaseCmd, clusterName):
  regionServers = zkClient.get_children("/hbase/"+clusterName+"/rs")
  f = open("hlog_roll.txt", "w")
  for rs in regionServers:
    f.write("hlog_roll '%s'\n" % rs)
  f.write("exit\n")
  f.close()
  cmd = hbaseCmd + " hlog_roll.txt"
  status, output = commands.getstatusoutput(cmd)
  if status != 0:
    return False
  return True

def listZkNodes(zkClient, clusterName, peerId):
    rsNode = "/hbase/" + clusterName + "/replication/rs"
    if not zkClient.exists(rsNode):
      print "Error", rsNode, "not Exists!"
      return False

    f = open("zk_nodes.txt", "w")
    regionServers = zkClient.get_children(rsNode)
    for rs in regionServers:
      machineNode = "/hbase/" + clusterName + "/replication/rs/" + rs
      peers = zkClient.get_children(machineNode)

      for peer in peers:
        if not peer.startswith(peerId):
          continue

        peerNode = machineNode + "/" + peer
        logs = zkClient.get_children(peerNode)
        logs = sorted(logs)
        for log in logs:
          logNode = peerNode + "/" + log
          data, status = zkClient.get(logNode)
          if data == "":
            f.write("%s\t%s\n" % (logNode, 0))
          else:
            strLen = len(data) - 5
            formatStr = "!i"+str(strLen)+"s"
            idLen, strIdAndReplicaOffset = struct.unpack(formatStr, data[1:])
            processAndMachine = strIdAndReplicaOffset[:idLen]
            readPosition = strIdAndReplicaOffset[idLen:]
            f.write("%s\t%s\n" % (logNode, readPosition))
    f.close()
    return True


def listHdfsLogs(hdfsCmd, clusterName):
  f = open("hdfs_logs.txt", "w")
  cmd = '''%s dfs -ls  /hbase/%s/.logs/*/''' % (hdfsCmd, clusterName)
  status, output = commands.getstatusoutput(cmd)
  if status != 0:
    return False
  f.write(output)
  f.write("\n")

  cmd = '''%s dfs -ls  /hbase/%s/.oldlogs >> hdfs_logs.txt''' % (hdfsCmd, clusterName)
  status, output = commands.getstatusoutput(cmd)
  if status != 0:
    return False
  f.write(output)
  return True

def compareZkNodesAndHLogs():
  f = open("zk_nodes.txt", "r")
  content = f.read()
  f.close()

  zkPositions = {}

  PATTERN = re.compile("/.*?/.*?/.*?/.*?/.*?/.*?/(.*?)\s(\d+)", re.DOTALL)
  zkNodes = re.findall(PATTERN, content)
  for log in zkNodes:
    logName = log[0]
    logPosition = long(log[1])
    zkPositions[logName] = logPosition

  f = open("hdfs_logs.txt", "r")
  content = f.read()
  f.close()
  PATTERN = re.compile("^[^ ]+?\ +?[^ ]+?\ +?[^ ]+?\ +?[^ ]+?\ +?(\d+?)\ +?[^ ]+?\ +?[^ ]+?\ +?/hbase/.*?/.*?logs/([^ ]+?)$", re.S|re.M)
  hlogLength = {}
  hlogs = re.findall(PATTERN, content)
  for hlog in hlogs:
    name = ""
    fields = hlog[1].split("/")
    if len(fields) == 1:
      name = fields[0]
    elif len(fields) == 2:
      name = fields[1]
    else:
      print "Error invalid hlog:", hlog[1]
      continue

    length = long(hlog[0])
    hlogLength[name] = length

  if len(hlogLength) == 0:
    print "no hlogs, perhapse due to privilege"
    return False

  if len(zkPositions) == 0:
    print "no zk nodes, perhapse due to privilege"
    return False

  f = open("compare_result.txt", "w")
  bytesLeft = 0

  hlogNotExist = 0
  zkEqualsHLog = 0
  zkBiggerThanHLog = 0
  notCount = 0
  zkLessThanHLog = 0

  for logName in zkPositions.keys():
    zkPosition = zkPositions[logName]
    if not hlogLength.has_key(logName):
      f.write("Warn_HLogNotExist %s %s\n" % (logName, zkPosition))
      hlogNotExist += 1
    else: 
      length = hlogLength[logName]
      if zkPosition == length:
        zkEqualsHLog += 1
        f.write("OK_ZKEqualsHLog %s %s\n" % (logName, zkPosition))
      elif zkPosition > length:
        zkBiggerThanHLog += 1
        f.write("Error_ZKBiggerThanHLog %s %s %s\n" % (logName, zkPosition, length))
      else:
        if length == 134 and zkPosition == 0:
          notCount += 1
          f.write("OK_NotCount %s %s %s\n" % (logName, zkPosition, length))
        else:
          zkLessThanHLog += 1
          bytesLeft += length - zkPosition
          f.write("Warn_ZKLessThanHLog %s %s %s\n" % (logName, zkPosition, length))
  f.close()
  
  print "Error_ZKBiggerThanHLog", zkBiggerThanHLog
  print "Warn_HLogNotExist", hlogNotExist
  print "Warn_ZKLessThanHLog", zkLessThanHLog
  print "OK_NotCount", notCount
  print "OK_ZKEqualsHLog", zkEqualsHLog
  print "Result_NotReplicated", bytesLeft, "bytes"
  if bytesLeft == 0:
    return True
  return False
  

def run(deployCmd, zkQuorums, clusterName, peerId, doLogRoll):
  hdfsCmd="%s shell hdfs %s" % (deployCmd, clusterName)
  hbaseCmd="%s shell hbase %s shell" % (deployCmd, clusterName)
  zkClient = initZkClient(zkQuorums)

  if doLogRoll:
    ret = logRoll(zkClient, hbaseCmd, clusterName)
    if not ret:
      print "Error logRoll failed!"
      return False

  ret = listZkNodes(zkClient, clusterName, peerId)
  if not ret:
    print "Error listZkNodes failed!"
    return False
  
  ret = listHdfsLogs(hdfsCmd, clusterName)
  if not ret:
    print "Error listHdfsLogs failed!"
    return False

  ret = compareZkNodesAndHLogs()
  if not ret:
    print "compareZkNodesAndHLogs failed!"
    return False
  return True

if __name__ == "__main__":
  parser = OptionParser()  
  parser.add_option("-z", "--zk_quorums", dest="zk_quorums",  help="zk quorums") 
  parser.add_option("-c", "--cluster", dest="cluster",  help="hbase cluster name")
  parser.add_option("-p", "--peer_id", dest="peer_id",  help="peer id")
  parser.add_option("-d", "--deploy_cmd", dest="deploy_cmd",  help="deploy cmd")
  parser.add_option("-t", "--retries", dest="retries",  help="retries number")
  parser.add_option("-r", "--log_roll", action="store_true", dest="log_roll", default=False, help="whether do log_roll")

  (options, args) = parser.parse_args() 
  if options.zk_quorums is None:
    parser.error("zk_quorums is null")

  if options.cluster is None:
    parser.error("cluster is null")

  if options.peer_id is None:
    parser.error("peer_id is null")

  if options.deploy_cmd is None:
    parser.error("deploy_cmd is null")

  i = 0
  while i < int(options.retries):
    ret = run(options.deploy_cmd, options.zk_quorums, options.cluster, options.peer_id, options.log_roll)
    if ret:
      break
    i += 1
    time.sleep(5)
    print "Retries ", i, "times..."

