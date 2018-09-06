#!/bin/bash

base_branch=$1
base_commit=$2

git checkout $base_branch
git pull
git format-patch $base_commit
git checkout branch-2-mdh3
sed -i 's/org.apache.hbase.thirdparty/com.xiaomi.infra.thirdparty/g' *patch
git am *patch
rm *patch
