#!/bin/bash

commit=$1
git format-patch -1 $commit
sed -i 's/org.apache.hbase.thirdparty/com.xiaomi.infra.thirdparty/g' *patch
git am *patch
rm *patch
