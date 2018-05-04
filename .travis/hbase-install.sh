#! /bin/bash

VER=1.4.4
DST=/usr/lib
wget -O hbase-bin.tar.gz http://www-eu.apache.org/dist/hbase/$VER/hbase-$VER-bin.tar.gz
tar xzf hbase-bin.tar.gz
rm -f hbase-$VER/conf/hbase-site.xml
mv .travis/hbase-site.xml hbase-$VER/conf
sudo mkdir -p $DST
sudo mv hbase-$VER $DST/hbase
sudo $DST/hbase/bin/start-hbase.sh
while sudo hbase $DST/hbase/bin/hbase hbck 2>/dev/null | grep -q -i "INCONSISTENT"; do echo -n "." ; done
echo " HBase started !"
