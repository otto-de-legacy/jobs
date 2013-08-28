#!/bin/bash

VERSION=$1
RELEASE=$2
PLAYVERSION="1.2.6"
## Init.
CURRENT_VERSION=$(echo ${VERSION} | sed 's/\"//g' | sed 's/\-//g' | sed 's/\SNAPSHOT//g')


## get play Framework 1.2x and other things needed
wget http://downloads.typesafe.com/play/1.2.6/play-${PLAYVERSION}.zip
git clone https://github.com/nikos/pingbot
git clone git@git.lhotse.ov.otto.de:lhotse-product


mkdir -p ft2_observer_rpm/var/opt/tomcat/webapps
mkdir -p ft2_observer_rpm/var/run/pingbot/
mkdir -p ft2_observer_rpm/etc/init.d
cp -rv lhotse-product/dashboard ft2_observer_rpm/var/opt/tomcat/webapps/
cp -rv pingbot ft2_observer_rpm/var/opt/

# get the config for pingbot
cp -v play1app/conf/* ft2_observer_rpm/var/opt/pingbot/play1app/conf/
cp -v pingbot.init ft2_observer_rpm/etc/init.d/pingbot

unzip play-${PLAYVERSION}.zip -d ft2_observer_rpm/var/opt/
mydir=${pwd}
cd ft2_observer_rpm/var/opt/
ln -s play-${PLAYVERSION} play
cd $mydir

#Set some Permissions and autostart Job-executer
echo "chmod +x /etc/init.d/pingbot; chown -R tomcat:tomcat /var/opt/pingbot;chown -R tomcat:tomcat /var/opt/play; mysqladmin create if not exists pingbot; chkconfig --add pingbot; service pingbot start" > ft2_observer_rpm/var/opt/init-service.sh

cd ft2_observer_rpm

ls

fpm --rpm-user tomcat --rpm-group users -v ${CURRENT_VERSION} --iteration ${RELEASE} -s dir -t rpm \
--directories var/opt/tomcat/webapps/dashboard --directories var/opt/play --directories var/opt/play-${PLAYVERSION} \
--directories var/opt/pingbot --directories var/run/pingbot -n lhotse-ft2_observer --after-install var/opt/init-service.sh .
#rpm -qlp lhotse-ft2_observer-${CURRENT_VERSION}-${RELEASE}.x86_64.rpm

#########################################################

# wget -O - --no-check-certificate http://repo.lhotse.ov.otto.de/apache-tomcat-$JOB_EXEC_VERSION.tar.gz |tar zxf - 
# fpm --rpm-user tomcat --rpm-group tomcat --prefix /var/opt -v $JOB_EXEC_VERSION -s dir -t rpm --directories apache-tomcat-$JOB_EXEC_VERSION -n "lhotse-tomcat" .
# rpm -qlp lhotse-tomcat-$TOMCAT_VERSION-1.x86_64.rpm | grep "/var/opt/apache-tomcat-$TOMCAT_VERSION/lib/catalina.jar"
