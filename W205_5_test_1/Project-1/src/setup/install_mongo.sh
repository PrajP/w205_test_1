
cp /root/W205_5_group_allan_eric_praj/Project-1/src/setup/mongodb.repo /etc/yum.repos.d/

#vim /etc/yum.repos.d/mongodb.repo

#[mongodb]
#name=MongoDB Repository
#baseurl=http://downloads-distro.mongodb.org/repo/redhat/os/x86_64/
#gpgcheck=0
#enabled=1

yum install mongo-10gen mongo-10gen-server
#mkdir /data/mongodb

#cp /root/W205_5_group_allan_eric_praj/Project-1/src/setup/mongod.conf /etc/

service mongod start


#mongodb.conf change dbpath="/data/mongodb"
