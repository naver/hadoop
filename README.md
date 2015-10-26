This repository is based on Apache Hadoop 2.7.1 for Naver C3.
You can see patch histories in commit logs.

# Build
``mvn clean package install -Pdist,native -DskipTests -Dtar -Dmaven.javadoc.skip=true -Dsnappy.prefix=/snappy-build-dir -Drequire.snappy=true -Dsnappy.lib=/snappy-build-dir/lib -Dbundle.snappy=true -Dcontainer-executor.conf.dir=/etc/hadoop/``

