This repository is based on Apache Hadoop 2.7.1 for Naver C3.
You can see patch histories in commit logs.

# Build
See ``BUILDING.txt`` to install required packages

```
REV={NUMBER} # add 1 to lastest tag which checking git tag
git tag r$REV
mvn clean package install -Dversion-info.scm.commit=${REV} -Pdist,native -DskipTests -Dtar -Dmaven.javadoc.skip=true -Dcontainer-executor.conf.dir=/etc/hadoop/ -Dsnappy.prefix=/snappy-build-dir -Drequire.snappy=true -Dsnappy.lib=/snappy-build-dir/lib -Dbundle.snappy=true
git push --tags
```

# Package Naming
We are using a separate package for HDFS and YARN, so the packages's name is like this:

* YARN: ``hadoop-yarn-2.7.1-r${REV}-arch-${OS}-x86_64.tar.gz``
* HDFS: ``hadoop-hdfs-2.7.1-r${REV}-arch-${OS}-x86_64.tar.gz``

