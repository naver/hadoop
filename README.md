## What is this repository?
This repository is based on Apache Hadoop 2.7.1 source code.
It is used to make Naver's large scale multi-tenant hadoop cluster, which is called C3.
The C3 users can execute several data processing jobs with MapReduce, Spark and Hive on CPU, and execute Deep Learning algorithms on GPU.
And also they can run long-live applications on docker container.

Recently hadoop's new features is adding to Hadoop 2.8 or Hadoop 3.0.
However if you are using hadoop cluster for years in production, your hadoop version maybe is not hadoop 2.8 or 3.0, because these versions is not recommended for production cluster yet.
Thus you can't use very useful new features: GPU scheduler, docker container, several resource isolations(e.g. network outbound, disk).

We're applying and developing new features to this repository.
You can see histories in commit logs.

## Features
- GPU resource type for scheduling : [YARN-5517](https://issues.apache.org/jira/browse/YARN-5517)
- Docker Containers in LinuxContainerExecutor : [YARN-3611](https://issues.apache.org/jira/browse/YARN-3611)
- Network Outbound isolation : [YARN-2140](https://issues.apache.org/jira/browse/YARN-2140)
- Balanced DataNode's local disk use ratio in several different disk capacity : [Doc](https://github.com/naver/hadoop/wiki#balanced-datanodes-local-disk-use-ratio-in-several-different-disk-capacity)
- Choose native library considering node' OS automatically : [Doc](https://github.com/naver/hadoop/wiki#choose-native-library-considering-node-os-automatically)
- Multiple remote-app-log-dir for multiple NameNodes : See [yarn-default.xml](https://github.com/naver/hadoop/blob/master/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/resources/yarn-default.xml#L896)
- ...

## Build
See ``BUILDING.txt`` to check required packages and run ``install_requirements.sh`` to install.

If you installed required packages, there is ``source.me`` file which declare several environment variables.


```
source source.me
REV={NUMBER} # add 1 to lastest tag which checking git tag
git tag r$REV
mvn clean package install -Dversion-info.scm.commit=${REV} -Pdist,native -DskipTests -Dtar -Dmaven.javadoc.skip=true -Dcontainer-executor.conf.dir=/etc/hadoop/ -Dsnappy.prefix=$SNAPPY_PREFIX -Drequire.snappy=true -Dsnappy.lib=$SNAPPY_PREFIX/lib -Dbundle.snappy=true
git push --tags
```

If you want to build the package without native libraries, remove ``-Pnative`` option.

```
source source.me
REV={NUMBER} # check last tag using git tag command, then add 1
mvn clean package install -Dversion-info.scm.commit=${REV} -Pdist -DskipTests -Dtar -Dmaven.javadoc.skip=true -Dcontainer-executor.conf.dir=/etc/hadoop/
```

Currently if you build the hadoop package, we recommend running maven with -DskipTests. Depending on the build environment, maven tests can sometimes fail and some our improvements need to be fix.

## Package Naming
We are using a separate package for HDFS and YARN, so the packages's name is like this:

* YARN: ``hadoop-yarn-2.7.1-r${REV}-arch-${OS}-x86_64.tar.gz``
* HDFS: ``hadoop-hdfs-2.7.1-r${REV}-arch-${OS}-x86_64.tar.gz``
* MAPRED: ``hadoop-2.7.1-r${REV}.tar.gz``
  * this is package without native libraries

