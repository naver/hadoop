This repository is based on Apache Hadoop 2.7.1 for Naver C3.
You can see patch histories in commit logs.

# Build
See ``BUILDING.txt`` to check required packages and run ``install_requirements.sh`` to install.

If you installed required packages, there is ``source.me`` file which declare several environment variables.


```
source source.me
REV={NUMBER} # add 1 to lastest tag which checking git tag
git tag r$REV
mvn clean package install -Dversion-info.scm.commit=${REV} -Pdist,native -DskipTests -Dtar -Dmaven.javadoc.skip=true -Dcontainer-executor.conf.dir=/etc/hadoop/ -Dsnappy.prefix=$SNAPPY_PREFIX -Drequire.snappy=true -Dsnappy.lib=$SNAPPY_PREFIX/lib -Dbundle.snappy=true
git push --tags
```

If you want to build the package without native libraries, remove -Pnative option.

```
source source.me
REV={NUMBER} # check last tag using git tag command, then add 1
mvn clean package install -Dversion-info.scm.commit=${REV} -Pdist -DskipTests -Dtar -Dmaven.javadoc.skip=true -Dcontainer-executor.conf.dir=/etc/hadoop/
```

Currently if you build the hadoop package, we recommend running maven with -DskipTests. Depending on the build environment, maven tests can sometimes fail and some our improvements need to be fix.

# Package Naming
We are using a separate package for HDFS and YARN, so the packages's name is like this:

* YARN: ``hadoop-yarn-2.7.1-r${REV}-arch-${OS}-x86_64.tar.gz``
* HDFS: ``hadoop-hdfs-2.7.1-r${REV}-arch-${OS}-x86_64.tar.gz``
* MAPRED: ``hadoop-2.7.1-r${REV}.tar.gz``
  * this is package without native libraries

