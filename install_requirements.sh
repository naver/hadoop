#!/bin/bash

PREFIX=$1

if [ -z "$PREFIX" ];then
	echo "Usage: $0 <prefix>"
	echo " example: $0 /work/env"
	exit 1
fi

PREFIX=`readlink -f $PREFIX`

JDK=http://download.oracle.com/otn-pub/java/jdk/7u80-b15/jdk-7u80-linux-x64.tar.gz
MAVEN=http://mirror.apache-kr.org/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
PROTOC=https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz
CMAKE=https://cmake.org/files/v3.4/cmake-3.4.0.tar.gz
SNAPPY=https://github.com/google/snappy/releases/download/1.1.3/snappy-1.1.3.tar.gz


function simple_install(){

	local download_url=$1
	local prefix=$2
	local link=$3
	local tarball=`basename $download_url`
	local dirname=${tarball%-bin.tar.gz}
	dirname=${dirname%.tar.gz}

	if [ -d "$prefix/$dirname" ];then
		return 0
	fi

	pushd $prefix
	rm -rf $tarball || return 1
	wget $download_url -O $tarball --no-check-certificate > /dev/null || return 1
	tar xfz $tarball || return 1
	rm -rf $tarball || return 1
	ln -snf $dirname $link || return 1
	popd 
}


function build_install(){

	local download_url=$1
	local prefix=$2
	local link=$3
	local tarball=`basename $download_url`
	local dirname=${tarball%.tar.gz}

	if [ -d "$prefix/$dirname" ];then
		return 0
	fi

	local tmp_dir=$prefix/tmp
	mkdir -p $tmp_dir

	pushd $tmp_dir
	rm -rf $tarball || return 1
	wget $download_url -O $tarball --no-check-certificate > /dev/null || return 1
	tar xfz $tarball || return 1
	rm -rf $tarball || return 1
	pushd $dirname 
	./configure --prefix=$prefix/$dirname || return 1
	make || return 1
	make install || return 1
	popd 
	popd
	rm -rf $tmp_dir || return 1

	pushd $prefix
	ln -snf $dirname $link || return 1
	popd
}

function make_sourceme(){
	local prefix=$1
	shift
	local args=$@

	local snappy_prefix=
	local java_home=
	local path=
	for arg in $args
	do
		if [ "$arg" == "jdk" ];then
			java_home="$prefix/jdk"
		fi

		if [ "$arg" == "snappy" ];then
			snappy_prefix="$prefix/snappy"
		fi


		if [ -z "$path" ];then
			path="$prefix/$arg/bin"
		else
			path="$path:$prefix/$arg/bin"
		fi
	done


	for x in 1
	do

		echo "export JAVA_HOME=$java_home"
		echo "export PATH=$path:\$PATH"
		echo "export SNAPPY_PREFIX=$snappy_prefix"
	done > source.me

}


mkdir -p $PREFIX || exit 1

simple_install $JDK $PREFIX jdk || exit 1
simple_install $MAVEN $PREFIX mvn || exit 1
build_install $PROTOC $PREFIX protoc || exit 1
build_install $CMAKE $PREFIX cmake || exit 1
build_install $SNAPPY $PREFIX snappy || exit 1

make_sourceme $PREFIX jdk mvn protoc cmake snappy


# vim:bg=dark
