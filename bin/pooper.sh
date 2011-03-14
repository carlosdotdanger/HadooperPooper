#!/bin/bash
# resolve links - $0 may be a softlink

this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
    ls=`ls -ld "$this"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '.*/.*' > /dev/null; then
        this="$link"
    else
        this=`dirname "$this"`/"$link"
    fi
done

# convert relative path to absolute path
WHEREIAM=`pwd`
bin=`dirname "$this"`
script=`basename "$this"`
bin=`unset CDPATH; cd "$bin"; pwd`
this="$bin/$script"
POOPER_HOME=`cd $bin/..;pwd`
POOPER_JAR="$POOPER_HOME/HadooperPooper.jar"
unset CDPATH
cd $WHEREIAM

JAVA="java"

if [ -z "$POOPER_CONFIG" ] ; then
	if [ -f ~/.ec2/pooper.properties  ] ; then
		POOPER_CONFIG=~/.ec2/pooper.properties
	else
		if [ -f "$POOPER_HOME/config/pooper.properties"  ] ; then
			POOPER_CONFIG="$POOPER_HOME/config/pooper.properties"
		fi
	fi
fi

POOPER_LIB="$POOPER_HOME/lib"
#theincessant aws log messages will make you mental if you don't have this.

if [ -z $CLASSPATH ] ; then
	CLASSPATH="."
fi

for jar in $POOPER_LIB/*.jar ; do
   CLASSPATH=$CLASSPATH:$jar
done



JAVA_OPTS="-Dlogging.config=$POOPER_HOME/config/log4j.properties"
JAVA_OPTS="$JAVA_OPTS -Dclasspath=$CLASSPATH"
JAVA_OPTS="$JAVA_OPTS -Dpooper.config=$POOPER_CONFIG"


if [ "$1" == "login" ] ; then
	COMMAND=`$JAVA  $JAVA_OPTS -jar $POOPER_JAR get-login-command $2 | tail -n1`
else
	COMMAND="$JAVA  $JAVA_OPTS -jar $POOPER_JAR $1 $2 $3 $4 $5 $6"
fi
#echo "COMMAND:"
#echo "$COMMAND"
#echo "--------"
exec $COMMAND
