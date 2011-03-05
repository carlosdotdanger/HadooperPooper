#!/usr/bin/env bash
#example user data file based on the one found in the hadoop project's ec2 tools.
#I've left some redacted material from a working version for demonstration,
#so YMMV, much of this may be unnecessary depending on your setup.
# REMINDER - this file needs to be less than 16k to be passed in by ec2.

MASTER_HOST=%Master.Host% # 'Master.Host' is the only hard coded key
SECURITY_GROUPS=`wget -q -O - http://169.254.169.254/latest/meta-data/security-groups`
IS_MASTER=`echo $SECURITY_GROUPS | awk '{ a = match ($0, "-master$"); if (a) print "true"; else print "false"; }'`
if [ "$IS_MASTER" == "true" ]; then
 MASTER_HOST=`wget -q -O - http://169.254.169.254/latest/meta-data/local-hostname`
fi
HADOOP_HOME=`ls -d /usr/local/hadoop-*`
# Hadoop configuration
cat > $HADOOP_HOME/conf/core-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>fs.default.name</name>
  <value>hdfs://$MASTER_HOST:50001</value>
</property>
<property>
  <name>hadoop.tmp.dir</name>
  <value>/mnt/hadoop</value>
</property>
<property>
        <name>fs.s3n.awsSecretAccessKey</name>
        <value>%AWS.SecretKey%</value>
</property>
<property>
        <name>fs.s3n.awsAccessKeyId</name>
        <value>%AWS.AccessKey%</value>
</property>
<property>
        <name>fs.s3.awsSecretAccessKey</name>
        <value>%AWS.SecretKey%</value>
</property>
<property>
        <name>fs.s3.awsAccessKeyId</name>
        <value>%AWS.AccessKey%</value>
</property>
 <property>
 	<name>io.compression.codecs</name>
    <value>org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec,org.apache.hadoop.io.compress.BZip2Codec</value>
  </property>
  <property>
    <name>io.compression.codec.lzo.class</name>
    <value>com.hadoop.compression.lzo.LzoCodec</value>
  </property>
  <property>
    <name>mapred.compress.map.output</name>
    <value>true</value>
  </property>
  <property>
    <name>mapred.map.output.compression.codec</name>
    <value>com.hadoop.compression.lzo.LzoCodec</value>
  </property>
</configuration>
EOF

cat > $HADOOP_HOME/conf/mapred-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>mapred.job.tracker</name>
  <value>hdfs://$MASTER_HOST:50002</value>
</property>
<property>
<name>mapreduce.map.java.opts</name>
<value>-Xmx1024M</value>
</property>
<property>
<name>mapreduce.reduce.java.opts</name>
<value>-Xmx1024M</value>
</property>
  <property>
    <name>mapreduce.map.output.compress</name>
    <value>true</value>
  </property>
  <property>
    <name>mapreduce.map.output.compress.codec</name>
    <value>com.hadoop.compression.lzo.LzoCodec</value>
  </property>
</configuration>
EOF

cat > $HADOOP_HOME/conf/hdfs-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>dfs.replication</name>
  <value>%HDFS.Replication%</value>
</property>
<property>
  <name>dfs.data.dir</name>
  <value>/mnt/hadoop</value>
</property>
</configuration>
EOF
#you probably need something like this part:
# Start services
[ ! -f /etc/hosts ] &&  echo "127.0.0.1 localhost" > /etc/hosts

mkdir -p /mnt/hadoop/logs

# not set on boot
export USER="root"

if [ "$IS_MASTER" == "true" ]; then
  # only format on first boot
  [ ! -e /mnt/hadoop/dfs ] && "$HADOOP_HOME"/bin/hadoop namenode -format
  "$HADOOP_HOME"/bin/hadoop-daemon.sh start namenode
  "$HADOOP_HOME"/bin/hadoop-daemon.sh start jobtracker
else
  # Hadoop
  "$HADOOP_HOME"/bin/hadoop-daemon.sh start datanode
  "$HADOOP_HOME"/bin/hadoop-daemon.sh start tasktracker
fi
#totally optional stuff
#Installing additional apps/libs - 
#run.common,run.master and run.slave are kept in %S3.ApplicationBucket%
#after hadoop is up get-stack downloads the scripts and runs run.common
#then run.master or run.slave which handle type specific installs.
#this allows keeping the bare minimum on the AMI image and makes
# updates relatively painless.
POOPER_TMP=/mnt/poopertmp
cat > /usr/local/bin/get-stack <<EOF
mkdir -p $POOPER_TMP
cd $POOPER_TMP
$HADOOP_HOME/bin/hadoop fs -get %S3.ApplicationBucket%/scripts/run.* ./
cd $POOPER_TMP
sh $POOPER_TMP/run.common
cd $POOPER_TMP
sh $POOPER_TMP/run.__TYPE__
EOF
if [ "$IS_MASTER" == "true" ]; then
  sed -i -e "s|__TYPE__|master|g" /usr/local/bin/get-stack
else
  sed -i -e "s|__TYPE__|slave|g" /usr/local/bin/get-stack
fi
#envs needed for install scripts
export HADOOP_HOME="$HADOOP_HOME"
export POOPER_TMP="$POOPER_TMP"
export POOPER_APP_BUCKET="%S3.ApplicationBucket%"
sh /usr/local/bin/get-stack