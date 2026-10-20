import os

hadoop_conf = "C:/hadoop/etc/hadoop"
os.makedirs("D:/BigData/hadoop_data/hdfs/namenode", exist_ok=True)
os.makedirs("D:/BigData/hadoop_data/hdfs/datanode", exist_ok=True)

core_site = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
"""

hdfs_site = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///D:/BigData/hadoop_data/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///D:/BigData/hadoop_data/hdfs/datanode</value>
    </property>
    <property>
        <name>dfs.permissions.enabled</name>
        <value>false</value>
    </property>
</configuration>
"""

mapred_site = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
</configuration>
"""

yarn_site = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>localhost</value>
    </property>
</configuration>
"""

with open(os.path.join(hadoop_conf, "core-site.xml"), "w", encoding="utf-8") as f:
    f.write(core_site)
with open(os.path.join(hadoop_conf, "hdfs-site.xml"), "w", encoding="utf-8") as f:
    f.write(hdfs_site)
with open(os.path.join(hadoop_conf, "mapred-site.xml"), "w", encoding="utf-8") as f:
    f.write(mapred_site)
with open(os.path.join(hadoop_conf, "yarn-site.xml"), "w", encoding="utf-8") as f:
    f.write(yarn_site)

# Set JAVA_HOME in hadoop-env.cmd
hadoop_env_path = os.path.join(hadoop_conf, "hadoop-env.cmd")
env_lines = []
if os.path.exists(hadoop_env_path):
    with open(hadoop_env_path, "r", encoding="utf-8", errors="ignore") as f:
        env_lines = f.readlines()

new_env_lines = [line for line in env_lines if "set JAVA_HOME" not in line]
new_env_lines.insert(0, "set JAVA_HOME=C:/jdk-17\n")
with open(hadoop_env_path, "w", encoding="utf-8") as f:
    f.writelines(new_env_lines)

print("Hadoop native Windows configuration updated successfully in", hadoop_conf)
