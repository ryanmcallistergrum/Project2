# Project2
## Project Description
For Project2, my small group created a Spark application that processes COVID data. As part of this processing, we came up with 11 queries to analyze the data from different perspectives and visualized our findings in Apache Zeppelin.

## Technologies Used
- Java - version 1.8.0_311
- Scala - version 2.12.10
- Apache Spark - version 3.1.2
- Spark SQL - version 3.1.2
- YARN - version 3.3.0
- HDFS - version 3.3.0
- Git + GitHub
- Zeppelin - version 0.10.0

## Features
- Utilizes Spark DataFrames to create views of the COVID data for visualizing in Zeppelin.

## Getting Started
- (Note, these instructions only support Windows 10 and above for Windows platforms)
- First, download and install Git for your system
  - For Windows, navigate to https://git-scm.com/download/win and install
  - For Unix, use "sudo apt-get install git"
- Run the following Git command where you would like to create a copy of the project repository using either Command Prompt or PowerShell in Windows, or the Terminal in Unix:
  - git clone https://github.com/ryanmcallistergrum/Project2.git
- Install Java
  - For Windows, navigate to https://www.oracle.com/java/technologies/javase/javase8u211-later-archive-downloads.html and install
  - For Unix, use "sudo apt-get install openjdk-8-jre-headless openjdk-8-jdk"
- For Windows, enable WSL and install Ubuntu
  - Go to Control Panel -> Programs -> Turn Windows features on or off
  - Enable "Virtual Machine Platform", "Windows Subsystem for Linux", and then reboot
  - Open the Microsoft Store and install "Ubuntu"
    - Open it and complete initial setup
- (The following steps will be for Ubuntu WSL for Windows and native Unix platforms)
- Install Java (for Ubuntu WSL only) (refer to Unix instructions above)
- Install Scala
  - sudo apt-get install scala
- Install Spark
  - wget https://downloads.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
  - tar --extract -f spark-3.1.2-bin-hadoop3.2.tgz
  - sudo mv spark-3.1.2-bin-hadoop3.2 /opt/spark
- Install Hadoop
  - cd
  - wget https://mirrors.estointernet.in/apache/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz
  - tar --extract -f hadoop-3.3.0.tar.gz
- Install SSH
  - sudo apt-get install ssh
- Start SSH
  - sudo service ssh restart
- Setup Environment Variables
  - vi ~/.bashrc
  - Enter following into file (press i to enter Insert Mode)
    - export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
    - export HADOOP_HOME=~/hadoop-3.3.0
    - export SPARK_HOME=/opt/spark
    - export PYSPARK_PYTHON=/usr/bin/python3
    - export PATH=$PATH:$HADOOP_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin
    - export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
    - export HADOOP_OPTS="$HADOOP_OPTS -Djava.net.preferIPv4Stack=true -Djava.security.krb5.realm= -Djava.security.krb5.kdc="
  - Press Esc to exit Insert Mode
  - Enter ":wq" to save the file and exit vi
  - Enter "source ~/.bashrc" to initialize the changes
- Edit etc/hadoop/core-site.xml
  - vi $HADOOP_CONF_DIR/core-site.xml
  - Enter the following into the file:
    - \<configuration>
         \<property>
            \<name>hadoop.tmp.dir\</name>
            \<value>/usr/local/Cellar/hadoop/hdfs/tmp\</value>
            \<description>A base for other temporary directories.\</description>
         \</property> 
      \</configuration>
      \<configuration>
         \<property>
            \<name>fs.defaultFS\</name>
            \<value>hdfs://localhost:9000\</value>
         \</property>
      \</configuration>
  - Save and exit using ":wq"
- Edit etc/hadoop/hdfs-site.xml
  - vi $HADOOP_CONF_DIR/hdfs-site.xml
  - Enter the following into the file:
    - \<configuration>
         \<property>
            \<name>dfs.replication\</name>
            \<value>1\</value>
         \</property>
      \</configuration>
  - Save and exit using ":wq"
- Edit file etc/hadoop/mapred-site.xml
  - vi $HADOOP_CONF_DIR/mapred-site.xml
  - Enter the following into the file:
    - \<configuration>
         \<property>
            \<name>mapred.job.tracker\</name>
            \<value>localhost:9010\</value>
         \</property>
      \</configuration>
      \<configuration> 
         \<property>
            \<name>mapreduce.framework.name\</name>
            \<value>yarn\</value>
         \</property>
         \<property>
            \<name>mapreduce.application.classpath\</name>
            \<value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*\</value>
         \</property>
      \</configuration>
  - Save and exit using ":wq"
- Edit file etc/hadoop/yarn-site.xml
  - vi $HADOOP_CONF_DIR/yarn-site.xml
  - Enter the following into the file:
    - \<configuration>
         \<property>
            \<name>yarn.nodemanager.aux-services\</name>
            \<value>mapreduce_shuffle\</value>
         \</property>
         \<property>
            \<name>yarn.nodemanager.env-whitelist\</name>
            \<value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME\</value>
         \</property>
      \</configuration>
  - Save and exit using ":wq"
- Create Hadoop directories
  - mkdir -p /home/hadoop/hadoopinfra/hdfs/namenode
  - mkdir -p /usr/local/Cellar/hadoop/hdfs/tmp
- Set permissions on Hadoop directories
  - sudo chmod 777 /home/hadoop/hadoopinfra/hdfs/namenode
  - sudo chmod -R 777 /usr/local/Cellar/hadoop/hdfs/tmp
- Format the NameNode
  - $HADOOP_HOME/bin/hdfs namenode -format
- Verify HDFS Startup
  - $HADOOP_HOME/sbin/start-dfs.sh
  - $HADOOP_HOME/sbin/start-yarn.sh
  - jps
    - The following should show
      - SecondaryNameNode
      - DataNode
      - ResourceManager
      - NodeManager
      - NameNode
      - Jps
- Install Zeppelin
  - wget https://dlcdn.apache.org/zeppelin/zeppelin-0.10.0/zeppelin-0.10.0-bin-all.tgz
  - tar --extract -f zeppelin-0.10.0-bin-all.tgz
- Start Zeppelin
  - ~/zeppelin-0.10.0-bin-all/bin/zeppelin-daemon.sh start
  - Verify by navigating to http://localhost:8080
- Copy JAR to HDFS
  - For Windows
    - In Project2 directory, navigate to target\\scala-2.12 and copy project2_2.12-0.1.jar to \\\\wsl$\\Ubuntu\\home\\USERNAME
    - In Ubuntu WSL, execute "hdfs dfs -copyFromLocal ~/project2_2.12-0.1.jar /"
  - For Unix
    - Navigate to target\\scala-2.12 in Project2 directory and execute "hdfs dfs -copyFromLocal project2_2.12-0.1.jar /"
- Configure Spark Interpreter in Zeppelin
  - After navigating to http://localhost:8080, left-click the "anonymous" user in the upper-right and select "Interpreter"
  - In the search bar, search for "Spark"
    - In the resulting interpreter list, click "Edit" in the upper-right to enable editing the values
    - Search for "spark.jars" and enter the following:
      - hdfs://localhost:9000/project2_2.12-0.1.jar

## Usage
- After setup, create a new Notebook in Zeppelin by selecting "Notebook" at the top and selecting "Create new note" in the list.
- Afterwards, create a new paragraph and enter the following to create a TempView for each question:
  - %spark
  - val loader = new QueryLoader
  - loader.loadQuery(1).createOrReplaceTempView("question01")
    - ![Q1 Mortality Rate](/TablesAndGraphs/Q1GraphMortalityRate.png?raw=true)
    - ![Q1 Spread Rate](/TablesAndGraphs/Q1GraphSpreadRate.png?raw=true)
    - ![Q1 Table](/TablesAndGraphs/Q1Table.png?raw=true)
  - loader.loadQuery(2).createOrReplaceTempView("question02")
    - ![Q2 Graph](/TablesAndGraphs/Q2Graph.png?raw=true)
    - ![Q2 Table](/TablesAndGraphs/Q2Table.png?raw=true)
  - loader.loadQuery(3).createOrReplaceTempView("question03")
    - ![Q3 Graph](/TablesAndGraphs/Q3Graph.png?raw=true)
    - ![Q3 Table](/TablesAndGraphs/Q3Table.png?raw=true)
  - loader.loadQuery(4).createOrReplaceTempView("question04")
    - ![Q4 Graph](/TablesAndGraphs/Q4Graph.png?raw=true)
    - ![Q4 Table](/TablesAndGraphs/Q4Table.png?raw=true)
  - loader.loadQuery(5).createOrReplaceTempView("question05")
    - ![Q5 Graph](/TablesAndGraphs/Q5Graph.png?raw=true)
    - ![Q5 Table](/TablesAndGraphs/Q5Table.png?raw=true)
  - loader.loadQuery(6).createOrReplaceTempView("question06")
    - ![Q6 Graph](/TablesAndGraphs/Q6GraphV2.png?raw=true)
    - ![Q6 Table](/TablesAndGraphs/Q6TableV2.png?raw=true)
  - loader.loadQuery(7).createOrReplaceTempView("question07")
    - ![Q7 Graph](/TablesAndGraphs/Q7Graph.png?raw=true)
    - ![Q7 Table](/TablesAndGraphs/Q7Table.png?raw=true)
  - loader.loadQuery(8).createOrReplaceTempView("question08")
    - ![Q8 Log Legend](/TablesAndGraphs/Q8LogLegend.png?raw=true)
    - ![Q8 Log Graph](/TablesAndGraphs/Q8LogTable.PNG?raw=true)
    - ![Q8 Log Table](/TablesAndGraphs/Q8Table.png?raw=true)
  - loader.loadQuery(9).createOrReplaceTempView("question09")
    - ![Q9 Graph](/TablesAndGraphs/Q9Graph.png?raw=true)
    - ![Q9 Table Best](/TablesAndGraphs/Q9TableBest.png?raw=true)
    - ![Q9 Table Worst](/TablesAndGraphs/Q9TableWorse.png?raw=true)
  - loader.loadQuery(10).createOrReplaceTempView("question10")
    - ![Q10 Graph](/TablesAndGraphs/Q10Graph.png?raw=true)
    - ![Q10 Table](/TablesAndGraphs/Q10Table.png?raw=true)
  - loader.loadQuery(11).createOrReplaceTempView("question11")
    - ![Q11 Graph](/TablesAndGraphs/Q11.png?raw=true)
    - ![Q11 Graph Detail](/TablesAndGraphs/Q11Detailed.PNG?raw=true)
    - ![Q11 Table](/TablesAndGraphs/Q11Table.PNG?raw=true)
- Then, click the play button ("Run all paragraphs") in the top-left to load the views
- You can now create new paragraphs to explore each question by entering the following, choosing the question you want to see, into new paragraphs and running them:
  - %sql
  - select * from question01

## Contributors
- The following batchmates contributed to the project:
  - Bao Doan (https://github.com/baodoan95)
  - David Carlson (https://github.com/David-Carlson)
  - Jaime Hinojos (https://github.com/jaimehinojos18)

## License
- This project uses the following license: The Unlicense