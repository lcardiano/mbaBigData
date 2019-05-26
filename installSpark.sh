#/bin/bash
apt-get install maven -y
cd ~
mkdir sparkDownload
cd sparkDownload
wget http://ftp.unicamp.br/pub/apache/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz
tar -xvf spark-2.4*
mv spark-2.1* /usr/local/spark
cd /usr/local/spark
echo export SPARK_HOME=/usr/local/spark >> ~/.bashrc
echo export PATH=$SPARK_HOME/bin:$PATH~ >> ~/.bashrc             
