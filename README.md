# mbaBigData

### Criação de novo ambiente (CentOS 7)
Para a criação de um ambiente para utilização desse repositório, basta seguir os passos abaixo:
```shell

yum install -y https://centos7.iuscommunity.org/ius-release.rpm

yum -y install git python36u python36u-pip python36u-devel java-1.8.0-openjdk wget

pip3.6 install jupyter

systemctl stop firewalld

systemctl disable firewalld

setenforce 0

cd /opt

wget http://ftp.unicamp.br/pub/apache/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz

tar -xzvf spark-2.4.3-bin-hadoop2.7.tgz

ln -s spark-2.4.3-bin-hadoop2.7 spark

cd spark

./sbin/start-master.sh

~ acessar: http://[ip-master]:8080/ e verificar se está de pé ~

./sbin/start-slave.sh spark://[ip-master]:7077

cat << \EOF >> /etc/bashrc

export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=/usr/bin/python3.6
EOF

```
> Seguir esse passo-a-passo em um ambiente CentOS 7.6 ou superior, com acesso direto à internet.




###### [Créditos](https://opensource.com/article/18/11/pyspark-jupyter-notebook) por ter dado a base necessária para criar esse tutorial.
