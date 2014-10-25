========
拉取kafka的数据到mysql的脚本 <br/>

安装依赖包的方式:<br/>
1.sudo pip install -Iv https://github.com/mumrah/kafka-python/archive/master.zip  安装最新的版本,直接安装的存在bug.还有在执行的时候缺少依赖包six .执行sudo easy_install six<br/>
2.sudo pip install MySQL-python<br/>
<br/>
如果pip没装的话,在ubuntu里面可以执行sudo apt-get install python-pip<br/>
sudo apt-get install gcc <br/>
sudo apt-get install python-dev <br/>
sudo apt-get install libmysqlclient-dev <br/>

如果已经安装了可以按照如下方式卸载:<br/>
sudo pip uninstall kafka-python