# hbase.docker

## summary
Run a hbase standalone service in docker.  
Use hbase tarball from project https://github.com/sel-fish/hbase-1.2.0-cdh5.7.0   
This project changes a little bit to original hbase-1.2.0-cdh5.7.0 as described [here](https://github.com/sel-fish/hbase-1.2.0-cdh5.7.0/releases/tag/mj23)

## tutorial

1. build image
  ```
git clone https://github.com/sel-fish/hbase.docker.git
cd hbase.docker
docker build -t debian-hbase .
```

2. run container
  ```
docker run -d -h $(hostname) -p 2181:2181 -p 60000:60000 -p 60010:60010 -p 60020:60020 -p 60030:60030 --name hbase debian-hbase
```

3. check the log of hbase
  ```
docker logs `docker ps -a |grep hbase |awk '{print $1}'`
```


