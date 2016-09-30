(ns hdfs-client.core
  (:import [org.apache.hadoop.hdfs DFSClient]
           [org.apache.hadoop.conf Configuration]
           [java.net URI]))

(let [client (new DFSClient (new URI "hdfs://192.168.1.3:9000") (new Configuration))]
  #_(with-open [stream (.create client "/user/larluo/a/a/a" true)]
    #_(.write stream (.getBytes "aaa"))
    
      )
  (.mkdirs client "/user/hive/warehouse/ods.db/d_bolome_orders/p_date=2016-06-28")
  (.rename client "/user/larluo/a/a" "/user/hive/warehouse/ods.db/d_bolome_orders/p_date=2016-06-28")
  ) 
