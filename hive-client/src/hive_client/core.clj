(ns hive-client.core
  (:require [clojure.java.jdbc :as j]
            [taoensso.timbre :refer [info debug warn set-level!]]))

(set-level! :trace) 
(def hive-db {:classname "org.apache.hive.jdbc.HiveDriver"
              :subname "//192.168.1.2:10000/ods"
               :subprotocol "hive2"
               :user "spiderdt"
               :password "spiderdt"})

(with-open  [con (j/get-connection hive-db)]
  #_(.execute (j/prepare-statement con "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hive-hbase-handler-2.1.0.jar"))
  #_(.execute (j/prepare-statement con "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hbase-common-1.1.1.jar"))
  #_(.execute (j/prepare-statement con "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hbase-client-1.1.1.jar "))
  #_(.execute (j/prepare-statement con "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hbase-hadoop2-compat-1.1.1.jar"))
  #_(.execute (j/prepare-statement con "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hbase-server-1.1.1.jar"))
  #_(.execute (j/prepare-statement con "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hbase-protocol-1.1.1.jar"))
  #_(.execute (j/prepare-statement con "set hive.fetch.task.conversion=none"))
  #_(time (doall (j/result-set-seq (.executeQuery (j/prepare-statement con "select * from hbase.d_sample_data where rowkey = '2012-09-17#BUS041000#0000000013762663'") )) ) ) 
  (let [rs (.executeQuery (j/prepare-statement con "select * from hbase.d_sample_data where rowkey = '2012-09-17#BUS041000#0000000013762663'"))]
    (println "fetch result")
    (doall (j/result-set-seq rs))
)  )

#_(j/query hive-db "select * from d_bolome_events")
#_(.getMetaData (j/get-connection hive-db)) 
#_(with-open  [con (j/get-connection hive-db)]
  (.execute (j/prepare-statement con "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hive-hbase-handler-2.1.0.jar"))
  (.execute (j/prepare-statement con "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hbase-common-1.1.1.jar"))
  (.execute (j/prepare-statement con "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hbase-client-1.1.1.jar "))
  (.execute (j/prepare-statement con "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hbase-hadoop2-compat-1.1.1.jar"))
  (.execute (j/prepare-statement con "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hbase-server-1.1.1.jar"))
  (.execute (j/prepare-statement con "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hbase-protocol-1.1.1.jar"))
  
  #_(.execute (j/prepare-statement con "set hive.fetch.task.conversion=none"))
  (time (doall (j/result-set-seq (.executeQuery (j/prepare-statement con "select * from d_bolome_events")))))
  )

(comment
  
  (j/query hive-db "select inventory_id from  hbase.d_sample_data")
  (j/query hive-db "select * from hbase.d_sample_data")
  (j/query hive-db "select count(*) from hbase.d_sample_data")
  (j/query hive-db "select count(1) from hbase.d_sample_data")
  (j/execute! hive-db "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hive-hbase-handler-2.1.0.jar")
  (j/db-do-commands hive-db "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hive-hbase-handler-2.1.0.jar")
  (j/query hive-db "add jar /home/spiderdt/work/git/spiderdt-env/cluster/tarball/apache-hive-2.1.0-bin/lib/hive-hbase-handler-2.1.0.jar")
  (j/metadata-query )
  (j/get-connection hive-db)
  )






