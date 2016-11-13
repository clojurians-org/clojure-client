(ns hdfs-client.core
  (:require [taoensso.timbre :refer [info debug warn set-level!]])
  (:import [org.apache.hadoop.hdfs DFSClient]
           [org.apache.hadoop.conf Configuration]
           [java.net URI]
           [org.apache.hadoop.hdfs.protocol HdfsFileStatus]))

(set-level! :trace) 
(let [client-aaa (new DFSClient (new URI "hdfs://192.168.1.3:9000") (new Configuration))]
  #_(with-open [stream (.create client "/user/larluo/a/a/a" true)]
    #_(.write stream (.getBytes "aaa"))
    
      )
  #_(.mkdirs client "/user/hive/larluo")
  #_(.rename client "/user/larluo/a/a" "/user/hive/warehouse/ods.db/d_bolome_orders/p_date=2016-06-28" )
  (.listPaths client-aaa "/user" HdfsFileStatus/EMPTY_NAME)
  ) 

(do
  (import '[java.net InetSocketAddress])
  (import '[java.io BufferedInputStream BufferedOutputStream ByteArrayOutputStream
            DataOutputStream DataInputStream])
  (import '[com.google.protobuf ByteString])
  (import '[org.apache.hadoop.ipc.protobuf RpcHeaderProtos 
            RpcHeaderProtos$RpcKindProto
            RpcHeaderProtos$RpcRequestHeaderProto$OperationProto
            RpcHeaderProtos$RpcRequestHeaderProto
            RpcHeaderProtos$RpcSaslProto
            RpcHeaderProtos$RpcSaslProto$SaslState
            ])
  
  (def socket (.createSocket (javax.net.SocketFactory/getDefault)) )
  (doto socket (.setKeepAlive true ))
  (.connect socket (new InetSocketAddress "192.168.1.3" 9000)  )
  (doto socket (.setSoTimeout 0))
  
  (def saslHeader (-> (doto (RpcHeaderProtos$RpcRequestHeaderProto/newBuilder)
                        (.setRpcOp RpcHeaderProtos$RpcRequestHeaderProto$OperationProto/RPC_FINAL_PACKET)
                        (.setRpcKind RpcHeaderProtos$RpcKindProto/RPC_PROTOCOL_BUFFER)
                        (.setCallId (int -33)) ; SASL
                        (.setClientId (ByteString/copyFrom (byte-array [])) )   ; DUMMY_CLIENT_ID
                        (.setRetryCount -1) ; INVALID_RETRY_COUNT
                        ) .build))

    (def negotiateRequest (-> (doto (RpcHeaderProtos$RpcSaslProto/newBuilder)
                                (.setState (RpcHeaderProtos$RpcSaslProto$SaslState/NEGOTIATE) )
                                ) .build))
    
    (def out (->> (.getOutputStream socket) (new BufferedOutputStream) (new DataOutputStream)) )
    
     (doto out (.writeBytes "hrpc")        ; HEADER
          (.writeByte 9)  ; CURRENT_VERSION
          (.writeByte 0)   ; serviceClass 
          (.writeByte -33) ; SASL
          )

     (def bos (new ByteArrayOutputStream))
    (.writeDelimitedTo saslHeader bos)
    (.writeDelimitedTo negotiateRequest bos)
    (def sasl-negotiate (.toByteArray bos) )
    (doto out (.writeInt (count sasl-negotiate))
          #_(.write sasl-negotiate)
          #_(.flush))
    (.writeDelimitedTo saslHeader out)
    (.writeDelimitedTo saslHeader out)
    (.flush out)

    (def in (->> (.getInputStream socket) (new BufferedInputStream) (new DataInputStream)) )
    (def length (.readInt in))
    length
    (def str (slurp in))
    str
    (seq (.getBytes str) ) 
    )

