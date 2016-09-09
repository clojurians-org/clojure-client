(ns mysql-client.core
  (:require [clojure.java.jdbc :as j]))

(def mysql-db {:dbtype "mysql"
               :dbname "state_store"
               :user "state_store"
               :password "spiderdt"})

(j/query mysql-db "select * from data_items")



