(ns jepsen-pikiwidb.append
  "Tests for transactional list append."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.pprint :refer [pprint]]
            [elle.core :as elle]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [util :as util :refer [parse-long]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as append]
            [jepsen.redis [client :as rc]]
            [taoensso.carmine :as car :refer [wcar]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn apply-mop!
  "Executes a micro-operation against a Carmine connection. This gets used in
  two different ways. Executed directly, it returns a completed mop. In a txn
  context, it's executed for SIDE effects, which must be reconstituted later."
  [conn [f k v :as mop]]
  (case f
    :r      [f k (wcar conn (car/lrange k 0 -1))]           ; 返回操作命令
    :append (do (wcar conn (car/rpush k (str v))) mop)      ; 直接执行，并返回m op
    )
  )

(defn parse-read
  "Turns reads of [:r :x ['1' '2'] into reads of [:r :x [1 2]]."
  [conn [f k v :as mop]]
  (try
    (case f
      :r [f k (mapv [parse-ong] v)]
      :append mop)
    (catch ClassCastException e
      (throw+ {:type        :unexpected-read-type
               :key         k
               :value       v}))))
               ; We're getting QUEUED in response to non-MULTI operations; I'm
               ; trying to figure out why, and to get debugging info, I'm gonna
               ; log whatever happens from an EXEC here.
               ;:exec-result (wcar conn (car/exec))}))))

(defrecord Client [conn]                                    ; `defrecord` 是 Clojure 中用于定义记录类型的宏。记录类型是类似于结构体（struct）或类（class）的数据结构，用于将一组相关的数据组合在一起。
  client/Client                                             ; Client 实现 client/Client 协议

  (open! [this test node]
    (rc/delay-exceptions 5
                         (let [c (rc/open node)]
                           ; (info :conn c)
                           (assoc this :conn (rc/open node)))))

  (setup! [_ test])

  (invoke! [_ test op]
    (rc/with-exceptions op #{}
      (rc/with-conn conn
        (->> (if (< 1 (count (:value op)))                  ; 取出`op`这个数据结构中的`:value`键对应的值，然后计算这个值的长度（假设它是一个集合或者序列），最后判断这个长度是否大于1。
               ; We need a transaction
               (->> (:value op)
                    ; Perform micro-ops for side effects
                    (mapv (partial apply-mop! conn))
                    ; In a transaction
                    (rc/with-txn conn)
                    ; And zip results back into the original txn
                    (mapv (fn [[f k v] r]
                            [f k (case f
                                   :r      r
                                   :append v)])
                          (:value op)))

               ; Just execute the mop directly, without a txn
               (->> (:value op)
                    (mapv (partial apply-mop! conn))))

             ; Parse integer reads
             (mapv (partial parse-read conn))               ; 对一个集合中的每个元素使用带有固定参数`conn`的`parse-read`函数进行处理，并返回处理后的结果向量。
             ; Returning that completed txn as an OK op
             (assoc op :type :ok, :value)                   ; 更新了映射`op`，为其添加了`:type`键，值为`:ok`，以及`:value`键，值为`nil`。
        )
      )
    )
  )

  (teardown! [_ test])

  (close! [this test]
    (rc/close! conn)))

(defn workload
  "A list append workload."
  [opts]
  (-> (append/test {; Exponentially distributed, so half of the time it's gonna
                    ; be one key, 3/4 of ops will use one of 2 keys, 7/8 one of
                    ; 3 keys, etc.
                    :key-count          (:key-count opts 12)
                    :min-txn-length     1
                    :max-txn-length     (:max-txn-length opts 4)
                    :max-writes-per-key (:max-writes-per-key opts 128)
                    :consistency-models [:strict-serializable]})
      (assoc :client (Client. nil))))
