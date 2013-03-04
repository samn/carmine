(ns taoensso.carmine.connections
  "Handles life cycle of socket connections to Redis server. Connection pool is
  implemented using Apache Commons pool. Adapted from redis-clojure.

  Ref. Sentinel client spec: http://redis.io/topics/sentinel-clients"
  {:author "Peter Taoussanis"}
  (:require [taoensso.carmine (utils :as utils) (protocol :as protocol)])
  (:import  java.net.Socket
            [java.io BufferedInputStream DataInputStream BufferedOutputStream]
            [org.apache.commons.pool KeyedPoolableObjectFactory]
            [org.apache.commons.pool.impl GenericKeyedObjectPool]))

;; Hack to allow cleaner separation of ns concerns
(utils/declare-remote taoensso.carmine/ping
                      taoensso.carmine/auth
                      taoensso.carmine/select
                      taoensso.carmine/add-sentinel-server!)

;; Interface for socket connections to Redis server
(defprotocol IConnection
  (get-spec    [conn])
  (in-stream   [conn])
  (out-stream  [conn])
  (conn-alive? [conn])
  (close-conn  [conn]))

(defrecord Connection [^Socket socket spec]
  IConnection
  (get-spec    [_] spec)
  (in-stream   [_] (-> (.getInputStream socket)
                       (BufferedInputStream.)
                       (DataInputStream.)))
  (out-stream  [_] (-> (.getOutputStream socket)
                       (BufferedOutputStream.)))
  (conn-alive? [this]
    (if (:listener? spec)
      true ; TODO Waiting on Redis update, Ref. http://goo.gl/LPhIO
      (= "PONG" (try (protocol/with-context this (taoensso.carmine/ping))
                     (catch Exception _)))))
  (close-conn  [_] (.close socket)))

;; Interface for a pool of socket connections
(defprotocol IConnectionPool
  (get-conn     [pool spec])
  (release-conn [pool conn] [pool conn exception])
  (clear-conns  [pool] [pool spec]))

(declare resolve-spec! ^:dynamic *pool*) ; For Sentinel support

(defrecord ConnectionPool [^GenericKeyedObjectPool pool]
  IConnectionPool
  (get-conn     [_ spec] (binding [*pool* pool] (.borrowObject pool spec)))
  (release-conn [_ conn] (.returnObject pool (get-spec conn) conn))
  (release-conn [_ conn exception] (.invalidateObject pool (get-spec conn)
                                                      conn))
  (clear-conns  [_]      (.clear pool))
  (clear-conns  [_ spec] (.clear pool spec)))

(defn make-new-connection
  "Actually creates and returns a new socket connection."
  [spec]
  (let [{:keys [host port password timeout db] :as spec} (resolve-spec! spec)
        socket (doto (Socket. ^String host ^Integer port)
                 (.setTcpNoDelay true)
                 (.setKeepAlive true)
                 (.setSoTimeout ^Integer timeout))
        conn (Connection. socket spec)]
    (when password (protocol/with-context conn (taoensso.carmine/auth password)))
    (when (not (zero? db)) (protocol/with-context conn
                             (taoensso.carmine/select (str db))))
    conn))

(defrecord NonPooledConnectionPool []
  IConnectionPool
  (get-conn     [_ spec] (make-new-connection spec))
  (release-conn [_ conn] (close-conn conn))
  (release-conn [_ conn exception] (close-conn conn))
  (clear-conns  [_])
  (clear-conns  [_ spec]))

(def non-pooled-connection-pool
  "A degenerate connection pool. Gives us a pool-like interface for non-pooled
  connections."
  (NonPooledConnectionPool.))

(defn make-connection-factory []
  (reify KeyedPoolableObjectFactory
    (makeObject      [_ spec] (make-new-connection spec))
    (activateObject  [_ spec conn])
    (validateObject  [_ spec conn] (conn-alive? conn))
    (passivateObject [_ spec conn])
    (destroyObject   [_ spec conn] (close-conn conn))))

(defn set-pool-option [^GenericKeyedObjectPool pool [opt v]]
  (case opt
    :max-active                    (.setMaxActive pool v)
    :max-total                     (.setMaxTotal pool v)
    :min-idle                      (.setMinIdle pool v)
    :max-idle                      (.setMaxIdle pool v)
    :max-wait                      (.setMaxWait pool v)
    :lifo?                         (.setLifo pool v)
    :test-on-borrow?               (.setTestOnBorrow pool v)
    :test-on-return?               (.setTestOnReturn pool v)
    :test-while-idle?              (.setTestWhileIdle pool v)
    :when-exhausted-action         (.setWhenExhaustedAction pool v)
    :num-tests-per-eviction-run    (.setNumTestsPerEvictionRun pool v)
    :time-between-eviction-runs-ms (.setTimeBetweenEvictionRunsMillis pool v)
    :min-evictable-idle-time-ms    (.setMinEvictableIdleTimeMillis pool v)
    (throw (Exception. (str "Unknown pool option: " opt))))
  pool)

;;;; Redis Sentinel (EXPERIMENTAL)

(utils/defonce* sentinel-groups     "{group-name [[ip port]] ...]}" (atom {}))
(utils/defonce* last-resolved-specs "{spec resolved-spec}"          (atom {}))
(def ^:dynamic *pool* nil)

(comment (taoensso.carmine/add-sentinel-server! "my-app" "127.0.0.1"))

(defn- resolve-spec!
  "Ensures spec has a :host and :port (either statically, or as resolved via
  the Redis Sentinel discovery procedure).

  When called by a connection pool, clears any pool connections whose spec
  previously resolved differently. I.e. close pool connections any time the
  pool's resolved master address changes."

  ;; TODO Waiting for commands.json to be updated with Sentinel commands.
  ;; TODO Possible optimization: also have a rate-limited
  ;; `sort-sentinel-servers!` fn that will sort servers by their response time.

  [spec]
  (if-not (:sentinel-group spec)
    spec ; => Static :host and :port

    ;; Resolve master :host and :port via Sentinel (never cache!)
    (let [last-resolved-spec (@last-resolved-specs spec)
          resolved-spec ; TODO
          (let []
            ;; * Iterate list of [ip port] pairs in sentinel group, trying to
            ;;   connect to each with short :sentinel-timeout. (never cache)
            ;;
            ;;   ON A SUCCESSFUL CONNECTION:
            ;;     * Try `SENTINEL get-master-addr-by-name :sentinel-master`
            ;;       to return ip:port, null, or -IDONTKNOW.
            ;;
            ;;     ON SUCCESSFUL ip:port
            ;;       * Use as Redis master for pipeline.
            ;;       * Ensure sentinel is at top of group.
            ;;       * When not rate-limited, `SENTINEL get-master-add-by-name`
            ;;         and add any new sentinel servers to the END of our group.
            ;;
            ;;   ON NO SUCCESSFUL ip:port
            ;;     * No sentinel could be contacted => an error saying so.
            ;;     * All sentinels replied with null => Sentinels don't know
            ;;       requested master name.
            ;;     * >= 1 sentinels replied with -IDONTKNOW => Sentinels don't
            ;;       know the address of the requested master.
            )]

      ;; Maintain last-resolved-specs
      (when (and *pool* (not= resolved-spec last-resolved-spec))
        (swap! last-resolved-specs assoc spec resolved-spec)
        (when last-resolved-spec
          (clear-conns *pool* last-resolved-spec)))

      resolved-spec)))