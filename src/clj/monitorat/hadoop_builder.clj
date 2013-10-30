(ns monitorat.hadoop-builder
  (:gen-class)
  (:require [clojure-hadoop.defjob :as defjob]
            [clojure-hadoop.wrap :as wrap]
            [clojure-hadoop.job :as job]
            [clojure-hadoop.imports :as imp]
            [cheshire.core :as json]
            [clojure.string :as str]
            [digest]
            )
  (:use [clojure.tools.cli :only [cli]])
  (:import [com.google.common.collect Sets MinMaxPriorityQueue]
           [org.apache.hadoop.io NullWritable]
           [java.util Set Map]
           [java.math BigDecimal])
  )

(imp/import-io)

;;;; aggregator
(defn sum [values]
  (reduce #(+ %1 %2) 0 values)
  )

(defn max-of-coll [values]
  (condp = (count values)
    0 nil,
    1 (first values)
    (reduce max values)))

(defn min-of-coll [values]
  (condp = (count values)
    0 nil,
    1 (first values)
    (reduce min values)))

(defn avg [values]
  (long (.divide (bigdec (sum values))
           (bigdec (count values))
           0
           BigDecimal/ROUND_HALF_UP)))

(defn tp [perc, values]
  (let [pg  (.create (MinMaxPriorityQueue/expectedSize (long (* (- 1 perc) (count values)))))
        n (inc (long (* (- 1M (bigdec  perc)) (count values))))]
    (doseq [v values]
      (if (>= (.size pg) n)
        (when (> v (.peekFirst pg))
          (.pollFirst pg)
          (.add pg v))
        (.add pg v)
        ))
    (.peekFirst pg)))

(def AGGREGATORS {:sum sum,
                  :count count,
                  :max max-of-coll
                  :min min-of-coll
                  :avg avg
                  :tp50 (partial tp 0.5)
                  :tp90 (partial tp 0.9)
                  :tp99 (partial tp 0.99)
                  :latest last
                  })

;;; power-set for set and map
(defmulti power-set class)

(defmethod power-set clojure.lang.Seqable [s]
  (set (Sets/powerSet (set s))))

(defmethod power-set Map [m]
  (if (= 0 (count m))
    [{}]
    (map
     #(select-keys m %) 
     (power-set (keys m)))))

;;; cartesian product
(defn cartesian-product [seq-of-seq]
  (Sets/cartesianProduct (apply list (map set seq-of-seq))))

(prefer-method power-set Map clojure.lang.Seqable)

(defmethod power-set :default [m]
  (println "this is default power-set:" (class m)))


(defn mapper [k v]
  (let [{:keys [user-id, metric-name, timestamp, dimensions, value]} (merge {:dimensions {}} (json/parse-string v true)),
        sub-dimensions-coll (power-set dimensions)]
    (map (fn [sub-dimensions]
           [[user-id metric-name (into (sorted-map) sub-dimensions)]
            (sorted-map :timestamp timestamp, :value value)])
         sub-dimensions-coll)))


(defn reducer [k, v-fn]
  (let [values (map #(:value %) (v-fn))
        [user-id metric-name dimensions] k]
    (for [[agg-name agg-fn] AGGREGATORS]
      [nil {:user-id user-id,
            :metric-name metric-name,
            :dimensions dimensions,
            :aggregator (name agg-name)
            :value (agg-fn (map #(bigdec %) values))
            }]
      )))

(defn reduce-writer [context k v]
  (.write
   context 
   (NullWritable/get)
   (Text. (json/generate-string v))
   ))


(defjob/defjob job
  :map mapper
  :map-reader wrap/int-string-map-reader
  :reduce reducer
  :reduce-writer reduce-writer
  :input "/var/tmp/tt/buildtest/input.txt"
  :output "/var/tmp/tt/buildtest/output"
  :input-format :text
  :output-format :text
  :compress-output false 
  :output-compressor "gzip"
  :replace true
  :reduce-tasks 6
  )


(defn -main [& args]
  (let [[ opts _ help] (cli args
                            ["--input" "hadoop job input files"]
                            ["--output" "hadoop job output"])]
    (println (type (:input ( job))))
    (job/run #(merge (job) opts))
    ))
