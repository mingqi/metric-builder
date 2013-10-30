(defproject metric-builder "0.1.0"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [clojure-hadoop "1.4.3"]
                 [cheshire "5.2.0"]
                 [com.google.guava/guava "14.0.1"]
                 [org.clojure/tools.cli "0.2.2"]
                 [digest "1.4.3"]
                 ]
  ;:aot [monitorat.hadoop-builder]
  :source-paths ["src/clj"]
  :main monitorat.hadoop-builder)
