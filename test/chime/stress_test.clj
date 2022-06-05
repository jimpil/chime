(ns chime.stress-test
  (:require ;[clojure.test :refer :all]
            [chime.scheduler :as scheduler]
            [chime.times :as times])
  (:import (java.time Instant)
           (java.time.temporal ChronoUnit)))

(def vthread-factory
  "A `ThreadFactory` that produces virtual-threads."
  (-> (Thread/ofVirtual)
      (.name "chime-" 1)
      .factory))

(defn- calculate-drift
  [^Instant i]
  (let [now (times/now)]
    (println "Drift-ms:" (.between ChronoUnit/MILLIS i now))))

(defn stress [n-schedules]
  (let [sched (scheduler/chiming-agent
                {:thread-factory vthread-factory
                 :on-finished (partial println "Finished-")})
        intervals (vec (range 1 61))
        ids (range n-schedules)
        times-fn #(->> (rand-nth intervals)
                       times/every-n-seconds
                       (take 10))]
    (->> (partial vector calculate-drift times-fn)
         (repeatedly)
         (zipmap ids)
         (scheduler/schedule! sched))

    (Thread/sleep 500)
    (->> (vals @sched)
         (map deref)
         (every? #{:chime.schedule/done}))
    )
  )

;(stress 1000)
