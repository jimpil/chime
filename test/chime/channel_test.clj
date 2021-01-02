(ns chime.channel-test
  (:require [chime.channel :refer [chime-ch]]
            [chime.schedule-test :refer [check-timeliness!]]
            [clojure.test :refer :all]
            [clojure.core.async :as a :refer [go-loop]]
            [chime.times :as times])
  (:import [java.time Instant Duration]
           (java.time.temporal ChronoUnit)))

(deftest test-chime-ch
  (let [now (Instant/now)
        times [(.minusSeconds now 2)
               (.plusSeconds now 1)
               (.plusSeconds now 2)]
        chimes (chime-ch times)
        proof (atom [])]

    (a/<!! (go-loop []
             (when-let [msg (a/<! chimes)]
               (swap! proof conj [msg (Instant/now)])
               (recur))))

    (is (= times (mapv first @proof)))
    (check-timeliness! (rest @proof))))

(deftest test-channel-closing
  (let [now (Instant/now)
        times [(.minusSeconds now 2)
               (.plusSeconds now 1)
               (.plusSeconds now 2)]
        chimes (chime-ch times)
        proof (atom [])]
    (a/<!! (a/go
             (swap! proof conj (a/<! chimes))
             (swap! proof conj (a/<! chimes))
             (a/close! chimes)
             (when-let [v (a/<! chimes)]
               (swap! proof conj v))))
    (is (= (butlast times) @proof))))

(deftest test-after-a-pause-past-items-aren-t-skipped
  ;; test case for 0.1.5 bugfix - thanks Nick!
  (let [proof (atom [])
        ch (chime-ch (->> (times/periodic-seq
                            (-> (.plusSeconds (Instant/now) 2)
                                (.truncatedTo (ChronoUnit/SECONDS)))
                            (Duration/ofSeconds 1))
                          (take 3)))]

    (swap! proof conj [(a/<!! ch) (Instant/now)])
    (check-timeliness! (rest @proof))
    (a/<!! (a/timeout 4000))
    ;; Pending timestamps come through in the past.
    (is (a/<!! ch) "A time value is returned, and not nil")
    (is (a/<!! ch) "A time value is returned, and not nil")))
