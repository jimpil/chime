(ns chime.schedule-test
  (:require [clojure.test   :refer :all]
            [chime.schedule :refer [chime-at] :as schedule]
            [chime.channel  :refer [chime-ch]]
            [chime.times :as times])
  (:import (java.time Instant Duration)))

(defn check-timeliness!
  "Checks whether the chimes actually happened at the time for they were scheduled."
  [proof]
  (doseq [[value taken-at] proof
          :let [diff (->> [value taken-at]
                          (map #(.toEpochMilli ^Instant %))
                          ^long (apply -)
                          (Math/abs))]]
    (is (< diff 20)
          (format "Expected to run at ±%s but run at %s, i.e. diff of %dms" value taken-at diff))))

(deftest test-chime-at
  (let [times [(.minusSeconds (Instant/now) 2)
               (.plusSeconds (Instant/now) 1)
               (.plusSeconds (Instant/now) 2)]
        proof (atom [])]
    (with-open [sched (chime-at times
                                    (fn [t]
                                      (swap! proof conj [t (Instant/now)])))]
      (Thread/sleep 2500))
    (is (= times (mapv first @proof)))
    (check-timeliness! (rest @proof))))

(deftest empty-times
  (testing "Empty or completely past sequences are acceptable"
    (let [proof (atom false)]
      (chime-at [] identity {:on-finished (fn [] (reset! proof true))})

      (is @proof))))

(deftest test-on-finished
  (let [proof (atom false)]
    (chime-at [(.plusMillis (Instant/now) 500) (.plusMillis (Instant/now) 1000)]
              (fn [t])
              {:on-finished (fn [] (reset! proof true))})
    (Thread/sleep 1200)
    (is @proof)))

(deftest test-error-handler
  (testing "returning true from error-handler continues the schedule"
    (let [proof (atom [])
          sched (chime-at [(.plusMillis (Instant/now) 500)
                           (.plusMillis (Instant/now) 1000)]
                          (fn [time]
                            (throw (ex-info "boom!" {:time time})))
                          {:error-handler (fn [e]
                                            (swap! proof conj e)
                                            true)})]
      (is (not= ::timeout (deref sched 1500 ::timeout)))
      (is (= 2 (count @proof)))
      (is (every? ex-data @proof))))

  (testing "returning false from error-handler stops the schedule"
    (let [proof (atom [])
          sched (chime-at [(.plusMillis (Instant/now) 500)
                           (.plusMillis (Instant/now) 1000)]
                          (fn [time]
                            (throw (ex-info "boom!" {:time time})))
                          {:error-handler (fn [e]
                                            (swap! proof conj e)
                                            false)})]
      (is (not= ::timeout (deref sched 1500 ::timeout)))
      (is (= 1 (count @proof)))
      (is (every? ex-data @proof)))))

(deftest test-long-running-jobs
  (let [proof (atom [])
        !latch (promise)
        now (Instant/now)
        times (->> (times/periodic-seq now (Duration/ofMillis 500))
                   (take 3))
        sched (chime-at times
                        (fn [time]
                          (swap! proof conj [time (Instant/now)])
                          (Thread/sleep 750)))]

    (is (not= ::nope (deref sched 4000 ::nope)))
    (is (= times (map first @proof)))
    (check-timeliness! (map vector
                            (->> (times/periodic-seq now (Duration/ofMillis 750))
                                 (take 3))
                            (map second @proof)))))

(deftest test-cancelling-overrunning-task
  (let [!proof (atom [])
        !error (atom nil)
        !latch (promise)]
    (with-open [sched (-> (times/periodic-seq (Instant/now) (Duration/ofSeconds 1))
                          (chime-at
                            (fn [now]
                              (swap! !proof conj now)
                              (Thread/sleep 3000))
                            {:error-handler (fn [e]
                                              (reset! !error e))
                             :on-finished   (fn []
                                              (deliver !latch nil))}))]
      (Thread/sleep 2000)
      (future-cancel sched)) ;; must be explicit!

    (is (not= ::timeout (deref !latch 500 ::timeout)))
    (is (= 1 (count @!proof)))
    (is (instance? InterruptedException @!error))))

(deftest test-only-call-on-finished-once-36
  (let [!count (atom 0)
        now (Instant/now)]
    (with-open [sched (chime-at [(.plusMillis now 500)
                                 (.plusMillis now 500)]
                                (fn [t])
                                {:on-finished #(swap! !count inc)})]
      (Thread/sleep 1000))

    (is (= 1 @!count))))

(deftest test-cancellation-works-even-in-the-face-of-overrun-past-tasks
  (let [proof (atom [])
        do-stuff (fn [now]
                   ;; some overrunning task:
                   (swap! proof conj now)
                   (Thread/sleep 5000))
        sched (chime-at (rest (times/periodic-seq (Instant/now) (Duration/ofSeconds 1)))
                                do-stuff)]
    (Thread/sleep 3000)
    (schedule/shutdown-now! sched)
    (is (= 1 (count @proof)))))

(deftest test-empty-or-completely-past-sequences-are-acceptable
  (let [proof (atom false)]
    (chime-at (map #(.minusSeconds (Instant/now) (* 60 %)) [5 4 3 2])
              identity
              {:on-finished (fn [] (reset! proof true))})
    (while (not @proof))
    (is @proof))

  (let [proof (atom false)]
    (chime-at []
              identity
              {:on-finished (fn [] (reset! proof true))})
    (while (not @proof))
    (is @proof)))
