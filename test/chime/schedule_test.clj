(ns chime.schedule-test
  (:require [clojure.test :refer :all]
            [chime.schedule :refer [chime-at] :as schedule]
            ;[chime.channel :refer [chime-ch]]
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
                            {:drop-overruns? true
                             :error-handler (fn [e] (reset! !error e))
                             :on-aborted    (fn [] (deliver !latch ::done))}))]
      (Thread/sleep 2000)
      ;; interrupt the task which is currently sleeping
      (future-cancel sched))

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

(deftest immutable-sanity-check
  (let [factory  (-> (Thread/ofVirtual)
                     (.name "chime-" 0)
                     .factory)
        state    (atom [])
        rec      (partial swap! state conj)
        instants (take 10 (times/every-n-seconds))
        sched    (->> {:thread-factory factory
                       :on-finished (partial rec "DONE!")
                       :on-aborted  (partial rec "ABORTED!")}
                      (chime-at instants rec))]
    (is (every? true? (schedule/skip-next-n! sched 2)))
    (is (false? (future-done? sched)))
    (is (= :chime.schedule/done (schedule/wait-for sched)))
    (is (true? (future-done? sched)))
    ;; 10 (jobs) - 2 (skipped) + 1 (on-finished)
    (is (= 9 (count @state)))
    (is (= "DONE!" (peek @state)))))

(deftest mutable-sanity-check
  (let [factory  (-> (Thread/ofVirtual)
                     (.name "chime-" 0)
                     .factory)
        state    (atom [])
        rec      (partial swap! state conj)
        instants (take 10 (times/every-n-seconds))
        sched    (->> {:thread-factory factory
                       :on-finished (partial rec "DONE!")
                       :on-aborted  (partial rec "ABORTED!")
                       :mutable? true}
                      (chime-at instants rec))]
    (is (true? (schedule/skip-next?! sched)))
    (is (false? (future-done? sched)))
    (schedule/append-relative-to-last! sched #(.plusSeconds ^Instant % 2))
    (is (= :chime.schedule/done (schedule/wait-for sched)))
    (is (true? (future-done? sched)))
    ;; 10 (jobs) - 1 (skipped) + 1 (appended) + 1 (on-finished)
    (is (= 11 (count @state)))
    (is (= "DONE!" (peek @state)))
    ;; assert that all pairs have a time-diff of 1 second
    ;; except the last one which has 2 seconds
    (let [pairs (partition 2 1 (butlast @state))
          [[^Instant x ^Instant y] wo-last] ((juxt last butlast) pairs)
          sec1 (Duration/ofSeconds 1)]
      (is (= (Duration/ofSeconds 2)
             (Duration/between x y)))
      (is (every? (fn [[x y]]
                    (= sec1 (Duration/between x y)))
                  wo-last)))))
