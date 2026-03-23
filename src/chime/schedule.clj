(ns chime.schedule
  "Lightweight scheduling library."
  (:require [clojure.tools.logging :as log]
            [chime.times :as times])
  (:import (clojure.lang IDeref IBlockingDeref IPending ISeq IAtom2 PersistentQueue IAtom)
           (java.time Instant Clock ZonedDateTime)
           (java.time.temporal ChronoUnit)
           (java.util.concurrent ThreadFactory TimeUnit ScheduledFuture ScheduledThreadPoolExecutor)
           (java.lang AutoCloseable)
           (java.util.concurrent.atomic AtomicLong)))

(def ^:private default-thread-factory
  (let [thread-no (AtomicLong. 0)]
    (reify ThreadFactory
      (newThread [_ r]
        (doto (Thread. r)
          (.setName (str "chime-" (.incrementAndGet thread-no))))))))

(defn default-error-handler
  ([e]
   (default-error-handler nil e))
  ([id e]
   (log/warn e (cond-> "Error running scheduled fn"
                       (some? id) (str \space id)))
   true))

(def ^:private seq-step
  (juxt first rest))

(defn- atom-step [a]
  (let [[old _] (swap-vals! a pop)]
    [(first old) a]))

(defn- mutable-times
  [times]
  (atom (into PersistentQueue/EMPTY times)))

(defn interruptable*
  "Returns a function which wraps <f>
   with a `Thread.isInterrupted()` check.
   Worth considering when using `shutdown-now!` or `skip-next!`."
  [f]
  (fn [x]
    (when-not (.isInterrupted (Thread/currentThread))
      (f x))))

(defmacro interruptable
  "Like `interruptable*`, but for arbitrary code that
   doesn't care about the argument passed to job-fns."
  [& body]
  `(interruptable* (fn [~'_] ~@body)))

(defn chime-at
  "Calls <f> with the current time, at every time in the <times> sequence.

  ```
  (:require [chime.schedule :as chime])
  (:import [java.time Instant])

  (let [now (Instant/now)]
    (chime/chime-at
      [(.plusSeconds now 2)
       (.plusSeconds now 4)]
      (fn [time]
        (println \"Chiming at\" time)))
  ```

  If one of those times is in the past (e.g. by accident), or a job spills over to
  the next time (i.e. overunning), it will (by default) be scheduled with no delay
  (i.e. 'push-forward' semantics). If you don't want that to happen, use the <drop-overruns?>
  option (i.e. 'catch-up' semantics).

  Providing a custom <thread-factory> is supported, but optional (see `default-thread-factory`).
  Providing a custom <clock> is supported, but optional (see `times/*clock*`).
  Providing a custom (1-arg) `error-handler` is supported, but optional (see `default-error-handler`).
  Return truthy from this function to continue the schedule (the default), or falsy to shut it down.

  Returns an AutoCloseable that you can `.close` in order to shutdown the schedule.
  You can also deref the return value to wait for the schedule to finish.

  When the schedule is either manually closed (w/o an <on-aborted> callback) or exhausted,
  the <on-finished> callback will be called. If <on-aborted> has been provided it will be
  called instead (only on manual close)."

  (^AutoCloseable [times f] (chime-at times f nil))

  (^AutoCloseable [times f {:keys [error-handler on-finished on-aborted ^ThreadFactory thread-factory clock drop-overruns? mutable?]
                            :or {error-handler  default-error-handler
                                 thread-factory default-thread-factory ;; loom-friendly (i.e. virtual threads)
                                 clock          times/*clock*
                                 mutable?       false}}]
   (let [times (cond-> times mutable? mutable-times)
         step* (if mutable? atom-step seq-step)
         pool-size (if (-> thread-factory
                           (.newThread (constantly nil))
                           (.isVirtual))
                     0 ;; don't pool virtual-threads!
                     1)
         pool   (ScheduledThreadPoolExecutor. pool-size thread-factory)
         !latch (promise)
         done?  (partial realized? !latch)
         current (atom nil)
         f      (bound-fn* f)
         error! (bound-fn* error-handler)
         next-times (atom nil)]
     (letfn [(close! [f!]
               (when-not (done?)
                 (deliver !latch ::done)
                 (and f! (f!))
                 (.shutdown pool)))

             (schedule-loop [times]
               (let [[curr-time times] (step* times)]
                 (letfn [(task []
                          (if (try
                                (when-not (done?)
                                  ;; provide the object provided by the user
                                  ;; (as opposed to Instant converted)
                                  (f curr-time)
                                  true)
                                (catch Exception e
                                  (try
                                    (error! e)
                                    (catch Exception e
                                      (log/error e "error calling chime error-handler, stopping schedule")))))

                            (do
                              (reset! next-times (if mutable? (-> times deref seq) times))
                              (schedule-loop times))
                            (close! on-finished)))]

                  (if curr-time
                    (let [dlay (->> (times/to-instant curr-time)
                                    (.between ChronoUnit/MILLIS (times/now clock)))]
                      (if (or (pos? dlay)
                              (not drop-overruns?))
                        (->> (.schedule pool ^Runnable task (max (long 0) dlay) TimeUnit/MILLISECONDS)
                             (reset! current))
                        (recur times))) ;; drop job that missed its target
                    (close! on-finished)))))]

       ;; kick-off the schedule loop
       (schedule-loop times)

       (reify ;; the returned object represents 2 things
         AutoCloseable ;; whole-schedule
         (close [_] (close! on-aborted))

         IDeref ;; whole-schedule
         (deref [_] (deref !latch))

         IBlockingDeref ;; whole-schedule
         (deref [_ ms timeout-val] (deref !latch ms timeout-val))

         IPending ;; whole-schedule
         (isRealized [_] (done?))

         ScheduledFuture ;; next-job
         (cancel [_ interrupt?] ;; expose interrupt facilities (opt-in)
           (when-let [^ScheduledFuture fut @current]
             (let [ret (or (.isCancelled fut)
                           (.cancel fut interrupt?))]
               (when (and (true? ret)
                          (not (done?)))
                 ;; don't forget to re-schedule starting
                 ;; with the job AFTER the cancelled one
                 (cond-> (swap! next-times rest)
                         mutable? mutable-times
                         true schedule-loop))
             ret)))
         (getDelay [_ time-unit] ;; expose remaining time until next chime
           (when-let [^ScheduledFuture fut @current]
             (.getDelay fut time-unit)))

         IAtom
         (swap [_ f]
           (if mutable?
             (swap! next-times f)
             (throw (UnsupportedOperationException. "Schedule NOT mutable!"))))
         (swap [_ f arg1]
           (if mutable?
             (swap! next-times f arg1)
             (throw (UnsupportedOperationException. "Schedule NOT mutable!"))))
         (swap [_ f arg1 arg2]
           (if mutable?
             (swap! next-times f arg1 arg2)
             (throw (UnsupportedOperationException. "Schedule NOT mutable!"))))
         (swap [_ f arg1 arg2 more]
           (if mutable?
             (swap! next-times (partial apply f) arg1 arg2 more)
             (throw (UnsupportedOperationException. "Schedule NOT mutable!"))))

         IAtom2
         (swapVals [_ f]
           (if mutable?
             (swap-vals! next-times f)
             (throw (UnsupportedOperationException. "Schedule NOT mutable!"))))
         (swapVals [_ f arg1]
           (if mutable?
             (swap-vals! next-times f arg1)
             (throw (UnsupportedOperationException. "Schedule NOT mutable!"))))
         (swapVals [_ f arg1 arg2]
           (if mutable?
             (swap-vals! next-times f arg1 arg2)
             (throw (UnsupportedOperationException. "Schedule NOT mutable!"))))
         (swapVals [_ f arg1 arg2 more]
           (if mutable?
             (swap-vals! next-times (partial apply f) arg1 arg2 more)
             (throw (UnsupportedOperationException. "Schedule NOT mutable!"))))

         ;ISeq ;; whole schedule
         ;(first [_] (some-> next-times deref first))
         ;;; not to be used directly (danger of holding the head)
         ;;; but more  to support seq fns like `second`, `nth` etc
         ;(next [_] (some-> next-times deref next))
         ;(more [_] (some-> next-times deref rest))

         )))))

(defn chime-at-fixed-rate
  "A version of `chime-at`, optimized for non-mutable/infinite schedules using a fixed <interval> (in millis).
   Like `chime-at` the schedule is considered finished when the task handler throws AND the error-handler doesn't return truthy.
   On the other hand, the schedule is considered aborted, when `.close()` is called on it."
  [^long interval f {:keys [error-handler on-finished on-aborted ^ThreadFactory thread-factory clock initial-delay-supplier]
                     :or {error-handler  default-error-handler
                          thread-factory default-thread-factory ;; loom-friendly (i.e. virtual threads)
                          clock          times/*clock*
                          initial-delay-supplier  (constantly 0)}}]
  (let [pool-size (if (-> thread-factory
                          (.newThread (constantly nil))
                          (.isVirtual))
                    0 ;; don't pool virtual-threads!
                    1)
        pool   (ScheduledThreadPoolExecutor. pool-size thread-factory)
        !latch (promise)
        done?  (partial realized? !latch)
        f      (bound-fn* f)
        error! (bound-fn* error-handler)
        current (atom nil)
        close! (fn [f!]
                 (when-not (done?)
                   (deliver !latch ::done)
                   (and f! (f!))
                   (.shutdown pool)))
        task (fn []
               (try
                 ;; simulate the fact that the schedule stops if the task-handler throws,
                 ;; AND the error-handler doesn't return truthy (or itself throws).
                 (when-not (done?)
                   (f (times/now clock)))
                 (catch Exception e
                   (try
                     (when-not (error! e)
                       (close! on-finished))
                     (catch Exception e
                       (log/error e "error calling chime error-handler, stopping schedule")
                       (close! on-finished))))))
        schedule-loop (fn [^long dlay]
                        (->> (.scheduleAtFixedRate pool task dlay interval TimeUnit/MILLISECONDS)
                             (reset! current)))]
    (schedule-loop (initial-delay-supplier)) ;; kick things off
    (reify ;; the returned object represents 2 things
      AutoCloseable ;; whole-schedule
      (close [_] (close! on-aborted))

      IDeref ;; whole-schedule
      (deref [_] (deref !latch))

      IBlockingDeref ;; whole-schedule
      (deref [_ ms timeout-val] (deref !latch ms timeout-val))

      IPending ;; whole-schedule
      (isRealized [_] (done?))

      ScheduledFuture ;; next-job
      (cancel [_ interrupt?] ;; expose interrupt facilities (opt-in)
        (let [^ScheduledFuture fut @current
              until-next (.getDelay fut TimeUnit/MILLISECONDS)
              ret (or (.isCancelled fut)
                      (.cancel fut interrupt?))]
          (when (and (true? ret)
                     (not (done?)))
            ;; don't forget to re-schedule starting
            ;; with the job AFTER the cancelled one
            (schedule-loop (+ until-next interval)))
          ret))

      (getDelay [_ time-unit] ;; expose remaining time until next chime
        (let [^ScheduledFuture fut @current]
          (.getDelay fut time-unit)))
      ))
  )

;; HIGH-LEVEL API REFLECTING THE SEMANTICS OF THE CONSTRUCT ABOVE
;; ==============================================================

(defn skip-next!
  "Cancels the upcoming chime, potentially abruptly,
   as it may have already started. The rest of the schedule
   will remain unaffected, unless the interruption is handled
   by the error-handler (i.e. `InterruptedException`), and it
   returns falsy, or throws (the default one returns true).
   Returns true if already cancelled."
  [sched]
  (future-cancel sched))

(defn until-next
  "Returns the remaining time (in millis by default)
   until the next non-cancelled chime."
  (^long [sched]
   (until-next sched TimeUnit/MILLISECONDS))
  (^long [^ScheduledFuture sched time-unit]
   (.getDelay sched time-unit)))

(defn skip-next?!
  "Like `skip-next!`, but only if the upcoming task
   hasn't already started (with millisecond tolerance)."
  [^ScheduledFuture sched]
  (when (pos? (until-next sched))
    (skip-next! sched)))

(defn skip-next-n!
  "Cancels the next <n> tasks.
   Returns a vector of booleans (per `skip-next!`)"
  [^ScheduledFuture sched n]
  (into []
        (comp (map (fn [_] (skip-next! sched)))
              (take-while true?))

        (range n)))

(defn shutdown!
  "Gracefully closes the entire schedule (per `pool.shutdown()`).
   If the next task hasn't started yet, it will be cancelled,
   otherwise it will be allowed to finish."
  [^AutoCloseable sched]
  (-> (doto sched (.close))
      skip-next?!))

(defn shutdown-now!
  "Attempts a graceful shutdown (per `shutdown!`), but if the latest task
   is already happening attempts to interrupt it. Semantically equivalent
   to `pool.shutdownNow()`."
  [sched]
  (or (shutdown! sched)
      (skip-next! sched)))

(defn finished?
  "Returns true if the entire schedule has finished, false otherwise."
  [sched]
  (realized? sched))

(defn wait-for
  "Blocking call for waiting until the schedule finishes,
   or the provided <timeout-ms> has elapsed. Useful as the
   last expression in `with-open`."
  ([sched]
   (deref sched))
  ([sched timeout-ms timeout-val]
   (deref sched timeout-ms timeout-val)))

(defn next-at
  "Returns the (future) `ZonedDateTime` when the next chime will occur,
   or nil if it has already started (with millisecond tolerance), or cancelled."
  (^ZonedDateTime [sched]
   (next-at sched times/*clock*))
  (^ZonedDateTime [sched ^Clock clock]
   (let [remaining (until-next sched)]
     (when (pos? remaining)
       (-> (ZonedDateTime/now clock)
           (.plus remaining ChronoUnit/MILLIS))))))

(defn- append-with*
  [f sched]
  (doto sched (swap! f)))

(defn append-absolute!
  "Assuming a mutable schedule which has not finished,
   appends the specified <times> to it. These should be
   *after* the last one already in."
  [sched & times]
  (-> (fn [tsq]
        (cond-> tsq
                (seq times)
                (into times)))
      (append-with* sched)))

(defn append-relative-to-last!
  "Assuming a mutable schedule which has not finished,
   appends the result of `(offset-fn last-chime)` into it."
  [sched offset-fn]
  (-> (fn [ts]
        (if-let [t (last ts)]
          (conj ts (offset-fn t))
          ts))
      (append-with* sched)))

(comment

  (def vthread-factory
    "A `ThreadFactory` that produces virtual-threads."
    (-> (Thread/ofVirtual)
        (.name "chime-" 0)
        .factory))

  ;; MUTABLE TIMES EXAMPLE
  (defn times []
    (->> (times/every-n-seconds 5)
         (take 10)
         ))
  (def sched (chime-at (times) println {:thread-factory vthread-factory
                                        :mutable? true
                                      }))
  (skip-next?! sched)
  (until-next sched)
  (append-relative-to-last! sched #(.plusSeconds ^Instant % 2))
  (shutdown! sched)
  (future-done? sched)

  (with-open [sch (chime-at
                    (take 10 (times/every-n-seconds 1))
                    println
                    {:thread-factory vthread-factory
                     :on-finished #(println "done")
                     :mutable? true})]
    (append-relative-to-last! sch #(.plusSeconds ^Instant % 2))

    ;(wait-for sch)
    (wait-for sch 12000 :timeout) ;; occasionally succeeds
    )

  (require '[clj-async-profiler.core :as prof])
  (prof/profile
    (let [sch (chime-at
                (take 100 (times/every-n-seconds 1))
                println
                {:on-finished #(println "done")
                 :mutable? true})]
      ;; simulate async mutation
      (chime-at [(-> (times/now)
                     (.plusSeconds 20))]
                (fn [_] (append-relative-to-last! sch #(.plusSeconds ^Instant % 2)))
                {:on-finished #(println "mutated `sch`")})

      (wait-for sch))
    )

  (let [counter (atom 0)]
    (def sched
      (chime-at-fixed-rate
        5000
        (fn [x]
          (when (not= 3 @counter) ;; return falsey the 4th time
            (swap! counter inc)
            (println x)))
        {:thread-factory vthread-factory
         :initial-delay-supplier #(* 1000 (inc (rand-int 10)))
         :on-aborted #(println "aborted")})))

  (future-cancel sched)
  (.close sched)
  )
