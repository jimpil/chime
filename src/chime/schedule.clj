(ns chime.schedule
  "Lightweight scheduling library."
  (:require [clojure.tools.logging :as log]
            [chime.times :refer [to-instant]])
  (:import (clojure.lang IDeref IBlockingDeref IPending)
           (java.time Instant Clock)
           (java.time.temporal ChronoUnit)
           (java.util.concurrent Executors ScheduledExecutorService ThreadFactory TimeUnit ScheduledFuture)
           (java.lang AutoCloseable Thread$UncaughtExceptionHandler)))

;; --------------------------------------------------------------------- time helpers
(defonce utc-clock (Clock/systemUTC))

(def ^:dynamic *clock*
  "The clock used to determine 'now'; you can override it with `binding` for
  testing purposes."
  utc-clock)

(defn now
  "Returns a date time for the current instant.
   No-arg arity uses *clock*."
  (^Instant []
   (now *clock*))
  (^Instant [^Clock clock]
   (Instant/now clock)))


(def ^:private default-thread-factory
  (let [!count (atom 0)]
    (reify ThreadFactory
      (newThread [_ r]
        (doto (Thread. r)
          (.setName (format "chime-%d" (swap! !count inc))))))))

(defn default-error-handler
  ([e]
   (default-error-handler nil e))
  ([_ e]
   (log/warn e "Error running scheduled fn")
   true))

(defn chime-at
  "Calls `f` with the current time at every time in the `times` sequence.

  ```
  (:require [chime.core :as chime])
  (:import [java.time Instant])

  (let [now (Instant/now)]
    (chime/chime-at [(.plusSeconds now 2)
                     (.plusSeconds now 4)]
                    (fn [time]
                      (println \"Chiming at\" time)))
  ```

  Returns an AutoCloseable that you can `.close` to stop the schedule.
  You can also deref the return value to wait for the schedule to finish.

  Providing a custom `thread-factory` is supported, but optional (see `chime.core/default-thread-factory`).
  Providing a custom `clock` is supported, but optional (see `chime.core/utc-clock`).

  When the schedule is either cancelled or finished, will call the `on-finished` handler.

  You can pass an error-handler to `chime-at` - a function that takes the exception as an argument.
  Return truthy from this function to continue the schedule, falsy to cancel it.
  By default, Chime will log the error and continue the schedule (see `chime.core/default-error-handler`)."

  (^AutoCloseable [times f] (chime-at times f nil))

  (^AutoCloseable [times f {:keys [error-handler on-finished thread-factory clock drop-overruns?]
                            :or {error-handler  default-error-handler
                                 thread-factory default-thread-factory ;; loom-friendly (i.e. virtual threads)
                                 clock          utc-clock}}]
   (let [pool (Executors/newSingleThreadScheduledExecutor thread-factory)
         !latch (promise)
         current (atom nil)
         f      (bound-fn* f)
         error! (bound-fn* error-handler)
         done!  (some-> on-finished bound-fn*)]
     (letfn [(close []
               (.shutdown pool)
               (when (and (deliver !latch nil) done!)
                 (done!)))

             (schedule-loop [[time & times]]
               (letfn [(task []
                         (if (try
                               (when-not (realized? !latch)
                                 (f time)
                                 true)
                               (catch Exception e
                                 (try
                                   (error! e)
                                   (catch Exception e
                                     (log/error e "error calling chime error-handler, stopping schedule")))))

                           (schedule-loop times)
                           (close)))]

                 (if time
                   (let [dlay (.between ChronoUnit/MILLIS (now clock) time)]
                     (if (or (pos? dlay)
                             (not drop-overruns?))
                       (->> (.schedule pool ^Runnable task dlay TimeUnit/MILLISECONDS)
                            (reset! current))
                       (recur times)))
                   (close))))]

       ;; kick-off the schedule loop
       (schedule-loop (map to-instant times))

       (reify
         AutoCloseable
         (close [_] (close))

         IDeref
         (deref [_] (deref !latch))

         IBlockingDeref
         (deref [_ ms timeout-val] (deref !latch ms timeout-val))

         IPending
         (isRealized [_] (realized? !latch))

         ScheduledFuture
         (cancel [_ interrupt?] ;; expose interrupt facilities (opt-in)
           (when-let [^ScheduledFuture fut @current]
             (or (.isCancelled fut)
                 (.cancel fut interrupt?))))
         (getDelay [_ time-unit] ;; expose remaining time until next chime
           (when-let [^ScheduledFuture fut @current]
             (.getDelay fut time-unit)))
         (isCancelled [_]
           (when-let [^ScheduledFuture fut @current]
             (.isCancelled fut))))))))

;; HIGH-LEVEL API REFLECTING THE SEMANTICS OF THE CONSTRUCT ABOVE
;; ==============================================================

(defn cancel-next!
  "Cancels the next upcoming chime, potentially abruptly,
   as it may have already started. The rest of the schedule
   will remain unaffected, unless the interruption is handled
   by the error-handler (i.e. `InterruptedException`), and it
   returns falsey, or throws (the default one returns true).
   Returns true if already cancelled."
  ([sched]
   (cancel-next! sched true)) ;; essentially the same as `future-cancel`
  ([^ScheduledFuture sched interrupt?]
   (.cancel sched interrupt?)))

(defn until-next
  "Returns the remaining time (in millis by default)
   until the next chime (via `ScheduledFuture.getDelay()`)."
  ([sched]
   (until-next sched TimeUnit/MILLISECONDS))
  ([^ScheduledFuture sched time-unit]
   (.getDelay sched time-unit)))

(defn cancel-next?!
  "Like `cancel-next!`, but only if the next task
   hasn't already started (millisecond tolerance)."
  [^ScheduledFuture sched]
  (when (pos? (until-next sched))
    (cancel-next! sched)))

(defn shutdown!
  "Gracefully closes the entire schedule (per `pool.shutdown()`).
   If the next task hasn't started yet, it will be cancelled,
   otherwise it will be allowed to finish."
  [^AutoCloseable sched]
  (-> (doto sched (.close))
      cancel-next?!))

(defn shutdown-now!
  "Attempts a graceful shutdown (per `shutdown!`), but if the latest task
   is already happening attempts to interrupt it. Semantically equivalent
   to `pool.shutdownNow()` when called with <interrupt?> = true (the default)."
  ([sched]
   (shutdown-now! sched true))
  ([sched interrupt?]
   (or (shutdown! sched)
       (cancel-next! sched interrupt?))))

(defn wait-for
  "Blocking call for waiting until the schedule finishes,
   or the provided <timeout-ms> has elapsed."
  ([sched]
   (deref sched))
  ([sched timeout-ms timeout-val]
   (deref sched timeout-ms timeout-val)))

(defn next-at
  "Returns the (future) `Instant` when the next chime will occur,
   or nil if it has already started (millisecond tolerance)."
  (^Instant [sched]
   (next-at sched utc-clock))
  (^Instant [sched clock]
   (let [remaining (until-next sched)]
     (when (pos? remaining)
       (.plusMillis (now clock) remaining)))))
