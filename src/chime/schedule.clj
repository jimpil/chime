(ns chime.schedule
  "Lightweight scheduling library."
  (:require [clojure.tools.logging :as log]
            [chime.times :as times])
  (:import (clojure.lang IDeref IBlockingDeref IPending)
           (java.time Instant Clock ZonedDateTime)
           (java.time.temporal ChronoUnit)
           (java.util.concurrent Executors ScheduledExecutorService ThreadFactory TimeUnit ScheduledFuture)
           (java.lang AutoCloseable)
           (java.util.concurrent.atomic AtomicLong)))

(def ^:private default-thread-factory
  (let [thread-no (AtomicLong. 0)]
    (reify ThreadFactory
      (newThread [_ r]
        (doto (Thread. r)
          (.setName (format "chime-%d" (.incrementAndGet thread-no))))))))

(defn default-error-handler [e]
  (log/warn e "Error running scheduled fn")
  true)

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

  When the schedule is either manually cancelled or exhausted, the <on-finished> callback will be called."

  (^AutoCloseable [times f] (chime-at times f nil))

  (^AutoCloseable [times f {:keys [error-handler on-finished thread-factory clock drop-overruns?]
                            :or {error-handler  default-error-handler
                                 thread-factory default-thread-factory ;; loom-friendly (i.e. virtual threads)
                                 clock          times/*clock*}}]
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

             (schedule-loop [[curr-time & times]]
               (letfn [(task []
                         (if (try
                               (when-not (realized? !latch)
                                 (f curr-time)
                                 true)
                               (catch Exception e
                                 (try
                                   (error! e)
                                   (catch Exception e
                                     (log/error e "error calling chime error-handler, stopping schedule")))))

                           (schedule-loop times)
                           (close)))]

                 (if curr-time
                   (let [dlay (.between ChronoUnit/MILLIS (times/now clock) curr-time)]
                     (if (or (pos? dlay)
                             (not drop-overruns?))
                       (->> (.schedule pool ^Runnable task dlay TimeUnit/MILLISECONDS)
                            (reset! current))
                       (recur times)))
                   (close))))]

       ;; kick-off the schedule loop
       (schedule-loop (map times/to-instant times))

       (reify ;; the returned object represents 2 things
         AutoCloseable ;; whole-schedule
         (close [_] (close))

         IDeref ;; whole-schedule
         (deref [_] (deref !latch))

         IBlockingDeref ;; whole-schedule
         (deref [_ ms timeout-val] (deref !latch ms timeout-val))

         IPending ;; whole-schedule
         (isRealized [_] (realized? !latch))

         ScheduledFuture ;; next-job
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

(defn until-next-chime
  "Returns the remaining time (in millis by default)
   until the next chime (via `ScheduledFuture.getDelay()`)."
  ([sched]
   (until-next-chime sched TimeUnit/MILLISECONDS))
  ([^ScheduledFuture sched time-unit]
   (.getDelay sched time-unit)))

(defn cancel-next?!
  "Like `cancel-next!`, but only if the next task
   hasn't already started (millisecond tolerance)."
  [^ScheduledFuture sched]
  (when (pos? (until-next-chime sched))
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

(defn next-chime-at
  "Returns the (future) `ZonedDateTime` when the next chime will occur,
   or nil if it has already started (millisecond tolerance)."
  (^ZonedDateTime [sched]
   (next-chime-at sched times/*clock*))
  (^ZonedDateTime [sched ^Clock clock]
   (let [remaining (until-next-chime sched)]
     (when (pos? remaining)
       (-> (ZonedDateTime/now clock)
           (.plus remaining ChronoUnit/MILLIS))))))
