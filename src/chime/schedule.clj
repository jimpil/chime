(ns chime.schedule
  "Lightweight scheduling library."
  (:require [clojure.tools.logging :as log]
            [chime.times :as times])
  (:import (clojure.lang IDeref IBlockingDeref IPending ISeq)
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
         done?  (partial realized? !latch)
         current (atom nil)
         f      (bound-fn* f)
         error! (bound-fn* error-handler)
         done!  (some-> on-finished bound-fn*)
         next-times (atom nil)]
     (letfn [(close []
               (.shutdown pool)
               (when (and (deliver !latch nil) done!)
                 (done!)))

             (schedule-loop [[curr-time & times]]
               (letfn [(task []
                         (if (try
                               (when-not (done?)
                                 (f curr-time)
                                 true)
                               (catch Exception e
                                 (try
                                   (error! e)
                                   (catch Exception e
                                     (log/error e "error calling chime error-handler, stopping schedule")))))

                           (do
                             (reset! next-times times)
                             (schedule-loop times))
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
                 (some-> @next-times next schedule-loop))
             ret)))
         (getDelay [_ time-unit] ;; expose remaining time until next chime
           (when-let [^ScheduledFuture fut @current]
             (if (.isCancelled fut)
               -1
               (.getDelay fut time-unit))))
         (isCancelled [_]
           (when-let [^ScheduledFuture fut @current]
             (.isCancelled fut)))

         ISeq ;; whole schedule
         (first [_] (first @next-times))
         ;; not to be used directly (danger of holding the head)
         ;; but more  to support seq fns like `second`, `nth` etc
         (next [_] (next @next-times))
         (more [_] (rest @next)))))))

;; HIGH-LEVEL API REFLECTING THE SEMANTICS OF THE CONSTRUCT ABOVE
;; ==============================================================

(defn cancel-current!
  "Cancels the upcoming chime, potentially abruptly,
   as it may have already started. The rest of the schedule
   will remain unaffected, unless the interruption is handled
   by the error-handler (i.e. `InterruptedException`), and it
   returns falsy, or throws (the default one returns true).
   Returns true if already cancelled."
  [sched]
  (future-cancel sched))

(defn until-current
  "Returns the remaining time (in millis by default)
   until the currently scheduled chime (via `ScheduledFuture.getDelay()`).
   If it has been cancelled returns -1."
  ([sched]
   (until-current sched TimeUnit/MILLISECONDS))
  ([^ScheduledFuture sched time-unit]
   (.getDelay sched time-unit)))

(defn cancel-current?!
  "Like `cancel-current!`, but only if the upcoming task
   hasn't already started (with millisecond tolerance)."
  [^ScheduledFuture sched]
  (when (pos? (until-current sched))
    (cancel-current! sched)))

(defn shutdown!
  "Gracefully closes the entire schedule (per `pool.shutdown()`).
   If the next task hasn't started yet, it will be cancelled,
   otherwise it will be allowed to finish."
  [^AutoCloseable sched]
  (-> (doto sched (.close))
      cancel-current?!))

(defn shutdown-now!
  "Attempts a graceful shutdown (per `shutdown!`), but if the latest task
   is already happening attempts to interrupt it. Semantically equivalent
   to `pool.shutdownNow()`."
  [sched]
  (or (shutdown! sched)
      (cancel-current! sched)))

(defn is-shutdown?
  "Returns true if the entire schedule has been shut down,
   false otherwise."
  [sched]
  (realized? sched))

(defn current-cancelled?
  "Returns true if the current chime has been cancelled,
   false otherwise. A mere wrapper around `future-cancelled?`."
  [sched]
  (future-cancelled? sched))

(defn wait-for
  "Blocking call for waiting until the schedule finishes,
   or the provided <timeout-ms> has elapsed."
  ([sched]
   (deref sched))
  ([sched timeout-ms timeout-val]
   (deref sched timeout-ms timeout-val)))

(defn current-at
  "Returns the (future) `ZonedDateTime` when the next chime will occur,
   or nil if it has already started (with millisecond tolerance), or cancelled."
  (^ZonedDateTime [sched]
   (current-at sched times/*clock*))
  (^ZonedDateTime [sched ^Clock clock]
   (let [remaining (until-current sched)]
     (when (pos? remaining)
       (-> (ZonedDateTime/now clock)
           (.plus remaining ChronoUnit/MILLIS))))))
