(ns chime.scheduler
  "A higher-level scheduler construct built on top of `chime.schedule/chime-at` & `clojure.core/agent`.
   Remembers actively scheduled jobs, until they finish, or are manually cancelled."
  (:require [chime.schedule :as c]
            [chime.times :as times]
            [clojure.tools.logging :as log])
  (:import (clojure.lang Agent)
           (java.time Duration)))

(defn chiming-agent
  "Returns an `agent` dressed up as a scheduler.
   Can be configured with an :error-handler (2args - job-id/error), and
   :on-finished (1arg - job-id) callback. The state of the agent is
   a map from job-id => chime-return, and it will grow/shrink
   as jobs are scheduled/un-scheduled (or finished). Use it
   as the first argument to all functions in this namespace."
  ([]
   (chiming-agent nil))
  ([{:keys [error-handler on-finished on-aborted thread-factory]
     :or {error-handler c/default-error-handler}}]
   (agent {}
          :error-handler (fn [_ e] (log/warn e "`chiming-agent` error! Carrying-on with the schedule(s) ..."))
          ;; will become the options to `chime-at`
          :meta {:on-finished on-finished
                 :on-aborted on-aborted
                 :error-handler error-handler
                 :thread-factory thread-factory})))

(defmacro with-deref* [a]
  `(let [a# ~a]
     (if (instance? Agent a#)
       (deref a#)
       a#)))

(defn- forget-on
  [scheduler id handler!]
  (fn []
    (send-off scheduler dissoc id)
    (when handler! (handler! id))))

(defn- schedule1
  "Calls `(c/chime-at (times-fn) callback opts)`, where <opts>
   is essentially the metadata of the <scheduler> (agent).
   The `:on-finished` & `:on-aborted` handlers are enriched with
   self-removal from the scheduler's state."
  [scheduler id times-fn callback]
  (let [forget-on! (partial forget-on scheduler id)
        opts       (-> (meta scheduler)
                       (update :error-handler (fn [eh] (fn [e] (eh id e))))
                       (update :on-finished forget-on!)
                       (update :on-aborted (fn [ah] (some-> ah forget-on!))))]
    (c/chime-at (times-fn) callback opts)))

(defn- schedule*
  [scheduler id->job]
  (reduce-kv
    (fn [ret id [callback ts-fn]]
      (->> callback
           (schedule1 scheduler id ts-fn)
           (assoc ret id)))
    {}
    id->job))

(defn schedule!
  "Given a <scheduler> and map from ids => [job-fn times-fn],
   registers each job-fn (1-arg - the time) to be run per the
   outcome of times-fn (no-arg)."
  [scheduler id->job]
  (send-off scheduler
    (fn [jobs]
      (merge jobs (schedule* scheduler id->job)))))


(defn scheduled-ids
  "Returns the ids of all the ongoing jobs of this <scheduler>,
   or nil if there aren't any."
  [scheduler]
  (keys (with-deref* scheduler)))

(defn- unschedule1
  [shutdown-fn jobs id]
  (if-let [scheduled (get jobs id)]
    (do (shutdown-fn scheduled)
        (dissoc jobs id))
    jobs))

(defn- unschedule*
  [shutdown-fn jobs ids]
  (let [ids (if (= ::all ids) (keys jobs) ids)]
    (reduce (partial unschedule1 shutdown-fn) jobs ids)))

(defn unschedule!
  "Given a <scheduler>, gracefully un-schedules (per `chime.schedule/shutdown!`)
   the jobs referred to by <ids>. Triggers the `:on-aborted` handler (if present)
   otherwise the `:on-finished` one (see `scheduler` ctor)."
  ([scheduler]
   (unschedule! scheduler ::all)) ;; un-schedules everything
  ([scheduler ids]
   (unschedule! scheduler nil ids))
  ([scheduler dlay-millis ids]
   (let [f (fn [_] (send-off scheduler (partial unschedule* c/shutdown!) ids))]
     (if (and dlay-millis (pos-int? dlay-millis))
       (c/chime-at [(.plusMillis (times/now) dlay-millis)] f)
       (f nil))
     nil)))

(defn unschedule-now!
  "Like `unschedule!`, but uses `chime.schedule/shutdown-now!`."
  ([scheduler]
   (unschedule-now! scheduler :all)) ;; un-schedules everything
  ([scheduler ids]
   (unschedule-now! scheduler nil ids))
  ([scheduler dlay-millis ids]
   (let [f (fn [_] (send-off scheduler (partial unschedule* c/shutdown-now!) ids))]
     (if (and dlay-millis (pos-int? dlay-millis))
       (c/chime-at [(.plusMillis (times/now) dlay-millis)] f)
       (f nil))
     nil)))

(defn upcoming-chime-at
  "Returns the next `ZonedDateTime` object
   when the job with <id> will chime."
  [scheduler id]
  (let [state (with-deref* scheduler)]
    (some-> (get state id) c/next-at)))

(defn upcoming-chimes-at
  "Returns a map from job-id => ZonedDateTime."
  [scheduler]
  (let [state (with-deref* scheduler)]
    (update-vals state c/next-at)))

(defn until-next-chime
  "Returns a `java.time.Duration` representing the (time) distance
   from now (inclusive) until the next chime (exclusive) on this <scheduler>."
  ^Duration [scheduler]
  (when-some [t (-> scheduler
                    upcoming-chimes-at
                    vals
                    sort
                    first)]
    (Duration/between (times/now) t)))

(comment
  (require '[chime.times :as times])
  (def SCHEDULER (chiming-agent {:on-finished #(println "Job" % "finished...")
                                 :on-aborted #(println "Job" % "was aborted...")
                                 :thread-factory (-> (Thread/ofVirtual)
                                                     (.name "chime-" 0)
                                                     .factory)}))
  (schedule! SCHEDULER
             {:foo [(partial println "Hi")
                    times/every-n-seconds]

              :bar [(partial println "there")
                    #(take 5 (times/every-n-millis 1500))]})

  (upcoming-chimes-at SCHEDULER)
  (until-next-chime SCHEDULER)
  (unschedule! SCHEDULER nil [:foo])
  (scheduled-ids SCHEDULER) ;; => nil

  (->> (zipmap (range 1 51)
               (map (fn [n]
                      [(fn [_] (println "Hi from" (Thread/currentThread)))
                       (partial times/every-n-seconds n)])
                    (range 1 51)))
       (schedule! SCHEDULER))

  (unschedule! SCHEDULER)


  )
