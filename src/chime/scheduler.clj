(ns chime.scheduler
  "A higher-level scheduler construct built on top of `chime.schedule/chime-at` & `clojure.core/agent`.
   Remembers actively scheduled jobs, until they finish, or are manually cancelled."
  (:require [chime.schedule :as c]
            [chime.times :as times]
            [clojure.tools.logging :as log]))

(defn chiming-agent
  "Returns an `agent` dressed up as a scheduler.
   Can be configured with an :error-handler (2args - job-id/error), and
   :on-finished (1arg - job-id) callback. The state of the agent is
   a map from job-id => chime-return, and it will grow/shrink
   as jobs are scheduled/un-scheduled (or finished). Use it
   as the first argument to all functions in this namespace."
  ([]
   (chiming-agent nil))
  ([{:keys [error-handler on-finished]
     :or {error-handler c/default-error-handler}}]
   (agent {}
          :error-handler (fn [_ e] (log/warn e "`chiming-agent` error! Carrying-on with the schedule(s) ..."))
          ;; will become the options to `chime-at`
          :meta {:on-finished on-finished
                 :error-handler error-handler})))

(defn- forget-on-finish!
  [scheduler id finish!]
  (fn []
    (send-off scheduler dissoc id)
    (when finish! (finish! id))))

(defn- schedule1
  [scheduler id times-fn callback]
  (as-> id $OPTS
        (partial forget-on-finish! scheduler $OPTS)
        (update (meta scheduler) :on-finished $OPTS)
        (update $OPTS :error-handler (fn [eh] (fn [e] (eh id e))))
        (c/chime-at (times-fn) callback $OPTS)))

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

(defn- unschedule1
  [shutdown-fn jobs id]
  (if-let [scheduled (get jobs id)]
    (do (shutdown-fn scheduled)
        (dissoc jobs id))
    jobs))

(defn- unschedule*
  [shutdown-fn jobs ids]
  (reduce (partial unschedule1 shutdown-fn) jobs ids))

(defn unschedule!
  "Given a <scheduler>, gracefully un-schedules (per `chime.schedule/shutdown!`)
   the jobs referred to by <ids>. Triggers the `:on-finished` handler
   (see `scheduler` ctor)."
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
  ([scheduler ids]
   (unschedule-now! scheduler nil ids))
  ([scheduler dlay-millis ids]
   (let [f (fn [_] (send-off scheduler (partial unschedule* c/shutdown-now!) ids))]
     (if (and dlay-millis (pos-int? dlay-millis))
       (c/chime-at [(.plusMillis (times/now) dlay-millis)] f)
       (f nil))
     nil)))


(defn scheduled-ids
  "Returns the ids of all the ongoing jobs of this <scheduler>,
   or nil if there aren't any."
  [scheduler]
  (keys @scheduler))

(defn upcoming-chime-at
  "Returns the next `ZonedDateTime` object
   when the job with <id> will chime."
  [scheduler id]
  (some-> (get @scheduler id) c/current-at))

(defn upcoming-chimes-at
  "Returns a map from job-id => ZonedDateTime."
  [scheduler]
  (reduce-kv
    (fn [ret k v] (assoc ret k (c/current-at v)))
    {}
    @scheduler))

(comment
  (require '[chime.times :as times])
  (def SCHEDULER (chiming-agent {:on-finished #(println "Job" % "finished...")}))
  (schedule! SCHEDULER
             {:foo [(partial println "Hi")
                    times/every-n-seconds]

              :bar [(partial println "there")
                    #(take 5 (times/every-n-millis 1500))]})

  (upcoming-chimes-at SCHEDULER)
  (unschedule! SCHEDULER nil [:foo])
  (scheduled-ids SCHEDULER) ;; => nil
  )
