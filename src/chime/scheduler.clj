(ns chime.scheduler
  "A higher-level scheduler construct built on top of `chime.schedule/chime-at` & `clojure.core/agent`."
  (:require [chime.schedule :as c]))

(defn chiming-agent
  "Returns an `agent` dressed up as a scheduler.
   Can be configured with an :error-handler (1/2 args),
   and :on-finished (no-args). The state of the agent is
   a map from job-id => chime-return, and it will grow/shrink
   as jobs are scheduled/un-scheduled (or finished). Use it
   as the first argument to `schedule!`/`unschedule!`."
  ([]
   (chiming-agent nil))
  ([{:keys [error-handler on-finished]
     :or {error-handler c/default-error-handler}}]
   (agent {}
          :error-handler error-handler ;; reuse the error-handler - MUST support 1/2 args!
          :meta {:error-handler error-handler
                 :on-finished on-finished})))

(defn- forget-on-finish!
  [scheduler id finish!]
  (fn []
    (send-off scheduler dissoc id)
    (when finish! (finish!))))

(defn- schedule1
  [scheduler id times-fn callback]
  (->> (partial forget-on-finish! scheduler id)
       (update (meta scheduler) :on-finished)
       (c/chime-at (times-fn) callback)))

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
  [jobs id]
  (if-let [scheduled (get jobs id)]
    (do (c/shutdown! scheduled)
        (dissoc jobs id))
    jobs))

(defn- unschedule*
  [jobs ids]
  (reduce unschedule1 jobs ids))

(defn unschedule!
  "Given a <scheduler>, un-schedules the jobs referred to by <ids>.
   Triggers the `:on-finished` handler (see `scheduler` ctor)."
  ([scheduler ids]
   (unschedule! scheduler nil ids))
  ([scheduler dlay-millis ids]
   (let [f (fn [_] (send-off scheduler unschedule* ids))]
     (if (and dlay-millis (pos-int? dlay-millis))
       (c/chime-at [(.plusMillis (c/now) dlay-millis)] f)
       (f nil))
     nil)))

(defn active-jobs
  "Returns the ids of all the ongoing jobs of this <scheduler>,
   or nil if there aren't any."
  [scheduler]
  (keys @scheduler))

(defn upcoming-jobs-at
  [scheduler]
  (into {}
        (map (fn [[id sched]]
               [id (c/next-at sched)]))
        @scheduler))

(comment
  (require '[chime.times :as times])
  (def SCHEDULER (chiming-agent))
  (schedule! SCHEDULER
             {:foo [(partial println "Hi")
                    times/every-n-seconds]

              :bar [(partial println "there")
                    #(take 5 (times/every-n-millis 1500))]})

  (unschedule! SCHEDULER nil [:foo])
  (active-jobs SCHEDULER) ;; => nil
  )
