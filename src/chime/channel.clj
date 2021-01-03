(ns chime.channel
  (:require [clojure.core.async :as ca]
            [clojure.core.async.impl.protocols :as cap]
            [chime.schedule :refer [chime-at]])
  (:import (java.lang AutoCloseable)))

(defn chime-ch
  "Returns a read-only (core.async) channel that 'chimes' at every time in the <times> list.

  Arguments:
    times - (required) Sequence of java.util.Dates, java.time.Instant, java.time.ZonedDateTime, java.time.OffsetDateTime,
                       or msecs since epoch. Consumers of the returned channel will receive these as `Instant` objects.

    buffer - (optional but STRONGLY advised) Buffering semantics for the underlying write-channel.
             Allows for finer control (than `:drop-overruns?`) wrt what happens with over-runnning jobs (i.e. dropped/slided)
    error-handler - (optional) See `chime-at`
    on-finished - (optional) See `chime-at`
    clock - (optional) See `chime-at`
    drop-overruns? - (optional) See `chime-at` (prefer `:buffer` if you're not sure consumers can keep up with the schedule)

  Usage:

    (let [now (Instant/now)
          chimes (chime-ch [(.plusSeconds now -2) ; in the past - will be dropped (per :drop-overruns?).
                            (.plusSeconds now 2)
                            (.plusSeconds now 4)]
                            {:drop-overruns? true})]
      (go-loop [] ;; fast consumer
        (when-let [msg (<! chimes)]
          (prn \"Chiming at:\" msg)
            (recur))))

  There are extensive usage examples in the README"
  ([times]
   (chime-ch times nil))
  ([times {:keys [buffer error-handler on-finished clock drop-overruns?]}]

   (let [ch     (ca/chan buffer)
         ret-ch (promise)
         finish! (fn []
                   (ca/close! ch)
                   (and on-finished (on-finished)))
         sched (cond->> {:on-finished finish!}
                        clock          (merge {:clock clock})
                        drop-overruns? (merge {:drop-overruns? true})
                        error-handler  (merge {:error-handler
                                               (fn [e]
                                                 (or
                                                   ;; user's error-handler says to carry on with the schedule
                                                   (error-handler e)
                                                   ;; user's error-handler says to stop the schedule
                                                   (ca/close! @ret-ch)))})
                        true (chime-at times (fn [t] (ca/>!! ch t))))]
     (->> (reify
            cap/ReadPort
            (take! [_ handler]
              (cap/take! ch handler))

            cap/Channel
            (close! [_]
              (.close ^AutoCloseable sched)
              (ca/close! ch))
            (closed? [_]
              (cap/closed? ch)))

          (deliver ret-ch)
          deref))))


(comment
  (let [now (Instant/now)
        chimes (chime-ch [(.plusSeconds now -2) ;; will be ignored (per :drop-overruns?).
                          (.plusSeconds now 2)
                          (.plusSeconds now 4)]
                         {:drop-overruns? true})]
    ;; fast consumer who wants to ALWAYS be up-to-date
    ;; with the schedule (regardless of how many are dropped)
    (ca/go-loop []
      (when-let [msg (ca/<! chimes)]
        (prn "Chiming at:" msg)
        (recur))))
;; "Chiming at:" #object[java.time.Instant 0x431983da "2021-01-03T13:02:54.854149Z"]
;; "Chiming at:" #object[java.time.Instant 0x6f779b32 "2021-01-03T13:02:56.854149Z"]

  (let [now (Instant/now)
        chimes (chime-ch [(.plusSeconds now -2) ;; will be slided (per :buffer)
                          (.plusSeconds now 2)
                          (.plusSeconds now 4)]
                         {:buffer (ca/sliding-buffer 1)})]
    ;; slow consumer who doesn't always need to feel 'up-to-date'
    (Thread/sleep 2000)
    (prn (ca/<!! chimes))
    (Thread/sleep 2000)
    (prn (ca/<!! chimes))
    )
;; #object[java.time.Instant 0x47cda656 "2021-01-03T13:03:34.113710Z"]
;; #object[java.time.Instant 0x4d53d6e "2021-01-03T13:03:36.113710Z"]
  )