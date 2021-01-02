(ns chime.channel
  (:require [clojure.core.async :as ca]
            [clojure.core.async.impl.protocols :as cap]
            [chime.schedule :refer [chime-at]])
  (:import (java.lang AutoCloseable)))

(defn chime-ch
  "Returns a core.async channel that 'chimes' at every time in the times list.

  Arguments:
    times - (required) Sequence of java.util.Dates, java.time.Instant,
                       java.time.ZonedDateTime, java.time.OffsetDateTime, or msecs since epoch

    buffer - (optional but STRONGLY advised) Buffering semantics for the underlying write-channel
    error-handler - (optional) See `chime-at`
    on-finished - (optional) See `chime-at`

  Usage:

    (let [chimes (chime-ch [(.plusSeconds (Instant/now) -2) ; has already passed, will be ignored.
                            (.plusSeconds (Instant/now) 2)
                            (.plusSeconds (Instant/now) 2)])]
      (a/<!! (go-loop []
               (when-let [msg (<! chimes)]
                 (prn \"Chiming at:\" msg)
                 (recur)))))

  There are extensive usage examples in the README"
  ([times]
   (chime-ch times nil))
  ([times {:keys [buffer error-handler on-finished]}]

   (let [ch     (ca/chan buffer)
         ret-ch (promise)
         finish! (fn []
                   (ca/close! ch)
                   (and on-finished (on-finished)))
         sched (cond->> {:on-finished finish!}
                        error-handler (merge {:error-handler
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
