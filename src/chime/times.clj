(ns chime.times
  (:import (java.util Date)
           (java.time Instant ZonedDateTime OffsetDateTime Duration Period LocalTime)
           (java.time.temporal TemporalAmount)
           (java.sql Timestamp)))

(defprotocol ->Instant
  (->instant [obj]
    "Convert `obj` to an Instant instance."))

(extend-protocol ->Instant
  Date
  (->instant [date]
    (.toInstant date))

  Timestamp
  (->instant [timestamp]
    (.toInstant timestamp))

  Instant
  (->instant [inst] inst)

  Long
  (->instant [epoch-msecs]
    (Instant/ofEpochMilli epoch-msecs))

  ZonedDateTime
  (->instant [zdt]
    (.toInstant zdt))

  OffsetDateTime
  (->instant [odt]
    (.toInstant odt))
  )

(defn to-instant
  ^Instant [obj]
  (->instant obj))

(defn without-past-times
  "Given a (potentially infinite) list of successive <times>,
   drops the ones that are in the past (via `drop-while`)."
  ([times]
   (without-past-times times (Instant/now)))
  ([times now]
   (let [now-inst (to-instant now)]
     (->> times
          (drop-while #(.isBefore (to-instant %) now-inst))))))

(defn merge-schedules
  [left right]
  (lazy-seq
    (case [(boolean (seq left)) (boolean (seq right))]
      [false false] []
      [false true] right
      [true false] left
      [true true] (let [[l & lmore] left
                        [r & rmore] right]
                    (if (.isBefore (to-instant l) (to-instant r))
                      (cons l (merge-schedules lmore right))
                      (cons r (merge-schedules left rmore)))))))

(defn periodic-seq
  "Returns an infinite sequence of successive `Temporal` objects (concrete type per <start>),
   <duration-or-period> apart."
  [start ^TemporalAmount duration-or-period]
  (iterate #(.addTo duration-or-period %) start))

;; USEFUL PERIODS
;; ==============

(defn every-n-seconds
  "Returns an infinite sequence of successive `Instant` objects <n> second(s) apart
  (defaults to 1), starting at <from> (defaults to `Instant/now`)."
  ([]
   (every-n-seconds 1))
  ([n]
   (->> (Instant/now)
        (every-n-seconds n)
        next))
  ([n from]
   (periodic-seq from (Duration/ofSeconds n))))

(defn every-n-minutes
  "Returns an infinite sequence of successive `Instant` objects <n> minute(s) apart
  (defaults to 1), starting at <from> (defaults to `Instant/now`)."
  ([]
   (every-n-minutes 1))
  ([n]
   (->> (Instant/now)
        (every-n-minutes n)
        rest))
  ([n from]
   (periodic-seq from (Duration/ofMinutes n))))

(defn every-n-hours
  "Returns an infinite sequence of successive `Instant` objects <n> hour(s) apart
  (defaults to 1), starting at <from> (defaults to `Instant/now`)."
  ([]
   (every-n-hours 1))
  ([n]
   (->> (Instant/now)
        (every-n-hours n)
        next))
  ([n from]
   (periodic-seq from (Duration/ofHours n))))

(defn every-n-days
  "Returns an infinite sequence of successive `Instant` objects <n> days apart
  (defaults to 1), starting at <from> (defaults to `Instant/now`)."
  ([]
   (every-n-days 1))
  ([n]
   (->> (Instant/now)
        (every-n-days n)
        next))
  ([n from]
   (periodic-seq from (Period/ofDays n))))

(defn every-n-weeks
  "Returns an infinite sequence of successive `Instant` objects <n> weeks apart
  (defaults to 1), starting at <from> (defaults to `Instant/now`)."
  ([]
   (every-n-weeks 1))
  ([n]
   (->> (Instant/now)
        (every-n-weeks n)
        next))
  ([n from]
   (periodic-seq from (Period/ofWeeks n))))

(defn every-n-months
  "Returns an infinite sequence of successive `Instant` objects <n> months apart
  (defaults to 1), starting at <from> (defaults to `Instant/now`)."
  ([]
   (every-n-months 1))
  ([n]
   (->> (Instant/now)
        (every-n-months n)
        next))
  ([n from]
   (periodic-seq from (Period/ofMonths n))))
