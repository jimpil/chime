(ns chime.times
  (:require [clojure.string :as str])
  (:import (java.sql Timestamp)
           (java.time Clock DayOfWeek Duration Instant LocalDate LocalDateTime LocalTime Month OffsetDateTime Period YearMonth ZonedDateTime)
           (java.time.format TextStyle)
           (java.time.temporal TemporalAmount)
           (java.util Date Locale)))

(defonce ^Clock utc-clock (Clock/systemUTC))
(def ^{:dynamic true :tag 'java.time.Clock} *clock*
  "The clock used to determine 'now' (defaults to `Clock/systemUTC`). 
   See `with-clock` for re-binding it."
  utc-clock)

(defmacro with-clock
  [clock & body]
  `(binding [*clock* ~clock]
     ~@body))

(defn now
  "Wrapper around `Instant/now`. No-arg arity uses *clock*."
  (^Instant []
   (now *clock*))
  (^Instant [^Clock clock]
   (Instant/now clock)))

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
   (without-past-times times (now)))
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

(defn every-n-millis
  "Returns an infinite sequence of successive `Instant` objects <n> second(s) apart
  (defaults to 1), starting at <from> (defaults to `Instant/now`)."
  ([]
   (every-n-millis 100))
  ([n]
   (-> (now)
       (every-n-millis n)
       next))
  ([from n]
   (periodic-seq from (Duration/ofMillis n))))

(defn every-n-seconds
  "Returns an infinite sequence of successive `Instant` objects <n> second(s) apart
  (defaults to 1), starting at <from> (defaults to `Instant/now`)."
  ([]
   (every-n-seconds 1))
  ([n]
   (-> (now)
       (every-n-seconds n)
       next))
  ([from n]
   (periodic-seq from (Duration/ofSeconds n))))

(defn every-n-minutes
  "Returns an infinite sequence of successive `Instant` objects <n> minute(s) apart
  (defaults to 1), starting at <from> (defaults to `Instant/now`)."
  ([]
   (every-n-minutes 1))
  ([n]
   (-> (now)
       (every-n-minutes n)
       rest))
  ([from n]
   (periodic-seq from (Duration/ofMinutes n))))

(defn every-n-hours
  "Returns an infinite sequence of successive `Instant` objects <n> hour(s) apart
  (defaults to 1), starting at <from> (defaults to `Instant/now`)."
  ([]
   (every-n-hours 1))
  ([n]
   (-> (now)
       (every-n-hours n)
       next))
  ([from n]
   (periodic-seq from (Duration/ofHours n))))

(defn every-n-days
  "Returns an infinite sequence of successive `Instant` objects <n> days apart
  (defaults to 1), starting at <from> (defaults to `Instant/now`)."
  ([]
   (every-n-days 1))
  ([n]
   (-> (now)
       (every-n-days n)
       next))
  ([from n]
   (periodic-seq from (Period/ofDays n))))

;;==========================================
(defonce WORKDAYS
  #{DayOfWeek/MONDAY
    DayOfWeek/TUESDAY
    DayOfWeek/WEDNESDAY
    DayOfWeek/THURSDAY
    DayOfWeek/FRIDAY})

(defonce WEEKEND
  #{DayOfWeek/SATURDAY
    DayOfWeek/SUNDAY})

(defn every-day-at
  ([]
   (every-day-at (LocalTime/of 0 0)))
  ([^LocalTime lt]
   (-> lt
       (.adjustInto (ZonedDateTime/now ^Clock *clock*))
       (every-n-days 1))))

(defn some-days-at
  ([days-set]
   (some-days-at days-set (LocalTime/of 0 0)))
  ([days-set ^LocalTime lt]
   (->> (every-day-at lt)
        (filter (comp days-set #(.getDayOfWeek ^ZonedDateTime %))))))

(defn every-workday-at
  "Returns an infinite sequence of week-days (Mon-Fri)
   (ZonedDateTime instances at time <lt>)."
  ([]
   (every-workday-at (LocalTime/of 0 0)))
  ([^LocalTime lt]
   (some-days-at WORKDAYS lt)))

(defn every-weekend-at
  "Returns an infinite sequence of week-ends (Sat/Sun)
   (ZonedDateTime instances at time <lt>)."
  ([]
   (every-weekend-at (LocalTime/of 0 0)))
  ([^LocalTime lt]
   (some-days-at WEEKEND lt)))

(defn every-days-of-month-at
  [days ^LocalTime lt]
  (->> (some-days-at days lt)                         ;; all relevant days
       (partition-by #(.getMonth ^ZonedDateTime %)))) ;; partitioned into months

(defn every-first-day-of-month-at
  "Returns an infinite sequence of first <day> (e.g. Monday) in
   every month (ZonedDateTime instances at time <lt>)."
  ([day]
   (every-first-day-of-month-at day (LocalTime/of 0 0)))
  ([^DayOfWeek day ^LocalTime lt]
   (->> (every-days-of-month-at #{day} lt)
        (map first))))

(defn every-last-day-of-month-at
  "Returns an infinite sequence of last <day> (e.g. Monday) in
   every month (ZonedDateTime instances at time <lt>)."
  ([day]
   (every-last-day-of-month-at day (LocalTime/of 0 0)))
  ([^DayOfWeek day ^LocalTime lt]
   (->> (every-days-of-month-at #{day} lt)
        (map last))))

(defn every-first-working-day-of-month-at
  "Returns an infinite sequence of first working-day in
   every month (ZonedDateTime instances at time <lt>)."
  ([]
   (every-first-working-day-of-month-at (LocalTime/of 0 0)))
  ([^LocalTime lt]
   (->> (every-days-of-month-at WORKDAYS lt)
        (map first))))

(defn every-last-working-day-of-month-at
  "Returns an infinite sequence of last working-day in
   every month (ZonedDateTime instances at time <lt>)."
  ([]
   (every-last-working-day-of-month-at (LocalTime/of 0 0)))
  ([^LocalTime lt]
   (->> (every-days-of-month-at WORKDAYS lt)
        (map last))))
;;========================================================
(defonce MONTHS
  (let [months (map #(Month/of %) (range 1 13))]
    (zipmap (map #(.getDisplayName ^Month % TextStyle/FULL Locale/UK) months)
            months)))

(defn months*
  "Returns a set of Month objects, as specified by <months>.
   These can be either month-indices (1-12), or month-names
   (e.g. 'January', 'February' etc). Keywords are also supported."
  [& months]
  (->> months
       (map (fn [m]
              (if (string? m)
                (get MONTHS m)
                (if (keyword? m)
                  (recur (str/capitalize (name m)))
                  (Month/of m)))))
       set))

(defn- best-effort-month-day*
  "https://stackoverflow.com/questions/39046324/adding-month-to-specific-day-of-month-with-java-time"
  [month-day ^ZonedDateTime zdt]
  (->> zdt
       .toLocalDate
       .lengthOfMonth
       (min month-day)
       (.withDayOfMonth zdt)))

(defn every-month-at
  "Returns an infinite sequence of ZonedDateTime objects one month apart,
   starting with the next day-of-month & time specified. If the <month-day>
   provided is greater than 28 or 30, will adjust the months that don't have
   such indices per `(min month-day length-of-month)`."
  ([]
   (every-month-at 1))
  ([month-day]
   (every-month-at month-day (LocalTime/of 0 0)))
  ([^long month-day ^LocalTime lt]
   (map
     (partial best-effort-month-day* month-day)
     (-> (LocalDateTime/of
           (.withDayOfMonth (LocalDate/now) month-day)
           lt)
         (.adjustInto (ZonedDateTime/now ^Clock *clock*))
         (periodic-seq (Period/ofMonths 1))
         next))))

(defn some-months-at
  "Similar to `chime.times/every-month-at`, but only for the months provided."
  ([months-set]
   (some-months-at months-set 1))
  ([months-set month-day]
   (some-months-at months-set month-day (LocalTime/of 0 0)))
  ([months-set ^long month-day ^LocalTime lt]
   (->> (every-month-at month-day lt)
        (filter (comp months-set #(.getMonth ^ZonedDateTime %))))))

(defn every-month-end-at
  ([]
   (every-month-end-at (LocalTime/of 0 0)))
  ([^LocalTime lt]
   (->> (every-month-at 1 lt)
        (map (fn [^ZonedDateTime zdt]
               (-> (YearMonth/from zdt)
                   .atEndOfMonth ;; `java.time` is awesome!
                   (LocalDateTime/of lt)
                   (ZonedDateTime/of (.getZone zdt))))))))

(defn with-multiple-times
  "Utility for wrapping an existing generator with multiple 'at' times."
  [f at-times]
  (->> at-times
       (map #(LocalTime/parse ^String %))
       sort
       (map f)
       (apply interleave)))
;;===========================================================

