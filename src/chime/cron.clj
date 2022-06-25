(ns chime.cron
  (:require [clojure.string :as str]
            [chime.times :as times])
  (:import (java.time LocalDate DayOfWeek Year ZonedDateTime ZoneId Clock)
           (java.time.temporal TemporalAdjusters Temporal)))

;(set! *warn-on-reflection* true)

(def bounds
  {:year        {:lower 1970 :upper 9999}
   :day-of-week {:lower 1 :upper 7}
   :month       {:lower 1 :upper 12}
   :day         {:lower 1 :upper 31}
   :hour        {:lower 0 :upper 23}
   :minute      {:lower 0 :upper 59}
   :second      {:lower 0 :upper 59}})

(defn- check-bounds
  [k n]
  (when n
    (let [{:keys [lower upper]} (get bounds k)]
      (if (<= lower n upper)
        n
        (throw (ex-info "Value is not within bounds"
                        {:field-key k
                         :value     n
                         :lower lower
                         :upper upper}))))))

(def dow-index
  {"MON" 1
   "TUE" 2
   "WED" 3
   "THU" 4
   "FRI" 5
   "SAT" 6
   "SUN" 7})

(def month-index
  {"JAN" 1
   "FEB" 2
   "MAR" 3
   "APR" 4
   "MAY" 5
   "JUN" 6
   "JUL" 7
   "AUG" 8
   "SEP" 9
   "OCT" 10
   "NOV" 11
   "DEC" 12})

(defn all-digits? [^String s]
  (every? #(Character/isDigit ^char %) s))

(defn parse-single [s]
  (when s
    (if (all-digits? s)
      (Integer/parseInt s)
      (let [s (str/upper-case s)]
        (or (dow-index s)
            (month-index s))))))

(def item-pattern ;; TODO: we're allowing underscores
  #"(?:(?:(\w+)-(\w+))|(\w+)|\*|\?)(?:/(\w+))?")

(defn- parse-item
  [k s]
  (if-let [[s' from-str to-str val-str step-str] (re-matches item-pattern s)]
    (if-let [val (parse-single val-str)]
      {:from (check-bounds k val)
       :to   (if step-str (-> k bounds :upper)  val)
       :step (or (parse-single step-str) 1)}
      (when (not= "?" val-str)
        (let [{:keys [lower upper]} (bounds k)]
          {:from (check-bounds k (or (parse-single from-str) lower))
           :to   (check-bounds k (or (parse-single to-str) upper))
           :step (or (parse-single step-str) 1)})))
    (throw (ex-info (str "String '" s "' cannot be parsed as cron expression part")
                    {:field-key k
                     :part s}))))


(defn- parse-list [k s]
  (cond
    (and (= k :day)
         (= s "L"))
    [:flag/last-dom]

    (and (= k :day-of-week)
         (str/ends-with? s "L"))
    [:flag/last-dow (check-bounds :day-of-week (parse-single (str/join (drop-last s))))]

    :else
    (->> (str/split s #",")
       (keep (partial parse-item k))
       vec)))

(def default-cron-ks
  [:minute :hour :day :month :day-of-week])

(defn parse
  "Returns a schedule map for a `cron-expr` whose parts correspond to <ks>.
   Each part is a comma-separated list of ranges or single numbers.
   Each range can be specified either as '*' (all), or as '<lower>-<upper>'
   and may be followed by a '/<step>' expression.
   Supported field-keys with lower/upper bounds and defaults are as follows:

   - second => 0..59 - 0
   - minute => 0..59 - 0
   - hour   => 0..23 - 0
   - day    => 1..31 - 1
   - month  => 1..12 - 1
   - day-of-week => 1..7 - *
   - year   => 1970..9999 - *

  Example: '0,30 8-18/2 * * 1-5' results in schedule
  {:minute      [{:from 0, :to 0, :step 1}
                 {:from 30, :to 30, :step 1}],
   :hour        [{:from 8, :to 18, :step 2}],
   :day         [{:from 1, :to 31, :step 1}],
   :month       [{:from 1, :to 12, :step 1}],
   :day-of-week [{:from 1, :to 5, :step 1}]}"

  ([cron-expr]
   (parse default-cron-ks cron-expr))
  ([ks cron-expr]
   (let [items (-> cron-expr str/trim (str/split #"\s+"))]
     (if (= (count items) (count ks))
       (into {}
             (map (fn [k cron-expr]
                    [k (parse-list k cron-expr)])
                  ks
                  items))
       (throw
         (ex-info "Cron expression has more/less components than field-keys provided"
                  {:expr cron-expr
                   :fields ks}))))))


(defn- ranges->seq
  [ranges]
  (mapcat (fn [{:keys [from to step]}]
            (range from (inc to) step))
          ranges))

(defn at* [x]
  [{:from x :to x :step 1}])

(defn- last-dom* ^Temporal [^Temporal d]
  (.with d (TemporalAdjusters/lastDayOfMonth)))

(defn- last-dow-in-month ^Temporal [^DayOfWeek dow ^Temporal d]
  (.with d (TemporalAdjusters/lastInMonth dow)))

(defn last-dom? [^Temporal d]
  (= d (last-dom* d)))

(defn last-dow-in-month? [^DayOfWeek dow ^Temporal d]
  (= d (last-dow-in-month dow d)))

(defn day* [pred]
  (fn [^long year ^long month ^long day]
    (try
      (let [d (LocalDate/of year month day)]
        (and (or (nil? pred)
                 (pred d))
             (.getDayOfWeek d)))
      (catch Exception _ false))))

(defn times
  [parsed]
  (let [parsed (cond-> parsed (string? parsed) parse) ;; attempt to parse with default keys
        dom-flag-l? (= [:flag/last-dom] (:day parsed))
        dow-flag-l? (= :flag/last-dow (first (:day-of-week parsed)))
        dow (when-not dow-flag-l?
              (some->> (:day-of-week parsed) ranges->seq (map #(DayOfWeek/of %)) set))
        valid-day? (cond->> (day* (cond
                                    dom-flag-l? last-dom?
                                    dow-flag-l? (partial last-dow-in-month? (DayOfWeek/of (second (:day-of-week parsed))))
                                    :else nil))
                            (some? dow) (comp dow))
        at-first (at* 0)
        zone (.getZone times/*clock*)]
    (for [year (if-let [y (:year parsed)]
                   (ranges->seq y)
                   (range (.getValue (Year/now)) 9999))
            month  (ranges->seq (:month parsed at-first))
            day    (if (or dom-flag-l? dow-flag-l?)
                     (ranges->seq [{:from 21 :to 31 :step 1}])
                     (ranges->seq (:day parsed [{:from 1 :to 31 :step 1}])))
            hour   (ranges->seq (:hour parsed at-first))
            minute (ranges->seq (:minute parsed at-first))
            second (ranges->seq (:second parsed at-first))
            :when (valid-day? year month day)
            ]
        (ZonedDateTime/of
          ^long year
          ^long month
          ^long day
          ^long hour
          ^long minute
          ^long second
          0
          zone)
      )
    )
  )