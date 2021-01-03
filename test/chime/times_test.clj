(ns chime.times-test
  (:require [clojure.test :refer :all]
            [chime.times :as t])
  (:import (java.time.temporal ChronoUnit)
           (java.time ZonedDateTime LocalTime)))

(defn- interval-tester
  [^ChronoUnit cu interval]
  (fn [[l r]]
    (is (= interval (.between cu (t/to-instant l) (t/to-instant r))))))

(deftest useful-periods-tests

  (testing "millis"
    (let [interval 500
          times (take 50 (t/every-n-millis interval))]
      (is (every?
            (interval-tester ChronoUnit/MILLIS interval)
            (partition 2 times)))))

  (testing "seconds"
    (let [interval 2
          times (take 50 (t/every-n-seconds interval))]
      (is (every?
            (interval-tester ChronoUnit/SECONDS interval)
            (partition 2 times)))))

  (testing "minutes"
    (let [interval 3
          times (take 50 (t/every-n-minutes interval))]
      (is (every?
            (interval-tester ChronoUnit/MINUTES interval)
            (partition 2 times)))))

  (testing "hours"
    (let [interval 4
          times (take 50 (t/every-n-hours interval))]
      (is (every?
            (interval-tester ChronoUnit/HOURS interval)
            (partition 2 times)))))

  (testing "days"
    (let [interval 5
          times (take 50 (t/every-n-days interval))]
      (is (every?
            (interval-tester ChronoUnit/DAYS interval)
            (partition 2 times)))))
  )

(defn- time-tester
  [^LocalTime lt]
  (fn [^ZonedDateTime zdt]
    (is (= (.getHour lt) (.getHour zdt)))
    (is (= (.getMinute lt) (.getMinute zdt)))))

(deftest useful-days-test

  (testing "weekends-at"
    (let [lt (LocalTime/now)
          weekends (take 50 (t/every-weekend-at lt))]
      (is (every?
            (every-pred
              (fn [^ZonedDateTime zdt]
                (is (contains? t/WEEKEND (.getDayOfWeek zdt))))
              (time-tester lt))
            weekends))))

  (testing "workdays-at"
    (let [lt (LocalTime/now)
          weekends (take 50 (t/every-workday-at lt))]
      (is (every?
            (every-pred
              (fn [^ZonedDateTime zdt]
                (is (contains? t/WORKDAYS (.getDayOfWeek zdt))))
              (time-tester lt))
            weekends))))

  (testing "month-end-at"
    (let [lt (LocalTime/now)
          month-ends (take 50 (t/every-month-end-at lt))]
      (is (every?
            (every-pred
              (fn [^ZonedDateTime zdt]
                (if (= 2 (.getMonthValue zdt))
                  (is (>= (.getDayOfMonth zdt) 28))   ;; February  (28/29)
                  (is (>= (.getDayOfMonth zdt) 30)))) ;; any other (30/31)))
              (time-tester lt))
            month-ends))))

  (testing "first-working-day-of-month-at"
    (let [lt (LocalTime/now)
          month-ends (next ;; ignore the current month as we may be half-way through
                       (take 50 (t/every-first-working-day-of-month-at lt)))]
      (is (every?
            (every-pred
              (fn [^ZonedDateTime zdt]
                (is (contains? t/WORKDAYS (.getDayOfWeek zdt)))
                (is (<= (.getDayOfMonth zdt) 3)))
              (time-tester lt))
            month-ends)))
    )

  )
