(ns chime.times-test
  (:require [clojure.test :refer :all]
            [chime.times :as t])
  (:import (java.time.temporal ChronoUnit)
           (java.time ZonedDateTime LocalTime)))

(defn- tester-for
  [^ChronoUnit cu interval]
  (fn [[l r]]
    (= interval (.between cu (t/to-instant l) (t/to-instant r)))))

(deftest useful-periods-tests

  (testing "millis"
    (let [interval 500
          times (take 50 (t/every-n-millis interval))]
      (is (every?
            (tester-for ChronoUnit/MILLIS interval)
            (partition 2 times)))))

  (testing "seconds"
    (let [interval 2
          times (take 50 (t/every-n-seconds interval))]
      (is (every?
            (tester-for ChronoUnit/SECONDS interval)
            (partition 2 times)))))

  (testing "minutes"
    (let [interval 3
          times (take 50 (t/every-n-minutes interval))]
      (is (every?
            (tester-for ChronoUnit/MINUTES interval)
            (partition 2 times)))))

  (testing "hours"
    (let [interval 4
          times (take 50 (t/every-n-hours interval))]
      (is (every?
            (tester-for ChronoUnit/HOURS interval)
            (partition 2 times)))))

  (testing "days"
    (let [interval 5
          times (take 50 (t/every-n-days interval))]
      (is (every?
            (tester-for ChronoUnit/DAYS interval)
            (partition 2 times)))))
  )

(deftest useful-days-test

  (testing "weekends-at"
    (let [lt (LocalTime/now)
          weekends (take 50 (t/every-weekend-at lt))]
      (is (every?
            (fn [^ZonedDateTime zdt]
              (and (contains? t/WEEKEND (.getDayOfWeek zdt))
                   (= (.getHour lt) (.getHour zdt))
                   (= (.getMinute lt) (.getMinute zdt))))
            weekends))))

  (testing "workdays-at"
    (let [lt (LocalTime/now)
          weekends (take 50 (t/every-workday-at lt))]
      (is (every?
            (fn [^ZonedDateTime zdt]
              (and (contains? t/WORKDAYS (.getDayOfWeek zdt))
                   (= (.getHour lt) (.getHour zdt))
                   (= (.getMinute lt) (.getMinute zdt))))
            weekends))))

  (testing "month-end-at"
    (let [lt (LocalTime/now)
          month-ends (take 50 (t/every-month-end-at lt))]
      (is (every?
            (fn [^ZonedDateTime zdt]
              (and (if (= 2 (.getMonthValue zdt))
                     (>= (.getDayOfMonth zdt) 28)
                     (>= (.getDayOfMonth zdt) 30))
                   (= (.getHour lt) (.getHour zdt))
                   (= (.getMinute lt) (.getMinute zdt))))
            month-ends)))
    )

  (testing "first-working-day-of-month-at"
    (let [lt (LocalTime/now)
          month-ends (take 50 (t/every-first-working-day-of-month-at lt))]
      (is (every?
            (fn [^ZonedDateTime zdt]
              (and (contains? t/WORKDAYS (.getDayOfWeek zdt))
                   (<= (.getDayOfMonth zdt) 4)
                   (= (.getHour lt) (.getHour zdt))
                   (= (.getMinute lt) (.getMinute zdt))))
            month-ends)))
    )

  )
