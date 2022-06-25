(ns chime.cron-test
  (:require [clojure.test :refer :all]
            [chime.cron :as cron]
            [chime.times :as times])
  (:import (java.time ZonedDateTime DayOfWeek)))

(defn- dom* [^ZonedDateTime d] (.getDayOfMonth d))
(defn- dow* [^ZonedDateTime d] (.name (.getDayOfWeek d)))
(defn- hod* [^ZonedDateTime d] (.getHour d))
(defn- moh* [^ZonedDateTime d] (.getMinute d))
(defn- som* [^ZonedDateTime d] (.getSecond d))
(defn- moy* [^ZonedDateTime d] (.getMonthValue d))

(deftest cron-times
  (testing "expressions with default fields"

    (testing "At 12:00 p.m. (noon) every day"
      (let [expr "0 12 * * ?"
            days (->> expr cron/times (take 100))]
        (doseq [^ZonedDateTime d days]
          (is (= 12 (hod* d)))
          (is (zero? (moh* d))))
        )
      )
    (testing "12 pm (noon) every 5 days of every month, starting on the seventh day of the month - weekdays only"
      (let [expr "0 12 7/5 * mon-fri"
            chimes (->> expr cron/times (take 50))
            workday-name? (set (map (fn [^DayOfWeek dow] (.name dow)) times/WORKDAYS))]
        (doseq [t chimes]
          (is (workday-name? (dow* t)))
          (is (= [12 0 0] ((juxt hod* moh* som*) t)))
          (is (<= 7 (dom* t)))
          )
        )
      )
    (testing "Every five minutes starting at 1 p.m. and ending at 1:55 p.m. Then starting at 6 p.m. and ending at 6:55 p.m. (every day)"
      (let [expr "0-55/5 13,18 * * ?"
            days (->> expr cron/times (partition 12) (take 100))]
        (doseq [[one->two six->seven] (partition 6 days)]
          (is (apply = (map dom* one->two)))
          (is (apply = (map dom* six->seven)))
          (is (= (dom* (first one->two))
                 (dom* (first six->seven))))
          (is (every? #{13} (map hod* one->two)))
          (is (every? #{18} (map hod* six->seven)))
          )
        )
      )
    (testing "Every minute starting at 1 p.m. and ending at 1:05 p.m. (every day)"
      (let [expr "0-5 13 * * ?"
            days (->> expr cron/times (partition 6) (take 10))]
        (doseq [d days]
          (is (apply = (map dom* d)))
          (is (every? #{13} (map hod* d)))
          (is (= [0 1 2 3 4 5] (map moh* d))))
        )
      )

    (testing "At 1:15 p.m. and 1:45 p.m. on Tuesdays in the month of June"
      (let [expr "15,45 13 ? 6 Tue"
            tuesdays (->> expr cron/times (partition 2) (take 20))]
        (doseq [[one-fifteen one-forty-five] tuesdays]
          (is (= [6 13 15] ((juxt moy* hod* moh*) one-fifteen)))
          (is (= [6 13 45] ((juxt moy* hod* moh*) one-forty-five)))
          (is (= "TUESDAY" (dow* one-fifteen)))
          (is (= "TUESDAY" (dow* one-forty-five)))
          )
        )
      )
    (testing "At 9:30 a.m. every weekday (Mon-Fri)"
      (let [expr "30 9 ? * MON-FRI"
            weekdays (->> expr cron/times (partition 5) (take 20))]
        (doseq [mon-fri weekdays]
          (is (every? (partial = [9 30 0])
                      (map (juxt hod* moh* som*)
                           mon-fri)))
          (is (= ["MONDAY" "TUESDAY" "WEDNESDAY" "THURSDAY" "FRIDAY"]
                 (mapv dow* mon-fri)))
          )
        )
      )
    (testing "At 9:30 a.m. on the 15th day of every month"
      (let [expr "30 9 15 * ?"
            days (->> expr cron/times (take 50))]
        (doseq [mid-month days]
          (is (= [9 30 0] ((juxt hod* moh* som*) mid-month)))
          (is (= 15 (dom* mid-month)))
          )
        )
      )
    (testing "At 6 p.m. on the last day of every month"
      (let [expr "0 18 L * ?"
            days (->> expr cron/times (take 50))]
        (doseq [last-dom days]
          (is (= [18 0 0] ((juxt hod* moh* som*) last-dom)))
          (is (cron/last-dom? last-dom))
          )
        )
      )
    (testing "At 10:30 a.m. on the last Thursday of every month"
      (let [expr "30 10 ? * thuL" ;; 4L would also work
            days (->> expr cron/times (take 50))]
        (doseq [last-thursday days]
          (is (= [10 30 0] ((juxt hod* moh* som*) last-thursday)))
          (is (cron/last-dow-in-month? (DayOfWeek/of 4) last-thursday))
          )
        )
      )

    (testing "At 12 midnight every five days, starting on the 10th day of every month"
      (let [expr "0 0 10/5 * ?" ;; 10-31/5 would also work
            months (->> expr cron/times (partition-by moy*) (take 50))
            valid-days #{[10 15 20 25] ;; watch out for February!
                         [10 15 20 25 30]}]
        (doseq [days months]
          (is (contains? valid-days (map dom* days)))
          (is (every? #{[0 0 0]} (map (juxt hod* moh* som*) days)))
          )
        )
      )
    (testing "every five minutes of every hour of every day of every month and every day of the week"
      (let [expr "*/5 * * * * "
            chimes (->> expr cron/times (take 100))]
        (doseq [^ZonedDateTime t chimes]
          (is (zero? (rem (.getMinute t) 5)))
          )
        )
      )

    (testing "at 2:10 p.m. and at 2:44 p.m. every Wednesday and Friday in March"
      (let [expr "10,44 14 ? 3 WED,FRI"
            day-chimes (->> expr cron/times (partition 2) (take 50))
            valid-day? #{"WEDNESDAY" "FRIDAY"}]
        (doseq [[two-ten two-forty-four] day-chimes]
          (is (= 3 (moy* two-ten)))
          (is (= 3 (moy* two-forty-four)))
          (is (valid-day? (dow* two-ten)))
          (is (valid-day? (dow* two-forty-four)))
          (is (= [14 10] ((juxt hod* moh*) two-ten)))
          (is (= [14 44] ((juxt hod* moh*) two-forty-four)))

          )
        )
      )
    (testing "At 08:20 a.m. on every last Tuesday of every month"
      (let [expr "20 8 ? * 2L"
            chimes (->> expr cron/times (take 50))
            valid-day? #{"TUESDAY"}]
        (doseq [t chimes]
          (is (valid-day? (dow* t)))
          (is (= [8 20] ((juxt hod* moh*) t)))
          (is (cron/last-dow-in-month? (DayOfWeek/of 2) t))
          )
        )
      )

    )
  )
