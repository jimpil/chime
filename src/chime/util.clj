(ns chime.util)

(defmacro invoke-some
  [f & args]
  `(when-some [f# ~f]
     (f# ~@args)))
