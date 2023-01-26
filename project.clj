(defproject jimpil/chime "1.0.8"
  :description "Flexible scheduling primitives for Clojure"

  :url "https://github.com/jimpil/chime"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.11.1" :scope "provided"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.clojure/core.async "1.5.648" :scope "provided"]]
  :profiles
  {:dev {:dependencies [[com.clojure-goes-fast/clj-async-profiler "0.5.1"]]}}

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "--no-sign"]
                  ["deploy" ]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ;["vcs" "push"]
                  ]
  :deploy-repositories [["releases" :clojars]] ;; lein release :patch
  :signing {:gpg-key "jimpil1985@gmail.com"}
  )
