# jimpil/chime

Flexible (date/time-based) scheduling facilities for Clojure. 

## What
A fork of [jarohen/chime](https://github.com/jarohen/chime), taking the core idea into a slightly different direction.
I remain utterly grateful to the original maintainer, not only for the code, but also for our discussions.
If you're thinking of using this library, please read the original README first (90% of the things apply here too).

## Where
TODO

## Why 
The underlying idea behind `jarohen/chime` is very powerful. Its power stems from the fact that producing the times you want scheduled, 
is completely orthogonal to actually scheduling them. In other words, the right separation of concerns gave birth to opportunity/leverage.    
The implementation is equally clever with a lazy approach whereby each task is scheduled as soon as the previous one finishes.
Finally, its maintainer is a cool, responsive and easy to talk to guy. 

The thing is that, while `jarohen/chime` invented/discovered the absolute perfect separation of concerns wrt when VS what to schedule,
in my view it missed the mark regarding the right separation between the enduring/macroscopic notion of  the 'entire-schedule', 
and the ephemeral/microscopic notion of the 'upcoming-task'. In addition, it has made the (unfortunate in my eyes) decision to associate 
closing the entire schedule with `pool.shutdownNow()`, leaving little room for concerns such as `greceful shutdown` and 
`cancelling the upcoming (potentially already started) task, while keeping the overall schedule alive`. 
Lastly its default error-handler, in order to be correct (given the `shutdownNow` semantics), handles `InterruptedException` 
differently from any other exception. This is something that **should** be mimicked by user-provided error-handlers, 
but firstly it is not communicated anywhere (quite the contrary), and secondly it is kind of unreasonable. 
This has all been discussed quite extensively between me and the `jarohen/chime` maintainer. If you enjoy conversations like these
and are short of reading material, check [this](https://github.com/jarohen/chime/issues/37) out. 

Given the breaking nature of my vision/suggestions, it was mutually decided to instead fork and re-release as `jimpil/chime`.

## Practical implications
In order to avoid namespace collisions, this project has a slightly different project structure. The mapping is as follows:

- `chime.core` has become `chime.schedule`
- `chime.core-async` has become `chime.channel`

Furthermore, `chime.joda-time` and `chime.utils` don't exist here. `chime.joda-time` and the `clj-time` dependency are gone for good,
whereas the single function `merge-schedules` in `chime.utils` has been migrated to a brand new `chime.times` namespace, which contains
anything related to generating times (i.e. `periodic-seq`, `without-past-times` etc).

It will hopefully become apparent later on, but the single most important difference is how the two projects handle 'closing the schedule'.
`jarohen/chime` takes it upon itself to also cancel the (potentially already started) next-task (via `shutdownNow`), whereas `jimpil/chime` 
lets you decide on those two things separately. This also affects how errors are handled in the two projects 
(i.e. the default error-handler in `jimpil/chime` doesn't need to assign any particular importance to `InterruptedException` - this is up to the end-user to decide).     

Finally, this project aims to provide a higher-level (than `chime-at`), stateful scheduler construct which can deal which multiple jobs.

## How

### Decide on the schedule dates/times

Use `chime.times` to build your seq of scheduling times.`periodic-seq` is particularly useful for building fixed interval periods 
(in case the built-in ones don't suffice). Also, see [jarohen/chime](https://github.com/jarohen/chime) for some nice examples.

### `chime.schedule/chime-at` _[times chime-fn ?opts]_
This is the core scheduling primitive of `chime`. It simply takes the dates/times, and the (1-arg) function you want scheduled.
It returns an implementation of:

- `AutoCloseable` - reflecting the closeable nature of the schedule as a whole (NOT concerned with already scheduled tasks) 
- `IPending` - reflecting the potentially finite nature of the schedule
- `IDeref`/`IBlockingDeref` - reflecting the optionally blocking nature of the schedule
- `ScheduledFuture` (partially) - representing the currently scheduled task

If you're confident you understand that the first 3 are about the entire schedule, 
whereas the last one is about a single task, you can use the returned object as is.
That said, there is a somewhat higher-level api to assist you (see below).   

### `chime.schedule/cancel-current!` _[schedule]_
Cancels the next upcoming chime, potentially abruptly, as it may have already started. The rest of the schedule
will remain unaffected, unless the interruption is handled by the error-handler (i.e. `InterruptedException`), and it
returns falsey (or throws). The `cancel-next?!` variant does the same thing, unless the next task has already started.

### `chime.schedule/until-current` _[schedule]_
Returns the remaining time (in millis by default) until the next chime. 

### `chime.schedule/shutdown!` _[schedule]_
Gracefully closes the entire schedule (per `pool.shutdown()`). If the next task hasn't started yet, it will be cancelled,
otherwise it will be allowed to finish.

### `chime.schedule/shutdown-now!` _[schedule]_
Attempts a graceful shutdown (per `shutdown!`), but if the latest task is already happening attempts to interrupt it. 
Semantically equivalent to `pool.shutdownNow()`.

From a user's perspective I find the above separation of concerns rather empowering, as I can ask questions/make decisions about 
the next-task VS the entire-schedule separately - and this is essentially the crux of the divergence from `jarohen/chime`. 

### chime.channel/chime-ch _[times ?opts]_
Returns a readable `core.async` channel that 'chimes' at every time in the `times` list.
Unlike `jarohen/chime`, you're not allowed to pass in a `:ch` param, but you can pass a `:buffer` instead. 
Uses `chime-at` internally, so you can also pass `:error-handler`, and/or `:on-finished` callbacks.
Closing the returned read-channel (gracefully) stops the schedule, and closes the underlying write-channel (where the chime-fn writes).
This means that no more writes will succeed (even if there is something already scheduled).  
 
## Mutable schedule
It is often desirable to add times to a schedule which is already running. 
`jimpil/chime` supports this via the `schedule/mutable-times` fn. It basically wraps a (finite) 
sequence of times into an atom (which `chime-at` can work with). 

```clj
;; the mutable times
(def times 
  (->> (times/every-n-seconds 2)
       (take 10)))

;; the mutable schedule
(def sched (chime-at times println {:mutable? true})) 

;; cancelling the upcoming chime works
(cancel-current?! sched)  

;; append to the schedule (if not finished already)
(append-relative-to-last! sched #(.plusSeconds ^Instant % 2))
```

**It is on YOU to add to the schedule BEFORE it finishes!**

## Notes
The project was forked in its `v0.3.2` state (actually a little later but not super important). 


## Requirements 

- Java 8 or above (for `java.time`)

## License

Copyright © 2020 Dimitrios Piliouras

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.