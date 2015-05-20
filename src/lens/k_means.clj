(ns lens.k-means
  "http://www.learningclojure.com/2011/01/k-means-algorithm-for-clustering-data.html")

(defn distance [a b]
  (if (< a b) (- b a) (- a b)))

(defn average [& list] (/ (reduce + list) (count list)))

(defn closest [point means distance]
  (first (sort-by #(distance % point) means)))

(defn point-groups [means data distance]
  (group-by #(closest % means distance) data))

(defn new-means [average point-groups old-means]
  (for [o old-means]
    (if (contains? point-groups o)
      (apply average (get point-groups o))
      o)))

(defn iterate-means [data distance average]
  (fn [means] (new-means average (point-groups means data distance) means)))

(defn groups [data distance means]
  (vals (point-groups means data distance)))

(defn take-while-unstable
  ([s]
   (lazy-seq
     (if-let [s (seq s)]
       (cons (first s) (take-while-unstable (rest s) (first s))))))
  ([s last]
   (lazy-seq
     (if-let [s (seq s)]
       (if (= (first s) last) '() (take-while-unstable s))))))

(defn init-means [k data]
  (let [min (apply min data)
        max (apply max data)
        spacing (/ (distance min max) k)]
    (println :min min :max max :spacing spacing)
    (->> (iterate #(+ spacing %) (+ min (/ spacing 2)))
         (take k))))

(defn k-means [init-means data]
  (let [means (->> (iterate (iterate-means data distance average) init-means)
                   (take-while-unstable)
                   (last))]
    (point-groups means data distance)))
