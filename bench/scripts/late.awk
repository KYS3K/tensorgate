# Steady-state means (seconds 40-175) of a loadgen log from 07f5716 on, with lateness columns.
# usage: awk -v name=<label> -f late.awk run.log
function ms(v) { if (v ~ /^>/) return 10000; if (v == "-") return 0; if (v ~ /ms$/) { sub(/ms$/, "", v); return v + 0 } sub(/s$/, "", v); return v * 1000 }
$1 != "elapsed" && /\|/ { k++; if (k > 40 && k <= 175) {
  t += $6; lp50 += ms($11); lp99 += ms($12); lmx = ms($13); smx += lmx; if (lmx > top) top = lmx; if (lmx >= 1000) big++; c++ } }
END { printf "%-11s tok/s %6.0f | late p50 %5.1fms  p99 %6.1fms | late max avg %6.0fms worst %6.0fms  secs>=1s %3d/%d\n", name, t/c, lp50/c, lp99/c, smx/c, top, big, c }
