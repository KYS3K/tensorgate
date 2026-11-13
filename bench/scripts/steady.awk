# Steady-state means (seconds 40-175) of a loadgen log from before 07f5716 (no lateness columns).
# usage: awk -v name=<label> -f steady.awk run.log
function ms(v) { if (v ~ /^>/) return 10000; if (v ~ /ms$/) { sub(/ms$/, "", v); return v + 0 } sub(/s$/, "", v); return v * 1000 }
$1 != "elapsed" && /\|/ { n++; if (n > 40 && n <= 175) {
  t += $6; p99 = ms($12); mx = ms($13); s99 += p99; smx += mx
  if (mx > top) top = mx; if (mx >= 1000) big++; if (p99 > 100) slow++; c++ } }
END { printf "%-9s tok/s %6.0f | gap p99 avg %5.1fms, secs p99>100ms %3d/%d | gap max avg %5.0fms, worst %5.0fms, secs max>=1s %3d/%d\n", name, t/c, s99/c, slow, c, smx/c, top, big, c }
