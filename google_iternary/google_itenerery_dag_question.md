Here’s a clean way to reconstruct the itinerary (or return `null` if it’s impossible):

## Idea (linear time)

* Build `next[city] = destination` and track `in_degree` and `out_degree`.
* Valid path requirements:

    * Every ticket is used exactly once ⇒ each city has **out_degree ≤ 1** and **in_degree ≤ 1**.
    * The **start** city has `in_degree = 0` and `out_degree = 1` (unless there are zero trips).
    * Exactly one **end** city has `out_degree = 0` and `in_degree = 1`.
    * Number of unique tickets = number of steps in the chain you can walk from `start`.
* Walk from `start` following `next` for exactly `len(trips)` edges; collect the cities. If you ever get stuck early, see a fork, or loop, return `null`.

---

## Python

```python
from typing import List, Optional

def build_itinerary(start: str, trips: List[List[str]]) -> Optional[List[str]]:
    if not trips:
        return [start]  # trivial itinerary

    next_city = {}
    in_deg, out_deg = {}, {}

    # Build mapping and degrees; reject forks or duplicates
    for a, b in trips:
        if a in next_city:                 # two outgoing from same city → invalid
            return None
        next_city[a] = b
        out_deg[a] = out_deg.get(a, 0) + 1
        in_deg[b] = in_deg.get(b, 0) + 1
        in_deg.setdefault(a, in_deg.get(a, 0))
        out_deg.setdefault(b, out_deg.get(b, 0))

    # Degree checks
    # start must have in_deg 0
    if in_deg.get(start, 0) != 0:
        return None
    # all nodes must have in_deg≤1 & out_deg≤1 and at most one end node (out=0,in=1)
    for city in set(list(in_deg.keys()) + list(out_deg.keys())):
        if in_deg.get(city, 0) > 1 or out_deg.get(city, 0) > 1:
            return None

    # Walk the path using exactly len(trips) edges
    path = [start]
    cur = start
    for _ in range(len(trips)):
        if cur not in next_city:
            return None  # stuck early
        cur = next_city[cur]
        path.append(cur)

    # Must not have leftover edges or cycles beyond len(trips)
    if cur in next_city:
        return None

    return path
```

### Demo with your example

```python
trips = [["Amsterdam", "London"], ["Berlin", "Amsterdam"], ["Barcelona", "Berlin"], ["London", "Milan"]]
print(build_itinerary("Barcelona", trips))
# ['Barcelona', 'Berlin', 'Amsterdam', 'London', 'Milan']
```

---

## Java

```java
import java.util.*;

public class Itinerary {
    public static List<String> buildItinerary(String start, List<String[]> trips) {
        if (trips.isEmpty()) return Collections.singletonList(start);

        Map<String, String> next = new HashMap<>();
        Map<String, Integer> in = new HashMap<>(), out = new HashMap<>();

        for (String[] t : trips) {
            String a = t[0], b = t[1];
            if (next.containsKey(a)) return null; // fork from a
            next.put(a, b);
            out.put(a, out.getOrDefault(a, 0) + 1);
            in.put(b, in.getOrDefault(b, 0) + 1);
            in.putIfAbsent(a, in.getOrDefault(a, 0));
            out.putIfAbsent(b, out.getOrDefault(b, 0));
        }

        if (in.getOrDefault(start, 0) != 0) return null;

        Set<String> nodes = new HashSet<>();
        nodes.addAll(in.keySet());
        nodes.addAll(out.keySet());
        for (String c : nodes) {
            if (in.getOrDefault(c, 0) > 1 || out.getOrDefault(c, 0) > 1) return null;
        }

        List<String> path = new ArrayList<>();
        path.add(start);
        String cur = start;
        for (int i = 0; i < trips.size(); i++) {
            if (!next.containsKey(cur)) return null; // stuck early
            cur = next.get(cur);
            path.add(cur);
        }
        if (next.containsKey(cur)) return null; // extra cycle

        return path;
    }

    // quick test
    public static void main(String[] args) {
        List<String[]> trips = Arrays.asList(
            new String[]{"Amsterdam","London"},
            new String[]{"Berlin","Amsterdam"},
            new String[]{"Barcelona","Berlin"},
            new String[]{"London","Milan"}
        );
        System.out.println(buildItinerary("Barcelona", trips));
        // [Barcelona, Berlin, Amsterdam, London, Milan]
    }
}
```

This handles:

* unordered input
* branches/duplicates
* disconnected sets
* cycles
* verifying the given start really is the start

Returns the ordered list (or `null` when it can’t form a single chain using all trips).
