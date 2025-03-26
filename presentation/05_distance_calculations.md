# Distance Calculations

## Accurate Measurement of GPS Coordinates

![Distance](https://cdn.pixabay.com/photo/2016/10/28/11/57/tic-tac-toe-1777859_1280.jpg)

---

## Haversine Formula Implementation

```python
def haversine_distance(coord1, coord2):
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r
```

---

## Segment Analysis Importance

* **Short Segments (<1km)**: Typically represent normal distance between logs in continuous tracking
* **Medium Segments (1-5km)**: May indicate minor tracking interruptions or faster movement
* **Long Segments (>5km)**: Strong indicator of tracking issues or app being killed

> "By analyzing segment distribution, we can distinguish between continuous tracking and sporadic location updates." 