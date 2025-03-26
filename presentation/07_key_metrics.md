# Key Metrics & Indicators

## Understanding the Data That Matters

![Metrics](https://cdn.pixabay.com/photo/2017/12/04/03/13/abacus-2996213_1280.jpg)

---

## Critical Performance Metrics

1. **Average Distance Variance (%)**
   - Measures difference between GPS-calculated and manually-reported distance
   - Formula: `variance = abs(calculated_distance - manual_distance) / manual_distance * 100`

2. **Accurate Trips (<25% variance)**
   - Percentage of trips with reliable GPS tracking
   - Key indicator of overall tracking quality

3. **App Killed Issue Trips**
   - Trips where tracking app was likely terminated by OS
   - Identified by specific segment patterns

4. **Single Log Trips**
   - Critical failures where only one position was recorded
   - Indicates immediate app termination

5. **Segment Distance Distribution**
   - Percentage of total distance from short/medium/long segments
   - Reveals nature of tracking continuity

> "These metrics provide an objective framework for measuring improvement in tracking quality over time." 