--merges the trips with ids %(trip_ids)s into a single trip and inserts into %(trip_target)t
--the new trip distance is the sum of all merged trips and the activity with the longest distance is kept

WITH trips_to_merge AS (
  SELECT *
  FROM %(trip)t AS trip
  WHERE id = ANY(%(trip_ids)s)
  ORDER BY start_time
  ),
    merged_geom AS (
  SELECT array_agg(dp.wktnode) points
  FROM trips_to_merge, ST_DumpPoints(geom) dp(i,wktnode)
  )
INSERT INTO %(trip_target)t (id, user_id, start_time, end_time, distance, activity, geom, start_geom, end_geom)
SELECT MIN(id), user_id, MIN(start_time), MAX(end_time), SUM(distance),
       (SELECT activity FROM trips_to_merge ttm WHERE ttm.distance = MAX(trips_to_merge.distance)),
       ST_MakeLine(merged_geom.points),
       (SELECT start_geom FROM trips_to_merge ttm WHERE ttm.start_time = MIN(trips_to_merge.start_time)),
       (SELECT end_geom FROM trips_to_merge ttm WHERE ttm.end_time = MAX(trips_to_merge.end_time))
FROM trips_to_merge, merged_geom
GROUP BY user_id, merged_geom.points