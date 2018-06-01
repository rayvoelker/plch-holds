-- 
-- These are a series of queries that will create a temporary table of all 
-- PLCH holds that meet the shared baseline criteria for holds reports being
-- produced for PLCH staff to examine as possibly problematic 
-- 


-- Grab all of the bib level and volume level holds that exist in the system right now, 
-- and link them to a title (bib record):
-- Where holds are not INN-Reach, not ILL and not frozen
DROP TABLE IF EXISTS temp_plch_holds;
CREATE TEMP TABLE temp_plch_holds AS
SELECT
h.*,
p.ptype_code as patron_ptype_code,
p.home_library_code AS patron_home_library_code,
p.expiration_date_gmt AS patron_expiration_date_gmt,
p.block_until_date_gmt AS patron_block_until_date_gmt,
p.owed_amt AS patron_owed_amt,
p.activity_gmt AS patron_activity_gmt,
r.record_type_code,
r.record_num,
CASE
-- 	we are not going to look at item level holds as part of this report, but could be useful later on...
-- 	WHEN r.record_type_code = 'i' THEN (
-- 		SELECT
-- 		l.bib_record_id
-- 
-- 		FROM
-- 		sierra_view.bib_record_item_record_link as l
-- 
-- 		WHERE
-- 		l.item_record_id = h.record_id
-- 
-- 		LIMIT 1
-- 	)

	WHEN r.record_type_code = 'j' THEN (
		SELECT
		l.bib_record_id

		FROM
		sierra_view.bib_record_volume_record_link as l

		WHERE
		l.volume_record_id = h.record_id

		LIMIT 1
	)

	WHEN r.record_type_code = 'b' THEN (
		h.record_id
	)

	ELSE NULL

END AS bib_record_id

FROM
sierra_view.hold as h

LEFT OUTER JOIN
sierra_view.record_metadata as r
ON
  r.id = h.record_id

LEFT OUTER JOIN
sierra_view.patron_record as p
ON
  p.record_id = h.patron_record_id

WHERE
(r.record_type_code = 'b' OR r.record_type_code = 'j')
AND h.is_ir is false -- not INN-Reach
AND h.is_ill is false -- not ILL
AND h.is_frozen is false -- not frozen hold -- considering frozen holds for this
;
---


CREATE INDEX index_record_type_code ON temp_plch_holds (record_type_code);
CREATE INDEX index_bib_record_id ON temp_plch_holds (bib_record_id);
CREATE INDEX index_record_id ON temp_plch_holds (record_id);
---


ANALYZE temp_plch_holds;
---


-- remove all the rows where holds don't have a bib record with a cataloging date.
-- there may be a better way to do this, but I'm leaving it like it is for now
DELETE FROM
temp_plch_holds AS h

WHERE h.id IN (
	SELECT
	hs.id

	FROM
	temp_plch_holds as hs

	JOIN
	sierra_view.bib_record as b
	ON
	  b.record_id = hs.bib_record_id

	WHERE
	b.cataloging_date_gmt IS NULL
);
---


-- count active holds and active copies for volume record holds
DROP TABLE IF EXISTS temp_volume_level_holds_counts;
CREATE TEMP TABLE temp_volume_level_holds_counts AS
SELECT
-- r.record_type_code || r.record_num || 'a' as bib_record_num,
-- v.field_content as volume_number,
t.bib_record_id,
t.record_id,
t.record_type_code,
br.bcode2,
-- count the active holds
(
	SELECT
	COUNT(*)

	FROM
	temp_plch_holds as t1

	WHERE
	t1.record_id = t.record_id
	AND t1.patron_ptype_code IN (0, 1, 2, 3, 5, 6, 10, 11, 12, 15, 22, 30, 31, 32, 40, 41, 196)
	
) as count_active_holds,
-- count the items attached to the volume record
(
	SELECT
	COUNT(*)

	FROM
	sierra_view.volume_record_item_record_link as l

	JOIN
	sierra_view.item_record as i
	ON
	  i.record_id = l.item_record_id

	JOIN
	sierra_view.record_metadata as r
	ON
	  r.id = l.item_record_id

	WHERE
	l.volume_record_id = t.record_id
	AND i.is_suppressed IS false
	AND ( 
		i.item_status_code IN ('-', '!', 'b', 'p', '(', '@', ')', '_', '=', '+') 
		OR (i.item_status_code = 't' AND age(r.record_last_updated_gmt) < INTERVAL '60 days'  ) 
	)
) as count_active_copies,

-- count the number of copies on order ...
(
	SELECT
	SUM(c.copies)

	FROM
	sierra_view.bib_record_order_record_link as l

	LEFT OUTER JOIN
	sierra_view.order_record as o
	ON
	  o.record_id = l.order_record_id

	JOIN
	sierra_view.order_record_cmf as c
	ON
	  c.order_record_id = l.order_record_id

	LEFT OUTER JOIN
	sierra_view.order_record_received as r
	ON
	  r.order_record_id = l.order_record_id

	WHERE
	l.bib_record_id =  t.bib_record_id
	AND r.id IS NULL -- order is not received 
	AND c.location_code != 'multi'

	GROUP BY
	l.bib_record_id

) as count_copies_on_order

FROM
temp_plch_holds as t

JOIN
sierra_view.bib_record as br
ON
  br.record_id = t.bib_record_id

WHERE
t.record_type_code = 'j'
AND br.bcode2 NOT IN ('s','n') -- bcode2 don't include magazines (s) or newspapers ('n')

GROUP BY
t.bib_record_id,
t.record_id,
t.record_type_code,
br.bcode2
;
---


-- OUTPUT
-- produce our results for the volume level holds
SELECT
id2reckey(t.bib_record_id) || 'a' as bib_num,
id2reckey(t.record_id) || 'a' as vol_num,
v.field_content as vol,
t.count_active_holds,
t.count_active_copies,
COALESCE(t.count_copies_on_order, 0) as count_copies_on_order,
t.count_active_copies + COALESCE(t.count_copies_on_order, 0) as total_count_copies,
t.count_active_holds::float / ( t.count_active_copies + COALESCE(t.count_copies_on_order, 0) )::float AS ratio_holds_to_copies,
t.bcode2

FROM
temp_volume_level_holds_counts as t

LEFT OUTER JOIN
sierra_view.varfield as v
ON
  v.record_id = t.record_id -- t.record_id should be the volume record id
  AND v.varfield_type_code = 'v'

WHERE
t.count_active_copies > 0
AND t.count_active_holds > 0
AND (
	(
		t.bcode2 IN ('g')
		AND ( t.count_active_holds::float / ( t.count_active_copies + COALESCE(t.count_copies_on_order, 0) )::float
		) > 9.0::float
	)
	OR (
		t.bcode2 IN ('i', 'j', 'q')
		AND ( 
			t.count_active_holds::float / ( t.count_active_copies + COALESCE(t.count_copies_on_order, 0) )::float 
		) > 6.0::float
	)
	-- if bcode2 is none of the above, and it has a ratio above 3:1 show it.
	OR (
		t.bcode2 NOT IN ('g', 'i', 'j', 'q')
		AND ( 
			t.count_active_holds::float / ( t.count_active_copies + COALESCE(t.count_copies_on_order, 0) )::float 
		) > 3.0::float
	)
)

ORDER BY
t.bib_record_id,
t.record_id;
---


-- produce table for bib level holds
-- count active holds and active copies for volume record holds
DROP TABLE IF EXISTS temp_bib_level_holds_counts;
CREATE TEMP TABLE temp_bib_level_holds_counts AS
SELECT
-- r.record_type_code || r.record_num || 'a' as bib_record_num,
-- v.field_content as volume_number,
t.bib_record_id,
t.record_id,
t.record_type_code,
br.bcode2,
-- count the active holds
(
	SELECT
	COUNT(*)

	FROM
	temp_plch_holds as t1

	WHERE
	t1.record_id = t.record_id
	AND t1.patron_ptype_code IN (0, 1, 2, 3, 5, 6, 10, 11, 12, 15, 22, 30, 31, 32, 40, 41, 196)
	
) as count_active_holds,
-- count the items attached to the bib record
(
	SELECT
	COUNT(*)

	FROM
	sierra_view.bib_record_item_record_link as l

	JOIN
	sierra_view.item_record as i
	ON
	  i.record_id = l.item_record_id

	JOIN
	sierra_view.record_metadata as r
	ON
	  r.id = l.item_record_id

	WHERE
	l.bib_record_id = t.record_id
	AND i.is_suppressed IS false
	AND ( 
		i.item_status_code IN ('-', '!', 'b', 'p', '(', '@', ')', '_', '=', '+') 
		OR (i.item_status_code = 't' AND age(r.record_last_updated_gmt) < INTERVAL '60 days'  ) 
	)
) as count_active_copies,

-- count the number of copies on order ...
(
	SELECT
	SUM(c.copies)

	FROM
	sierra_view.bib_record_order_record_link as l

	LEFT OUTER JOIN
	sierra_view.order_record as o
	ON
	  o.record_id = l.order_record_id

	JOIN
	sierra_view.order_record_cmf as c
	ON
	  c.order_record_id = l.order_record_id

	LEFT OUTER JOIN
	sierra_view.order_record_received as r
	ON
	  r.order_record_id = l.order_record_id

	WHERE
	l.bib_record_id =  t.bib_record_id
	AND r.id IS NULL -- order is not received 
	AND c.location_code != 'multi'

	GROUP BY
	l.bib_record_id

) as count_copies_on_order

FROM
temp_plch_holds as t

JOIN
sierra_view.bib_record as br
ON
  br.record_id = t.bib_record_id

WHERE
-- records are of type bib
t.record_type_code = 'b'
AND br.bcode2 NOT IN ('s','n') -- bcode2 don't include magazines (s) or newspapers ('n')

GROUP BY
t.bib_record_id,
t.record_id,
t.record_type_code,
br.bcode2
;
---


-- OUTPUT
-- produce our results for the bib level holds.
-- consider these bibs now off the table for future reports

-- TODO -- remove these bib level holds from the "master list"
SELECT
id2reckey(t.bib_record_id) || 'a' as bib_num,

-- removed this column as these will just be coorisponding bib record numbers
-- id2reckey(t.record_id) || 'a' as vol_num,

-- REMOVE LATER ... don't need volume information on bib level holds
-- v.field_content as vol,
t.count_active_holds,
t.count_active_copies,
COALESCE(t.count_copies_on_order, 0) as count_copies_on_order,
t.count_active_copies + COALESCE(t.count_copies_on_order, 0) as total_count_copies,
t.count_active_holds::float / ( t.count_active_copies + COALESCE(t.count_copies_on_order, 0) )::float AS ratio_holds_to_copies,
t.bcode2,
t.bib_record_id,
t.record_id

FROM
temp_bib_level_holds_counts as t

-- REMOVE LATER ... don't need volume information on bib level holds
-- LEFT OUTER JOIN
-- sierra_view.varfield as v
-- ON
--   v.record_id = t.record_id -- t.record_id should be the volume record id
--   AND v.varfield_type_code = 'v'

WHERE
t.count_active_copies > 0
AND t.count_active_holds > 0
AND (
	(
		t.bcode2 IN ('g')
		AND ( t.count_active_holds::float / ( t.count_active_copies + COALESCE(t.count_copies_on_order, 0) )::float
		) > 9.0::float
	)
	OR (
		t.bcode2 IN ('i', 'j', 'q')
		AND ( 
			t.count_active_holds::float / ( t.count_active_copies + COALESCE(t.count_copies_on_order, 0) )::float 
		) > 6.0::float
	)
	-- if bcode2 is none of the above, and it has a ratio above 3:1 show it.
	OR (
		t.bcode2 NOT IN ('g', 'i', 'j', 'q')
		AND ( 
			t.count_active_holds::float / ( t.count_active_copies + COALESCE(t.count_copies_on_order, 0) )::float 
		) > 3.0::float
	)
)

ORDER BY
t.bcode2,
t.bib_record_id,
t.record_id;
---




-- 455267981348



-- test a bib record number to see why it maybe didn't appear on the report
-- SELECT
-- *
-- FROM
-- temp_bib_level_holds_counts as t
-- 
-- WHERE
-- t.bib_record_id = reckey2id('b2812314a')
-- 
-- LIMIT 100


---
-- JOIN
-- sierra_view.record_metadata as r
-- ON
--   r.id = t.bib_record_id
-- 
-- -- get the volume number
-- LEFT OUTER JOIN
-- sierra_view.varfield as v
-- ON
--   v.record_id = t.record_id -- t.record_id should be the volume record id
--   AND v.varfield_type_code = 'v'
-- 
-- WHERE
-- t.record_type_code = 'j'

-- limit 100















-- SELECT 
-- 
-- t.id,
-- t.is_frozen,
-- t.placed_gmt,
-- t.delay_days,
-- ( INTERVAL '1 day' * t.delay_days ) as interval_delay,
-- t.placed_gmt::timestamp + ( INTERVAL '1 day' * t.delay_days ) as not_wanted_before,
-- 
-- -- make a determination if we want to count the hold
-- CASE
-- 	WHEN delay_days = 0 THEN false
-- 	WHEN NOW()::timestamp >= t.placed_gmt::timestamp + ( INTERVAL '1 day' * t.delay_days ) THEN true
-- 	ELSE false
-- END as past_not_wanted_before,
-- t.patron_record_id
-- 
-- FROM 
-- temp_plch_holds as t
-- 
-- WHERE
-- t.patron_record_id = 481038535591
-- 
-- limit 100

-- sum up the results ...
-- SELECT
-- t.record_type_code,
-- count(t.record_type_code)
-- 
-- FROM
-- temp_plch_holds as t
-- 
-- GROUP BY
-- t.record_type_code;
---




-- do some counting ...
-- SELECT
-- h.bib_record_id,
-- id2reckey(h.bib_record_id) as bib_record_num,
-- h.record_type_code,
-- (
-- 	SELECT
-- 	count(l.item_record_id)
-- 
-- 	FROM
-- 	sierra_view.bib_record_item_record_link as l
-- 
-- 	WHERE
-- 	l.bib_record_id = h.bib_record_id
-- ) as count_items_linked,
-- (
-- 	SELECT
-- 	count(l.volume_record_id)
-- 
-- 	FROM
-- 	sierra_view.bib_record_volume_record_link as l
-- 
-- 	WHERE
-- 	l.bib_record_id = h.bib_record_id
-- ) as count_volumes_linked
-- 	
-- FROM
-- temp_plch_holds as h
-- 
-- WHERE
-- h.record_type_code = 'b'
-- OR h.record_type_code = 'j'
-- 
-- GROUP BY
-- h.bib_record_id,
-- h.record_type_code
-- 
-- ORDER BY
-- h.bib_record_id;
