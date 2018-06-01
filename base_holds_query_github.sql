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
	WHEN r.record_type_code = 'i' THEN (
		SELECT
		l.bib_record_id

		FROM
		sierra_view.bib_record_item_record_link as l

		WHERE
		l.item_record_id = h.record_id

		LIMIT 1
	)

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
h.is_frozen is false -- not frozen hold
AND h.is_ir is false -- not INN-Reach
AND h.is_ill is false -- not ILL
;
---


ANALYZE temp_plch_holds;
---


-- remove all the rows where holds don't have a bib record with a cataloging date
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


SELECT
*
from
temp_plch_holds limit 100
