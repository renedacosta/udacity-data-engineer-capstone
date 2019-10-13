-- check if summary has data (if so all tables must have data)
DO
$do$
BEGIN
	IF NOT EXISTS(
		SELECT 1
		FROM summary.summary
	) THEN
		RAISE EXCEPTION 'Summary table empty';
	END IF;
END
$do$;


-- check if only data from 2016 present
DO
$do$
BEGIN
	IF EXISTS(
		SELECT 1 
		FROM immigration .immigrants
		WHERE i94yr is null or i94yr <> 2016
	) THEN
		RAISE EXCEPTION 'Invalid data found';
	END IF;
END
$do$;

