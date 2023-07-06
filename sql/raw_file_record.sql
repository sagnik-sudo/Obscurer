CREATE OR REPLACE VIEW
  `gcds-oht33219u9-2023.obscurer_reporting.raw_file_record` AS
SELECT
  *
FROM
  `gcds-oht33219u9-2023.obscurer_meta.raw_file_meta_direct`
WHERE
  filename NOT LIKE 'processed%'
  AND filename NOT LIKE 'deidentified%'