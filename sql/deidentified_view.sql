CREATE OR REPLACE VIEW
  `gcds-oht33219u9-2023.obscurer_reporting.deidentified_view` AS
SELECT
  created,
  size,
  SUBSTRING(filename,14) AS file_name
FROM
  `gcds-oht33219u9-2023.obscurer_meta.deidentified_meta_direct`
WHERE
  filename LIKE 'deidentified%'