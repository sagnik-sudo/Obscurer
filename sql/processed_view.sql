CREATE OR REPLACE VIEW
  `gcds-oht33219u9-2023.obscurer_reporting.processed_view` AS
SELECT
  created,
  size,
  SUBSTRING(filename,11) AS file_name
FROM
  `gcds-oht33219u9-2023.obscurer_meta.processed_meta_direct`
WHERE
  filename LIKE 'processed%'