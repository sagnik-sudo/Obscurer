CREATE OR REPLACE VIEW `gcds-oht33219u9-2023.obscurer_reporting.medicines_found` AS
SELECT
  filename,
  STRING_AGG(medicine_name," ") AS medicine_names,
  recordstamp
FROM
  `gcds-oht33219u9-2023.obscurer_meta.medicines_found`
GROUP BY
  filename,
  recordstamp;