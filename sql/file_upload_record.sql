CREATE OR REPLACE VIEW
  `gcds-oht33219u9-2023.obscurer_reporting.file_upload_record` AS
SELECT
  filename,
  content_type,
  size,
  MAX(recordstamp) time_stamp
FROM
  `gcds-oht33219u9-2023.obscurer_meta.raw_files`
GROUP BY
  filename,
  content_type,
  size