CREATE OR REPLACE VIEW
  `gcds-oht33219u9-2023.obscurer_reporting.top_5_files` AS
SELECT
  deidentified.file_name,
  unprocessed.content_type,
  ROUND(SAFE_DIVIDE(unprocessed.size,1024),2) AS size
FROM
  `gcds-oht33219u9-2023.obscurer_reporting.deidentified_view` deidentified
LEFT JOIN
  `gcds-oht33219u9-2023.obscurer_reporting.file_upload_record` unprocessed
ON
  deidentified.file_name=CONCAT(unprocessed.filename,".txt")
ORDER BY
  size DESC
LIMIT
  10