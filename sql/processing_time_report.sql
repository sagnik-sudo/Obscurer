CREATE OR REPLACE VIEW
  `gcds-oht33219u9-2023.obscurer_reporting.processing_time_report` AS
SELECT
  processed_time.file_name,
  content_type,
  EXTRACT(second
  FROM
    processed_time.created-upload_time.time_stamp) AS processing_time_seconds,
FROM
  `gcds-oht33219u9-2023.obscurer_reporting.file_upload_record` upload_time
LEFT JOIN
  `gcds-oht33219u9-2023.obscurer_reporting.deidentified_view` processed_time
ON
  CONCAT(upload_time.filename,".txt") = processed_time.file_name
WHERE
  processed_time.file_name IS NOT NULL