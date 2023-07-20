CREATE OR REPLACE VIEW
  `gcds-oht33219u9-2023.obscurer_reporting.contenttype_size` AS
SELECT
  f.content_type,
  ROUND(SAFE_DIVIDE(SUM(u.size + d.size),(1024*1024)),2) AS Total_Size
FROM
  `gcds-oht33219u9-2023.obscurer_reporting.deidentified_view` d
JOIN
  `gcds-oht33219u9-2023.obscurer_reporting.unprocessed_file_record` u
ON
  CONCAT(u.filename,".txt") = d.file_name
LEFT JOIN
  `obscurer_reporting.file_upload_record` f
ON
  u.filename = f.filename
GROUP BY
  f.content_type