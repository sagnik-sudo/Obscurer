CREATE OR REPLACE VIEW
  `gcds-oht33219u9-2023.obscurer_reporting.unprocessed_file_record` AS
SELECT
  rawfilerecord.filename,
  rawfilerecord.size,
  created,
  fileuploadrecord.content_type,
FROM
  `gcds-oht33219u9-2023.obscurer_reporting.raw_file_record` rawfilerecord
LEFT OUTER JOIN
  `gcds-oht33219u9-2023.obscurer_reporting.file_upload_record` fileuploadrecord
ON
  rawfilerecord.filename = fileuploadrecord.filename