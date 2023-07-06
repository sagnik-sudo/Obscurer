CREATE OR REPLACE VIEW
  `gcds-oht33219u9-2023.obscurer_reporting.file_processed_summary` AS
SELECT
  unprocessed.filename filename,
  unprocessed.content_type,
  CASE
    WHEN MAX(unprocessed.filename) IS NOT NULL THEN TRUE
  ELSE
  FALSE
END
  AS unprocessed,
  CASE
    WHEN MAX(processed.file_name) IS NOT NULL THEN TRUE
  ELSE
  FALSE
END
  AS processed,
  CASE
    WHEN MAX(deidentified.file_name) IS NOT NULL THEN TRUE
  ELSE
  FALSE
END
  AS deidentified,
FROM
  `gcds-oht33219u9-2023.obscurer_reporting.unprocessed_file_record` unprocessed
LEFT JOIN
  `gcds-oht33219u9-2023.obscurer_reporting.processed_view` processed
ON
  CONCAT(unprocessed.filename,".txt") = processed.file_name
LEFT JOIN
  `gcds-oht33219u9-2023.obscurer_reporting.deidentified_view` deidentified
ON
  CONCAT(unprocessed.filename,".txt") = deidentified.file_name
WHERE
  unprocessed.content_type IS NOT NULL
GROUP BY
  filename,
  content_type