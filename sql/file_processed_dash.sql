CREATE OR REPLACE VIEW
  `gcds-oht33219u9-2023.obscurer_reporting.file_processed_dash` AS
SELECT
  Content_Type,
  COUNTIF(unprocessed=TRUE) AS Unprocessed_Count,
  COUNTIF(processed=TRUE) AS Processed_Count,
  COUNTIF(deidentified=TRUE) AS Deidentified_Count
FROM
  `obscurer_reporting.file_processed_summary`
GROUP BY
  1