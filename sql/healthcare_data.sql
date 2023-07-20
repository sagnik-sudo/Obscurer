CREATE OR REPLACE VIEW
  `gcds-oht33219u9-2023.obscurer_reporting.healthcare_data` AS
SELECT
  deidentified.file_name,
  medicines.medicine_names
FROM
  `gcds-oht33219u9-2023.obscurer_reporting.medicines_found` medicines
INNER JOIN
  `gcds-oht33219u9-2023.obscurer_reporting.deidentified_view` deidentified
ON
  CONCAT(medicines.filename,".txt") = deidentified.file_name