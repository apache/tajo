SELECT
  *
FROM (
  SELECT
    "tb1"."aGe",
    "tb1"."teXt",
    "Number"
  FROM
    "TESTDELIMITEDIDENTIFIERWITHMIXEDCHARACTERS" "tb1"

  UNION

  SELECT
    "aGe",
    "teXt",
    "Number"
  FROM
    "testDelimitedIdentifierWithMixedCharacters"
) T1
ORDER BY
  "aGe";

