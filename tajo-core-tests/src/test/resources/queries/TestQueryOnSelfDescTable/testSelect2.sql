select
  glossary.title,
  glossary."GlossDiv".title,
  glossary."GlossDiv".null_expected,
  glossary."GlossDiv"."GlossList"."GlossEntry"."SortAs"
from
  self_desc_table2
where
  glossary."GlossDiv"."GlossList"."GlossEntry"."Abbrev" = 'ISO 8879:1986'