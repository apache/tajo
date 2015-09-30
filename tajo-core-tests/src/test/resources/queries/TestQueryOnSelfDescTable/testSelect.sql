select
  glossary.title,
  glossary."GlossDiv".title,
  glossary."GlossDiv".null_expected,
  glossary."GlossDiv"."GlossList"."GlossEntry"."SortAs",
  glossary."GlossDiv"."GlossList"."GlossEntry"."Abbrev"
from
  self_desc_table2