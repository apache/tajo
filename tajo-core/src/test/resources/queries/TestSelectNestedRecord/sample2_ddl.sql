CREATE EXTERNAL TABLE ${0} (
  glossary RECORD (
    title TEXT,
    "GlossDiv" RECORD (
      "GlossEntry" RECORD (
        "ID" TEXT,
        "SortAs" TEXT,
        "GlossTerm" TEXT,
        "Acronym" TEXT,
        "Abbrev" TEXT,
        "GlossDef" RECORD (
          para TEXT
        ),

        "GlossSee" TEXT
      )
    )
  )
) USING JSON LOCATION ${path};