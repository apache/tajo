CREATE TABLE STATS (
  TID INT NOT NULL PRIMARY KEY,
  NUM_ROWS NUMBER(38),
  NUM_BYTES NUMBER(38),
  FOREIGN KEY (TID) REFERENCES TABLES (TID) ON DELETE CASCADE
)