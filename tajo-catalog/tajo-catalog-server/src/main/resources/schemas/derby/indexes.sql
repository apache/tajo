CREATE TABLE INDEXES (
--   INDEX_ID INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
  DB_ID INT NOT NULL REFERENCES DATABASES_ (DB_ID) ON DELETE CASCADE,
  TID INT NOT NULL REFERENCES TABLES (TID) ON DELETE CASCADE,
  INDEX_NAME VARCHAR(128) NOT NULL,
  METHOD CHAR(32) NOT NULL,
  EXPR_NUM INT NOT NULL,
  EXPRS VARCHAR(1024) NOT NULL,
  ASC_ORDERS VARCHAR(1024) NOT NULL,
  NULL_ORDERS VARCHAR(1024) NOT NULL,
  IS_UNIQUE BOOLEAN NOT NULL,
  IS_CLUSTERED BOOLEAN NOT NULL,
  PRED VARCHAR(4096),
  CONSTRAINT C_INDEXES_PK PRIMARY KEY (DB_ID, INDEX_NAME),
  CONSTRAINT C_INDEXES_ID_UNIQ UNIQUE (DB_ID, INDEX_NAME)
)