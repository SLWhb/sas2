DECLARE
  N NUMBER := 6;
  ANN_SUM NUMBER := 12345.22;
  LEN_AS NUMBER;
  ANN_STR VARCHAR2(8000) := ' ';
  
  subsum VARCHAR2(100);
BEGIN
  DBMS_OUTPUT.ENABLE;
  LEN_AS := LENGTH(TO_CHAR(ANN_SUM));
  ANN_STR := LPAD(ANN_STR, (LEN_AS+1)*N+1, '|'||TO_CHAR(ANN_SUM));
  --ANN_STR := LPAD(ANN_STR, LENGTH(ANN_STR)+(LEN_AS+1)*6, '|'||TO_CHAR(ANN_SUM));
  
  --ANN_STR := regexp_replace(ANN_STR,'(|)([0-9]+)','\2!', 1, 3);
  
  subsum := TO_CHAR(TO_NUMBER(regexp_substr(ANN_STR,'([0-9]+)', 1, N+1-3))+87657);
  ANN_STR := regexp_replace(ANN_STR,'(|)([0-9]+)',subsum, 1, N+1-3);
  
  DBMS_OUTPUT.PUT_LINE(TO_CHAR(LEN_AS)||' '||subsum||' - '||ANN_STR);
END;
/