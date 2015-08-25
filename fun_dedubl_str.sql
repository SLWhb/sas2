create or replace 
FUNCTION     FN_DEDUBL_STR(inSTR IN VARCHAR2, inDelim IN VARCHAR2 DEFAULT ',') 
  RETURN VARCHAR2
-- ========================================================================
-- ==	ФУНКЦИЯ     "Дедубликация слов по разделителю в строки"
-- ==	ОПИСАНИЕ:   схлопывает исходную строку inSTR в строку без дублей return_Str 
-- ==             по разделителю ','
-- ========================================================================
-- ==	СОЗДАНИЕ:		  24.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	25.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
IS
	returnStr VARCHAR2(4000);
BEGIN
	SELECT DISTINCT LISTAGG(STRING_DELIM,inDelim) WITHIN GROUP (ORDER BY REQUEST_ID) OVER(PARTITION BY REQUEST_ID) INTO returnStr
  FROM (select DISTINCT 1 as REQUEST_ID, TRIM(regexp_substr(inSTR, '[^'||inDelim||']+', 1, level)) STRING_DELIM from dual t
          CONNECT BY instr(trim(inDelim from inSTR), inDelim , 1, level - 1) > 0 
          ORDER BY STRING_DELIM);
	
  --DBMS_OUTPUT.put_line('returning str: '||returnStr);
  RETURN returnStr;
END;
