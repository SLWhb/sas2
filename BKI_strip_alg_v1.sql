DECLARE
  --TYPE T_str_bki IS TABLE OF VARCHAR2(600);
  --TYPE T_bki IS TABLE OF RESPONSE_RESULT_CREDS%ROWTYPE;
  --TBL_BKI_src T_bki;
  
  TBL_BKI_src T_TABLE_BKI; --исходная коллекция строк
  
  TBL_BKI_src_MOD T_TABLE_BKI; -- модифицированная коллекция строк.
  TBL_BKI_calc T_OBJECT_BKI; --выходная строка
BEGIN
	DBMS_OUTPUT.enable;
  --SELECT * BULK COLLECT INTO TBL_BKI_src FROM RESPONSE_RESULT_CREDS WHERE RESPONSE_ID=220462 ORDER BY CRED_DATE, CRED_ENDDATE;
  SELECT T_OBJECT_BKI(RR_CREDS_PK, RESPONSE_ID, CRED_ID, CRED_SUM, CRED_CURRENCY, CRED_DATE, CRED_ENDDATE, CRED_SUM_DEBT, CRED_TYPE, CRED_ACTIVE, ADD_831, ANNUITY, 0) 
          BULK COLLECT INTO TBL_BKI_src 
          FROM RESPONSE_RESULT_CREDS WHERE RESPONSE_ID=220462 ORDER BY CRED_DATE, CRED_ENDDATE;
  
  --модифицируем основные параметры БКИ: строчка, Даты начала и конца кредита.
  TBL_BKI_src_MOD := "FN$BKI_CORRECTING"(TBL_BKI_src); -- подгоняем в функции некоторые 

  TBL_BKI_calc := "FN$BKI_STRING_JOINING"(TBL_BKI_src_MOD); -- присваеваем редультат склейки строк
  
 	DBMS_OUTPUT.put_line('Count table 1 is: '||TO_CHAR(TBL_BKI_src_MOD.COUNT));   
END;
/