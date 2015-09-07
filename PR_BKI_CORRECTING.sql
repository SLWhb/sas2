create or replace PROCEDURE "PR$BKI_CORRECTING"(C_BKI IN T_OBJECT_BKI, C_BKI_MOD IN OUT T_OBJECT_BKI )
--Функция согласования даты зарытия и строки БКИ.
IS
  lenDT NUMBER(8);
  lenSTR NUMBER(8);
  lenDTsrc NUMBER(8);
BEGIN
  DBMS_OUTPUT.enable;
  
  C_BKI_MOD.ADD_831 := C_BKI.ADD_831;
  C_BKI_MOD.CRED_DATE := C_BKI.CRED_DATE;
  C_BKI_MOD.CRED_ENDDATE := C_BKI.CRED_ENDDATE;
  lenDTsrc := MONTHS_BETWEEN(C_BKI_MOD.CRED_DATE, C_BKI_MOD.CRED_ENDDATE)+1;
  
  IF (C_BKI_MOD.CRED_ENDDATE-C_BKI_MOD.CRED_DATE)>20000 OR (C_BKI_MOD.CRED_ENDDATE<C_BKI_MOD.CRED_DATE) or (C_BKI_MOD.CRED_ENDDATE IS NULL) THEN 
    C_BKI_MOD.CRED_ENDDATE := C_BKI_MOD.CRED_DATE; -- если дата аномальная, присваиваем дату начала
  END IF;
  
  IF SUBSTR(C_BKI_MOD.ADD_831,1,1)='C' THEN 
    C_BKI_MOD.ADD_831 := 'C'||LTRIM(C_BKI_MOD.ADD_831, 'C'); --первичное преобразование, отбрасываем закрытый период
    C_BKI_MOD.CRED_ENDDATE := ADD_MONTHS(C_BKI.CRED_DATE, LENGTH(C_BKI_MOD.ADD_831) ); 
  END IF;

  lenDT := MONTHS_BETWEEN(C_BKI_MOD.CRED_ENDDATE, C_BKI_MOD.CRED_DATE)+1; --добавляем +1 т.к. в строке первая буква это само открытие
  lenSTR := LENGTH(C_BKI_MOD.ADD_831);
  
  IF lenSTR>lenDT THEN
    C_BKI_MOD.CRED_ENDDATE := ADD_MONTHS(C_BKI_MOD.CRED_ENDDATE,(lenSTR-lenDT)); --уточняем фактический месяц закрытия кредита
    lenDT := MONTHS_BETWEEN(C_BKI_MOD.CRED_ENDDATE, C_BKI_MOD.CRED_DATE)+1;
  ELSE
    C_BKI_MOD.ADD_831 := LPAD(C_BKI_MOD.ADD_831, lenDT-1, 'x');
  END IF;
  
  --IF lenDTsrc^=lenDT THEN
    --DBMS_OUTPUT.put_line(TO_CHAR(C_BKI.CRED_DATE)||' '||TO_CHAR(C_BKI.CRED_ENDDATE)||' '||C_BKI.ADD_831||' '||TO_CHAR(lenDT));
  --END IF;
END;