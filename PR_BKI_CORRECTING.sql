create or replace FUNCTION "FN$BKI_CORRECTING"(C_BKI IN T_TABLE_BKI) RETURN T_TABLE_BKI
--Функция согласования даты закрытия и строки БКИ.
IS
  lenDT NUMBER(8);
  lenSTR NUMBER(8);
  lenDTsrc NUMBER(8);
  C_BKI_MOD T_TABLE_BKI;
  
  --аннуитетные параметры
  LEN_AS NUMBER;
BEGIN
  DBMS_OUTPUT.enable;
  C_BKI_MOD := C_BKI;
  FOR i IN COALESCE(C_BKI.FIRST, 1) .. COALESCE(C_BKI.LAST, 0) LOOP
      C_BKI_MOD(i).ADD_831 := C_BKI(i).ADD_831;
      C_BKI_MOD(i).CRED_DATE := TRUNC(C_BKI(i).CRED_DATE, 'MM');
      C_BKI_MOD(i).CRED_ENDDATE := C_BKI(i).CRED_ENDDATE;
      lenDTsrc := MONTHS_BETWEEN(C_BKI_MOD(i).CRED_DATE, C_BKI_MOD(i).CRED_ENDDATE)+1;
      
      --Если нужно в новом массиве заменить неправильные даты окончания кредита на более адекватные, то расскоментировать
      --IF (C_BKI_MOD(i).CRED_ENDDATE-C_BKI_MOD(i).CRED_DATE)>20000 OR (C_BKI_MOD(i).CRED_ENDDATE<C_BKI_MOD(i).CRED_DATE) or (C_BKI_MOD(i).CRED_ENDDATE IS NULL) THEN 
        --C_BKI_MOD(i).CRED_ENDDATE := C_BKI_MOD(i).CRED_DATE; -- если дата аномальная, присваиваем дату начала
      --END IF;
      
      --переопределяем фактическую дату закрытия
      IF SUBSTR(C_BKI_MOD(i).ADD_831,1,1)='C' THEN 
        C_BKI_MOD(i).ADD_831 := 'C'||LTRIM(C_BKI_MOD(i).ADD_831, 'C'); --первичное преобразование, отбрасываем закрытый период
        --C_BKI_MOD(i).CRED_ENDDATE := ADD_MONTHS(C_BKI_MOD(i).CRED_DATE, LENGTH(C_BKI_MOD(i).ADD_831) ); 
      END IF;
    
      lenDT := MONTHS_BETWEEN(C_BKI_MOD(i).CRED_ENDDATE, C_BKI_MOD(i).CRED_DATE)+1; --добавляем +1 т.к. в строке первая буква это само открытие
      lenSTR := LENGTH(C_BKI_MOD(i).ADD_831);
      
      IF lenSTR>lenDT THEN
        C_BKI_MOD(i).CRED_ENDDATE := ADD_MONTHS(C_BKI_MOD(i).CRED_ENDDATE,(lenSTR-lenDT)); --уточняем фактический месяц закрытия кредита
        lenDT := MONTHS_BETWEEN(C_BKI_MOD(i).CRED_ENDDATE, C_BKI_MOD(i).CRED_DATE)+1;
      /*ELSE
        C_BKI_MOD(i).ADD_831 := LPAD(C_BKI_MOD(i).ADD_831, lenDT-1, 'x');*/ --добавляем доп. знаки до максимальной даты
      END IF;
      
  C_BKI_MOD(i).ANN_STR := ' ';
  C_BKI_MOD(i).ANN := "FN$BKI_CALC_ANNUITY"(C_BKI_MOD(i));
  LEN_AS := LENGTH(TO_CHAR( C_BKI_MOD(i).ANN ));
  C_BKI_MOD(i).ANN_STR := LPAD(C_BKI_MOD(i).ANN_STR, (LEN_AS+1)*lenDT+1, '|'||TO_CHAR(C_BKI_MOD(i).ANN ));
  DBMS_OUTPUT.PUT_LINE(TO_CHAR(lenDT)||' - '||TO_CHAR(LEN_AS)||' - '||C_BKI_MOD(i).ANN_STR);
  
  END LOOP;
  RETURN (C_BKI_MOD);
END;