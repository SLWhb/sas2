create or replace FUNCTION "FN$BKI_STRING_JOINING"(TBL_BKI_read IN T_TABLE_BKI) RETURN T_OBJECT_BKI
IS
  BKI_CHAR_ALL CONSTANT VARCHAR(40) := 'xR-C0123456789BSWNTI'; --строка ранжирования символов
  
  str_cur_tmp VARCHAR2(4000);
  str_next_tmp VARCHAR2(4000);
    
  tmp_char1 VARCHAR2(5); tmp_char2 VARCHAR2(2); -- для хранения текущих схлопываемых символов
  f_str VARCHAR2(200); -- для сохранения начальных символов
  f_str_cnt NUMBER(8); -- кол-во начальных символов
  
  TBL_BKI_rez T_OBJECT_BKI;
  str_cur VARCHAR2(4000);
  ann_cur VARCHAR2(8000) := ' ';
  --date_cur DATE;
BEGIN
  DBMS_OUTPUT.enable;
  TBL_BKI_rez := TBL_BKI_read(1);
  
  -- цикл схлопывания строки КИ.
  FOR i IN COALESCE(TBL_BKI_read.FIRST+1, 1) .. COALESCE(TBL_BKI_read.LAST, 0) LOOP
    IF TBL_BKI_read.COUNT=1 THEN
      str_cur := TBL_BKI_read(1).ADD_831;
      EXIT;
    END IF;
        str_cur := '';
        ann_cur := ' ';
        str_cur_tmp := NVL(TBL_BKI_rez.ADD_831, '0');
        str_next_tmp := NVL(TBL_BKI_read(i).ADD_831, '0');
        --date_cur := NVL();
        
        f_str_cnt := ROUND(MONTHS_BETWEEN(TBL_BKI_rez.CRED_DATE, TBL_BKI_read(i).CRED_DATE));
        --DBMS_OUTPUT.put_line(str_next_tmp||'======2s');
         
        -- согласуем начала строк
        IF f_str_cnt>0 THEN -- date_next - минимальна. Обрезаем излишех у i+1 строки, и заменяем дату
          f_str := SUBSTR(str_next_tmp, -ABS(f_str_cnt), ABS(f_str_cnt));
          str_next_tmp := SUBSTR(str_next_tmp, 1, LENGTH(str_next_tmp)-ABS(f_str_cnt));
          TBL_BKI_rez.CRED_DATE := TBL_BKI_read(i).CRED_DATE;
          
          --ann_cur := TO_CHAR(TO_NUMBER((regexp_substr(TBL_BKI_read(i).ANN_STR,'([0-9]+)', 1, LENGTH(TBL_BKI_read(i).ADD_831)+f_str_cnt+1-i)))+i)||'|'||ann_cur;
        ELSE -- date_cur - минимальна. Обрезаем излишех у i строки
          f_str := SUBSTR(str_cur_tmp, -ABS(f_str_cnt),ABS(f_str_cnt));
          str_cur_tmp := SUBSTR(str_cur_tmp, 1, LENGTH(str_cur_tmp)-ABS(f_str_cnt));
          
          --ann_cur := SUBSTR(TBL_BKI_rez.ANN_STR, LENGTH(TBL_BKI_rez.ANN_STR)-LENGTH(TBL_BKI_read(i).ANN_STR), 400);
        END IF; 
        
        -- Цикл определения приоритетного символа в строке.
        FOR j IN 1 .. GREATEST(LENGTH(str_cur_tmp), LENGTH(str_next_tmp))
        LOOP
          tmp_char1 := SUBSTR(str_cur_tmp, -j, 1);
          tmp_char2 := SUBSTR(str_next_tmp, -j, 1);
        
          IF NVL(INSTR(BKI_CHAR_ALL, tmp_char1),0)>=NVL(INSTR(BKI_CHAR_ALL, tmp_char2), 0) THEN
            str_cur := CONCAT(tmp_char1 , str_cur);
          ELSE
            str_cur := CONCAT(tmp_char2, SUBSTR(str_cur, 1, j));
          END IF;  
        END LOOP;
        -- Результирующая строка
        TBL_BKI_rez.ADD_831 := CONCAT(str_cur, f_str);
        

        
        --DBMS_OUTPUT.put_line(TO_CHAR(i)||'-i-'||TO_CHAR(TBL_BKI_read(i).CRED_DATE)||' = '||' =join='||TBL_BKI_rez.ADD_831||' < '||TBL_BKI_read(i).ADD_831);
        
          --конкатенация аннуитетов
          --FOR j IN 1 .. f_str_cnt 
          --LOOP
          IF f_str_cnt>0 THEN
            --ann_cur := LPAD('|', (LENGTH(TO_CHAR(TBL_BKI_read(i).ANN))+1)*f_str_cnt+1, '|');
            ann_cur := TBL_BKI_read(i).ANN_STR;
             DBMS_OUTPUT.put_line(TO_CHAR( f_str_cnt)||' ann- '||ann_cur);
          ELSE
            --ann_cur := LPAD('|', (LENGTH(TO_CHAR(TBL_BKI_rez.ANN))+1)*f_str_cnt+1, '|'||TO_CHAR(TBL_BKI_rez.ANN));
            ann_cur := TBL_BKI_rez.ANN_STR;
             DBMS_OUTPUT.put_line(TO_CHAR( f_str_cnt)||' ann- '||ann_cur);
          END IF;
          
          TBL_BKI_rez.ANN_STR := ann_cur;
          --END LOOP;
         
  END LOOP;
  
  DBMS_OUTPUT.put_line((TBL_BKI_rez.ADD_831));
  RETURN (TBL_BKI_rez);
END;