create or replace PROCEDURE "PR$BKI_STRING_JOINING"(str_cur IN VARCHAR2, date_cur IN DATE
                                                  , str_next IN VARCHAR2, date_next IN DATE
                                                  ,str_join OUT VARCHAR2, date_begin_join OUT DATE)
IS
  BKI_CHAR_ALL CONSTANT VARCHAR(40) := 'xCR-0123456789BSWNTI'; --строка ранжирования
  
  str_cur_tmp VARCHAR2(600);
  str_next_tmp VARCHAR2(600);
  
  tmp_char1 VARCHAR2(5); tmp_char2 VARCHAR2(2); -- для хранения текущих схлопываемых символов
  
  f_str VARCHAR2(200); -- для сохранения самих символов
  f_str_cnt NUMBER(8); -- кол-во начальных символов
BEGIN
  DBMS_OUTPUT.enable;
  str_cur_tmp := NVL(str_cur, '0');
  str_next_tmp := NVL(str_next, '0');
  
  f_str_cnt := ROUND(MONTHS_BETWEEN(date_cur, date_next));
  
  IF f_str_cnt>0 THEN
    f_str := SUBSTR(str_next_tmp, -ABS(f_str_cnt), ABS(f_str_cnt));
    str_next_tmp := SUBSTR(str_next_tmp, 1, LENGTH(str_next_tmp)-ABS(f_str_cnt));
    date_begin_join := date_cur;
  ELSE
    f_str := SUBSTR(str_cur_tmp, -ABS(f_str_cnt),ABS(f_str_cnt));
    str_cur_tmp := SUBSTR(str_cur_tmp, 1, LENGTH(str_cur_tmp)-ABS(f_str_cnt));
    date_begin_join := date_next;
  END IF;  
  
  FOR i IN 1 .. GREATEST(LENGTH(str_cur_tmp), LENGTH(str_next_tmp))
  LOOP
    tmp_char1 := SUBSTR(str_cur_tmp, -i, 1);
    tmp_char2 := SUBSTR(str_next_tmp, -i, 1);
  
    IF NVL(INSTR(BKI_CHAR_ALL, tmp_char1),0)>=NVL(INSTR(BKI_CHAR_ALL, tmp_char2), 0) THEN
      str_join := CONCAT(tmp_char1 , str_join);
      --DBMS_OUTPUT.put_line((str_join)|| ' one IF');
    ELSE
      str_join := CONCAT(tmp_char2, str_join);
      --DBMS_OUTPUT.put_line((str_join)|| ' two IF');
    END IF;  
  END LOOP;
  str_join := CONCAT(str_join, f_str);
  --DBMS_OUTPUT.put_line((str_join));
END;