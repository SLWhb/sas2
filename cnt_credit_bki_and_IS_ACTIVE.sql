/* БКИ cnt_credit_bki


*/

SELECT COUNT(CASE WHEN is_active = 'Y' AND is_own = 'N' THEN 1 END) cnt_credit_bki
  FROM (SELECT r.is_active
             , r.is_own
             , r.request_react_id
             , MAX(r.request_react_id) OVER (PARTITION BY r.request_id) last_react
          FROM L_CREDIT_HISTORY_MATCHER r
         WHERE r.request_id = :request_id AND r.person_type = 1)
WHERE request_react_id = last_react;

PROC SQL;
	CONNECT TO oracle(USER=sas_monitoring 
					password="{SAS002}75F37A295B7FD553239C7ECB45649FB525058B5B"
					PATH='scoringst02');

	CREATE TABLE TRAKHACH._cnt_credit_bki AS 
	SELECT *
	FROM CONNECTION TO oracle(
			SELECT REQUEST_ID
				,COUNT(CASE WHEN is_active='Y' AND is_own='N' THEN 1 END) cnt_credit_bki
			FROM (SELECT r.REQUEST_ID
					,is_active
					,is_own
					,request_react_id
					,MAX(request_react_id) OVER (PARTITION BY request_id) last_react
				FROM L_CREDIT_HISTORY_MATCHER
				WHERE CREATED_DATE>sysdate - INTERVAL '12' month person_type = 1
					AND is_active = 'Y' AND is_own = 'N')
			WHERE request_react_id = last_react);


quit;

/*ДАЛЕЕ ОПРЕДЕЛЕНИЕ АКТИВНОСТИ КРЕДИТА
	Активность кредита определяется на этапе заполнения таблицы СREDIT_HISTORY#LOG
	•	Активность кредитов ВЭБ определяется по таблице kredits_account_modify, если запись в таблице есть значит N, иначе Y.
	•	Активность кредитов БКИ определяется по таблице D_CORRESPOND_ACTIVE_CREDIT (N - кредит закрыт, Y – кредит активен)

	В случае, если дедублицированных строк > 1, то L_CREDIT_HISTORY_MATCHER заполняется по следующему принципу:
	•	Принадлежность к кредиту ВЭБ is_own = Y
	•	наиболее актуальная дата обновления информации в БКИ modification_date 
	•	наиболее приоритетное БКИ
*/

CREATE OR REPLACE FUNCTION FN$GET_SIGN_ACTIVE_CREDIT(pBki_code VARCHAR2, pAccount_state NUMBER)
  RETURN VARCHAR2
IS
  -- Переменные
  cResult VARCHAR2(1);
BEGIN
  -- Определяем признак активности кредита
  IF pBki_code = 'veb' THEN
    IF var.get_char('_credit_jur_contract') IS NOT NULL THEN
      cResult := CASE var.evaluate_num('_account_modify_cnt') WHEN 0 THEN 'Y' ELSE 'N' END;
    END IF;
  ELSE
    BEGIN
      SELECT is_active
        INTO cResult
        FROM D_CORRESPOND_ACTIVE_CREDIT
       WHERE bki_code = pBki_code AND account_state = pAccount_state;
    -- Обрабатываем ошибки
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        cResult := '-';
    END;
  END IF;

  -- Возвращаем результат
  RETURN cResult;
END;

_credit_jur_contract = номер договора

var.evaluate_num('_account_modify_cnt') = SELECT COUNT(account) FROM kredits_account_modify@kredit WHERE account = '{_credit_jur_contract}'
