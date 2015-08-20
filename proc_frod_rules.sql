

CREATE OR REPLACE PROCEDURE FROD_RULES_RUN_ALL(v_request_id IN NUMBER) as
-- ========================================================================
-- ==	ПРОЦЕДУРА		"ЗАПУСК ПРОВЕРКИ ПО ФРОД ПРАВИЛАМ"
-- ==	ОПИСАНИЕ:		В каждом вызове процедуры происходит вставка в целевую таблицу SFF.FROD_RULES
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN
	SFF.FROD_RULES_VERIFY_F01(v_request_id);
	SFF.FROD_RULES_VERIFY_H01(v_request_id);
  SFF.FROD_RULES_VERIFY_H02(v_request_id);
  SFF.FROD_RULES_VERIFY_H03(v_request_id);
  SFF.FROD_RULES_VERIFY_H04(v_request_id);
  SFF.FROD_RULES_VERIFY_H05(v_request_id);
  SFF.FROD_RULES_VERIFY_H06(v_request_id);
  SFF.FROD_RULES_VERIFY_H07(v_request_id);
  SFF.FROD_RULES_VERIFY_H08(v_request_id);
  SFF.FROD_RULES_VERIFY_H09(v_request_id);
	SFF.FROD_RULES_VERIFY_P01(v_request_id);
  SFF.FROD_RULES_VERIFY_P02(v_request_id);
END;
/

/*EXECUTE SFF.FROD_RULES_RUN_ALL(46356733);
EXECUTE SFF.FROD_RULES_VERIFY_F01(46380378);
EXECUTE SFF.FROD_RULES_VERIFY_P01(46549350);
EXECUTE SFF.FROD_RULES_VERIFY_P02(46935619); 
EXECUTE SFF.FROD_RULES_VERIFY_H01(46663956); 
EXECUTE SFF.FROD_RULES_VERIFY_H01(46822226); */

/*SELECT COUNT(*) FROM APPLICATIONS WHERE REQUEST_ID>=51562394;
select * from APPLICATIONS_FROD WHERE REQUEST_ID=46357025;
select * from APPLICATIONS_FROD WHERE FIRST_LFM='АДТ' AND LEN_LFM='26-30' ;
select count(REQUEST_ID) from APPLICATIONS;*/ /*40629204 -676471*/

CREATE OR REPLACE PROCEDURE FROD_RULES_VERIFY_F01(v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 01. Ошибки_ФИОДР 1"
-- ==	ОПИСАНИЕ:		отличаются на 1 букву фамилии, 1 цифра в дате рождения
-- ========================================================================
-- ==	СОЗДАНИЕ:		19.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN
	INSERT INTO SFF.FROD_RULE (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
						FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
						TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
						REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
						FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
						INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
		SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,C_R.STATUS_ID
				,C_R.SCORE_TREE_ROUTE_ID
				,C_R.CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Ошибки_ФИОДР' as TYPE_REL
				,'Высокая доля вероятности ошибки в ФИО и дате рождения по сравнению с ранее указанными' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID AS PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'-' as INFO_EQ
				,'Фио+Др.:'||SRC.FIO||' '||SRC.DR as INFO_NEQ
				,'Фио+Др.:'||APP.FIO||' '||APP.DR as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
			ON SRC.FIRST_LFM = APP.FIRST_LFM /*AND SRC.FIRST_F = APP.FIRST_F AND SRC.FIRST_M = APP.FIRST_M */
				AND SRC.LEN_LFM = APP.LEN_LFM 
				AND SRC.REQ_DATE > APP.REQ_DATE 
				AND SRC.RA_REGION = APP.RA_REGION 
				AND SRC.PERSON_ID ^= APP.PERSON_ID 
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id /*AND C_R.REQUEST_ID=46376896*/
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.FIO IS NULL 
				/*RULE: определяем условие фрод-правила*/
				AND UTL_MATCH.EDIT_DISTANCE(SRC.FIO, APP.FIO)+UTL_MATCH.EDIT_DISTANCE(SRC.DR, APP.DR)=1 
				/*AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-360*2 AND SRC.REQ_DATE*/
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2);
	COMMIT;
END;
/

CREATE OR REPLACE PROCEDURE FROD_RULES_VERIFY_P01(v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 02. Фальсификация паспорта 1"
-- ==	ОПИСАНИЕ:		Один и тот же номер паспорта у клиентов с различными ФИО (возможно не полное различие)
-- ==					, период сравнения 120 дней. 
-- ==					Необходимо исключить случаи различия из-за смены ФИО при замужестве, опечатках 
-- ==					(SER+NUM = , ФИО^= за 120 дней)
-- ========================================================================
-- ==	СОЗДАНИЕ:		19.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.08.2015 (ТРАХАЧЕВ В.В.)
-- ======================================================================== 
BEGIN
	INSERT INTO SFF.FROD_RULE (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
						FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
						TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
						REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
						FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
						INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
		SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,C_R.STATUS_ID
				,C_R.SCORE_TREE_ROUTE_ID
				,C_R.CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Фальсификация_паспорта_1' as TYPE_REL
				,'Паспорт клиента совпадает с другим паспортом, ФИО различается, период сравнения 120 дней. не родственники' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Паспорт:'||SRC.PASSPORT as INFO_EQ
				,'Фио+Др.:'||SRC.FIO||' '||SRC.DR as INFO_NEQ
				,'Фио+Др.:'||APP.FIO||' '||APP.DR as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
			ON /*RULE: определяем фрод-правило поиска */
				SRC.PASSPORT = APP.PASSPORT AND SRC.FIO ^= APP.FIO 
					AND SRC.PERSON_ID^=APP.PERSON_ID 
					AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-121 AND SRC.REQ_DATE
					AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.PASSPORT IS NULL 
				AND NOT SRC.FIO IS NULL AND NOT APP.FIO IS NULL
				/*AND UTL_MATCH.EDIT_DISTANCE(SRC.FIO, APP.FIO)+UTL_MATCH.EDIT_DISTANCE(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/;
	COMMIT;
END;
/

CREATE OR REPLACE PROCEDURE FROD_RULES_VERIFY_P02(v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 03. Фальсификация паспорта 2"
-- ==	ОПИСАНИЕ:		совпадает паспорт, не совпадает регион проживания
-- ==					, период для сравнения – 90 дней. ФИО в правиле не участвует
-- ==					(SER+NUM = , РЕГИОН ПРОЖ^= за 90 дней)
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN
	INSERT INTO SFF.FROD_RULE (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
						FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
						TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
						REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
						FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
						INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
		SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,C_R.STATUS_ID
				,C_R.SCORE_TREE_ROUTE_ID
				,C_R.CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Фальсификация_паспорта_2' as TYPE_REL
				,'Паспорт клиента совпадает с другим паспортом, регион не совпадает за последние 90 дней.' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
                  					OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID as PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Паспорт:'||SRC.PASSPORT as INFO_EQ
				,'Рег.прж'||SRC.LA_REGION as INFO_NEQ
				,'Рег.прж'||APP.LA_REGION as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
				ON /*RULE: определяем фрод-правило поиска */
					SRC.PASSPORT=APP.PASSPORT 
						AND SRC.LA_REGION^=APP.LA_REGION 
					AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.PASSPORT IS NULL 
				AND NOT SRC.LA_REGION='-' AND NOT APP.LA_REGION='-'
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/;
	COMMIT;
END;
/


CREATE OR REPLACE PROCEDURE FROD_RULES_VERIFY_H01(v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 04. Яма 1"
-- ==	ОПИСАНИЕ:		Регистрационная яма_1 – совпадает адрес места проживания
-- ==					, не совпадает ФИО, период для сравнения 90 дней. Не родственники
-- ==					(АДРЕС ПРОЖ =, ФИО^= 90 дней)
-- ==					Правка: увеличен период сравнения до 180 дней
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN
	INSERT INTO SFF.FROD_RULE (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
						FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
						TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
						REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
						FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
						INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
		SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,C_R.STATUS_ID
				,C_R.SCORE_TREE_ROUTE_ID
				,C_R.CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Яма_1' as TYPE_REL
				,'У клиента совпадает адрес проживания с другим, не совпадает ФИО  за последние 90 дней, не родственники' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
                  					OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID as PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Адр.прж'||SRC.LA_REGION||', '||SRC.LA_DISTRICT||', '||SRC.LA_CITY||', '||SRC.LA_STREET
						||', '||SRC.LA_HOUSE||', '||SRC.LA_BUILDING||', '||SRC.LA_APARTMENT as INFO_EQ
				,'Фио+Др.:'||SRC.FIO||' '||SRC.DR as INFO_NEQ
				,'Фио+Др.:'||APP.FIO||' '||APP.DR as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
				ON /*RULE: определяем фрод-правило поиска */
					SRC.LA_REGION=APP.LA_REGION 
						AND SRC.LA_DISTRICT=APP.LA_DISTRICT
						AND SRC.LA_CITY=APP.LA_CITY
						AND SRC.LA_STREET=APP.LA_STREET
						AND SRC.LA_HOUSE=APP.LA_HOUSE
						AND SRC.LA_BUILDING=APP.LA_BUILDING
						AND SRC.LA_APARTMENT=APP.LA_APARTMENT
						AND SRC.FIO ^= APP.FIO /*AND SRC.DR ^= APP.DR*/
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91*2 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.LA_REGION='-' AND NOT SRC.LA_REGION IS NULL
				AND NOT SRC.LA_APARTMENT='-' AND NOT SRC.LA_APARTMENT IS NULL
				AND NOT SRC.FIO IS NULL AND NOT APP.FIO IS NULL
				AND NOT SRC.DR IS NULL AND NOT APP.DR IS NULL
				AND NOT SRC.PERSON_ID=APP.PERSON_ID
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2);
	COMMIT;
END;
/

CREATE OR REPLACE PROCEDURE FROD_RULES_VERIFY_H02(v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 05. Яма 2"
-- ==	ОПИСАНИЕ:		совпадает адрес места проживания
-- ==					, не совпадает паспорт, период для сравнения 90 дней. Не родственники
-- ==					(АДРЕС ПРОЖ =, SER+NUM ^= за 90 дней)
-- ==					Правка: увеличен период сравнения до 180 дней
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN
	INSERT INTO SFF.FROD_RULE (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
						FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
						TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
						REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
						FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
						INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
		SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,C_R.STATUS_ID
				,C_R.SCORE_TREE_ROUTE_ID
				,C_R.CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Яма_2' as TYPE_REL
				,'У клиента совпадает адрес проживания с другим, не совпадает паспорт  за последние 90 дней., не родственники' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
                  					OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Адр.прж:'||SRC.LA_REGION||', '||SRC.LA_DISTRICT||', '||SRC.LA_CITY||', '||SRC.LA_STREET
						||', '||SRC.LA_HOUSE||', '||SRC.LA_BUILDING||', '||SRC.LA_APARTMENT as INFO_EQ
				,'Паспорт:'||SRC.PASSPORT as INFO_NEQ
				,'Паспорт:'||APP.PASSPORT as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
				ON /*RULE: определяем фрод-правило поиска */
					SRC.LA_REGION=APP.LA_REGION 
						AND SRC.LA_DISTRICT=APP.LA_DISTRICT
						AND SRC.LA_CITY=APP.LA_CITY
						AND SRC.LA_STREET=APP.LA_STREET
						AND SRC.LA_HOUSE=APP.LA_HOUSE
						AND SRC.LA_BUILDING=APP.LA_BUILDING
						AND SRC.LA_APARTMENT=APP.LA_APARTMENT
						AND SRC.PASSPORT ^= APP.PASSPORT /*AND SRC.DR ^= APP.DR*/
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91*2 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.LA_REGION='-' AND NOT SRC.LA_REGION IS NULL
				AND NOT SRC.LA_APARTMENT='-' AND NOT SRC.LA_APARTMENT IS NULL
				AND NOT SRC.PASSPORT IS NULL AND NOT APP.PASSPORT IS NULL
				AND NOT SRC.PERSON_ID=APP.PERSON_ID
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2);
	COMMIT;
END;
/


CREATE OR REPLACE PROCEDURE FROD_RULES_VERIFY_H03(v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 06. Яма 3"
-- ==	ОПИСАНИЕ:		совпадает паспорт, не совпадает адрес места проживания
-- ==					, период для сравнения 90 дней. 
-- ==					(SER+NUM =, АДРЕС ПРОЖ ^= за 90 дней)
-- ==					Правка: увеличен период сравнения до 180 дней
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN
	INSERT INTO SFF.FROD_RULE (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
						FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
						TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
						REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
						FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
						INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
		SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,C_R.STATUS_ID
				,C_R.SCORE_TREE_ROUTE_ID
				,C_R.CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Яма_3' as TYPE_REL
				,'У клиента совпадает паспорт с другим, не совпадает адрес проживания за последние 90 дней' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Паспорт:'||SRC.PASSPORT as INFO_EQ
				,'Адр.прж:'||SRC.LA_REGION||', '||SRC.LA_DISTRICT||', '||SRC.LA_CITY||', '||SRC.LA_STREET
						||', '||SRC.LA_HOUSE||', '||SRC.LA_BUILDING||', '||SRC.LA_APARTMENT as INFO_NEQ
				,'Адр.прж:'||APP.LA_REGION||', '||APP.LA_DISTRICT||', '||APP.LA_CITY||', '||APP.LA_STREET
						||', '||APP.LA_HOUSE||', '||APP.LA_BUILDING||', '||APP.LA_APARTMENT as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
				ON /*RULE: определяем фрод-правило поиска */
					SRC.PASSPORT = APP.PASSPORT 
						AND SRC.LA_REGION^=APP.LA_REGION
						AND SRC.LA_DISTRICT^=APP.LA_DISTRICT
						AND SRC.LA_CITY^=APP.LA_CITY
						AND SRC.LA_STREET^=APP.LA_STREET
						AND SRC.LA_HOUSE^=APP.LA_HOUSE
						AND SRC.LA_BUILDING^=APP.LA_BUILDING
						AND SRC.LA_APARTMENT^=APP.LA_APARTMENT
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91*2 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.PASSPORT IS NULL
				AND NOT SRC.LA_REGION='-' AND NOT APP.LA_REGION='-' AND NOT SRC.LA_REGION IS NULL AND NOT APP.LA_REGION IS NULL 
				AND NOT SRC.LA_APARTMENT='-' AND NOT APP.LA_APARTMENT='-' AND NOT SRC.LA_APARTMENT IS NULL AND NOT APP.LA_APARTMENT IS NULL
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/;
	COMMIT;
END;
/


CREATE OR REPLACE PROCEDURE FROD_RULES_VERIFY_H04(v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 07. Яма 4"
-- ==	ОПИСАНИЕ:		совпадает адрес места проживания
-- ==					, не совпадает номер мобильного телефона, период для сравнения 90 дней, Не родственники
-- ==					(АДРЕС ПРОЖ =, МОБ ТЕЛ ^= за 90 дней, нет связи в FAMILY_REL ) 
-- ==					Правка: увеличен период сравнения до 180 дней
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN
	INSERT INTO SFF.FROD_RULE (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
						FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
						TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
						REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
						FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
						INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
		SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,C_R.STATUS_ID
				,C_R.SCORE_TREE_ROUTE_ID
				,C_R.CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Яма_4' as TYPE_REL
				,'У клиента совпадает адрес проживания с другим, не совпадает сотовый  за последние 90 дней., не родственники' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Адр.прж:'||SRC.LA_REGION||', '||SRC.LA_DISTRICT||', '||SRC.LA_CITY||', '||SRC.LA_STREET
						||', '||SRC.LA_HOUSE||', '||SRC.LA_BUILDING||', '||SRC.LA_APARTMENT as INFO_EQ
				,'Тел.моб:'||TO_CHAR(SRC.MOBILE) as INFO_NEQ
				,'Тел.моб:'||TO_CHAR(APP.MOBILE) as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
				ON /*RULE: определяем фрод-правило поиска */
					SRC.LA_REGION=APP.LA_REGION
						AND SRC.LA_DISTRICT=APP.LA_DISTRICT
						AND SRC.LA_CITY=APP.LA_CITY
						AND SRC.LA_STREET=APP.LA_STREET
						AND SRC.LA_HOUSE=APP.LA_HOUSE
						AND SRC.LA_BUILDING=APP.LA_BUILDING
						AND SRC.LA_APARTMENT=APP.LA_APARTMENT
						AND SRC.MOBILE ^= APP.MOBILE
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91*2 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.LA_REGION='-'AND NOT SRC.LA_REGION IS NULL AND NOT SRC.LA_APARTMENT='-' AND NOT SRC.LA_APARTMENT IS NULL
				AND NOT SRC.MOBILE IS NULL AND NOT APP.MOBILE IS NULL
				AND NOT SRC.PERSON_ID=APP.PERSON_ID
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2);
	COMMIT;
END;
/


CREATE OR REPLACE PROCEDURE FROD_RULES_VERIFY_H05(v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 08. Яма 5"
-- ==	ОПИСАНИЕ:		совпадает мобильный телефон
-- ==					, но не совпадает адрес проживания, период для сравнения 90 дней. 
-- ==					(МОБ ТЕЛ =, АДР ПРОЖ ^= за 90 дней) 
-- ==					Правка: увеличен период сравнения до 180 дней
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN
	INSERT INTO SFF.FROD_RULE (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
						FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
						TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
						REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
						FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
						INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
		SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,C_R.STATUS_ID
				,C_R.SCORE_TREE_ROUTE_ID
				,C_R.CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Яма_5' as TYPE_REL
				,'У клиента совпадает сотовый с другим, не совпадает адрес проживания за последние 90 дней.' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Тел.моб:'||TO_CHAR(SRC.MOBILE) as INFO_EQ
				,'Адр.прж:'||SRC.LA_REGION||', '||SRC.LA_DISTRICT||', '||SRC.LA_CITY||', '||SRC.LA_STREET
						||', '||SRC.LA_HOUSE||', '||SRC.LA_BUILDING||', '||SRC.LA_APARTMENT as INFO_NEQ
				,'Адр.прж:'||APP.LA_REGION||', '||APP.LA_DISTRICT||', '||APP.LA_CITY||', '||APP.LA_STREET
						||', '||APP.LA_HOUSE||', '||APP.LA_BUILDING||', '||APP.LA_APARTMENT as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
				ON /*RULE: определяем фрод-правило поиска */
					SRC.MOBILE = APP.MOBILE
						AND SRC.LA_REGION^=APP.LA_REGION 
						AND SRC.LA_DISTRICT^=APP.LA_DISTRICT
						AND SRC.LA_CITY^=APP.LA_CITY
						AND SRC.LA_STREET^=APP.LA_STREET
						AND SRC.LA_HOUSE^=APP.LA_HOUSE
						AND SRC.LA_BUILDING^=APP.LA_BUILDING
						AND SRC.LA_APARTMENT^=APP.LA_APARTMENT
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91*2 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.MOBILE IS NULL
				AND NOT SRC.LA_REGION='-'AND NOT SRC.LA_REGION IS NULL AND NOT SRC.LA_APARTMENT='-' AND NOT SRC.LA_APARTMENT IS NULL
				AND NOT APP.LA_REGION='-'AND NOT APP.LA_REGION IS NULL AND NOT APP.LA_APARTMENT='-' AND NOT APP.LA_APARTMENT IS NULL
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/;
	COMMIT;
END;
/


CREATE OR REPLACE PROCEDURE FROD_RULES_VERIFY_H06(v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 09. Яма 6"
-- ==	ОПИСАНИЕ:		совпадает номер мобильного телефона
-- ==					, не совпадает домашний телефон (фактическое место проживания). Одинаковый OBJECTS_ID
-- ==					, период для сравнения 90 дней. (МОБ ТЕЛ =, ДОМ ТЕЛ ^= за 90 дней) 
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN
	INSERT INTO SFF.FROD_RULE (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
						FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
						TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
						REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
						FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
						INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
		SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,C_R.STATUS_ID
				,C_R.SCORE_TREE_ROUTE_ID
				,C_R.CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Яма_6' as TYPE_REL
				,'У клиента не совпадает домашний телефон с ранее указанным, совпадает сотовый за последние 90 дней' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Тел.моб:'||TO_CHAR(SRC.MOBILE) as INFO_EQ
				,'Тел.дом:'||TO_CHAR(SRC.HOME_PHONE) as INFO_NEQ
				,'Тел.дом:'||TO_CHAR(APP.HOME_PHONE) as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
				ON /*RULE: определяем фрод-правило поиска */
					SRC.MOBILE = APP.MOBILE
						AND SRC.HOME_PHONE ^= APP.HOME_PHONE
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.MOBILE IS NULL
				AND NOT SRC.HOME_PHONE IS NULL AND NOT APP.HOME_PHONE IS NULL
				AND SRC.PERSON_ID=APP.PERSON_ID
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/;
	COMMIT;
END;
/


CREATE OR REPLACE PROCEDURE FROD_RULES_VERIFY_H07(v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 10. Яма 7"
-- ==	ОПИСАНИЕ:		совпадает домашний телефон, не совпадает личный телефон
-- ==					, период для сравнения 90 дней
-- ==					, клиенты не связаны родственной связью. 
-- ==					(ДОМ ТЕЛ =, МОБ ТЕЛ ^= за 90 дней, нет связи в FAMILY_REL) 
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN
	INSERT INTO SFF.FROD_RULE (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
						FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
						TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
						REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
						FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
						INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
		SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,C_R.STATUS_ID
				,C_R.SCORE_TREE_ROUTE_ID
				,C_R.CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Яма_7' as TYPE_REL
				,'У клиента совпадает домашний телефон с другим, не совпадает сотовый, не родственники за последние 90 дней' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Тел.дом:'||TO_CHAR(SRC.HOME_PHONE) as INFO_EQ
				,'Тел.моб:'||TO_CHAR(SRC.MOBILE) as INFO_NEQ
				,'Тел.моб:'||TO_CHAR(APP.MOBILE) as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
				ON /*RULE: определяем фрод-правило поиска */
					SRC.HOME_PHONE = APP.HOME_PHONE
						AND SRC.MOBILE ^= APP.MOBILE
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.HOME_PHONE IS NULL
				AND NOT SRC.MOBILE IS NULL AND NOT APP.MOBILE IS NULL
				AND NOT SRC.PERSON_ID=APP.PERSON_ID
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2);
	COMMIT;
END;
/


CREATE OR REPLACE PROCEDURE FROD_RULES_VERIFY_H08(v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 11. Яма 8"
-- ==	ОПИСАНИЕ:		совпадает место проживания
-- ==					, не совпадает телефон по месту проживания, период для сравнения 90 дней. 
-- ==					, Не родственники. Разные OBJECTS_ID
-- ==					(АДР ПРОЖ =, ДОМ ТЕЛ ^= за 90 дней)
-- ==					Правка: увеличен период сравнения до 180 дней
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN
	INSERT INTO SFF.FROD_RULE (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
						FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
						TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
						REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
						FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
						INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
		SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,C_R.STATUS_ID
				,C_R.SCORE_TREE_ROUTE_ID
				,C_R.CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Яма_8' as TYPE_REL
				,'У клиента совпадает место проживания с другим, не совпадает домашний телефон, не родственники за последние 90 дней.' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Адр.прж:'||SRC.LA_REGION||', '||SRC.LA_DISTRICT||', '||SRC.LA_CITY||', '||SRC.LA_STREET
						||', '||SRC.LA_HOUSE||', '||SRC.LA_BUILDING||', '||SRC.LA_APARTMENT as INFO_EQ
				,'Тел.дом:'||TO_CHAR(SRC.HOME_PHONE) as INFO_NEQ
				,'Тел.дом:'||TO_CHAR(APP.HOME_PHONE) as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
				ON /*RULE: определяем фрод-правило поиска */
					SRC.LA_REGION=APP.LA_REGION
						AND SRC.LA_DISTRICT=APP.LA_DISTRICT
						AND SRC.LA_CITY=APP.LA_CITY
						AND SRC.LA_STREET=APP.LA_STREET
						AND SRC.LA_HOUSE=APP.LA_HOUSE
						AND SRC.LA_BUILDING=APP.LA_BUILDING
						AND SRC.LA_APARTMENT=APP.LA_APARTMENT
						AND SRC.HOME_PHONE ^= APP.HOME_PHONE
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91*2 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.LA_REGION='-' AND NOT SRC.LA_REGION IS NULL AND NOT SRC.LA_APARTMENT='-' AND NOT SRC.LA_APARTMENT IS NULL
				AND NOT SRC.HOME_PHONE IS NULL AND NOT APP.HOME_PHONE IS NULL
				AND NOT SRC.PERSON_ID=APP.PERSON_ID
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2);
	COMMIT;
END;
/

CREATE OR REPLACE PROCEDURE FROD_RULES_VERIFY_H09(v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 12. Яма 9"
-- ==	ОПИСАНИЕ:		совпадает паспорт, не совпадает адрес регистрации
-- ==					, период для сравнения 90 дней. 
-- ==					(SER+NUM =, АДР РЕГ^= за 90 дней)
-- ==					Правка: увеличен период сравнения до 180 дней
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN
	INSERT INTO SFF.FROD_RULE (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
						FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
						TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
						REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
						FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
						INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
		SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,C_R.STATUS_ID
				,C_R.SCORE_TREE_ROUTE_ID
				,C_R.CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Яма_9' as TYPE_REL
				,'У клиента совпадает паспорт с ранее указанным, не совпадает адрес регистрации за последние 90 дней' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Паспорт:'||SRC.PASSPORT as INFO_EQ
				,'Адр.рег:'||SRC.RA_REGION||', '||SRC.RA_DISTRICT||', '||SRC.RA_CITY||', '||SRC.RA_STREET
						||', '||SRC.RA_HOUSE||', '||SRC.RA_BUILDING||', '||SRC.RA_APARTMENT as INFO_NEQ
				,'Адр.рег:'||APP.RA_REGION||', '||APP.RA_DISTRICT||', '||APP.RA_CITY||', '||APP.RA_STREET
						||', '||APP.RA_HOUSE||', '||APP.RA_BUILDING||', '||APP.RA_APARTMENT as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
				ON /*RULE: определяем фрод-правило поиска */
					SRC.PASSPORT=APP.PASSPORT
						AND SRC.RA_REGION^=APP.RA_REGION 
						AND SRC.RA_DISTRICT^=APP.RA_DISTRICT
						AND SRC.RA_CITY^=APP.RA_CITY
						AND SRC.RA_STREET^=APP.RA_STREET
						AND SRC.RA_HOUSE^=APP.RA_HOUSE
						AND SRC.RA_BUILDING^=APP.RA_BUILDING
						AND SRC.RA_APARTMENT^=APP.RA_APARTMENT
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91*2 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.PASSPORT IS NULL
				AND NOT SRC.RA_REGION='-' AND NOT SRC.RA_REGION IS NULL AND NOT SRC.RA_APARTMENT='-' AND NOT SRC.RA_APARTMENT IS NULL
				AND NOT APP.RA_REGION='-' AND NOT APP.RA_REGION IS NULL AND NOT APP.RA_APARTMENT='-' AND NOT APP.RA_APARTMENT IS NULL
				AND SRC.PERSON_ID=APP.PERSON_ID
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/;
	COMMIT;
END;
/



