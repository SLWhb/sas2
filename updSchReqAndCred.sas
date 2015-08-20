OPTIONS SYMBOLGEN VALIDVARNAME=ANY REPLACE;
LIBNAME OUTLIB "/SAS_CS/URD/Trakhachev_V/Test_Table" COMPRESS=YES;

LIBNAME PEREGUDO  "/SAS_CS/OAO/Peregudov_O" COMPRESS=YES;

libname COM_DIC '/SAS_LIB/COM_DIC' COMPRESS=YES;

LIBNAME URD '/SAS_CS/URD/Common' COMPRESS=YES;

%LET CURTMPLIB = TRAKHACH; /* TRAKHACH вместо WORK*/
%LET WRITELIB = PEREGUDO;  /* OUTLIB вместо PEREGUDO*/

*%LET firstdate=%sysfunc(datetime(),dt19.);
%PUT &sysdate9.;

/*==============МАКРООБЪЯВЛЕНИЯ=============*/
*объявление макропеременных;
DATA _NULL_;
	FORMAT  t_curDate $50.;
	FORMAT  t_prevDate $50.;
	t_curDate=PUT(DATETIME() ,DATETIME19.);
	t_curDate=SUBSTR(t_curDate,1,10)||":00:00:00";

	t_prevDate=PUT(DATETIME()-86400*200 ,DATETIME19.);
	t_prevDate=SUBSTR(t_prevDate, 1, 10)||":00:00:00";

	PUT t_prevDate;
	PUT t_curDate;
	CALL SYMPUT("m_t_curDate", strip(t_curDate));
	CALL SYMPUT("m_t_prevDate", strip(t_prevDate));
RUN;
%PUT &m_t_curDate;
%PUT &m_t_prevDate;

/*ВВЕДИТЕ ДАТЫ В ЧЕТЫРЕХ ЗАПРОСАХ*/
*========================ТАБЛИЦА 1===========================;
PROC SQL;
	CREATE TABLE &CURTMPLIB..SOC (COMPRESS=YES) AS 
	SELECT t1.REQUEST_ID, 
			t1.REQ_CREATE_DATE_ID, 
			t1.REQ_CREATED_DATE, 
			t1.REQ_SCHEMS_ID, 
			t1.CURR_SCHEMS_ID, 
			t1.CUR_RATE_REQ_CREATE, 
			t1.LAST_REQ_STATUS_ID, 
			t1.REQ_CREATE_GROUP_ID, 
			t1.LAST_KE_GROUP_ID, 
			t1.SCORE_TREE_ROUTE_ID, 
			t1.IS_INSURANCE_PROGRAM, 
			t1.TYPE_REQUEST_ID, 
			t1.CRED_OPEN_GROUP_ID, 
			t1.REQ_CREATE_ADDRESS_ID, 
			t1.GROUPS_BY_ACCESS_POINT_ID
	FROM _HDSM_OD.WH_PCCR_REQUEST_DIM t1
	WHERE t1.TYPE_REQUEST_ID = 1 AND t1.CURR_SCHEMS_ID NOT = 66 
			AND t1.REQ_CREATED_DATE>="&m_t_prevDate."dt
			/*AND (t1.REQ_CREATED_DATE BETWEEN "&m_t_prevDate."dt AND "&m_t_curDate."dt)*/
			;
CREATE INDEX REQUEST_ID ON &CURTMPLIB..SOC(REQUEST_ID);
QUIT;

*========================ТАБЛИЦА 2===========================;
PROC SQL;
	CREATE TABLE &CURTMPLIB..SOC_REQ_CUBE1 (COMPRESS=YES) AS 
	SELECT DISTINCT t1.REQUEST_ID, 
		t1.PERSON_ID, 
		t1.TIME_PERIOD_ID, 
		t1.REQ_CREATE_DATE_ID, 
		t1.REQ_CREATED_DATE, 
		t1.CONTR_OPEN_DATE_ID,
		t1.AGGR_REQ_SUMM,
		t1.AGGR_GIVE_SUMM, 
		t1.REQ_SCHEMS_ID, 
		t1.CUR_RATE_REQ_CREATE, 
		t1.CUR_RATE_CONTR_OPEN, 
		t1.CONTR_PERIOD_ID, 
		t1.REQ_PERIOD_ID, 
		t1.HIST_ANN_PERSON_SUM
	FROM _HDSM_WH.WH_SOCIAL_REQUEST_CUBE AS t1
	WHERE t1.REQ_CREATED_DATE>="&m_t_prevDate."dt AND t1.REQ_CREATED_DATE<"&sysdate9.:00:00:00"dt
		AND t1.TIME_PERIOD_ID-t1.REQ_CREATE_DATE_ID <= 30;
QUIT;
DATA _NULL_;
	part1='SOCIAL_'||TRANWRD(PUT(MONTH(DATEPART(DATETIME())), 2.), ' ', '0')
				||"_"||PUT(YEAR(DATEPART(DATETIME())), 4.);
	part2='SOCIAL_'||TRANWRD(PUT(MONTH(INTNX('month', DATEPART(DATETIME()),-1)),2.),' ','0')
		||"_"||PUT(YEAR(INTNX('month', DATEPART(DATETIME()), -1)),d4.);
	part3='SOCIAL_'||TRANWRD(PUT(MONTH(INTNX('month', DATEPART(DATETIME()),-2)),2.),' ','0')
		||"_"||PUT(YEAR(INTNX('month', DATEPART(DATETIME()), -2)),4.);
	PUT part1;
	CALL SYMPUT("part1", strip(part1));
	CALL SYMPUT("part2", strip(part2));
	CALL SYMPUT("part3", strip(part3));
RUN;
%PUT &part1.;
%PUT &part2.;
%PUT &part3.;

PROC SQL;
connect to oracle(user=aaprakhov 
		password="{SAS002}706634535AF06CC318AFD2641853F528" path='bist_ds1');

*CREATE TABLE &CURTMPLIB..SOC_REQ_CUBE1 (compress=yes) as
	SELECT * FROM connection to oracle (
		SELECT 
			REQUEST_ID, 
			PERSON_ID, 
			TIME_PERIOD_ID, 
			REQ_CREATE_DATE_ID, 
			REQ_CREATED_DATE, 
			CONTR_OPEN_DATE_ID,
			AGGR_REQ_SUMM,
			AGGR_GIVE_SUMM, 
			REQ_SCHEMS_ID, 
			CUR_RATE_REQ_CREATE, 
			CUR_RATE_CONTR_OPEN, 
			CONTR_PERIOD_ID, 
			REQ_PERIOD_ID, 
			HIST_ANN_PERSON_SUM
		FROM WH_DVTB.WH_SOCIAL_REQUEST_CUBE PARTITION (&part3.)
		WHERE TIME_PERIOD_ID-REQ_CREATE_DATE_ID <= 30
		UNION
		SELECT 
			REQUEST_ID, 
			PERSON_ID, 
			TIME_PERIOD_ID, 
			REQ_CREATE_DATE_ID, 
			REQ_CREATED_DATE, 
			CONTR_OPEN_DATE_ID,
			AGGR_REQ_SUMM,
			AGGR_GIVE_SUMM, 
			REQ_SCHEMS_ID, 
			CUR_RATE_REQ_CREATE, 
			CUR_RATE_CONTR_OPEN, 
			CONTR_PERIOD_ID, 
			REQ_PERIOD_ID, 
			HIST_ANN_PERSON_SUM
		FROM WH_DVTB.WH_SOCIAL_REQUEST_CUBE PARTITION (&part2.)
		WHERE TIME_PERIOD_ID-REQ_CREATE_DATE_ID <= 30
		UNION
		SELECT 
			REQUEST_ID, 
			PERSON_ID, 
			TIME_PERIOD_ID, 
			REQ_CREATE_DATE_ID, 
			REQ_CREATED_DATE, 
			CONTR_OPEN_DATE_ID,
			AGGR_REQ_SUMM,
			AGGR_GIVE_SUMM, 
			REQ_SCHEMS_ID, 
			CUR_RATE_REQ_CREATE, 
			CUR_RATE_CONTR_OPEN, 
			CONTR_PERIOD_ID, 
			REQ_PERIOD_ID, 
			HIST_ANN_PERSON_SUM
		FROM WH_DVTB.WH_SOCIAL_REQUEST_CUBE PARTITION (&part1.)
		WHERE TIME_PERIOD_ID-REQ_CREATE_DATE_ID <= 30
	)
	WHERE REQ_CREATED_DATE>"&sysdate9.:00:00:00"dt-86400*70
		AND TIME_PERIOD_ID-REQ_CREATE_DATE_ID <= 30
;
DISCONNECT FROM oracle;
QUIT;
PROC SQL;
CREATE INDEX REQUEST_ID ON &CURTMPLIB..SOC_REQ_CUBE1(REQUEST_ID);
QUIT;

*========================ТАБЛИЦА 3===========================;
PROC SQL;
	*CREATE TABLE &CURTMPLIB..REQESTS_2 (COMPRESS=YES) AS 
	SELECT DISTINCT t1.REQUEST_ID, 
		t1.CLIENT_TYPE1, 
		t1.CLIENT_TYPE4, 
		t1.CLIENT_TYPE5, 
		t1.CLIENT_TYPE6
	FROM COMMON.REQUESTS AS t1
	WHERE t1.CREATED_DATE>="&m_t_prevDate."dt AND t1.CREATED_DATE<"&sysdate9.:00:00:00"dt
	;
	
	CREATE TABLE &CURTMPLIB.._PPV_SHORT AS
		SELECT PERSON_ID, LAST_DT_MONTH_SREZ
		,CLIENT_TYPE1
		,CLIENT_TYPE4
		,CLIENT_TYPE5
		,CLIENT_TYPE6
		FROM COMMON.PERSONS_PORTF_VEB_SHORT 
		WHERE LAST_DT_MONTH_SREZ>=DATEPART("&sysdate9.:00:00:00"dt-86400*200);
	CREATE INDEX PERSON_ID ON &CURTMPLIB.._PPV_SHORT(PERSON_ID);
	CREATE INDEX LAST_DT_MONTH_SREZ ON &CURTMPLIB.._PPV_SHORT(LAST_DT_MONTH_SREZ);

QUIT;

*========================ТАБЛИЦА 4===========================;
/*PROC SQL;
	CREATE TABLE &CURTMPLIB..PARAM AS 
	SELECT DISTINCT t1.REQUEST_ID, 
		t1.REQ_CREATED_DATE, 
		t1.'Продуктовый сегмент'n, 
		t1.'Cash/CC'n, 
		t1.'Продукт'n, 
		t1.'Группа по продукту'n, 
		t1.'Продуктовый сегмент (заявленный)'n, 
		t1.'Cash/CC (заявленный)'n, 
		t1.'Продукт (заявленный)'n, 
		t1.'Группа по продукту (заявленный)'n, 
		t1.'Сумма выдачи по группам'n, 
		t1.'Заявленная сумма по группам'n, 
		t1.'Срок кредита (последний продукт)'n, 
		t1.'Срок кредита (заявл. продукт)'n, 
		t1.'Срок кредита (по выданным)'n, 
		t1.'Тип клиента'n, 
		t1.'Канал'n, 
		t1.'Территориальное управление'n, 
		t1.'Регион'n, 
		t1.'Населенный пункт'n, 
		t1.'Ручное рассмотрение'n, 
		t1.'Страховка (по выданным)'n, 
		t1.'PD по группам'n, 
		t1.'PD (из ХД) по группам'n,
		t1.'ЕВ клиента по имеющ. кредитам'n, 
		t1.'ЕВ клиента с учетом выдачи'n, 
		t1.'Продолжительность КИ по группам'n, 
		t1.'Бизнесс-процесс'n, 
		t1.CLIENT_TYPE1, 
		t1.CLIENT_TYPE4, 
		t1.CLIENT_TYPE5, 
		t1.CLIENT_TYPE6, 
		t1.'Расстояние от ФМЖ до ДО'n, 
		t1.'Расстояние от ФМЖ до ДО 2'n
	FROM &WRITELIB..PARAM t1
	WHERE t1.REQ_CREATED_DATE<"&m_t_prevDate."dt
	ORDER BY REQUEST_ID, REQ_CREATED_DATE ;
QUIT;*/


*========================ТАБЛИЦА 5===========================;
PROC SQL;
	CREATE TABLE &WRITELIB..VIDA AS 
	SELECT DISTINCT t1.REQUEST_ID, 
		t1.PCCR_PERSON_ID, 
		/* sum_v */
		(t1.SUMMA_DOG*t1.COURSE_OPEN) AS sum_v, 
		t1.OPEN_DATE, 
		t1.OPEN_DATE_ID, 
		t1.PERIOD, 
		  t2.KK_CASH,
		t1.L_KI_VEB, 
		/* strahovka */
		(CASE WHEN (t9.KIND_CRED_NAME CONTAINS "0.6" OR t9.KIND_CRED_NAME CONTAINS "0.16" OR t9.KIND_CRED_NAME CONTAINS "0.4" OR t9.KIND_CRED_NAME CONTAINS "0.7" OR t9.KIND_CRED_NAME CONTAINS "0.19" OR t9.KIND_CRED_NAME CONTAINS "0.3" OR t9.KIND_CRED_NAME CONTAINS "0.15" OR t9.KIND_CRED_NAME CONTAINS "0.25" OR t9.KIND_CRED_NAME CONTAINS "0.89")
			THEN "Аннуитетная страховка" 
			ELSE (CASE 	WHEN t10.SUMMA_INS_BONUS NOT IS MISSING THEN "Единовременная страховка" 
				ELSE "Без страховки" END)
		END) AS strahovka, 
		/* stavka_%_po_kred */
		(t1.CRED_PRC) AS 'stavka_%_po_kred'n, 
		/* kanal_po_kk */
		(CASE WHEN (t9.CRED_TYPE_SHORT_NAME) CONTAINS "OVER" 
			THEN CASE WHEN t1.SCORE_TREE_ROUTE_ID=1 THEN "Кросс-продажи" ELSE 
			CASE WHEN t1.SCORE_TREE_ROUTE_ID=35 THEN "Кросс-продажи вкладчикам" ELSE 
			(CASE WHEN t7.GROUPS_ID=6263 THEN "Касса ВЭБ" ELSE "Улица" END)
			END
			END
			ELSE "КЭШ"
		END) AS kanal_po_kk, 
		/* annuitet */
		(CASE WHEN (t9.CRED_TYPE_SHORT_NAME) CONTAINS "OVER" THEN (t1.REQ_SUMMA*t1.COURSE_OPEN)*0.05+(t1.REQ_SUMMA*t1.COURSE_OPEN)*((t1.CRED_PRC)/12/100) 
			ELSE (CASE WHEN t1.PERIOD NOT=0 
				THEN (t1.REQ_SUMMA*t1.COURSE_OPEN)*(((t1.CRED_PRC)/12/100)+(((t1.CRED_PRC)/12/100)/(((1+(t1.CRED_PRC)/12/100)**(t1.PERIOD))-1))) 
			ELSE 0 END)
		END) AS annuitet
	FROM _HDSM_OD.WH_CRED_STATIC_CUBE (WHERE = (DATA_PROVIDER_ID ^= 5)) t1
			INNER JOIN &CURTMPLIB..SOC (KEEP = REQUEST_ID) AS t3 
				ON (t1.REQUEST_ID = t3.REQUEST_ID)
			LEFT JOIN COM_DIC.SPRAV_PROD AS t2 
				ON (t1.PCCR_CRED_SCHEME_ID = t2.SCHEMS_ID)
			INNER JOIN _HDSM_WH.PCCR_GROUPS_DIM AS t7 
				ON (t1.REGISTRATION_GROUP_ID = t7.DIMENSION_KEY)
			LEFT JOIN _HDSM_OD.WH_ODB_KIND_CREDITS_DIM AS t9 
				ON (t1.WH_KIND_CREDIT_ID = t9.WH_ID)
			LEFT JOIN _HDSM_OD.WH_CRED_INSURANCE/*(KEEP = REQUEST_ID SUMMA_INS_BONUS)*/ AS t10 
				ON (t1.REQUEST_ID = t10.REQUEST_ID)
	WHERE t1.DATA_PROVIDER_ID NOT = 5;
QUIT;


*========================ТАБЛИЦА 6===========================;
PROC SQL;
	CREATE TABLE &WRITELIB..PARAM_DAY AS 
		SELECT DISTINCT t1.REQUEST_ID, 
			t1.REQ_CREATED_DATE, 
			/* Продуктовый сегмент */
			(t21.GROUP) AS 'Продуктовый сегмент'n, 
			/* Cash/CC */
			(t21.KK_CASH) AS 'Cash/CC'n, 
			t26.RETAIL_PRODUCT_NAME AS 'Продукт'n, 
			t26.'Группа'n AS 'Группа по продукту'n, 
			/* Продуктовый сегмент (заявленный) */
			(t20.GROUP) AS 'Продуктовый сегмент (заявленный)'n, 
			/* Cash/CC (заявленный) */
			(t20.KK_CASH) AS 'Cash/CC (заявленный)'n, 
			t25.RETAIL_PRODUCT_NAME AS 'Продукт (заявленный)'n, 
			t25.'Группа'n AS 'Группа по продукту (заявленный)'n, 
			/* Сумма выдачи по группам */
			(CASE WHEN t19.Sum_v<=50000 THEN '1. 50 т.р. и менее'
			WHEN t19.Sum_v<=100000 THEN '2. 51-100 т.р.' 
			WHEN t19.Sum_v<=150000 THEN '3. 101-150 т.р.' 
			WHEN t19.Sum_v<=200000 THEN '4. 151-200 т.р.' 
			WHEN t19.Sum_v<=300000 THEN '5. 201-300 т.р.' 
			WHEN t19.Sum_v>300000 THEN '6. более 300 т.р.' ELSE '7. -'
			END) AS 'Сумма выдачи по группам'n, 
			/* Заявленная сумма по группам */
			(CASE  
				WHEN t2.AGGR_REQ_SUMM*t1.CUR_RATE_REQ_CREATE<=50000
				THEN '1. 50 т.р. и менее'
				WHEN t2.AGGR_REQ_SUMM*t1.CUR_RATE_REQ_CREATE<=100000
				THEN '2. 51-100 т.р.'
				WHEN t2.AGGR_REQ_SUMM*t1.CUR_RATE_REQ_CREATE<=150000
				THEN '3. 101-150 т.р.'
				WHEN t2.AGGR_REQ_SUMM*t1.CUR_RATE_REQ_CREATE<=200000
				THEN '4. 151-200 т.р.'
				WHEN t2.AGGR_REQ_SUMM*t1.CUR_RATE_REQ_CREATE<=300000
				THEN '5. 201-300 т.р.'
				WHEN t2.AGGR_REQ_SUMM*t1.CUR_RATE_REQ_CREATE>300000
				THEN '6. более 300 т.р.' ELSE '7. -'
			END) AS 'Заявленная сумма по группам'n, 
			/* Срок кредита (последний продукт) */
			(CASE WHEN t21.KK_CASH='КК' THEN t21.KK_CASH ELSE t23.Srok_cred_groups END) AS 'Срок кредита (последний продукт)'n, 
			/* Срок кредита (заявл. продукт) */
			(CASE WHEN t20.KK_CASH='КК' THEN t20.KK_CASH ELSE t22.Srok_cred_groups END) AS 'Срок кредита (заявл. продукт)'n, 
			/* Срок кредита (по выданным) */
			(CASE WHEN t19.KK_CASH='КК' THEN t19.KK_CASH ELSE t24.Srok_cred_groups END) AS 'Срок кредита (по выданным)'n, 
			/* Тип клиента */
			(CASE  
				WHEN t4.DURATION IS MISSING THEN 'Новый'
				WHEN t4.DURATION>=6 AND t5.SUMM_DEBT>0
				THEN 'Действующий, сформированная КИ'
				WHEN t4.DURATION<6 AND t5.SUMM_DEBT>0
				THEN 'Действующий, несформированная КИ'
				WHEN t4.DURATION>=6
				THEN 'Спящий/ушедший, сформированная КИ'
				WHEN t4.DURATION<6
				THEN 'Спящий/ушедший, несформированная КИ'
				ELSE 'Прочий'  
			END) AS 'Тип клиента'n, 
			/* Канал */
			(CASE  
				WHEN t6.GROUPS_NAME CONTAINS 'DAF99' OR t7.SALE_NAME CONTAINS 'КОНТАКТ-ЦЕНТР' THEN '2. Канал телемаркетинг'
				WHEN t7.SALE_NAME CONTAINS 'ИНТЕРНЕТ-ЦЕНТР' THEN '3. Интернет-канал'
				WHEN (case when (case when t9.GROUPS_ID is missing then -1 else t9.GROUPS_ID end) not in (6092,8497) then t9.SALE_NAME else t8.SALE_NAME end) CONTAINS 'АГЕНТСК' OR (case when (case when t9.GROUPS_ID is missing then -1 else t9.GROUPS_ID end) not in (6092,8497)            then t9.SALE_NAME else t8.SALE_NAME end) IN ('АВТО УРМ','КРЕДИТНЫЙ БРОКЕР','ИНТЕРНЕТ - МАГАЗИН') THEN '4. POS-канал' 
				WHEN (case when (case when t9.GROUPS_ID is missing then -1 else t9.GROUPS_ID end) not in (6092,8497) then t9.SALE_NAME else t8.SALE_NAME end) CONTAINS 'ИНТЕРНЕТ-БАНК' THEN '5. Канал дистанционной выдачи кредитов' 
				WHEN (case when (case when t9.GROUPS_ID is missing then -1 else t9.GROUPS_ID end) not in (6092,8497) then t9.SALE_NAME else t8.SALE_NAME end) IN ('АККРЕДИТАЦИЯ АГЕНТА','ПАКЕТНАЯ РЕГИСТРАЦИЯ ЗАЯВОК') THEN '6. -'  ELSE '1. Очный канал' END) AS 'Канал'n, 
				/* Территориальное управление */
			(CASE WHEN ((case when (case when t9.GROUPS_ID is missing then -1 else t9.GROUPS_ID end) not in (6092,8497) then t9.ADDR_REGION else t8.ADDR_REGION end) IN ('КРАСНОЯРСКИЙ','БУРЯТИЯ','ИРКУТСКАЯ','ХАКАСИЯ','ЗАБАЙКАЛЬСКИЙ', 'ИРКУТСКАЯ ОБЛ УСТЬ-ОРДЫНСКИЙ БУРЯТСКИЙ','ЧИТИНСКАЯ','ЗАБАЙКАЛЬСКИЙ КРАЙ АГИНСКИЙ БУРЯТСКИЙ')) THEN "Сибирское ТУ Домашняя зона"
				WHEN (case when (case when t9.GROUPS_ID is missing then -1 else t9.GROUPS_ID end) not in (6092,8497) then t9.ADDR_TERRITORY else t8.ADDR_TERRITORY end)='Сибирское ТУ' THEN "Сибирское ТУ Новая зона" 
				when (case when t9.GROUPS_ID is missing then -1 else t9.GROUPS_ID end) not in (6092,8497) then t9.ADDR_TERRITORY else t8.ADDR_TERRITORY 
			END) AS 'Территориальное управление'n, 
			/* Регион */
			(case
				when (case when t9.GROUPS_ID is missing then -1 else t9.GROUPS_ID end) not in (6092,8497)
				then t9.ADDR_REGION
				else t8.ADDR_REGION
			end) AS 'Регион'n, 
			/* Населенный пункт */
			(case
				when (case when t9.GROUPS_ID is missing then -1 else t9.GROUPS_ID end) not in (6092,8497)
				then t9.ADDR_CITY
				else t8.ADDR_CITY
			end) AS 'Населенный пункт'n, 
			/* Ручное рассмотрение */
			(t10.IS_MANUAL) AS 'Ручное рассмотрение'n, 
			t19.strahovka AS 'Страховка (по выданным)'n, 
			/* PD по группам */
			(CASE 
				WHEN (case when t1.REQ_CREATED_DATE<'1Oct2014:0:0:0'dt then t11.pd_20_28 else t3.PD end)<=5 
					THEN "1. 0-5%"
				WHEN (case when t1.REQ_CREATED_DATE<'1Oct2014:0:0:0'dt then t11.pd_20_28 else t3.PD end)<=10 
					THEN "2. 6-10%"
				WHEN (case when t1.REQ_CREATED_DATE<'1Oct2014:0:0:0'dt then t11.pd_20_28 else t3.PD end)<=15 
					THEN "3. 11-15%"
				WHEN (case when t1.REQ_CREATED_DATE<'1Oct2014:0:0:0'dt then t11.pd_20_28 else t3.PD end)<=20 
					THEN "4. 16-20%"
				WHEN (case when t1.REQ_CREATED_DATE<'1Oct2014:0:0:0'dt then t11.pd_20_28 else t3.PD end)<=25 
					THEN "5. 21-25%"
				WHEN (case when t1.REQ_CREATED_DATE<'1Oct2014:0:0:0'dt then t11.pd_20_28 else t3.PD end)<=30 
					THEN "6. 26-30%"
				WHEN (case when t1.REQ_CREATED_DATE<'1Oct2014:0:0:0'dt then t11.pd_20_28 else t3.PD end)>30 
					THEN "7. более 30%"
				ELSE "8. Неизвестно"
			END) AS 'PD по группам'n, 
			/* PD (из ХД) по группам. Добавлена в скрипт 16.02.2015. Заявка на добавление от  13.02.2015 */
			(CASE WHEN t3.PD IS MISSING THEN "8. Неизвестно"
				WHEN t3.PD<=5 THEN "1. 0-5%"
				WHEN t3.PD<=10 THEN "2. 6-10%"
				WHEN t3.PD<=15 THEN "3. 11-15%"
				WHEN t3.PD<=20 THEN "4. 16-20%"
				WHEN t3.PD<=25 THEN "5. 21-25%"
				WHEN t3.PD<=30 THEN "6. 26-30%"
				WHEN t3.PD>30 THEN "7. более 30%"
				else "8. Неизвестно"
			END) AS 'PD (из ХД) по группам'n, 
			/* ЕВ клиента по имеющ. кредитам */
			(CASE  
				WHEN (MAX(t2.HIST_ANN_PERSON_SUM))<=5000   THEN "1. До 5000 р."
				WHEN (MAX(t2.HIST_ANN_PERSON_SUM))<=10000   THEN "2. 5001-10000 р."
				WHEN (MAX(t2.HIST_ANN_PERSON_SUM))<=15000   THEN "3. 10001-15000 р."
				WHEN (MAX(t2.HIST_ANN_PERSON_SUM))<=20000   THEN "4. 15001-20000 р."
				WHEN (MAX(t2.HIST_ANN_PERSON_SUM))<=25000   THEN "5. 20001-25000 р."
				WHEN (MAX(t2.HIST_ANN_PERSON_SUM))>25000   THEN "6. Более 25000 р."
				ELSE "7. Не определено"
			END) AS 'ЕВ клиента по имеющ. кредитам'n, 
			/* ЕВ клиента с учетом выдачи */
			(case when t19.REQUEST_ID NOT IS missing THEN
				CASE 
				WHEN (MAX(t2.HIST_ANN_PERSON_SUM)+t19.annuitet)<=5000 THEN "1. До 5000 р."
				WHEN (MAX(t2.HIST_ANN_PERSON_SUM)+t19.annuitet)<=10000 THEN "2. 5001-10000 р."
				WHEN (MAX(t2.HIST_ANN_PERSON_SUM)+t19.annuitet)<=15000 THEN "3. 10001-15000 р."
				WHEN (MAX(t2.HIST_ANN_PERSON_SUM)+t19.annuitet)<=20000 THEN "4. 15001-20000 р."
				WHEN (MAX(t2.HIST_ANN_PERSON_SUM)+t19.annuitet)<=25000 THEN "5. 20001-25000 р."
				WHEN (MAX(t2.HIST_ANN_PERSON_SUM)+t19.annuitet)>25000 THEN "6. Более 25000 р."
				ELSE "7. Не определено" END 
				ELSE "8. Не было выдачи"
			end) AS 'ЕВ клиента с учетом выдачи'n, 
			/* Продолжительность КИ по группам */
			(CASE  
				WHEN (t4.DURATION IS MISSING) OR (t4.DURATION<6) THEN "a. До 6 мес."
				WHEN t4.DURATION<12 THEN "b. 6-11 мес."
				WHEN t4.DURATION<24 THEN "c. 12-23 мес."
				WHEN t4.DURATION<36 THEN "d. 24-35 мес."
				WHEN t4.DURATION<60 THEN "e. 36-59 мес."
				WHEN t4.DURATION>=60 THEN "f. 60 мес. и более"
				ELSE "g. Не определено"
			END) AS 'Продолжительность КИ по группам'n, 
			/* Бизнесс-процесс */
			(case
				when t16.groups_name contains 'UTP00' then "9. ДВК"
				when t1.REQ_SCHEMS_ID=3060 then "8. TOP UP"
				when t6.groups_name contains "(PBK01)" then "7. Preapprove"
				when t6.groups_name contains "(KAS01)" or (case when (case when t9.GROUPS_ID is missing then -1 
				else t9.GROUPS_ID end) not in (6092,8497) then t9.ADDR_TERRITORY else t8.ADDR_TERRITORY end)="КАССА ВЭБ" then "6. Касса ВЭБ"
				when t16.groups_name contains 'KIB' then "5. Кредиты в ИБ"
				when t1.SCORE_TREE_ROUTE_ID=1 or t1.SCORE_TREE_ROUTE_ID=35 then "4. Мгновенный кросс"
				when t6.groups_name contains "КОНТАКТ-ЦЕНТР" then "2. КЦ"
				when t6.groups_name contains "INT" then "3. Сайт"
				else "1. Очное оформление"
			end) AS 'Бизнесс-процесс'n, 
			t17.CLIENT_TYPE1, 
			t17.CLIENT_TYPE4, 
			t17.CLIENT_TYPE5, 
			t17.CLIENT_TYPE6, 
			/* Расстояние от ФМЖ до ДО */
			(case
				when t18.dist is missing then "10. Не определено"
				when t18.dist <=0.5 then "1. До 0,5 км"
				when t18.dist <=1 then "2. 0,5-1 км"
				when t18.dist <=1.5 then "3. 1-1,5 км"
				when t18.dist <=3 then "4. 1,5-3 км"
				when t18.dist <=10 then "5. 3-10 км"
				when t18.dist <=30 then "6. 10-30 км"
				when t18.dist <=50 then "7. 30-50 км"
				when t18.dist <=100 then "8. 50-100 км"
				else "9. более 100 км"
			end) AS 'Расстояние от ФМЖ до ДО'n, 
			/* Расстояние от ФМЖ до ДО 2 */
			(case
				when t18.dist is missing then "9. Не определено"
				when t18.dist <=0.5 then "1. До 0,5 км"
				when t18.dist <=1 then "2. 0,5-1 км"
				when t18.dist <=2 then "3. 1-2 км"
				when t18.dist <=5 then "4. 2-5 км"
				when t18.dist <=20 then "5. 5-20 км"
				when t18.dist <=40 then "6. 20-40 км"
				when t18.dist <=60 then "7. 40-60 км"
				else "8. более 60 км"
			end) AS 'Расстояние от ФМЖ до ДО 2'n
	FROM &CURTMPLIB..SOC AS t1
	LEFT JOIN &CURTMPLIB..SOC_REQ_CUBE1 AS t2 ON (t1.REQUEST_ID = t2.REQUEST_ID)
	LEFT JOIN _HDSM_WH.WH_SOCIAL_STATIC_CUBE AS t3 
		ON (t2.REQUEST_ID = t3.REQUEST_ID)
	INNER JOIN _HDSM_WH.WH_SCORING_PERSON_PARAMS AS t4 ON (t3.SCORE_PARAMS_VEB_REF = t4.WH_ID)
	LEFT JOIN _HDSM_WH.WH_SCORE_SEGMENT_LOG AS t5 ON (t3.REQUEST_ID = t5.REQUEST_ID)
	INNER JOIN _HDSM_WH.PCCR_GROUPS_DIM AS t6 ON (t1.REQ_CREATE_ADDRESS_ID = t6.DIMENSION_KEY)
	INNER JOIN _HDSM_WH.PCCR_GROUPS_DIM AS t7 ON (t1.REQ_CREATE_GROUP_ID = t7.DIMENSION_KEY)
	INNER JOIN _HDSM_WH.PCCR_GROUPS_DIM AS t8 ON (t1.LAST_KE_GROUP_ID = t8.DIMENSION_KEY)
	INNER JOIN _HDSM_WH.PCCR_GROUPS_DIM AS t9 ON (t1.GROUPS_BY_ACCESS_POINT_ID = t9.DIMENSION_KEY)
	INNER JOIN _HDSM_WH.WH_REQ_CONSIDER_SIGNS_DIM t10 ON (t3.CONSIDER_SIGNS_REF = t10.WH_ID)
	LEFT JOIN /*&WRITELIB.*/ COMMON.REQUESTS AS t11 ON (t1.REQUEST_ID = t11.REQUEST_ID AND t11.CREATED_DATE<"01OCT2014:00:00:00"dt)
	INNER JOIN _HDSM_WH.DIM_DATE_TIME AS t12 ON (t2.TIME_PERIOD_ID = t12.DIMENSION_KEY)
	INNER JOIN _HDSM_WH.DIM_DATE_TIME AS t13 ON (t1.REQ_CREATE_DATE_ID = t13.DIMENSION_KEY)
	INNER JOIN _HDSM_WH.PCCR_REQ_STATUS_DIM AS t14 ON (t1.LAST_REQ_STATUS_ID = t14.DIMENSION_KEY)
	INNER JOIN _HDSM_WH.PCCR_SCHEMS_DIM AS t15 ON (t1.CURR_SCHEMS_ID = t15.SCHEMS_ID)
	INNER JOIN _HDSM_WH.PCCR_GROUPS_DIM AS t16 ON (t1.CRED_OPEN_GROUP_ID = t16.DIMENSION_KEY)
	/*LEFT JOIN &CURTMPLIB..REQESTS_2 AS t17 ON (t1.REQUEST_ID = t17.REQUEST_ID)*/
	LEFT JOIN &CURTMPLIB.._PPV_SHORT /*COMMON.PERSONS_PORTF_VEB_SHORT*/ AS t17 
		ON (COALESCE(t2.PERSON_ID,-1) = t17.PERSON_ID 
			AND INTNX('month',DATEPART(COALESCE(t2.REQ_CREATED_DATE,-1)),0,'end')=t17.LAST_DT_MONTH_SREZ)
	LEFT JOIN /*&WRITELIB..DIST*/ URD.PERS_OTD_MINDIST_SECOND_FINAL AS t18 ON (t2.PERSON_ID = t18.PERSON_ID)
	LEFT JOIN &WRITELIB..VIDA AS t19 ON (t1.REQUEST_ID = t19.REQUEST_ID)
	LEFT JOIN COM_DIC.SPRAV_PROD AS t20 ON (t1.REQ_SCHEMS_ID = t20.SCHEMS_ID)
	LEFT JOIN COM_DIC.SPRAV_PROD AS t21 ON (t1.CURR_SCHEMS_ID = t21.SCHEMS_ID)
	LEFT JOIN &WRITELIB..PB_PERIODGROUPS AS t22 ON (t2.REQ_PERIOD_ID = t22.PERIOD)
	LEFT JOIN &WRITELIB..PB_PERIODGROUPS AS t23 ON (t2.CONTR_PERIOD_ID = t23.PERIOD)
	LEFT JOIN &WRITELIB..PB_PERIODGROUPS AS t24 ON (t19.PERIOD = t24.PERIOD)
	LEFT JOIN &WRITELIB..SCHEMS_PROD AS t25 ON (t1.REQ_SCHEMS_ID = t25.SCHEMS_ID)
	LEFT JOIN &WRITELIB..SCHEMS_PROD AS t26 ON (t1.CURR_SCHEMS_ID = t26.SCHEMS_ID)
	WHERE t12.L_MONTH_ID = t13.L_MONTH_ID 
		AND t14.STATUS_NAME not in ('кредитная карта','хоум-кредит','хоум-пкб',
						'хоум-пкб 3','юникредит банк-пкб(к)','юникредит банк-пкб(а)','хкф-пкб','хкф4-пкб','хкф-пкб-а',
						'камабанк-вэб(к)','камабанк-пкб(а)','камабанк')
		AND t15.RETAIL_PRODUCT_NAME not in ('100% НАДЕЖНЫЙ', 'ВЕРИФИКАЦИЯ ЮРИДИЧЕСКИХ ЛИЦ'
				,'ВКЛАД ПРЕМЬЕР','ВКЛАД С ДОСТАВКОЙ','ЗАРПЛАТНЫЙ ТБС','ЛИЗИНГ'
				,'КУПЛЕННЫЕ КРЕДИТЫ','МЕЖБАНКОВСКИЙ КРЕДИТ','НА ПОКУПКУ АКЦИЙ'
				,'НА ПОКУПКУ ОБОРУДОВАНИЯ','НА ПОПОЛНЕНИЕ ОБОРОТНЫХ СРЕДСТВ'
				,'НА РАЗВИТИЕ БИЗНЕСА','НА СТРОИТЕЛЬСТВО','ПОДГРУЗКИ'
				,'ПРЕДВАРИТЕЛЬНАЯ ЭКСПРЕСС - ПОДДЕРЖКА','РОЗНИЧНЫЙ ДЕПОЗИТ НЕПОПОЛНЯЕМЫЙ'
				,'РОЗНИЧНЫЙ ДЕПОЗИТ ПОПОЛНЯЕМЫЙ','РОЗНИЧНЫЙ ТБС','СБЕРЕГАТЕЛЬНЫЙ ТБС'
				,'СБЕРКНИЖКА','ТЕСТОВЫЙ РОЗНИЧНЫЙ ПРОДУКТ 3')
				
	GROUP BY t1.REQUEST_ID,
			t1.REQ_CREATED_DATE,
			(t21.GROUP),
			(t21.KK_CASH),
			t26.RETAIL_PRODUCT_NAME,
			t26.'Группа'n,
			(t20.GROUP),
			(t20.KK_CASH),
			t25.RETAIL_PRODUCT_NAME,
			t25.'Группа'n,
			(CALCULATED 'Сумма выдачи по группам'n),
			(CALCULATED 'Заявленная сумма по группам'n),
			(CALCULATED 'Срок кредита (последний продукт)'n),
			(CALCULATED 'Срок кредита (заявл. продукт)'n),
			(CALCULATED 'Срок кредита (по выданным)'n),
			(CALCULATED 'Тип клиента'n),
			(CALCULATED 'Канал'n),
			(CALCULATED 'Территориальное управление'n),
			(CALCULATED 'Регион'n),
			(CALCULATED 'Населенный пункт'n),
			(t10.IS_MANUAL),
			t19.strahovka,
			(CALCULATED 'PD по группам'n),
			(CALCULATED 'PD (из ХД) по группам'n),
			(CALCULATED 'Продолжительность КИ по группам'n),
			(CALCULATED 'Бизнесс-процесс'n),
			t17.CLIENT_TYPE1,
			t17.CLIENT_TYPE4,
			t17.CLIENT_TYPE5,
			t17.CLIENT_TYPE6,
			(CALCULATED 'Расстояние от ФМЖ до ДО'n),
			(CALCULATED 'Расстояние от ФМЖ до ДО 2'n)
		ORDER BY REQUEST_ID, REQ_CREATED_DATE;
QUIT;

*========================ТАБЛИЦА 7 ФИН PARAM===========================;
/*PROC SQL;
	CREATE TABLE &WRITELIB..PARAM (compress = yes) AS 
		SELECT * FROM &CURTMPLIB..PARAM
		 UNION 
		SELECT * FROM &WRITELIB..PARAM_DAY
	;
Quit;*/

/*DATA &WRITELIB..PARAM;
	SET &WRITELIB..PARAM;
	*BY REQUEST_ID REQ_CREATED_DATE;
	WHERE REQ_CREATED_DATE<"&m_t_prevDate."dt
RUN;
PROC APPEND BASE=&WRITELIB..PARAM DATA=&WRITELIB..PARAM_DAY FORCE;
RUN;*/

/* модернизированное обновление таблицы */
DATA &WRITELIB..PARAM;
	UPDATE &WRITELIB..PARAM &WRITELIB..PARAM_DAY UPDATEMODE=NOMISSINGCHECK;
	BY REQUEST_ID REQ_CREATED_DATE;
RUN;


/* ====================================ЭТАП 2. (создание таблицы FACTORS_FOR_OPEN_LOANS*/
/*определяю добавочное условие add_condition. (Ранее инициализация была пустой)*/
%LET add_condition = ("&sysdate9.:00:00:00"dt-360*86400);

%PUT ============>Установлено Добавочное условие &add_condition;

/*ВВЕДИТЕ ДАТЫ В ДВУХ ЗАПРОСАХ*/
*=2=======================ТАБЛИЦА 1===========================;
PROC SQL;
	CREATE TABLE &CURTMPLIB..FOR_BKI AS 
		SELECT DISTINCT t1.REQUEST_ID, 
			/* REQUEST_REACT_ID */
			(MAX(t1.REQUEST_REACT_ID)) AS REQUEST_REACT_ID
			FROM _SPSM_SC.L_FLOW_PRICE_LEVEL t1
		WHERE (t1.CREATED_DATE >= &add_condition.)
		GROUP BY t1.REQUEST_ID;
QUIT;
PROC SQL;
	CREATE INDEX REQUEST_ID_REACT ON &CURTMPLIB..FOR_BKI(REQUEST_ID,REQUEST_REACT_ID);
QUIT;

*=2=======================ТАБЛИЦА 2===========================;
PROC SQL;
	CREATE TABLE &CURTMPLIB..VIDA_OPEN AS 
		SELECT DISTINCT t1.SRC_CRED_ID, 
			t1.REQUEST_ID, 
			t1.DATA_PROVIDER_ID, 
			t1.PCCR_PERSON_ID, 
			t1.SCORE_TREE_ROUTE_ID, 
			/* sum_v */
			(t1.SUMMA_DOG*t1.COURSE_OPEN) AS sum_v, 
			t1.OPEN_DATE, 
			t1.OPEN_DATE_ID, 
			/* Месяц+Год выдачи кредита */
			(datepart(t3.L_MONTH_END_DATE)) FORMAT=RUSDFMY5. AS 'Месяц+Год выдачи кредита'n, 
			t3.L_MONTH_ID, 
			t3.L_MONTH_NAME_Y, 
			t1.CLOSE_DATE, 
			t1.CLOSE_DATE_ID, 
			t1.SOLD_DATE_ID, 
			/* organiz_kupiv_kred */
			(t13.NAME) AS organiz_kupiv_kred, 
			/* prodan_sum */
			(t1.SOLD_MAIN_DEBT_NT+t1.SOLD_MAIN_OVERDUE_NT) AS prodan_sum, 
			t2.Group_final, 
			t2.'Cash/CC'n, 
			t2.SCHEMS_NAME, 
			t16.GROUP, 
			t16.KK_CASH, 
			t15.'Группа'n, 
			t17.RETAIL_PRODUCT_NAME, 
			t1.PERIOD, 
			t1.L_KI_VEB, 
			t11.SUMM_DEBT, 
			t1.CNT_CREDIT_VEB, 
			t18.SALARY_SUM, 
			t18.WORKS_POST_SALARY, 
			t1.L_KI_VEB_BKI, 
			t20.PAYMENT_BKI, 
			t20.PAYMENT_VEB, 
			t20.CNT_CREDIT_VEB_BKI, 
			t21.SUMMA_DEBT_VEB_RATE, 
			t21.SUMMA_DEBT_BKI_RATE, 
			/* TU */
			(CASE WHEN t7.GROUPS_ID=6263 THEN t4.ADDR_TERRITORY ELSE t8.ADDR_TERRITORY END) AS TU, 
			/* tochka_oforml_naimen */
			(CASE WHEN t7.GROUPS_ID=6263 THEN t4.GROUPS_NAME ELSE t7.GROUPS_NAME END) AS tochka_oforml_naimen, 
			/* tochka_oforml_format */
			(t7.SALE_NAME) AS tochka_oforml_format, 
			/* tochka_otkr_format */
			(t6.SALE_NAME) AS tochka_otkr_format, 
			/* region */
			(CASE WHEN t7.GROUPS_ID=6263 THEN t4.ADDR_REGION ELSE t8.ADDR_REGION END) AS region, 
			/* naselen_punkt */
			(CASE WHEN t7.GROUPS_ID=6263 THEN t4.ADDR_CITY ELSE t8.ADDR_CITY END) AS naselen_punkt, 
			t8.TERRITORY_2015,
			t8.MAKROREGION,
			/* tochka_vidachi_naimenovanie */
			(CASE WHEN t7.GROUPS_ID=6263 THEN t4.GROUPS_NAME 
				ELSE t8.GROUPS_NAME END) AS tochka_vidachi_naimenovanie, 
			t5.IS_MANUAL AS priznak_ruki, 
			/* strahovka */
			(CASE WHEN t9.KIND_CRED_NAME CONTAINS "0.6" OR t9.KIND_CRED_NAME CONTAINS "0.16" 
						OR t9.KIND_CRED_NAME CONTAINS "0.4" OR t9.KIND_CRED_NAME CONTAINS "0.7" 
						OR t9.KIND_CRED_NAME CONTAINS "0.19" OR t9.KIND_CRED_NAME CONTAINS "0.3" 
						OR t9.KIND_CRED_NAME CONTAINS "0.15" OR t9.KIND_CRED_NAME CONTAINS "0.25" 
						OR t9.KIND_CRED_NAME CONTAINS "0.89" OR t9.KIND_CRED_NAME CONTAINS "0.99" 
						OR t9.KIND_CRED_NAME CONTAINS "0.16" OR t9.KIND_CRED_NAME CONTAINS "/0/0.8)" 
					THEN "Аннуитетная страховка" 
			ELSE (CASE WHEN t10.SUMMA_INS_BONUS NOT IS MISSING THEN "Единовременная страховка" 
					ELSE "Без страховки" END) END) AS strahovka, 
			/* stavka_%_po_kred */
			(t1.CRED_PRC) AS 'stavka_%_po_kred'n, 
			/* tip_kredita_pravila_ucheta */
			(t9.CRED_TYPE_SHORT_NAME) AS tip_kredita_pravila_ucheta, 
			/* pd_gruppa */
			(CASE WHEN (case when t1.OPEN_DATE<'1Sep2014:0:0:0'dt then t12.pd_20_28 else t1.PD end) is missing 
					THEN "8. Неизвестно"
				WHEN (case when t1.OPEN_DATE<'1Sep2014:0:0:0'dt then t12.pd_20_28 else t1.PD end)<=5 THEN "1. 0-5%"
				WHEN (case when t1.OPEN_DATE<'1Sep2014:0:0:0'dt then t12.pd_20_28 else t1.PD end)<=10 THEN "2. 6-10%"
				WHEN (case when t1.OPEN_DATE<'1Sep2014:0:0:0'dt then t12.pd_20_28 else t1.PD end)<=15 THEN "3. 11-15%"
				WHEN (case when t1.OPEN_DATE<'1Sep2014:0:0:0'dt then t12.pd_20_28 else t1.PD end)<=20 THEN "4. 16-20%"
				WHEN (case when t1.OPEN_DATE<'1Sep2014:0:0:0'dt then t12.pd_20_28 else t1.PD end)<=25 THEN "5. 21-25%"
				WHEN (case when t1.OPEN_DATE<'1Sep2014:0:0:0'dt then t12.pd_20_28 else t1.PD end)<=30 THEN "6. 26-30%"
				WHEN (case when t1.OPEN_DATE<'1Sep2014:0:0:0'dt then t12.pd_20_28 else t1.PD end)>30 THEN "7. более 30%"
				ELSE "8. Неизвестно" END) AS pd_gruppa,
			/* pd_gruppa_hd */
			(CASE WHEN t1.PD IS MISSING THEN "8. Неизвестно" 
				WHEN t1.PD<=5 THEN "1. 0-5%"
				WHEN t1.PD<=10 THEN "2. 6-10%"
				WHEN t1.PD<=15 THEN "3. 11-15%"
				WHEN t1.PD<=20 THEN "4. 16-20%"
				WHEN t1.PD<=25 THEN "5. 21-25%"
				WHEN t1.PD<=30 THEN "6. 26-30%"
				WHEN t1.PD>30 THEN "7. более 30%"
				ELSE "8. Неизвестно"
			END) AS pd_gruppa_hd, 
			/* kanal_po_kk */
			(CASE WHEN (t9.CRED_TYPE_SHORT_NAME) CONTAINS "OVER" 
					THEN (CASE WHEN t1.SCORE_TREE_ROUTE_ID=1 THEN "Кросс-продажи"
								WHEN t1.SCORE_TREE_ROUTE_ID=35 THEN "Кросс-продажи вкладчикам"
								WHEN t7.GROUPS_ID=6263 THEN "Касса ВЭБ"
							ELSE "Улица" END)
			ELSE "КЭШ" END) AS kanal_po_kk, 
			/* annuitet */
			(CASE WHEN t9.CRED_TYPE_SHORT_NAME CONTAINS "OVER"
				THEN ((t1.SUMMA_DOG*t1.COURSE_OPEN)*IFN(t1.OPEN_DATE<'5Mar2015:0:0:0'dt, 0.05, 0.03)
							+(t1.SUMMA_DOG*t1.COURSE_OPEN)*((t1.CRED_PRC)/12/100))
				ELSE (CASE WHEN t1.PERIOD NOT=0 and t1.PERIOD NOT is missing 
						THEN (t1.SUMMA_DOG*t1.COURSE_OPEN)*(((t1.CRED_PRC)/12/100)
							+(((t1.CRED_PRC)/12/100)/(((1+(t1.CRED_PRC)/12/100)**(t1.PERIOD))-1)))
					ELSE 0 END)
			END) AS annuitet,
			t1.SUMMA_ALL_OTHER_CR_IN_MON AS 'Совок. ЕВ на мом выд'n,
			/* ЕВ клиента с учетом выдачи */
			(COALESCE(t1.SUMMA_ALL_OTHER_CR_IN_MON, 0)+
						(CASE WHEN t9.CRED_TYPE_SHORT_NAME CONTAINS "OVER"
							THEN (CASE WHEN t1.OPEN_DATE<'5Mar2015:0:0:0'dt 
										THEN (t1.SUMMA_DOG*t1.COURSE_OPEN)*0.05
											+(t1.SUMMA_DOG*t1.COURSE_OPEN)*((t1.CRED_PRC)/12/100) 
										ELSE (t1.SUMMA_DOG*t1.COURSE_OPEN)*0.03
											+(t1.SUMMA_DOG*t1.COURSE_OPEN)*((t1.CRED_PRC)/12/100) END)
							ELSE (CASE WHEN t1.PERIOD NOT=0 AND t1.PERIOD NOT IS MISSING 
										THEN (t1.SUMMA_DOG*t1.COURSE_OPEN)*(((t1.CRED_PRC)/12/100)
											+(((t1.CRED_PRC)/12/100)/(((1+(t1.CRED_PRC)/12/100)**(t1.PERIOD))-1)))
									ELSE 0 END ) END ) 
			) AS 'ЕВ клиента с учетом выдачи'n
		FROM _HDSM_OD.WH_CRED_STATIC_CUBE t1
		LEFT JOIN &WRITELIB..PB_PRODUCTGROUPS t2 ON (t1.PCCR_CRED_SCHEME_ID = t2.SCHEMS_ID)
		LEFT JOIN _HDSM_WH.DIM_DATE_TIME t3 ON (t1.OPEN_DATE_ID = t3.DIMENSION_KEY)
		LEFT JOIN _HDSM_WH.PCCR_GROUPS_DIM t4 ON (t1.GIVEN_ADDRESS_ID = t4.DIMENSION_KEY)
		LEFT JOIN _HDSM_WH.PCCR_GROUPS_DIM t6 ON (t1.GIVEN_GROUP_ID = t6.DIMENSION_KEY)
		LEFT JOIN _HDSM_WH.PCCR_GROUPS_DIM t7 ON (t1.REGISTRATION_GROUP_ID = t7.DIMENSION_KEY)
		LEFT JOIN _HDSM_WH.PCCR_GROUPS_DIM t8 ON (t1.OPENING_GROUP_ID = t8.DIMENSION_KEY)
		LEFT JOIN _HDSM_WH.WH_REQ_CONSIDER_SIGNS_DIM t5 ON (t1.CONSIDER_SIGNS_REF = t5.WH_ID)
		LEFT JOIN _HDSM_OD.WH_ODB_KIND_CREDITS_DIM t9 ON (t1.WH_KIND_CREDIT_ID = t9.WH_ID)
		LEFT JOIN _HDSM_OD.WH_CRED_INSURANCE t10 ON (COALESCE(t1.REQUEST_ID,-1) = t10.REQUEST_ID)
		LEFT JOIN _HDSM_WH.WH_SCORE_SEGMENT_LOG t11 ON (COALESCE(t1.REQUEST_ID,-1) = t11.REQUEST_ID)
		LEFT JOIN COMMON.REQUESTS t12 ON (COALESCE(t1.REQUEST_ID,-1) = t12.REQUEST_ID)
		LEFT JOIN _HDSM_OD.WH_CLIENT t13 ON (t1.WH_SOLD_CLIENT_ID = t13.WH_ID)
		LEFT JOIN &WRITELIB..SCHEMS_PROD t15 ON (t1.PCCR_CRED_SCHEME_ID = t15.SCHEMS_ID)
		LEFT JOIN COM_DIC.SPRAV_PROD t16 ON (t1.PCCR_CRED_SCHEME_ID = t16.SCHEMS_ID)
		LEFT JOIN _HDSM_WH.PCCR_SCHEMS_DIM t17 ON (t1.PCCR_CRED_SCHEME_ID = t17.SCHEMS_ID)
		LEFT JOIN _HDSM_OD.WH_PCCR_REQUEST_CLIENT_DIM t18 ON (COALESCE(t1.REQUEST_ID,-1) = t18.REQUEST_ID)
		LEFT JOIN &CURTMPLIB..FOR_BKI t19 ON (COALESCE(t1.REQUEST_ID,-1) = t19.REQUEST_ID)
		LEFT JOIN _SPSM_SC.L_FLOW_PRICE_LEVEL t20 
			ON (COALESCE(t19.REQUEST_ID,-1) = t20.REQUEST_ID) AND (t19.REQUEST_REACT_ID = t20.REQUEST_REACT_ID)
		LEFT JOIN _PKSM_KR.L_SCORING_CALC_PARAMETERS t21 
			ON (COALESCE(t19.REQUEST_ID,-1) = t21.REQUEST_ID) AND (t19.REQUEST_REACT_ID = t21.REQUEST_REACT_ID)
/* ВВЕДИТЕ ДАТУ, с которой нужно добавить кредиты в отчет. Формат даты – '1Jan2010:0:0:0'dt */
		WHERE t1.DATA_PROVIDER_ID NOT = 5 AND (t1.OPEN_DATE>=&add_condition.);
QUIT;

*=2=======================ТАБЛИЦА 3===========================;
PROC SQL;
	CREATE TABLE &CURTMPLIB..factor AS 
	SELECT DISTINCT t1.SRC_CRED_ID, 
			t1.DATA_PROVIDER_ID, 
			t1.OPEN_DATE_ID, 
			t1.OPEN_DATE, 
			/* Дата открытия */
			(datepart(t1.OPEN_DATE)) FORMAT=DDMMYYP10. AS 'Дата открытия'n, 
			t1.'Месяц+Год выдачи кредита'n,
			t1.GROUP AS 'Продуктовый сегмент'n, 
			t1.KK_CASH AS 'КК/КЭШ'n, 
			t1.Group_final AS Group_final, 
			t1.'Cash/CC'n, 
			t1.'Группа'n AS 'Группа по продуктам'n, 
			/* Продукт: наименование */
			(t1.RETAIL_PRODUCT_NAME) AS 'Продукт: наименование'n, 
			/* Сумма выдачи по группам */
			(CASE WHEN t1.Sum_v is missing or t1.Sum_v=0 THEN '10. -' 
				WHEN t1.Sum_v<=50000 THEN '1. 50 т.р. и менее' 
				WHEN t1.Sum_v<=100000 THEN '2. 51-100 т.р.'
				WHEN t1.Sum_v<=150000 THEN '3. 101-150 т.р.'
				WHEN t1.Sum_v<=200000 THEN '4. 151-200 т.р.'
				WHEN t1.Sum_v<=300000 THEN '5. 201-300 т.р.'
				WHEN t1.Sum_v<=500000 THEN '6. 301-500 т.р.'
				WHEN t1.Sum_v<=1000000 THEN '7. 501-1000 т.р.'
				WHEN t1.Sum_v<=3000000 THEN '8. 1-3 млн.р.'
				WHEN t1.Sum_v>3000000 THEN '9. более 3 млн.р.'
				ELSE '10. -'
			END) AS 'Сумма выдачи по группам'n, 
			/* Срок кредита по группам */
			(CASE WHEN t1.'Cash/CC'n='КК' THEN t1.'Cash/CC'n ELSE t2.Srok_cred_groups END) AS 'Срок кредита по группам'n, 
			/* Тип клиента */
			(CASE WHEN t1.L_KI_VEB IS MISSING THEN 'Новый' ELSE 
				(CASE WHEN t1.L_KI_VEB>=6 AND t1.SUMM_DEBT>0 THEN 'Действующий, сформированная КИ' ELSE 
				(CASE WHEN t1.L_KI_VEB<6 AND t1.SUMM_DEBT>0 THEN 'Действующий, несформированная КИ' ELSE 
				(CASE WHEN t1.L_KI_VEB>=6 THEN 'Спящий/ушедший, сформированная КИ' ELSE
				(CASE WHEN t1.L_KI_VEB<6 THEN 'Спящий/ушедший, несформированная КИ' ELSE 'Прочий'
				END)END)END)END)END) AS 'Тип клиента'n, 
			/* Канал */
			(CASE WHEN t1.tochka_oforml_naimen CONTAINS 'DAF99' OR t1.tochka_oforml_format CONTAINS 'КОНТАКТ-ЦЕНТР'
					THEN '2. Канал телемаркетинг'
				WHEN t1.tochka_oforml_format CONTAINS 'ИНТЕРНЕТ-ЦЕНТР' 
					THEN '3. Интернет-канал'
				WHEN t1.tochka_otkr_format CONTAINS 'АГЕНТСК' 
							OR t1.tochka_otkr_format IN('АВТО УРМ','КРЕДИТНЫЙ БРОКЕР', 'ИНТЕРНЕТ - МАГАЗИН') 
					THEN '4. POS-канал'
				WHEN t1.tochka_otkr_format CONTAINS 'ИНТЕРНЕТ-БАНК' OR t1.tochka_vidachi_naimenovanie CONTAINS 'UTP00'
					THEN '5. Канал дистанционной выдачи кредитов'
				WHEN t1.tochka_otkr_format IN('АККРЕДИТАЦИЯ АГЕНТА','ПАКЕТНАЯ РЕГИСТРАЦИЯ ЗАЯВОК')
					THEN '6. -' 
				ELSE '1. Очный канал'
			END) AS 'Канал'n, 
			/* Территориальное управление */
			(CASE WHEN (t1.region IN ('КРАСНОЯРСКИЙ','БУРЯТИЯ','ИРКУТСКАЯ','ХАКАСИЯ','ЗАБАЙКАЛЬСКИЙ',
						'ИРКУТСКАЯ ОБЛ УСТЬ-ОРДЫНСКИЙ БУРЯТСКИЙ','ЧИТИНСКАЯ','ЗАБАЙКАЛЬСКИЙ КРАЙ АГИНСКИЙ БУРЯТСКИЙ'))
					THEN "Сибирское ТУ Домашняя зона"
				ELSE (CASE WHEN t1.TU='Сибирское ТУ' THEN "Сибирское ТУ Новая зона"
							ELSE t1.TU END ) END ) AS 'Территориальное управление'n, 
			t1.region AS 'Регион'n, 
			t1.TERRITORY_2015 AS 'МакроТУ'n, 
			t1.MAKROREGION AS 'МакроРегион'n, 
			t1.naselen_punkt AS 'Населенный пункт'n, 
			t1.priznak_ruki AS 'Ручное рассмотрение'n, 
			t1.strahovka AS 'Страховка'n, 
			t1.pd_gruppa AS 'PD по группам'n,
			t1.pd_gruppa_hd AS 'PD по группам (ХД)'n,
			/* ЕВ клиента по группам */
			(CASE WHEN (t1.'ЕВ клиента с учетом выдачи'n) is missing or (t1.'ЕВ клиента с учетом выдачи'n)=0 
					THEN "7. Не определено"
				WHEN (t1.'ЕВ клиента с учетом выдачи'n)<=5000 THEN "1. До 5000 р."
				WHEN (t1.'ЕВ клиента с учетом выдачи'n)<=10000 THEN "2. 5001-10000 р."
				WHEN (t1.'ЕВ клиента с учетом выдачи'n)<=15000 THEN "3. 10001-15000 р."
				WHEN (t1.'ЕВ клиента с учетом выдачи'n)<=20000 THEN "4. 15001-20000 р."
				WHEN (t1.'ЕВ клиента с учетом выдачи'n)<=25000 THEN "5. 20001-25000 р."
				WHEN (t1.'ЕВ клиента с учетом выдачи'n)>25000 THEN "6. Более 25000 р."
				ELSE "7. Не определено"  END) AS 'ЕВ клиента по группам'n, 
          /* ЕВ клиента ВЭБ+БКИ(с мар15) */
			(CASE WHEN (t1.'ЕВ клиента с учетом выдачи'n) is missing or (t1.'ЕВ клиента с учетом выдачи'n)=0 
						THEN "7. Не определено"
				WHEN (t1.'ЕВ клиента с учетом выдачи'n+COALESCE(t1.PAYMENT_BKI,0))<=5000 THEN "1. До 5000 р."
				WHEN (t1.'ЕВ клиента с учетом выдачи'n+COALESCE(t1.PAYMENT_BKI,0))<=10000 THEN "2. 5001-10000 р."
				WHEN (t1.'ЕВ клиента с учетом выдачи'n+COALESCE(t1.PAYMENT_BKI,0))<=15000 THEN "3. 10001-15000 р."
				WHEN (t1.'ЕВ клиента с учетом выдачи'n+COALESCE(t1.PAYMENT_BKI,0))<=20000 THEN "4. 15001-20000 р."
				WHEN (t1.'ЕВ клиента с учетом выдачи'n+COALESCE(t1.PAYMENT_BKI,0))<=25000 THEN "5. 20001-25000 р."
				WHEN (t1.'ЕВ клиента с учетом выдачи'n+COALESCE(t1.PAYMENT_BKI,0))>25000 THEN "6. Более 25000 р."
				ELSE "7. Не определено"  
			END) AS 'ЕВ клиента ВЭБ+БКИ(с мар15)'n, 
			/* Продолжительность КИ по группам */
			(CASE WHEN (t1.L_KI_VEB IS MISSING) OR (t1.L_KI_VEB<6) THEN "a. До 6 мес." 
				WHEN t1.L_KI_VEB<12 THEN "b. 6-11 мес."
				WHEN t1.L_KI_VEB<24 THEN "c. 12-23 мес."
				WHEN t1.L_KI_VEB<36 THEN "d. 24-35 мес."               
				WHEN t1.L_KI_VEB<60 THEN "e. 36-59 мес."               
				WHEN t1.L_KI_VEB>=60 THEN "f. 60 мес. и более"
				ELSE "g. Не определено"
			END) AS 'Продолжительность КИ по группам'n, 
			/* Прод-ть КИ по группам ВЭБ+БКИ */
			(CASE WHEN (t1.L_KI_VEB_BKI IS MISSING) OR (t1.L_KI_VEB_BKI<6) THEN "a. До 6 мес." 
				WHEN t1.L_KI_VEB_BKI<12 THEN "b. 6-11 мес."
				WHEN t1.L_KI_VEB_BKI<24 THEN "c. 12-23 мес."
				WHEN t1.L_KI_VEB_BKI<36 THEN "d. 24-35 мес."
				WHEN t1.L_KI_VEB_BKI<60 THEN "e. 36-59 мес."
				WHEN t1.L_KI_VEB_BKI>=60 THEN "f. 60 мес. и более"
				ELSE "g. Не определено"
			END) AS 'Прод-ть КИ по группам ВЭБ+БКИ'n, 
			/* Канал_2 */
			(CASE WHEN t1.tochka_vidachi_naimenovanie CONTAINS "UTP00" THEN "Дистанционная выдача" 
				ELSE (CASE WHEN t1.RETAIL_PRODUCT_NAME="ЕДИНЫЙ-ТОП" THEN "Топ-ап"
						WHEN t1.RETAIL_PRODUCT_NAME CONTAINS "ЛЕГКИЙ ПЛАТЕЖ" then "Топ-даун" 
				ELSE (CASE WHEN t1.tochka_oforml_format='ПАКЕТНАЯ РЕГИСТРАЦИЯ ЗАЯВОК' THEN "Preapproved" 
				ELSE (CASE WHEN t1.kanal_po_kk='Улица' THEN "Внешний спрос на КК" 
				ELSE (CASE WHEN t1.kanal_po_kk='КЭШ' THEN "Остальные КЭШ-кредиты"
						ELSE t1.kanal_po_kk END)
				END)END)END)
			END) AS 'Канал_2'n, 
			/* Бизнесс-процесс */
			(CASE WHEN t1.tochka_vidachi_naimenovanie contains 'UTP00' then "9. ДВК"
				WHEN t1.RETAIL_PRODUCT_NAME contains "ЛЕГКИЙ ПЛАТЕЖ" then "10. TOP DOWN"
				WHEN t1.RETAIL_PRODUCT_NAME="ЕДИНЫЙ-ТОП" then "8. TOP UP"
				WHEN t1.tochka_oforml_naimen contains "(PBK01)" then "7. Preapprove"
				WHEN t1.tochka_oforml_naimen contains "(KAS01)" or t1.TU="КАССА ВЭБ" then "6. Касса ВЭБ"
				WHEN t1.tochka_vidachi_naimenovanie contains 'KIB' then "5. Кредиты в ИБ"
				WHEN t1.SCORE_TREE_ROUTE_ID=1 or t1.SCORE_TREE_ROUTE_ID=35 then "4. Мгновенный кросс"
				WHEN t1.tochka_oforml_naimen contains "КОНТАКТ-ЦЕНТР" then "2. КЦ"
				WHEN t1.tochka_oforml_naimen contains "INT" then "3. Сайт"
				ELSE "1. Очное оформление" 
			END) AS 'Бизнесс-процесс'n, 
			t6.CLIENT_TYPE1,
			t6.CLIENT_TYPE4,
			t6.CLIENT_TYPE5,
			t6.CLIENT_TYPE6,
			/* Расстояние от ФМЖ до ДО */
			(case when t5.dist is missing then "10. Не определено"
				when t5.dist <=0.5 then "1. До 0,5 км"
				when t5.dist <=1 then "2. 0,5-1 км"
				when t5.dist <=1.5 then "3. 1-1,5 км"
				when t5.dist <=3 then "4. 1,5-3 км"
				when t5.dist <=10 then "5. 3-10 км"
				when t5.dist <=30 then "6. 10-30 км"
				when t5.dist <=50 then "7. 30-50 км"
				when t5.dist <=100 then "8. 50-100 км"
				else "9. более 100 км"
				end) AS 'Расстояние от ФМЖ до ДО'n, 
			/* Расстояние от ФМЖ до ДО 2 */
			(case
				when t5.dist is missing then "9. Не определено"
				when t5.dist <=0.5 then "1. До 0,5 км"
				when t5.dist <=1 then "2. 0,5-1 км"
				when t5.dist <=2 then "3. 1-2 км"
				when t5.dist <=5 then "4. 2-5 км"
				when t5.dist <=20 then "5. 5-20 км"
				when t5.dist <=40 then "6. 20-40 км"
				when t5.dist <=60 then "7. 40-60 км"
				else "8. более 60 км"
				end) AS 'Расстояние от ФМЖ до ДО 2'n,
			/* З/п заявленная по группам */
			(case when (t1.SALARY_SUM is missing or t1.SALARY_SUM=0) then "13. Неизвестно"
				when t1.SALARY_SUM <=5000 then "1. до 5 т.р."
				when t1.SALARY_SUM <=10000 then "2. 6-10 т.р."
				when t1.SALARY_SUM <=20000 then "3. 11-20 т.р."
				when t1.SALARY_SUM <=30000 then "4. 21-30 т.р."
				when t1.SALARY_SUM <=40000 then "5. 31-40 т.р."
				when t1.SALARY_SUM <=50000 then "6. 41-50 т.р."
				when t1.SALARY_SUM <=60000 then "7. 51-60 т.р."
				when t1.SALARY_SUM <=70000 then "8. 61-70 т.р."
				when t1.SALARY_SUM <=80000 then "9. 71-80 т.р."
				when t1.SALARY_SUM <=90000 then "10. 81-90 т.р."
				when t1.SALARY_SUM <=100000 then "11. 91-100 т.р."
				else "12. более 100 т.р."
				end) AS 'З/п заявленная по группам'n,
			/* З/п из классиф. по группам */
			(case when (t1.WORKS_POST_SALARY is missing or t1.WORKS_POST_SALARY=0) then "13. Неизвестно"
				when t1.WORKS_POST_SALARY <=5000 then "1. до 5 т.р."
				when t1.WORKS_POST_SALARY <=10000 then "2. 6-10 т.р."
				when t1.WORKS_POST_SALARY <=20000 then "3. 11-20 т.р."
				when t1.WORKS_POST_SALARY <=30000 then "4. 21-30 т.р."
				when t1.WORKS_POST_SALARY <=40000 then "5. 31-40 т.р."
				when t1.WORKS_POST_SALARY <=50000 then "6. 41-50 т.р."
				when t1.WORKS_POST_SALARY <=60000 then "7. 51-60 т.р."
				when t1.WORKS_POST_SALARY <=70000 then "8. 61-70 т.р."
				when t1.WORKS_POST_SALARY <=80000 then "9. 71-80 т.р."
				when t1.WORKS_POST_SALARY <=90000 then "10. 81-90 т.р."
				when t1.WORKS_POST_SALARY <=100000 then "11. 91-100 т.р."
				else "12. более 100 т.р."
				end) AS 'З/п из классиф. по группам'n,
			/* PTI по заявл. доходу ВЭБ */
			(CASE WHEN t1.SALARY_SUM<>0 and t1.SALARY_SUM<1000000 AND t1.SALARY_SUM NOT IS MISSING 
							AND t1.'ЕВ клиента с учетом выдачи'n NOT IS MISSING AND t1.'ЕВ клиента с учетом выдачи'n<>0
					THEN (t1.'ЕВ клиента с учетом выдачи'n)/t1.SALARY_SUM 
				ELSE . 
			END) AS 'PTI по заявл. доходу ВЭБ'n,
			/* PTI по модел. доходу ВЭБ */
			(CASE WHEN t1.WORKS_POST_SALARY<>0 and t1.WORKS_POST_SALARY<1000000 AND t1.WORKS_POST_SALARY NOT IS MISSING 
						AND t1.'ЕВ клиента с учетом выдачи'n NOT IS MISSING AND t1.'ЕВ клиента с учетом выдачи'n<>0
					THEN (t1.'ЕВ клиента с учетом выдачи'n)/t1.WORKS_POST_SALARY 
				ELSE . 
			END) AS 'PTI по модел. доходу ВЭБ'n,
			/* PTI заявл.дох. ВЭБ+БКИ(с мар15) */
			(CASE WHEN t1.SALARY_SUM<>0 and t1.SALARY_SUM<1000000 and t1.SALARY_SUM not is missing 
					AND t1.'ЕВ клиента с учетом выдачи'n NOT IS MISSING AND t1.'ЕВ клиента с учетом выдачи'n<>0
				THEN SUM(t1.'ЕВ клиента с учетом выдачи'n,t1.PAYMENT_BKI)/t1.SALARY_SUM 
				ELSE . 
			END) AS 'PTI заявл.дох. ВЭБ+БКИ(с мар15)'n,
			/* PTI модел.дох. ВЭБ+БКИ(с мар15) */
			(CASE WHEN t1.WORKS_POST_SALARY<>0 and t1.WORKS_POST_SALARY<1000000 AND t1.WORKS_POST_SALARY not is missing 
					and t1.'ЕВ клиента с учетом выдачи'n not is missing and t1.'ЕВ клиента с учетом выдачи'n<>0
					THEN SUM(t1.'ЕВ клиента с учетом выдачи'n,t1.PAYMENT_BKI)/t1.WORKS_POST_SALARY 
				ELSE . 
			END) AS 'PTI модел.дох. ВЭБ+БКИ(с мар15)'n,
			/* Кол-во кредитов ВЭБ */
			(t1.CNT_CREDIT_VEB) AS 'Кол-во кредитов ВЭБ'n, 
			/* Кол-во кредитов ВЭБ+БКИ */
			(CASE WHEN (t1.CNT_CREDIT_VEB_BKI-t1.CNT_CREDIT_VEB)<0 then 0 else (t1.CNT_CREDIT_VEB_BKI-t1.CNT_CREDIT_VEB) end) AS 'Кол-во кредитов ВЭБ+БКИ'n, 
			/* Доля долга ВЭБ в общем долге */
			( t1.SUMMA_DEBT_VEB_RATE/SUM(t1.SUMMA_DEBT_VEB_RATE
									,IFN(t1.SUMMA_DEBT_BKI_RATE<0, 0, t1.SUMMA_DEBT_BKI_RATE))
			) AS 'Доля долга ВЭБ в общем долге'n 
		FROM &CURTMPLIB..VIDA_OPEN t1
		LEFT JOIN &WRITELIB..PB_PERIODGROUPS t2 ON (t1.PERIOD = t2.PERIOD)
		LEFT JOIN URD.PERS_OTD_MINDIST_SECOND_FINAL t5 ON (t1.PCCR_PERSON_ID = t5.PERSON_ID)
		/*LEFT JOIN COMMON.REQUESTS t6 ON (t1.REQUEST_ID = t6.REQUEST_ID)*/
		LEFT JOIN COMMON.PERSONS_PORTF_VEB_SHORT AS t6 
			ON (t1.PCCR_PERSON_ID = t6.PERSON_ID 
			AND INTNX('month',DATEPART(t1.OPEN_DATE),0,'end')=t6.LAST_DT_MONTH_SREZ)
;

QUIT;


*=2=====Дополнительный шаг определение извлеченных данных для логирования;
	*---PARAM_DAY;
PROC SQL;
SELECT MIN(REQ_CREATED_DATE) FORMAT=DATETIME19. INTO :min_DATE_param
	FROM &WRITELIB..PARAM_DAY;
SELECT MAX(REQ_CREATED_DATE) FORMAT=DATETIME19. INTO :max_DATE_param
	FROM &WRITELIB..PARAM_DAY;
SELECT IFN(MISSING(COUNT(*)), 0, COUNT(*)) INTO :cnt_DATE_param
	FROM (SELECT DISTINCT REQ_CREATED_DATE FROM &WRITELIB..PARAM_DAY);

	*---FACTORS;
SELECT MIN(OPEN_DATE) FORMAT=DATETIME19. INTO :min_DATE_factors
	FROM &CURTMPLIB..FACTOR;
SELECT MAX(OPEN_DATE) FORMAT=DATETIME19. INTO :max_DATE_factors
	FROM &CURTMPLIB..FACTOR;
SELECT IFN(MISSING(COUNT(*)), 0, COUNT(*)) INTO :cnt_DATE_factors
	FROM (SELECT DISTINCT OPEN_DATE FROM &CURTMPLIB..FACTOR);
QUIT;

PROC SQL;
	DELETE FROM &WRITELIB..FACTORS_FOR_OPEN_LOANS where (OPEN_DATE>=&add_condition.);
QUIT;

*=2=======================ТАБЛИЦА 4 FIN ===========================;
PROC APPEND BASE=&WRITELIB..FACTORS_FOR_OPEN_LOANS
		data=&CURTMPLIB..factor FORCE;
RUN;

PROC SORT DATA=&WRITELIB..FACTORS_FOR_OPEN_LOANS NODUP;
	BY SRC_CRED_ID;
RUN;

DATA &WRITELIB..FACTORS_FOR_OPEN_LOANS;
	SET &WRITELIB..FACTORS_FOR_OPEN_LOANS;
RUN;

PROC SQL;
	*DROP TABLE &CURTMPLIB..SOC;
	*DROP TABLE &CURTMPLIB..SOC_REQ_CUBE1;
	*DROP TABLE &CURTMPLIB..REQESTS_2;
	*DROP TABLE &CURTMPLIB..PARAM;
	*DROP TABLE &CURTMPLIB..VIDA_OPEN;
	*DROP TABLE &CURTMPLIB..FACTORS_FOR_OPEN_LOANS;
	*DROP TABLE &CURTMPLIB..EV;
	*DROP TABLE &CURTMPLIB..FACTOR;

QUIT;


%PUT Результат обновления данных в таблице &WRITELIB..PARAM с &m_t_curDate. по &m_t_prevDate.;
%PUT Фактически обновлено с &min_DATE_param. по &max_DATE_param.;

%PUT Результат обновления данных в таблице &WRITELIB..FACTORS_FOR_OPEN_LOANS (&add_condition.);
%PUT Фактически добавлено с &min_DATE_factors. по &max_DATE_factors. (&cnt_DATE_factors.);


*=========Макрообъявления для отправления сообщений==========;
%macro M_Singature;
	Put "</br>";
	Put "С Уважением, Трахачев Вячеслав Валерьевич </br>";
	Put "ПАО Восточный экспресс банк </br>";
	Put "тел. вн. 37443 </br>";
%mend M_Singature;

%macro SendEmail(fromUser=vvtrahachev@orient.root.biz, mes_about_rezult=);
	FileName SENDMAIL Email FROM="&fromUser." TYPE='text/html'
		to=("vvtrahachev@orient.root.biz" "nvlyalin@express-bank.ru" "opperegudov@orient.root.biz" "vpvershinina@express-bank.ru" "uvteploukhova@orient.root.biz")
		SUBJECT="Результат обновления PARAM и FACTORS %sysfunc(date(),ddmmyy9.)";

	Data _NULL_;
		File SENDMAIL;

		Put "Добрый день! </br>";
		Put "&mes_about_rezult.";
		%M_Singature;
	Run;
%mend SendEmail;

%SendEmail(fromUser=SAS_Scheduler@URD
	, mes_about_rezult=Результат обновления данных в таблице &WRITELIB..PARAM: (&cnt_DATE_param. дн. ) &min_DATE_param.-&max_DATE_param. </br></br>Добавлено в &WRITELIB..FACTORS_FOR_OPEN_LOANS: (&cnt_DATE_factors. дн. ) &min_DATE_factors.-&max_DATE_factors. );

*%PUT Результат обновления данных в таблице &WRITELIB..PARAM с &m_t_curDate. по &m_t_prevDate. </br>
 Фактически обновлено с <b>&min_DATE_param. по &max_DATE_param.</b>  </br>
Результат обновления данных в таблице &WRITELIB..FACTORS_FOR_OPEN_LOANS (&add_condition.)  </br>
Фактически добавлено с <b>&min_DATE_factors. по &max_DATE_factors.</b>;
