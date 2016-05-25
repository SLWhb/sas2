/*ССЫЛКИ НА статью про очередь
  http://www.foxbase.ru/oracle-programming/oracle-streams-advanced-queuing-bystryy-start.htm
  http://www.foxbase.ru/oracle-programming/oracle-advanced-queuing-notification.htm
*/
--=============прочитать из очереди в цикле
--SET serveroutput ON


	DECLARE
	deq_options     DBMS_AQ.dequeue_options_t;
	msg_properties  DBMS_AQ.message_properties_t;
	msg_handle      RAW(16);
	msg             msg_PKK;
	BEGIN

	FOR i IN 1..500 LOOP
		deq_options.navigation := DBMS_AQ.FIRST_MESSAGE;
		deq_options.consumer_name := 'SNAUSER';
		DBMS_AQ.DEQUEUE(
		   queue_name          =>     'QUEUE_PKK',
		   dequeue_options     =>     deq_options,
		   message_properties  =>     msg_properties,
		   payload             =>     msg,
		   msgid               =>     msg_handle);


		DBMS_OUTPUT.PUT_LINE('msg_Id: '|| msg.msg_Id);
		DBMS_OUTPUT.PUT_LINE('msg_date: '||msg.msg_date);
		DBMS_OUTPUT.PUT_LINE('request_id: '||msg.request_id);
		/*DBMS_OUTPUT.PUT_LINE('old_status_id: '||msg.old_status_id);
		DBMS_OUTPUT.PUT_LINE('v_new_status_id: '||msg.v_new_status_id);*/
				

		insert into SNA_dequeue_msg
		values (msg.msg_Id,msg.msg_date, msg.request_id, msg.old_status_id,msg.v_new_status_id,systimestamp);

		COMMIT;
		END LOOP;
	END;
	/


--======================Проверка заявки была ли она на ручном рассмотрении
	SELECT * FROM (select aa.*,
		   dense_rank() over (partition by aa.request_id order by aa.react_date) row_num --упорядочиваем реакции андеррайтеров 
	 from kredit.c_request_react@DBLINK_PKK aa
	left join kredit.groups@DBLINK_PKK bb on aa.react_group_id = bb.groups_id 
		and BB.SERVICES_ID in (3, 5, 6) -- группы андеррайтеров
		and AA.REACT_USER_ID <> 1512142 -- исключаем скоринг
	where aa.request_id = 47495932
	)
	where row_num = 1; 
	/
	-- выбираем первую реакцию андеррайтера по заявке


	SELECT * 
	  FROM (select aa.REQUEST_ID as rid, /*aa.request_id,*/ aa.react_group_id, gg.SERVICES_ID, aa.REACT_USER_ID
				,aa.react_date
				, aa.request_new_status_id
				, aa.request_old_status_id
			   ,dense_rank() over (partition by aa.request_id order by aa.react_date) row_num --упорядочиваем реакции андеррайтеров 
		  FROM /*SNAUSER.SNA_DEQUEUE_MSG sdm
		  LEFT JOIN*/ kredit.c_request_react@DBLINK_PKK aa
		  /*ON sdm.REQUEST_ID=aa.REQUEST_ID*/
		  LEFT JOIN  kredit.groups@DBLINK_PKK gg
			ON aa.react_group_id = gg.groups_id and gg.SERVICES_ID in (3, 5, 6)
								  and aa.REACT_USER_ID <> 1512142
		  WHERE /*sdm.STAMP>='17.09.15 00:00:08'*/ aa.REQUEST_ID BETWEEN 50018453 AND 51413818 
									AND REACT_DATE BETWEEN TO_DATE('30-07-2015','dd-mm-yyyy') AND TO_DATE('31-07-2015','dd-mm-yyyy')
			   /* AND EXISTS(SELECT * FROM  kredit.groups@DBLINK_PKK bb WHERE aa.REQUEST_ID=sdm.REQUEST_ID 
								  AND aa.react_group_id = bb.groups_id 
								  and BB.SERVICES_ID in (3, 5, 6) -- группы андеррайтеров
								  and AA.REACT_USER_ID <> 1512142) -- исключаем скоринг*/
				AND (AA.REACT_USER_ID <> 1512142 and gg.SERVICES_ID in (3, 5, 6))
	) 
	where row_num = 1; 
	/

--================просто проверки
alter system kill session '57,45511';

insert into SNAUSER.SNA_LINKS_
(/*REQUEST_ID, PERSON_ID,*/ LABEL/*, LINK_TYPE, GROUP, WEIGTH*/) values ('test');

EXECUTE SNAUSER.DATA_PKK(50146054);


SELECT * FROM SNAUSER.APPLICATIONS WHERE REQUEST_ID=50146054;
SELECT * FROM SFF.APPLICATIONS WHERE REQUEST_ID=50146054;
SELECT * FROM SNAUSER.SNA_LINKS WHERE PERSON_ID='10000356' /*AND LABEL IN'ООО "ТД "СЛАВЯНСКИЙ ХЛЕБ"'*/;

EXECUTE DBMS_SCHEDULER.DROP_JOB('SYS.DBMS_AQADM_SYS.REGISTER_DRIVER', TRUE);

SELECT * from sys.reg$ ;


--==============регистрация чтения с очереди
DBMS_AQ.REGISTER (
   reg_list     IN  SYS.AQ$_REG_INFO_LIST,
   count        IN  NUMBER);
   
DBMS_AQ.UNREGISTER (
   reg_list     IN  SYS.AQ$_REG_INFO_LIST,
   reg_count    IN  NUMBER);
   
   
BEGIN
  DBMS_AQ.UNREGISTER (SYS.AQ$_REG_INFO_LIST
  (
  SYS.AQ$_REG_INFO('queue_PKK:SNAUSER',DBMS_AQ.NAMESPACE_AQ,
  'plsql://DEMO_QUEUE_CALLBACK_PROCEDURE',HEXTORAW('FF'))
  ),1
  );
END;
/


--==============ОПЕРАЦИИ С ОЧЕРЕДЬЮ
		select * from SNA_DEQUEUE_MSG_NOAKT WHERE REQUEST_ID=111111111 ORDER BY STAMP DESC;

		select * from SNA_LINKS_;

		select MAX(STAMP) from SNA_DEQUEUE_MSG;

		select sdm.REQUEST_ID, sdm.STAMP, sdm.msg_id, app.REQUEST_ID AS RID_APP 
			,(SELECT COUNT(*) FROM SNA_DEQUEUE_MSG sdm_c 
						WHERE STAMP>='13.12.15 00:00:08' 
							  AND NOT EXISTS(SELECT * FROM APPLICATIONS WHERE REQUEST_ID=sdm_c.REQUEST_ID)) cnt_not_exists
			,(SELECT COUNT(*) FROM SNA_DEQUEUE_MSG sdm_c 
						WHERE STAMP>='25.12.15 00:00:08' 
							  AND EXISTS(SELECT * FROM APPLICATIONS WHERE REQUEST_ID=sdm_c.REQUEST_ID)) cnt_exists
		  from SNA_DEQUEUE_MSG sdm 
		  LEFT OUTER JOIN SNAUSER.APPLICATIONS app
		  ON sdm.REQUEST_ID=app.REQUEST_ID /*and app.REQUEST_ID IS NULL*/
		  WHERE sdm.STAMP>='13.12.15 00:41:08'
			  /* AND app.REQUEST_ID IS NULL */;


		SELECT MSGID, USER_DATA, (USER_DATA).REQUEST_ID, ENQ_TIME, a.state, PRIORITY, enq_uid
			FROM AQ_TAB a 
			WHERE /*ENQ_TIME>=TO_DATE('05-08-2015','dd-mm-yyyy') */
			  (USER_DATA).REQUEST_ID IN(52973649) 
			  /*AND*/ /*(USER_DATA).REQUEST_ID=51563476*/
			  /*AND STATE=3*/
			  ORDER BY ENQ_TIME ;
		/*51563577*/

		SELECT USER_DATA, (USER_DATA).REQUEST_ID, ENQ_TIME, a.state 
			FROM AQ_TAB a 
			WHERE ENQ_TIME>='13.12.15 09:00:08,021476000'
			  /*(USER_DATA).REQUEST_ID=51563573*/  /*AND*/ /*(USER_DATA).REQUEST_ID=51563476*/
			  /*AND STATE=3*/
			  ORDER BY ENQ_TIME desc ;

		--статистика поминутная
		SELECT COUNT((USER_DATA).REQUEST_ID) cnt_RID
			  /*ENQ_TIME*/
			  ,TO_CHAR(TO_DATE(TO_CHAR(ENQ_TIME, 'dd-mm-yyyy hh24:mi')
								, 'dd-mm-yyyy hh24:mi')+(10/24)
					  , 'hh24:mi') as date_minute
			/*,TO_CHAR(ENQ_TIME, 'dd-mm-yyyy hh24')||' час' TIME_minute*/
			FROM AQ_TAB a 
      --смещение на 10 часов назад. Т.е. 21.09.15 13:59:59 - это 20.09.15 23:59:59
			WHERE ENQ_TIME>='12.12.15 14:00:00' AND ENQ_TIME<'26.12.15 14:00:00'
			   
			  GROUP BY TO_CHAR(TO_DATE(TO_CHAR(ENQ_TIME, 'dd-mm-yyyy hh24:mi')
								, 'dd-mm-yyyy hh24:mi')+(10/24)
					  , 'hh24:mi')
			  ORDER BY TO_CHAR(TO_DATE(TO_CHAR(ENQ_TIME, 'dd-mm-yyyy hh24:mi')
								, 'dd-mm-yyyy hh24:mi')+(10/24)
					  , 'hh24:mi') ;

		SELECT distinct "SUBSCRIBER#", COUNT(*) cnt 
			FROM "AQ$_AQ_TAB_H" a 
			  GROUP BY "SUBSCRIBER#" ;

		SELECT distinct a.state, COUNT(*) cnt 
			FROM AQ_TAB a 
			WHERE ENQ_TIME>=TO_DATE('13-12-2015','dd-mm-yyyy')
			/*WHERE (USER_DATA).REQUEST_ID=51563476*/
			  GROUP BY state ;

		SELECT * FROM APPLICATIONS WHERE REQUEST_ID IN(52959489,52959517,52959538);

		SELECT REQUEST_ID, CREATED_GROUP_ID, STATUS_ID, score_tree_route_id
			FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID IN(51513976);
		SELECT * FROM KREDIT.C_REQUEST_REACT@DBLINK_PKK WHERE REQUEST_ID IN(51513976);

		select MIN(STAMP), MAX(STAMP) from SNA_DEQUEUE_MSG WHERE REQUEST_ID>50518130;



		SELECT object_name, object_type
			FROM   user_objects
			WHERE  object_name like '%QUEUE%';
			

		/*alter system kill session '48,3153';
		alter system kill session '442,22187';
		alter system kill session '70,48917';*/


		SELECT COUNT(*) cnt /*MSGID, USER_DATA, (USER_DATA).REQUEST_ID, ENQ_TIME, a.state, PRIORITY, enq_uid*/
			FROM AQ_TAB a 
			WHERE ENQ_TIME>TO_DATE('15-09-2015','dd-mm-yyyy') AND ENQ_TIME<TO_DATE('16-09-2015','dd-mm-yyyy') and
			  EXISTS(SELECT * FROM SNA_DEQUEUE_MSG WHERE REQUEST_ID=(a.USER_DATA).REQUEST_ID)
			  /*AND*/ /*(USER_DATA).REQUEST_ID=51563476*/

			  ORDER BY ENQ_TIME ;


--====================запросы с тестовыми возможностями
	select *
		from SNAUSER.AQ_TAB
		model dimension by(dummy) measures(0 v1,0 v2,0 v3,0 v4) rules(
			 v1[any] = TO_CHAR(REQUEST_ID, 'dd-mm-yyyy hh24:mi')
			,v2[any] = v1[cv()]||'new'
			,v3[any] = v1[cv()]+v2[cv()]
			,v4[any] = v2[cv()]+v3[cv()]-v1[cv()]
		);

		select *
		from dual
		model dimension by(dummy) measures(0 v1,0 v2,0 v3,0 v4) rules(
			 v1[any] = 2*2
			,v2[any] = v1[cv()]*3
			,v3[any] = v1[cv()]+v2[cv()]
			,v4[any] = v2[cv()]+v3[cv()]-v1[cv()]
		);


		SELECT * FROM RP_BNK WHERE BNK_CODE IN(90001);

		SELECT DISTINCT BNK_CODE, SYSCOMMENT, SYSCOMMENT_SHOW FROM SCORING.D_BNK@SPR ORDER BY BNK_CODE;




		/*CREATE TABLE TRAKHACH.C_REQUEST_REACT_SALE as*/
			SELECT C_REQ.REQUEST_ID
				,C_REQ.REQUEST_REACT_ID
				,C_REQ.REACT_GROUP_ID
				,C_REQ.REQUEST_NEW_STATUS_ID
				,C_REQ.REQUEST_CREDIT_ID
				,GROUPS_DIM.SALE_ID
			,GROUPS_DIM.SALE_NAME
			,GROUPS_DIM.GROUPS_NAME
			FROM KREDIT.C_REQUEST_REACT@DBLINK_PKK C_REQ
			LEFT OUTER JOIN WH_DVTB.PCCR_GROUPS_DIM@HD GROUPS_DIM 
				ON (GROUPS_DIM.DIMENSION_KEY = C_REQ.REACT_GROUP_ID)
			WHERE C_REQ.REQUEST_ID=43862824
			ORDER BY C_REQ.REQUEST_ID, C_REQ.REQUEST_REACT_ID;
		  
			SELECT ID, REQUEST_ID, REQUEST_REACT_ID, FORMAT_GROUPS, FORMAT_GROUPS_OFORM
				/*,C_REQ.GRO*/
			FROM SCORING.L_FLOW_PRICE_LEVEL@SPR C_REQ
			WHERE C_REQ.REQUEST_ID=43862824;
		  
		SELECT * FROM 
		(SELECT cr.REQUEST_ID, crr.REQUEST_REACT_ID
		  ,TO_CHAR(cr.CREATED_DATE, 'dd-mm-yyyy hh24:mi:ss') as cr
		  ,TO_CHAR(cr.MODIFICATION_DATE, 'dd-mm-yyyy hh24:mi:ss') as modif
		  ,TO_CHAR(crr.REACT_DATE, 'dd-mm-yyyy hh24:mi:ss') as react
		  ,ROUND(cr.MODIFICATION_DATE-crr.REACT_DATE, 3) as dist_date
		  ,dense_rank() over (partition by cr.request_id order by crr.react_date DESC) row_num
		  FROM KREDIT.C_REQUEST@DBLINK_PKK cr
			LEFT OUTER JOIN KREDIT.C_REQUEST_REACT@DBLINK_PKK crr
				ON (crr.REQUEST_ID = cr.REQUEST_ID)
			WHERE cr.REQUEST_ID BETWEEN 52000000 AND 52900000)
		  WHERE ROW_NUM=1 AND NOT react=modif AND ABS(DIST_DATE)>0
			;
			
--ДОБАВЛЕНИЕ В ОЧЕРЕДЬ  
DECLARE
    enqueue_options DBMS_AQ.enqueue_options_t;
    msg_properties  DBMS_AQ.message_properties_t;
    msg_handle      RAW(16);
    msg             MSG_PKK;
BEGIN
    --SNAUSER.MSG_PKK('201510161417026569657936825','2015-10-16 00:00:00.0',54089620,10,2)
    msg := MSG_PKK('201510161417026569657936825','24.12.2015', 111111111,1,10);
    DBMS_AQ.ENQUEUE(
       queue_name              => 'queue_PKK',
       enqueue_options         => enqueue_options,
       message_properties      => msg_properties,
       payload                 => msg,
       msgid                   => msg_handle);
    COMMIT;
END;
/

--=======================
CREATE OR REPLACE TYPE AQ_PKK_SNA_TYPE AS OBJECT
(
  msg_id VARCHAR2(50)
, msg_date DATE
, request_id NUMBER
, objects_id NUMBER
, score_tree_route_id NUMBER
, created_group_id NUMBER
, type_request_id NUMBER
, old_status_id NUMBER
, new_status_id NUMBER
);
/
-- создание таблицы очередей

BEGIN
  DBMS_AQADM.CREATE_QUEUE_TABLE(queue_table => 'AQ_PKK_SNA_TBL', queue_payload_type => 'AQ_PKK_SNA_TYPE', multiple_consumers => TRUE);
END;
/
-- создаем собственно очередь

BEGIN
  DBMS_AQADM.DROP_QUEUE(queue_name => 'AQ_PKK_QUEUE', queue_table => 'AQ_PKK_TBL');
END;
/

  /*остановка очереди*/
begin 
  dbms_aqadm.start_queue (queue_name => 'AQ_PKK_SNA'); 
end;
/

/*остановка очереди, удаление очереди, удаление таблицы, удаление типа */
begin
  dbms_aqadm.stop_queue(queue_name  => 'AQ_PKK_QUEUE');
  dbms_aqadm.drop_queue(queue_name  => 'AQ_PKK_QUEUE');
  dbms_aqadm.drop_queue_table(queue_table => 'AQ_PKK_TBL');
  execute immediate 'drop type AQ_PKK_TYPE';
end;
/


select * from AQ$AQ_PKK_TBL;

EXECUTE dbms_aqadm.drop_queue_table (queue_table        => 'SNAUSER.AQ_PKK_TBL');

DECLARE     
  subs sys.aq$_agent; 
BEGIN     
  subs :=  sys.aq$_agent('SNAUSER', NULL, NULL);     
  DBMS_AQADM.ADD_SUBSCRIBER(        queue_name  =>  'aq_pkk_sna',        subscriber  =>  subs); 
END;
/



BEGIN
  DBMS_AQ.UNREGISTER (SYS.AQ$_REG_INFO_LIST
  (SYS.AQ$_REG_INFO('"SNAUSER"."AQ_PKK_SNA":"SCHEDULER$_EVENT_AGENT"',DBMS_AQ.NAMESPACE_AQ,
  'plsql://sys.dbms_isched.event_notify',HEXTORAW('FF'))
  ), 1);
END;
/


--=========================================
create TABLE SNAUSER.AQ_PKK_SNA_LOG_MESSAGE
  (MSG_ID VARCHAR2(50)
, MSG_DATE DATE
, REQUEST_ID NUMBER
, OBJECTS_ID NUMBER
, SCORE_TREE_ROUTE_ID NUMBER
, CREATED_GROU_ID NUMBER
, TYPE_REQUEST_ID NUMBER
, OLD_STATUS_ID NUMBER
, NEW_STATUS_ID NUMBER
, DATE_INSERT TIMESTAMP
);



select * from AQ$AQ_PKK_SNA_TBL;
select COUNT(*) from AQ$AQ_PKK_SNA_TBL;

select COUNT(*) from AQ$AQ_TAB;


-- Выборка 8 сообщений
declare
  r_dequeue_options     DBMS_AQ.DEQUEUE_OPTIONS_T;
  r_message_properties  DBMS_AQ.MESSAGE_PROPERTIES_T;
  v_message_handle      RAW(16);
  p_MSG                 AQ_PKK_SNA_TYPE;
  
  /*l_msg_id raw(16);
  l_deq_opt dbms_aq.dequeue_options_t;
  l_msg_prop dbms_aq.message_properties_t;
  l_payload AQ_PKK_SNA_TYPE;*/
begin
   r_dequeue_options.consumer_name := 'STATUS';
   --r_dequeue_options.wait := DBMS_AQ.NO_WAIT; -- =DBMS_AQ.NO_WAIT - ставим отсутсвие ожидания 
  for i in 1..1 loop
    dbms_aq.dequeue(
      queue_name         => 'AQ_PKK_SNA',
      --queue_name         => 'QUEUE_PKK',
      dequeue_options    => r_dequeue_options,
      message_properties => r_message_properties,
      payload            => p_MSG,
      msgid              => v_message_handle
    );
    dbms_output.put_line(p_MSG.request_id);
   INSERT INTO SNAUSER.AQ_PKK_SNA_LOG_MESSAGE (msg_Id
                  ,msg_date
                  ,request_id
                  ,objects_id 
                  ,score_tree_route_id 
                  ,created_group_id 
                  ,type_request_id
                  ,old_status_id
                  ,new_status_id
                  ,DATE_INSERT
                  ,CONSUMER_NAME)
            VALUES (p_MSG.msg_Id
                  ,p_MSG.msg_date
                  ,p_MSG.request_id
                  ,p_MSG.objects_id 
                  ,p_MSG.score_tree_route_id 
                  ,p_MSG.created_group_id 
                  ,p_MSG.type_request_id
                  ,p_MSG.old_status_id
                  ,p_MSG.new_status_id
                  ,systimestamp
                  ,r_dequeue_options.consumer_name) ;
  end loop;
  commit;
end;
/

select COUNT(*) from AQ$AQ_PKK_SNA_TBL;
select COUNT(*) from AQ$AQ_TAB;

SET SERVEROUTPUT ON
BEGIN
  DBMS_OUTPUT.ENABLE;
  DBMS_OUTPUT.PUT_LINE('Вывод сообщений настроен :)');
END;
/


CREATE OR REPLACE PROCEDURE SNAUSER.PR$AQ_PKK_SNA_DEQUEUE
  r_dequeue_options     DBMS_AQ.DEQUEUE_OPTIONS_T;
  r_message_properties  DBMS_AQ.MESSAGE_PROPERTIES_T;
  v_message_handle      RAW(16);
  p_MSG                 AQ_PKK_SNA_TYPE;
  
  /*l_msg_id raw(16);
  l_deq_opt dbms_aq.dequeue_options_t;
  l_msg_prop dbms_aq.message_properties_t;
  l_payload AQ_PKK_SNA_TYPE;*/
begin
   r_dequeue_options.consumer_name := 'STATUS';
   --r_dequeue_options.wait := DBMS_AQ.NO_WAIT; -- =DBMS_AQ.NO_WAIT - ставим отсутсвие ожидания 
  -for i in 1..1 loop
    dbms_aq.dequeue(
      queue_name         => 'AQ_PKK_SNA',
      --queue_name         => 'QUEUE_PKK',
      dequeue_options    => r_dequeue_options,
      message_properties => r_message_properties,
      payload            => p_MSG,
      msgid              => v_message_handle
    );
    --dbms_output.put_line(p_MSG.request_id);
   INSERT INTO SNAUSER.AQ_PKK_SNA_LOG_MESSAGE (msg_Id
                  ,msg_date
                  ,request_id
                  ,objects_id 
                  ,score_tree_route_id 
                  ,created_group_id 
                  ,type_request_id
                  ,old_status_id
                  ,new_status_id
                  ,DATE_INSERT
                  ,CONSUMER_NAME)
            VALUES (p_MSG.msg_Id
                  ,p_MSG.msg_date
                  ,p_MSG.request_id
                  ,p_MSG.objects_id 
                  ,p_MSG.score_tree_route_id 
                  ,p_MSG.created_group_id 
                  ,p_MSG.type_request_id
                  ,p_MSG.old_status_id
                  ,p_MSG.new_status_id
                  ,systimestamp
                  ,r_dequeue_options.consumer_name) ;
  --end loop;
  commit;
end;
/


SELECT * FROM AQ_PKK_SNA_LOG_MESSAGE WHERE NOT SFF.FN_IS_HAND_RID(REQUEST_ID)=1 AND CREATED_GROUP_ID^=11455;
--51413417 50808707
SELECT REQUEST_ID, REQUEST_REACT_ID, REQUEST_OLD_STATUS_ID, REQUEST_NEW_STATUS_ID, REACT_DATE
  FROM KREDIT.C_REQUEST_REACT@DBLINK_PKK WHERE REQUEST_ID=50808707;
  
 /*
1. Убираем с регистарции процессы считывания UNREGISTER
-неактуальное предположение 2. Делаем stop dequeue для нужных очередей
2. Ждем когда процесс DBMS_SHEDULER исчезнет (около 2 минут, убивать в крайнем случае)
3. Ставим нужный процесс заново на регитсрацию
 - Обращаем внимание на пользователя считывающего сообщение
*/

SET SERVEROUTPUT ON
BEGIN
  DBMS_OUTPUT.ENABLE;
  DBMS_OUTPUT.PUT_LINE('Вывод сообщений настроен :)');
END;
/

--92 шт. 89
select COUNT(*) from AQ$AQ_PKK_SNA_TBL;
select COUNT(*) from AQ$AQ_TAB;


SELECT SUM((CASE WHEN (USER_DATA).CREATED_GROUP_ID=11455 THEN 1 ELSE 0 END)) as cnt_RID_11455
    ,SUM((CASE WHEN (USER_DATA).CREATED_GROUP_ID^=11455 THEN 1 ELSE 0 END)) as cnt_RID_NOT_11455
    ,COUNT((USER_DATA).REQUEST_ID) cnt_RID
			  /*ENQ_TIME*/
			  ,TO_CHAR(TO_DATE(TO_CHAR(ENQ_TIME, 'dd-mm-yyyy hh24')
								, 'dd-mm-yyyy hh24')+(10/24)
					  , 'mm-dd hh24')||':00' as date_minute
			/*,TO_CHAR(ENQ_TIME, 'dd-mm-yyyy hh24')||' час' TIME_minute*/
			FROM SNAUSER.AQ$AQ_PKK_SNA_TBL a 
      --смещение на 10 часов назад. Т.е. 21.09.15 13:59:59 - это 20.09.15 23:59:59
			WHERE ENQ_TIME>='05.05.16 00:00:00' AND ENQ_TIME<'30.05.16 14:00:00'
			   /*AND (USER_DATA).CREATED_GROUP_ID=11455*/
			  GROUP BY TO_CHAR(TO_DATE(TO_CHAR(ENQ_TIME, 'dd-mm-yyyy hh24')
								, 'dd-mm-yyyy hh24')+(10/24)
					  , 'mm-dd hh24')
			  ORDER BY TO_CHAR(TO_DATE(TO_CHAR(ENQ_TIME, 'dd-mm-yyyy hh24')
								, 'dd-mm-yyyy hh24')+(10/24)
					  , 'mm-dd hh24') ;

--убрать с регистрации считывание
BEGIN
  DBMS_AQ.UNREGISTER (SYS.AQ$_REG_INFO_LIST
  (SYS.AQ$_REG_INFO('"SNAUSER"."QUEUE_PKK":"SNAUSER"',DBMS_AQ.NAMESPACE_AQ
  ,'plsql://snauser.DEMO_QUEUE_CALLBACK_PROCEDURE',HEXTORAW('FF'))
  ), 1);
END;
/

BEGIN
  dbms_aq.UNREGISTER(sys.aq$_reg_info_list(sys.aq$_reg_info
                  ('SNAUSER.AQ_PKK_SNA:STATUS', DBMS_AQ.NAMESPACE_AQ,
                         'plsql://SNAUSER.PR$AQ_PKK_SNA_CALLBACK',
                          HEXTORAW('FF')) ) ,1);
END;
/

BEGIN
  DBMS_AQ.REGISTER (SYS.AQ$_REG_INFO_LIST
  (SYS.AQ$_REG_INFO('"SNAUSER"."QUEUE_PKK":"SNAUSER"',DBMS_AQ.NAMESPACE_AQ
  ,'plsql://snauser.DEMO_QUEUE_CALLBACK_PROCEDURE',HEXTORAW('FF'))
  ), 1);
END;
/

BEGIN
  dbms_aq.REGISTER(sys.aq$_reg_info_list(sys.aq$_reg_info
                  ('SNAUSER.AQ_PKK_SNA:STATUS', DBMS_AQ.NAMESPACE_AQ,
                         'plsql://SNAUSER.PR$AQ_PKK_SNA_CALLBACK',
                          HEXTORAW('FF')) ) ,1);
END;
/


SELECT * from sys.reg$ ;
--проверка состояния очередей
SELECT * FROM ALL_QUEUES;
--просмотр сообщений
SELECT * FROM AQ_TAB ;
SELECT * FROM AQ_PKK_SNA_TBL;
--Посмотреть,  кто подписан на сообщения заданной очереди: ALL_QUEUE_SUBSCRIBERS или USER_QUEUE_SUBSCRIBERS
SELECT * FROM ALL_QUEUE_SUBSCRIBERS ORDER BY consumer_name;
--
select * from DBA_SUBSCR_REGISTRATIONS;

/*Для того, чтобы эта процедура вызывалась автоматически при поступлении сообщения в очередь
    , необходимо зарегистрировать соответствующее уведомление при помощи dbms_aq.register
  В dbms_aq.register передается список параметров регистрации, следовательно
    , можно за один раз зарегистрировать несколько уведомлений. Регистрационная информация включает в себя:
  Название подписчика, задается в виде: schema_name.queue_name:subscriber_name
*/

--GПРОВЕРКА МЕСТА
SELECT ue.TABLESPACE_NAME
        , ue.tab
        , ue.mb
        ,ufs.mb_free
      FROM (SELECT TABLESPACE_NAME, SEGMENT_NAME as tab, SUM(BYTES)/1024/1024 as mb 
        FROM DBA_EXTENTS --WHERE SEGMENT_NAME LIKE 'AQ$AQ_PKK_SNA_TBL%'
        GROUP BY TABLESPACE_NAME , SEGMENT_NAME) ue
      LEFT JOIN (SELECT TABLESPACE_NAME, SUM(BYTES)/1024/1024 as mb_free FROM DBA_FREE_SPACE 
                          WHERE TABLESPACE_NAME='SNAUSER'
                          GROUP BY TABLESPACE_NAME) ufs
        ON ue.TABLESPACE_NAME=ufs.TABLESPACE_NAME
;


select a.tablespace_name ,
    round(a.bytes_alloc / 1024 / 1024, 2) m_alloc,
    round(nvl(b.bytes_free, 0) / 1024 / 1024, 2) m_free,
    round((a.bytes_alloc - nvl(b.bytes_free, 0)) / 1024 / 1024, 2) m_used,
    round(maxbytes/1048576,2) Max
    from ( select f.tablespace_name,
    sum(f.bytes) bytes_alloc,
    sum(decode(f.autoextensible, 'YES',f.maxbytes,'NO', f.bytes)) maxbytes
    from dba_data_files f
    group by tablespace_name) a,
    ( select f.tablespace_name,
    sum(f.bytes) bytes_free
    from dba_free_space f
    group by tablespace_name) b
    where a.tablespace_name = b.tablespace_name (+) ; 


--паралельные сесси job
  
--простейшая холостая демонстрация DBMS_JOB для выполнения параллельных запросов
CREATE TABLE CUSTLOG(ELAPSED_CENTISECONDS NUMBER);

DECLARE
 j NUMBER;
 job_string varchar2(1000) :=
  'DECLARE
    s NUMBER := DBMS_UTILITY.get_time;
    t_N NUMBER;
  BEGIN 
    DBMS_OUTPUT.ENABLE;
    DBMS_OUTPUT.PUT_LINE(TO_CHAR(s));
    FOR i in 1..20 LOOP
      --PR$UPD_PHONES_RANK_BETWEEN(0,1453588);
      SELECT COUNT(*) INTO t_N FROM DUAL;
      INSERT INTO ODS.CUSTLOG VALUES (DBMS_UTILITY.get_time-s);
      COMMIT;
    END LOOP;
  END;';
BEGIN 
  DBMS_OUTPUT.ENABLE;
  FOR i in 1..1 LOOP
    DBMS_JOB.SUBMIT(j,job_string);
    DBMS_OUTPUT.PUT_LINE(TO_CHAR(j));  
  END LOOP;
END;
/


--запуск DBMS_JOB для глобальных обновлений
DECLARE
 j NUMBER;
 job_script VARCHAR2(1000);
 job_script_src VARCHAR2(1000) := 
    'BEGIN
      PR$UPD_RANK_BETWEEN_WORKS(p1, p2);
      COMMIT;
    END;';
    /*'BEGIN
      PR$UPD_PHONES_RANK_BETWEEN(p1, p2);
      COMMIT;
    END;'*/
    /*'BEGIN
      PR$UPD_RANK_BETWEEN_ADDRESS(p1, p2);
      COMMIT;
    END;';*/
  p_ID_Start NUMBER;
  p_ID_End NUMBER;
BEGIN 
  DBMS_OUTPUT.ENABLE;
  FOR i in 1..15 LOOP
    SELECT MIN(OBJECTS_ID), MAX(OBJECTS_ID) INTO p_ID_Start, p_ID_End
      --FROM (SELECT OBJECTS_ID, dense_rank() OVER (ORDER BY OBJECTS_ID) as rw FROM Q_PKK_PHONES_RNK_MIS)
      --FROM (SELECT OBJECTS_ID, dense_rank() OVER (ORDER BY OBJECTS_ID) as rw FROM QUERY_FOR_PKK_ADR)
      FROM Q_FOR_PKK_WORKS_RNK
      WHERE rw BETWEEN (i-1)*30900 AND i*30900;

    job_script := REPLACE(job_script_src, 'p1', TO_CHAR(p_ID_Start));
    job_script := REPLACE(job_script, 'p2', TO_CHAR(p_ID_End));
    
    DBMS_JOB.SUBMIT(j, job_script);
    DBMS_OUTPUT.PUT_LINE(TO_CHAR(p_ID_Start)||' - '||TO_CHAR(p_ID_End)||', i='||TO_CHAR(i)||', j='||TO_CHAR(j));
    COMMIT;
  END LOOP;
END;
/

BEGIN
  --FOR i in 349..397
    DBMS_JOB.REMOVE(1473);
    /*DBMS_JOB.REMOVE(1475);
    DBMS_JOB.REMOVE(1476);
    DBMS_JOB.REMOVE(1477);
    DBMS_JOB.REMOVE(1478);
    DBMS_JOB.REMOVE(1479);
    DBMS_JOB.REMOVE(1480);
    DBMS_JOB.REMOVE(1481);
    DBMS_JOB.REMOVE(1482);*/
  --END LOOP;
END;
/

SELECT MIN(OBJECTS_ID), MAX(OBJECTS_ID), min(RW), max(RW)
      FROM Q_FOR_PKK_WORKS_RNK WHERE rw BETWEEN (1-1)*100 AND 10*100;
      
SELECT COUNT(*), MIN(RW), MAX(RW) FROM Q_FOR_PKK_WORKS_RNK WHERE RW BETWEEN 3554920 AND	3625000;
SELECT qw.*, wr.WORKS_RANK FROM Q_FOR_PKK_WORKS_RNK qw
  LEFT JOIN PKK_WORKS_INFO wr
    ON qw.OBJECTS_ID=wr.OBJECTS_ID
  WHERE RW BETWEEN 3554920-2 AND 3554920+5
  ORDER BY RW;
SELECT * FROM PKK_WORKS_INFO WHERE OBJECTS_ID IN (1366);
SELECT * FROM PKK_WORKS_INFO WHERE OBJECTS_ID IN (1366);

    SELECT MIN(OBJECTS_ID), MAX(OBJECTS_ID)
      FROM (SELECT OBJECTS_ID, dense_rank() OVER (ORDER BY OBJECTS_ID) as rw FROM Q_PKK_PHONES_RNK_MIS)
      WHERE rw BETWEEN (1-1)*50000 AND 3*50000;

SELECT * FROM dba_jobs; 
select * from dba_jobs_running;
select * from dba_scheduler_running_jobs;

SELECT * FROM ODS.PKK_PHONES WHERE  OBJECTS_ID IN(9);
SELECT * FROM ODS.PKK_ADDRESS_HISTORY WHERE  OBJECTS_ID IN(46275813);

  
  SET SERVEROUTPUT ON
BEGIN
  DBMS_OUTPUT.ENABLE;
  DBMS_OUTPUT.PUT_LINE('Вывод сообщений настроен :)');
END;
/


SELECT MIN(OBJECTS_ID), MAX(OBJECTS_ID)
      FROM (SELECT OBJECTS_ID, dense_rank() OVER (ORDER BY OBJECTS_ID) as rw FROM QUERY_FOR_PKK_ADR)
      WHERE rw BETWEEN (45-1)*250000 AND 45*250000;
           
SELECT COUNT(*) FROM Q_PKK_PHONES_RNK_MIS WHERE OBJECTS_ID>46275813;
