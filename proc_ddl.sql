
  CREATE OR REPLACE PROCEDURE "SNAUSER"."CREATE_SOCIAL_NETWORK_ANALIZE" (msg_Id IN Varchar2, msg_date IN DATE, request_id IN NUMBER, old_status_id IN NUMBER, v_new_status_id IN NUMBER, err_code OUT NUMBER, err_message OUT VARCHAR2)
IS
  msg MSG_PKK; -- сообщение
  enqueue_options DBMS_AQ.enqueue_options_t;
  msg_properties  DBMS_AQ.message_properties_t;
  msg_handle      RAW(16);
BEGIN 

  err_code := 0;
  err_message := 'SUCCESS';
  
  msg:= MSG_PKK(msg_id, msg_date, request_id, old_status_id, v_new_status_id);
    DBMS_AQ.ENQUEUE(
      queue_name           => 'QUEUE_PKK',
      enqueue_options      => enqueue_options,
      message_properties   => msg_properties,
      payload              => msg,
      msgid                => msg_handle);   
      
EXCEPTION
  WHEN OTHERS THEN
    err_code:= -1;
    err_message:= sqlerrm;
END;
  CREATE OR REPLACE PROCEDURE "SNAUSER"."DEMO_QUEUE_CALLBACK_PROCEDURE" (
context  RAW,
reginfo  SYS.AQ$_REG_INFO,
descr    SYS.AQ$_DESCRIPTOR,
payload  RAW,
payloadl NUMBER
) AS
-- ========================================================================
-- ==	ПРОЦЕДУРА     "Считывание очереди, для регистрации в событии планировщика (REGISTER)"
-- ==	ОПИСАНИЕ:	    Считывание очередных сообщений в очереди. Основной метод.
-- ==	              Сообщение из таблицы очередей удаляется с задержкой (~ 10 сек.)  
-- == Внимание!     После запуска процесс может останавливаться с осложнениями.
-- ==               Тогда надо пытаться остановить очередь, убить процесс и т.п. Пока он не перестанет восстанавливаться.              
-- ========================================================================
-- ==	СОЗДАНИЕ:		  06.08.2015 (ТИМОНИН И.Е.)
-- ==	МОДИФИКАЦИЯ:	05.10.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================

r_dequeue_options    DBMS_AQ.DEQUEUE_OPTIONS_T;
r_message_properties DBMS_AQ.MESSAGE_PROPERTIES_T;
v_message_handle     RAW(16);
 msg             msg_PKK;
/*o_payload            demo_queue_payload_type;*/

BEGIN

    r_dequeue_options.msgid := descr.msg_id;
    r_dequeue_options.consumer_name := descr.consumer_name;
    
    DBMS_AQ.DEQUEUE(
    queue_name         => descr.queue_name,
    dequeue_options    => r_dequeue_options,
    message_properties => r_message_properties,
    payload            =>msg,
    msgid              => v_message_handle
    );
  
    --если заявка пошла через ручное рассмотрение, то записать ее в таблицу SNA_dequeue_msg.
    IF SFF.FN_CHECK_HAND_RID(msg.request_id)=1 THEN   
    		--DBMS_OUTPUT.PUT_LINE('msg_Id: '|| msg.msg_Id||'; msg_date: '||msg.msg_date||'; request_id: '||msg.request_id);
        INSERT INTO SNAUSER.SNA_dequeue_msg
          VALUES (msg.msg_Id,msg.msg_date, msg.request_id, msg.old_status_id, msg.v_new_status_id, systimestamp);
        COMMIT; --подтверждаем когда REQUEST_ID прошел проверку.
    ELSE --иначе добавляем в таблицу заявок, не прошедших проверку на ручное рассмотрение. 
        -- можно закомменить, если таблица не будет использоваться.
        INSERT INTO "SNAUSER"."SNA_DEQUEUE_MSG_NOAKT"
          VALUES (msg.msg_Id,msg.msg_date, msg.request_id, msg.old_status_id, msg.v_new_status_id, systimestamp);
    END IF;

    COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SNAUSER"."DATA_PKK" (v_request_id IN NUMBER)
AS    
  req$in number;
   
BEGIN

req$in := v_request_id;

delete from C_REQUEST_FULL_ST_RID;
delete from A_C_REQUEST_INF_PACK;
delete from A_C_REQUEST_CR_PACK;
delete from A_PHONES_FULL;
delete from A_PHONES_4SR_NEW;
delete from email;
delete from FIO4_HISTORY;
delete from ADDRESS_4SR;
delete from A_ADR_ORG_prev; 
delete from A_ADR_ORG;
delete from A_DOCS; 
delete from CONT_pre_full_1;
delete from CONT_pre_full_2;
delete from CONT_pre_full_12;
delete from CONT_pre_full_3;
delete from CONT_pre_full_123;
delete from applications_temp;


insert into C_REQUEST_FULL_ST_RID
select
REQUEST_ID 
,STATUS_ID 
,OBJECTS_ID as PERSON_ID
,CREATED_GROUP_ID 
,CREATED_DATE 

from C_REQUEST@dblink_pkk a
where a.request_id = v_request_id
;

insert into A_C_REQUEST_INF_PACK 
(select * from 
      (
      select
      REQUEST_INFO_ID 
      ,REQUEST_ID 
      ,FIO_ID 
      ,SEX_ID 
      ,BDATE 
      ,BADR_ID 
      ,MDOC_ID 
      ,MADR_ID 
      ,LADR_ID 
      ,DADR_ID
      ,EDUC
      /*,coalesce(to_number(WORK),-1) as work_id*/ --ERR 15.09.2015. неверная конструкция. Ошибки на WORK где несколько ID работ
      ,coalesce(to_number(SUBSTR(WORK, 1, INSTR(WORK, ' '))),-1) as work_id
      ,DOHOD_CALC          
      ,DOHOD_DECL_ALL 
      ,FAMILY
      ,row_number() over(partition by REQUEST_ID order by REQUEST_ID desc, REQUEST_INFO_ID desc) as rnmbr  
      
      from C_REQUEST_INFO@dblink_pkk a
      where a.request_id = v_request_id
      ) 
where rnmbr=1
)
;


insert into A_C_REQUEST_CR_PACK 
(select * from 
      (
      select
      REQUEST_ID 
      ,REQUEST_CREDIT_ID  
      ,PRIVILEGE_ID 
      ,CURRENCY_ID
      ,SCHEMS_ID
      ,SUMMA
      ,TYPE_CREDIT_ID
      ,row_number() over(partition by REQUEST_ID order by REQUEST_ID desc, REQUEST_CREDIT_ID desc) as rnmbr 
      
      from C_REQUEST_CREDIT@dblink_pkk a
      where a.request_id = v_request_id
      )
where rnmbr=1
)
;

insert into a_phones_full 
select * from 
(select 
PHONES_ID 
,OBJECTS_ID 
,OBJECTS_TYPE 
,PHONES_COMM 
,PHONES_AKT 
,PHONES_CREATED
,MODIFICATION_DATE 
,PHONE 
,row_number() over(partition by OBJECTS_ID, OBJECTS_TYPE order by MODIFICATION_DATE desc) as rnmbr 

from  PHONES@dblink_pkk
where objects_id in (select person_id from C_REQUEST_FULL_ST_RID) AND PHONES_AKT=1
) a
/*where rnmbr=1*/
;


insert into A_PHONES_4SR_NEW 
(select
--DISTINCT 
creq.request_id
,creq.person_id as objects_id
,to_number(COALESCE(homphn_f.phone, homphn.phone))  as home_phone
,to_number(COALESCE(wphn.phone, phn_org.phone)) AS work_phone
,to_number(ph_mob.phone) AS mobile
        
        
FROM C_REQUEST_FULL_ST_RID  creq
    LEFT JOIN A_C_REQUEST_INF_PACK  reqinf 
		ON creq.REQUEST_ID= reqinf.REQUEST_ID
	
    LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full  homphn /*телефон по адресу регистрации*/
		ON reqinf.MADR_ID=homphn.objects_id 
    AND homphn.OBJECTS_TYPE=8 
		AND homphn.phone is not null
		AND homphn.phones_akt=1 
	
    LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full homphn_f /*телефон по адресу факт. проживания*/
    ON reqinf.LADR_ID=homphn_f.objects_id
    AND homphn_f.OBJECTS_TYPE=8
    AND homphn_f.phone is not null
    AND homphn_f.phones_akt=1 
	
	  LEFT JOIN WORKS@dblink_pkk  works /*добавлено исключение избыточных телефонов для Пенсионеров/безработных 24.04.2015*/
    ON reqinf.WORK_ID = works.works_id
		AND COALESCE(works.org_vid,-1) NOT IN (0,5,98,99,100,101,102,105) /*почему то когда значение имеет . то результат не джойнится*/
  
	  LEFT JOIN ORG@dblink_pkk  org /*добавлено исключение избыточных телефонов для Пенсионеров/безработных 24.04.2015*/
		on works.org_id = org.org_id
    and COALESCE(org.org_vid,-1) NOT IN (0,5,98,99,100,101,102,105)
    and org.org_id=67790
	
    LEFT JOIN  /*PHONES@dblink_pkk*/a_phones_full wphn 
		on wphn.objects_id = works.works_id 
    AND wphn.OBJECTS_TYPE=works.OBJECTS_TYPE 
    AND wphn.phone is not null
    AND wphn.phones_akt=1
	
    LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full  phn_org 
		on phn_org.objects_id = org.org_id 
    AND phn_org.OBJECTS_TYPE=org.OBJECTS_TYPE 
    AND phn_org.phone is not null
    AND phn_org.phones_akt=1
	
    LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full  ph_mob /*мобильные телефоны - новый вариант. 24.04.2015*/
		on creq.person_id=ph_mob.objects_id
    AND ph_mob.OBJECTS_TYPE=2
    AND ph_mob.phone is not null
    AND ph_mob.phones_akt=1

WHERE 
to_number(COALESCE(homphn_f.phone, homphn.phone)) is not null
or to_number(COALESCE(wphn.phone, phn_org.phone)) is not null
or to_number(ph_mob.phone) is not null
)
;

insert into email 
(
select
EMAIL_ID
,EMAIL
,OBJECTS_ID
,OBJECTS_TYPE
,EMAIL_AKT

from  EMAIL@dblink_pkk
where EMAIL_AKT=1
and instr(email,'@')>0
and OBJECTS_ID in (select person_id from C_REQUEST_FULL_ST_RID)
)
;

insert into FIO4_HISTORY 
select
FH.FIO_ID
,FH.OBJECTS_ID
,FH.OBJECTS_TYPE
,FH.FIO_AKT
,F.FIO4SEARCH
    
from FIO_HISTORY@dblink_pkk FH
    left join FIO@dblink_pkk F
    on FH.FIO_ID=F.FIO_ID
where FH.OBJECTS_ID in (select person_id from C_REQUEST_FULL_ST_RID)
;


insert into ADDRESS_4SR 
(
SELECT 
DISTINCT
adr.address_id /*"ID адреса"*/
,adr.GEO_ID
,GEO.ADDRESS_STR AS ADDRESS_STR
,GEO.QUALITY_CODE 
,KLADR_SIT.SHOTNAME as SHOTNAME_CIT /*"Тип НП ФМЖ"*/
,KLADR_STR.SHOTNAME as SHOTNAME_STR /*"Тип улицы ФМЖ"*/
,COUNTRIES.COUNTRIES_ISO2 /*"Страна"*/
,reg.REGIONS_NAMES /*"Регион ФМЖ"*/	
,area.AREAS_NAMES /*"Район ФМЖ"*/
,CIT.CITIES_NAMES /*"НП ФМЖ"*/
,STREET.STREETS_NAMES /*"Улица ФМЖ"*/
,adr.HOUSE /*"Дом ФМЖ"*/
,adr.BUILD /*"Корпус ФМЖ"*/
,adr.FLAT /*"Квартира ФМЖ"*/
,adr.POSTOFFICE /*"ПочтИндекс"*/

FROM ADDRESS@dblink_pkk  adr 
    LEFT JOIN COUNTRIES@dblink_pkk  c 
  		on adr.COUNTRIES_ID=c.COUNTRIES_ID
    LEFT JOIN REGIONS_NAMES@dblink_pkk  reg   
  		on adr.REGIONS_UID=reg.REGIONS_UID
    LEFT JOIN AREAS_NAMES@dblink_pkk  area 
      on adr.AREAS_UID=area.AREAS_UID
	  LEFT JOIN CITIES_NAMES@dblink_pkk  CIT 
      on adr.CITIES_UID=CIT.CITIES_UID
	  LEFT JOIN STREETS_NAMES@dblink_pkk  STREET 
      on adr.STREETS_UID=STREET.STREETS_UID
	  LEFT JOIN COUNTRIES@dblink_pkk  COUNTRIES 
      on adr.COUNTRIES_ID = COUNTRIES.COUNTRIES_ID
    LEFT JOIN KLADR_SOCR@dblink_pkk  KLADR_SIT 
      ON CIT.CITIES_TYPE = KLADR_SIT.SOCR_ID
    LEFT JOIN KLADR_SOCR@dblink_pkk  KLADR_STR 
      ON STREET.STREETS_TYPE = KLADR_STR.SOCR_ID
    LEFT JOIN GEOCOORDINATES@dblink_pkk GEO 
      ON adr.GEO_ID = GEO.ID
where adr.address_id in (select MADR_ID from A_C_REQUEST_INF_PACK
                                    union
                                    select LADR_ID as MADR_ID from A_C_REQUEST_INF_PACK)
)
;


insert into A_ADR_ORG_prev 
(
SELECT 
W.WORKS_ID
,W.ORG_ID
,VO.ORG_NAME
,ah.ADDRESS_ID

FROM WORKS@dblink_pkk  W
    
    LEFT JOIN ORG@dblink_pkk VO
    ON VO.ORG_ID=W.ORG_ID
    
    left join ADDRESS_HISTORY@dblink_pkk ah
    on VO.org_id=ah.objects_id
    and VO.OBJECTS_TYPE=ah.OBJECTS_TYPE
    and ah.address_akt=1

where W.WORKS_ID in (select work_id from A_C_REQUEST_INF_PACK)
)
;

insert into A_ADR_ORG
(
SELECT 
DISTINCT
t1.works_id
,t1.org_id
,t1.org_name
,case when instr(geo.QUALITY_CODE,'GOOD')>0 then 1
        when geo.QUALITY_CODE is null then 0
        else to_number(substr(geo.QUALITY_CODE, instr(geo.QUALITY_CODE,'_')+1, length(geo.QUALITY_CODE)-instr(geo.QUALITY_CODE,'_')))
end as  QUALITY_CODE_N

,adr.address_id /*"ID адреса"*/
,adr.GEO_ID
,GEO.ADDRESS_STR AS ADDRESS_STR
,GEO.QUALITY_CODE 
,KLADR_SIT.SHOTNAME as ba_settlement /*"Тип НП ФМЖ" SHOTNAME_CIT*/
,KLADR_STR.SHOTNAME as SHOTNAME_STR /*"Тип улицы ФМЖ"*/
,COUNTRIES.COUNTRIES_ISO2 as ba_country/*"Страна"*/
,reg.REGIONS_NAMES as ba_region/*"Регион ФМЖ"*/	
,area.AREAS_NAMES as ba_district/*"Район ФМЖ"*/
,CIT.CITIES_NAMES as ba_city/*"НП ФМЖ"*/
,STREET.STREETS_NAMES as ba_street/*"Улица ФМЖ"*/
,adr.HOUSE as ba_house/*"Дом ФМЖ"*/
,adr.BUILD as ba_building/*"Корпус ФМЖ"*/
,adr.FLAT as ba_apartment/*"Квартира ФМЖ"*/
,adr.POSTOFFICE as ba_index/*"ПочтИндекс"*/

FROM A_ADR_ORG_prev t1

    left join ADDRESS@dblink_pkk  adr 
      on t1.address_id=adr.address_id    
    LEFT JOIN COUNTRIES@dblink_pkk  c 
  		on adr.COUNTRIES_ID=c.COUNTRIES_ID
    LEFT JOIN REGIONS_NAMES@dblink_pkk  reg   
  		on adr.REGIONS_UID=reg.REGIONS_UID
    LEFT JOIN AREAS_NAMES@dblink_pkk  area 
      on adr.AREAS_UID=area.AREAS_UID
	  LEFT JOIN CITIES_NAMES@dblink_pkk  CIT 
      on adr.CITIES_UID=CIT.CITIES_UID
	  LEFT JOIN STREETS_NAMES@dblink_pkk  STREET 
      on adr.STREETS_UID=STREET.STREETS_UID
	  LEFT JOIN COUNTRIES@dblink_pkk  COUNTRIES 
      on adr.COUNTRIES_ID = COUNTRIES.COUNTRIES_ID
    LEFT JOIN KLADR_SOCR@dblink_pkk  KLADR_SIT 
      ON CIT.CITIES_TYPE = KLADR_SIT.SOCR_ID
    LEFT JOIN KLADR_SOCR@dblink_pkk  KLADR_STR 
      ON STREET.STREETS_TYPE = KLADR_STR.SOCR_ID
    LEFT JOIN GEOCOORDINATES@dblink_pkk GEO 
      ON adr.GEO_ID = GEO.ID
)
;


insert into A_DOCS 
(
SELECT 
DH.DOCUMENTS_ID 
,DH.OBJECTS_ID 
,DH.OBJECTS_TYPE 
,DH.DOCUMENTS_AKT 
,DH.DOCUMENTS_CREATED
,D.DOCUMENTS_SERIAL
,D.DOCUMENTS_NUMBER
,D.DOCUMENTS_TYPE
,D.DOCUMENTS_ORGS

FROM DOCUMENTS_HISTORY@dblink_pkk  DH
      LEFT OUTER JOIN DOCUMENTS@dblink_pkk  D
      ON DH.DOCUMENTS_ID=D.DOCUMENTS_ID

WHERE D.DOCUMENTS_TYPE IN (21) AND DH.DOCUMENTS_AKT<>0
and dh.DOCUMENTS_ID in (select MDOC_ID from A_C_REQUEST_INF_PACK)
)
;

insert into CONT_pre_full_1 
SELECT
--DISTINCT
t1.PERSON_ID
,case when ph.OBJECTS_TYPE=200 then t1.PERSON_ID else t1.CONTACT_PERSON_ID end as CONTACT_PERSON_ID
,case when ph.OBJECTS_TYPE=200 then 'Телефоны контактных лиц' else t1.CONTACT_RELATION end as CONTACT_RELATION
,case when ph.OBJECTS_TYPE=200 then ph.PHONES_COMM else fh.FIO4SEARCH end as CONTACT_FIO
,case when ph.OBJECTS_TYPE=200 then to_number(ph.PHONE) else to_number(ph.PHONE) end as CONTACT_PHONE
	 
   
FROM (SELECT
         --DISTINCT 
        contprs.OBJECTS_ID AS PERSON_ID
				,contprs.CONTACT_OBJECTS_ID as CONTACT_PERSON_ID
				,COALESCE(fam_rel.family_rel_name, 'NULL') AS CONTACT_RELATION
        
        FROM CONTACT_PERSON@dblink_pkk  contprs
              LEFT JOIN FAMILY_RELATIONS@dblink_pkk  fam_rel 
              ON fam_rel.family_rel=contprs.family_rel
        WHERE contprs.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)

		UNION

        SELECT 
        --DISTINCT
        fam.OBJECTS_ID AS PERSON_ID
        ,fam.OB_ID AS CONTACT_PERSON_ID
        ,COALESCE(fam_rel.family_rel_name, 'NULL') AS CONTACT_RELATION
        
        FROM FAMILY@dblink_pkk  fam
        
              LEFT JOIN FAMILY_RELATIONS@dblink_pkk  fam_rel 
              ON fam_rel.family_rel=fam.family_rel
              WHERE fam.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)
          )  t1
          
          LEFT JOIN FIO4_HISTORY  fh
          on fh.OBJECTS_ID = t1.CONTACT_PERSON_ID 
          and fh.fio_akt=1
          LEFT JOIN A_PHONES_FULL/*PHONES@dblink_pkk*/  ph
          on ph.OBJECTS_ID = t1.CONTACT_PERSON_ID 
				  AND ph.OBJECTS_TYPE =2
          and ph.PHONES_AKT =1
WHERE ph.PHONE is not null
;


insert into CONT_pre_full_2 
(
SELECT 
--DISTINCT
t1.PERSON_ID

,case when ph.OBJECTS_TYPE=200 then t1.PERSON_ID else t1.CONTACT_PERSON_ID end as CONTACT_PERSON_ID
,case when ph.OBJECTS_TYPE=200 then 'Телефоны контактных лиц' else t1.CONTACT_RELATION end as CONTACT_RELATION
,case when ph.OBJECTS_TYPE=200 then ph.PHONES_COMM else fh.FIO4SEARCH end as CONTACT_FIO
,case when ph.OBJECTS_TYPE=200 then to_number(ph.PHONE) else to_number(ph.PHONE) end as CONTACT_PHONE

FROM (SELECT 
          --DISTINCT 
          contprs.OBJECTS_ID AS PERSON_ID
          ,contprs.CONTACT_OBJECTS_ID as CONTACT_PERSON_ID
          ,COALESCE(fam_rel.family_rel_name, 'NULL') AS CONTACT_RELATION
			
      FROM CONTACT_PERSON@dblink_pkk contprs
			LEFT JOIN FAMILY_RELATIONS@dblink_pkk fam_rel 
      ON fam_rel.family_rel=contprs.family_rel
			WHERE contprs.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)
      
		UNION
    
      SELECT 
      --DISTINCT 
      fam.OBJECTS_ID AS PERSON_ID
			,fam.OB_ID AS CONTACT_PERSON_ID
			,COALESCE(fam_rel.family_rel_name, 'NULL') AS CONTACT_RELATION
			
      FROM FAMILY@dblink_pkk fam
			LEFT JOIN FAMILY_RELATIONS@dblink_pkk fam_rel 
      ON fam_rel.family_rel=fam.family_rel
			
      WHERE fam.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)
      )  t1
      
      LEFT JOIN FIO4_HISTORY fh
      on fh.OBJECTS_ID = t1.CONTACT_PERSON_ID and fh.fio_akt=1
      LEFT JOIN  /*PHONES@dblink_pkk*/a_phones_full  ph
      on ph.OBJECTS_ID = t1.PERSON_ID 
      AND ph.OBJECTS_TYPE =200
	
  WHERE ph.PHONE IS NOT NULL
)
;

insert into CONT_pre_full_12
(
select * from
(
SELECT * FROM CONT_pre_full_1
UNION all
SELECT * FROM CONT_pre_full_2
) a1
)
;

insert into CONT_pre_full_3
(
SELECT
--DISTINCT
t1.PERSON_ID

,case when ph.OBJECTS_TYPE=200 then t1.PERSON_ID else t1.CONTACT_PERSON_ID end as CONTACT_PERSON_ID
,case when ph.OBJECTS_TYPE=200 then 'Телефоны контактных лиц' else t1.CONTACT_RELATION end as CONTACT_RELATION
,case when ph.OBJECTS_TYPE=200 then ph.PHONES_COMM else fh.FIO4SEARCH end as CONTACT_FIO
,case when ph.OBJECTS_TYPE=200 then to_number(ph.PHONE) else to_number(ph.PHONE) end as CONTACT_PHONE

FROM (SELECT DISTINCT src_ph.OBJECTS_ID AS PERSON_ID
          ,src_ph.OBJECTS_ID AS CONTACT_PERSON_ID
          ,'Телефоны контактных лиц' AS CONTACT_RELATION
          
          FROM  /*PHONES@dblink_pkk*/a_phones_full src_ph
          WHERE src_ph.OBJECTS_ID NOT IN (SELECT PERSON_ID FROM CONT_pre_full_12)
          AND src_ph.OBJECTS_TYPE=200
          AND src_ph.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)
          ) t1
          
          LEFT JOIN FIO4_HISTORY fh
          on fh.OBJECTS_ID = t1.CONTACT_PERSON_ID and fh.fio_akt=1
          LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full ph
          on ph.OBJECTS_ID = t1.PERSON_ID 
          AND ph.OBJECTS_TYPE =200

	WHERE NOT ph.PHONE is not null
)
;

insert into CONT_pre_full_123 
(
select * from
(
	SELECT * FROM CONT_pre_full_12
	UNION
	SELECT * FROM CONT_pre_full_3
) a2
)
;

--В contacts нет уникального идентификатора, поэтому трем все записи с одним REQUEST_ID
--и заново вставляем все
delete from snauser.contacts where REQUEST_ID=v_request_id;

insert into contacts
(
SELECT 
DISTINCT
C_REQ.REQUEST_ID
,PRE.PERSON_ID

,case when PRE.CONTACT_RELATION='Телефоны контактных лиц' then PRE.CONTACT_PHONE else PRE.CONTACT_PERSON_ID end as CONTACT_PERSON_ID
,case when PRE.CONTACT_RELATION='Телефоны контактных лиц' then 'Тел конт лиц' else PRE.CONTACT_RELATION end as CONTACT_RELATION


,UPPER(COALESCE(PRE.CONTACT_FIO, '-')) as CONTACT_FIO
,ltrim(rtrim(to_char(PRE.CONTACT_PHONE))) as CONTACT_PHONE
,1 AS IS_NEW_DATA

FROM C_REQUEST_FULL_ST_RID  C_REQ
      LEFT OUTER JOIN CONT_pre_full_123 PRE
      ON C_REQ.PERSON_ID=PRE.PERSON_ID

WHERE  PRE.CONTACT_PHONE is not null
/*AND ((C_REQ.CREATED_GROUP_I/D^=11455 AND C_REQ.STATUS_ID^=14)
    OR C_REQ.STATUS_ID=14)*/
)
;

--Тк merge работать не захотел
delete from snauser.applications where REQUEST_ID=v_request_id;

insert into snauser.applications
(
SELECT 
DISTINCT 

f00_C_RQ.REQUEST_ID
,f00_C_RQ.PERSON_ID AS PERSON_ID
,f00_C_RQ.CREATED_DATE AS REQ_DATE
,f01_C_RQ_CR.SUMMA AS REQ_SUMM
,(CASE WHEN f01_C_RQ_CR.CURRENCY_ID=1 THEN 'R'
  WHEN f01_C_RQ_CR.CURRENCY_ID=2 THEN 'U'
  WHEN f01_C_RQ_CR.CURRENCY_ID=3 THEN 'E'
  WHEN f01_C_RQ_CR.CURRENCY_ID=4 THEN 'C'
  WHEN f01_C_RQ_CR.CURRENCY_ID=5 THEN 'J'
END) AS REQ_SUMM_CURR
,f02_C_RQ_INF.BDATE AS BIRTHDAY
,f03_FIO.FIO4SEARCH as FIO

,COALESCE(f04_ADR_R.REGIONS_NAMES, '-') AS RA_REGION
,f04_ADR_R.AREAS_NAMES AS RA_DISTRICT
,f04_ADR_R.CITIES_NAMES AS RA_CITY
,f04_ADR_R.SHOTNAME_CIT as RA_SETTLEMENT 
,f04_ADR_R.STREETS_NAMES AS RA_STREET
,f04_ADR_R.HOUSE AS RA_HOUSE
,f04_ADR_R.BUILD AS RA_BUILDING
,f04_ADR_R.FLAT AS RA_APARTMENT
,f04_ADR_R.POSTOFFICE AS RA_INDEX

,COALESCE(f04_ADR_L.REGIONS_NAMES, '-') AS LA_REGION
,f04_ADR_L.AREAS_NAMES AS LA_DISTRICT
,f04_ADR_L.CITIES_NAMES AS LA_CITY
,f04_ADR_L.SHOTNAME_CIT as LA_SETTLEMENT 
,f04_ADR_L.STREETS_NAMES AS LA_STREET
,f04_ADR_L.HOUSE AS LA_HOUSE
,f04_ADR_L.BUILD AS LA_BUILDING
,f04_ADR_L.FLAT AS LA_APARTMENT
,f04_ADR_L.POSTOFFICE AS LA_INDEX

,COALESCE(f13_ORG_ADR_NEW.BA_REGION, '-') AS BA_REGION
,f13_ORG_ADR_NEW.BA_DISTRICT AS BA_DISTRICT
,f13_ORG_ADR_NEW.BA_CITY AS BA_CITY
,COALESCE(f13_ORG_ADR_NEW.BA_SETTLEMENT, '-') as BA_SETTLEMENT 
,f13_ORG_ADR_NEW.BA_STREET AS BA_STREET
,f13_ORG_ADR_NEW.BA_HOUSE AS BA_HOUSE
,f13_ORG_ADR_NEW.BA_BUILDING AS BA_BUILDING
,f13_ORG_ADR_NEW.BA_APARTMENT AS BA_APARTMENT
,f13_ORG_ADR_NEW.BA_INDEX AS BA_INDEX
,f13_ORG_ADR_NEW.ORG_NAME AS WORK_ORG_NAME

,f05_C_SCH.SCHEMS_NAME AS PRODUCT_TYPE
,f06_GRP.GROUPS_NAME AS REQ_CREATED_BRANCH
/*,_08_WH_SALARY.SALARY_SUM AS SALARY
,_08_WH_SALARY.EDUCATION_VID_ID AS EDUCATION
,_08_WH_SALARY.PENSION_SUM AS IS_PENSION*/
,SUBSTR(f09_DOC.DOCUMENTS_SERIAL,1,4)||SUBSTR(f09_DOC.DOCUMENTS_NUMBER,1,6) as PASSPORT
,f10_EMAIL.EMAIL
,f11_PHONE.MOBILE as MOBILE
,f11_PHONE.WORK_PHONE as WORK_PHONE
,f11_PHONE.HOME_PHONE as HOME_PHONE
--,null AS IS_NEW_CLIENT
--,null AS IS_NEW_DATA
--,null AS WORK_TYPE
      
FROM C_REQUEST_FULL_ST_RID f00_C_RQ
		
    LEFT OUTER JOIN a_c_request_cr_pack f01_C_RQ_CR
		ON f00_C_RQ.REQUEST_ID=f01_C_RQ_CR.REQUEST_ID
		
    LEFT OUTER JOIN a_c_request_inf_pack f02_C_RQ_INF
    ON f00_C_RQ.REQUEST_ID=f02_C_RQ_INF.REQUEST_ID
		
    LEFT OUTER JOIN fio4_history f03_FIO
    ON f00_C_RQ.PERSON_ID=f03_FIO.OBJECTS_ID  
    AND f03_FIO.FIO_AKT=1
		
    LEFT OUTER JOIN address_4sr f04_ADR_R
    ON f02_C_RQ_INF.MADR_ID=f04_ADR_R.ADDRESS_ID
		
    LEFT OUTER JOIN address_4sr f04_ADR_L
    ON f02_C_RQ_INF.LADR_ID=f04_ADR_L.ADDRESS_ID
		
    LEFT OUTER JOIN C_SCHEMS@dblink_pkk f05_C_SCH
		ON f01_C_RQ_CR.SCHEMS_ID=f05_C_SCH.SCHEMS_ID
		
    LEFT OUTER JOIN "GROUPS"@dblink_pkk f06_GRP
		ON f00_C_RQ.CREATED_GROUP_ID=f06_GRP.GROUPS_ID
		
    /*LEFT OUTER JOIN A_WH_PC_RD_ALL f08_WH_SALARY*/
			/*ON f00_C_RQ.REQUEST_ID=f08_WH_SALARY.REQUEST_ID*/
		
    LEFT OUTER JOIN A_DOCS f09_DOC
    ON f02_C_RQ_INF.MDOC_ID=f09_DOC.DOCUMENTS_ID
    AND DOCUMENTS_AKT=1
		
    LEFT OUTER JOIN EMAIL f10_EMAIL
    ON f00_C_RQ.PERSON_ID=f10_EMAIL.OBJECTS_ID
    AND f10_EMAIL.OBJECTS_TYPE = 2 AND f10_EMAIL.EMAIL_AKT = 1
		
    LEFT OUTER JOIN a_phones_4sr_new	f11_PHONE
    ON f00_C_RQ.REQUEST_ID=f11_PHONE.REQUEST_ID
    AND f00_C_RQ.PERSON_ID=f11_PHONE.OBJECTS_ID

		LEFT OUTER JOIN a_adr_org f13_ORG_ADR_NEW
    ON f02_C_RQ_INF.WORK_ID=f13_ORG_ADR_NEW.WORKS_ID

		/*WHERE 
    ((f00_C_RQ.CREATED_GROUP_ID^=11455 AND f00_C_RQ.STATUS_ID^=14)
						OR f00_C_RQ.STATUS_ID=14)*/
)
; 
   
--COMMIT;

  --если вдруг захочется напрямую в таблицы вносить, то расскоментировать + сделать такой insert для CONTACTS
/*insert into SFF.APPLICATIONS@SNA (REQUEST_ID,PERSON_ID,REQ_DATE,REQ_SUMM,REQ_SUMM_CURR,BIRTHDAY,FIO
      ,RA_REGION,RA_DISTRICT,RA_CITY,RA_SETTLEMENT,RA_STREET,RA_HOUSE,RA_BUILDING,RA_APARTMENT,RA_INDEX
      ,LA_REGION,LA_DISTRICT,LA_CITY,LA_SETTLEMENT,LA_STREET,LA_HOUSE,LA_BUILDING,LA_APARTMENT,LA_INDEX
      ,BA_REGION,BA_DISTRICT,BA_CITY,BA_SETTLEMENT,BA_STREET,BA_HOUSE,BA_BUILDING,BA_APARTMENT,BA_INDEX
      ,WORK_ORG_NAME,PRODUCT_TYPE,REQ_CREATED_BRANCH,PASSPORT,EMAIL,MOBILE,WORK_PHONE,HOME_PHONE)
    SELECT REQUEST_ID,PERSON_ID,REQ_DATE,REQ_SUMM,REQ_SUMM_CURR,BIRTHDAY,FIO
      ,RA_REGION,RA_DISTRICT,RA_CITY,RA_SETTLEMENT,RA_STREET,RA_HOUSE,RA_BUILDING,RA_APARTMENT,RA_INDEX
      ,LA_REGION,LA_DISTRICT,LA_CITY,LA_SETTLEMENT,LA_STREET,LA_HOUSE,LA_BUILDING,LA_APARTMENT,LA_INDEX
      ,BA_REGION,BA_DISTRICT,BA_CITY,BA_SETTLEMENT,BA_STREET,BA_HOUSE,BA_BUILDING,BA_APARTMENT,BA_INDEX
      ,WORK_ORG_NAME,PRODUCT_TYPE,REQ_CREATED_BRANCH,PASSPORT,EMAIL,MOBILE,WORK_PHONE,HOME_PHONE 
    FROM snauser.applications WHERE REQUEST_ID=v_request_id; 

INSERT INTO SFF.CONTACTS@SNA (REQUEST_ID, PERSON_ID, CONTACT_PERSON_ID, CONTACT_RELATION, CONTACT_FIO, CONTACT_PHONE, IS_NEW_DATA)
    SELECT REQUEST_ID, PERSON_ID, CONTACT_PERSON_ID, CONTACT_RELATION, CONTACT_FIO, CONTACT_PHONE, IS_NEW_DATA 
        FROM SNAUSER.CONTACTS WHERE REQUEST_ID=v_request_id;*/
COMMIT;
--SFF."PR$UPD_APPLICATIONS_FROD"@SNA(v_request_id);

  
EXCEPTION
    WHEN OTHERS
    THEN NULL;

END;
  CREATE OR REPLACE TRIGGER "SNAUSER"."SNA_QUEUE_FIRE_AFTER_INSERT" 
after insert ON SNA_DEQUEUE_MSG
for each row
DECLARE
  FLAG VARCHAR2(20);
BEGIN
  --01. заполняем APPLICATIONS и CONTACTS
  --data_PKK(:new.REQUEST_ID);
  DATA_PKK_MOD(:new.REQUEST_ID);
  --02. логируем успешный факт выполнения
  --FLAG := '0';
  SELECT TO_CHAR(COUNT(*)) INTO FLAG FROM SNAUSER.APPLICATIONS WHERE REQUEST_ID=:new.REQUEST_ID and rownum=1;
  INSERT INTO SNA_DEQUEUE_MSG_TRG_LOG
        (REQUEST_ID, RETURN_VAL, DT_LOG, FLAG_SELECTED) VALUES 
        (:new.REQUEST_ID, 'ОК: '||TO_CHAR(sysdate, 'dd-mm-yyyy hh24:mi:ss'), sysdate, FLAG);
  --03. Пополняем таблицу APPLICATIONS_FROD_tmp с временными данными для проверки по фрод-правилам
  SFF."PR$UPD_APPLICATIONS_FROD"@SNA(:new.REQUEST_ID);
  
  --04. 
EXCEPTION
    WHEN OTHERS
    THEN /*NULL; */
      --02. логируем НЕуспешный факт выполнения
      INSERT INTO SNA_DEQUEUE_MSG_TRG_LOG
        (REQUEST_ID, RETURN_VAL, DT_LOG) VALUES 
        (:new.REQUEST_ID, 'ERR: '||TO_CHAR(sysdate, 'dd-mm-yyyy hh24:mi:ss'), sysdate);
END;
ALTER TRIGGER "SNAUSER"."SNA_QUEUE_FIRE_AFTER_INSERT" ENABLE"
"
  CREATE OR REPLACE TRIGGER "SNAUSER"."SNA_LOGS_BEFORE_INSERT" 
before insert on SNA_LOGS
for each row
begin
  if :new.id_log is null then
    select SEQ_SNA_LOGS.nextval into :new.id_log from dual;
  end if;
end;
ALTER TRIGGER "SNAUSER"."SNA_LOGS_BEFORE_INSERT" ENABLE"
"
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_F01" (v_request_id IN NUMBER) IS
-- НЕАКТУАЛЬНО! ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 01. Ошибки_ФИОДР 1"
-- ==	ОПИСАНИЕ:		отличаются на 1 букву фамилии, 1 цифра в дате рождения
-- ========================================================================
-- ==	СОЗДАНИЕ:		19.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN 
	INSERT INTO SFF.FROD_RULE_DEMO (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
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
				--,'Ошибки_ФИО' as TYPE_REL
        ,'Ошибки_ФИО' as TYPE_REL
				,'Высокая доля вероятности ошибки в ФИО по сравнению с ранее указанными' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID AS PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'-(степень различия) '||TO_CHAR(UTL_MATCH.EDIT_DISTANCE(SRC.FIO, APP.FIO)) as INFO_EQ
				,'Фио+Др.:'||SRC.FIO||' '||SRC.DR as INFO_NEQ
				,'Фио+Др.:'||APP.FIO||' '||APP.DR as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
			ON SRC.FIRST_L = APP.FIRST_L AND SRC.FIRST_F = APP.FIRST_F AND SRC.FIRST_M = APP.FIRST_M 
				AND SRC.REQ_DATE > APP.REQ_DATE 
				AND SRC.RA_REGION = APP.RA_REGION 
        AND SRC.DR=APP.DR
				AND SRC.PERSON_ID ^= APP.PERSON_ID 
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id /*AND C_R.REQUEST_ID=46376896*/
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.FIO IS NULL AND NOT SRC.DR IS NULL 
				/*RULE: определяем условие фрод-правила*/
				AND UTL_MATCH.EDIT_DISTANCE(SRC.FIO, APP.FIO) BETWEEN 1 AND 2
				/*AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-360*2 AND SRC.REQ_DATE*/
			
              UNION ALL --разделяем
      
      SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,C_R.STATUS_ID
				,C_R.SCORE_TREE_ROUTE_ID
				,C_R.CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Ошибки_ДР' as TYPE_REL
				,'Высокая доля вероятности ошибки в Дате Рождения по сравнению с ранее указанными' as TYPE_REL_DESC
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
				,'-(степень различия) '||TO_CHAR(UTL_MATCH.EDIT_DISTANCE(SRC.DR, APP.DR)) as INFO_EQ
				,'Фио+Др.:'||SRC.FIO||' '||SRC.DR as INFO_NEQ
				,'Фио+Др.:'||APP.FIO||' '||APP.DR as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
			ON SRC.FIRST_L = APP.FIRST_L AND SRC.FIRST_F = APP.FIRST_F AND SRC.FIRST_M = APP.FIRST_M 
				AND SRC.REQ_DATE > APP.REQ_DATE
				AND SRC.RA_REGION = APP.RA_REGION
        AND SRC.FIO=APP.FIO
				AND SRC.PERSON_ID ^= APP.PERSON_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id /*AND C_R.REQUEST_ID=46376896*/
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.FIO IS NULL AND NOT SRC.DR IS NULL AND NOT SRC.FIO='-'
				/*RULE: определяем условие фрод-правила*/
				AND UTL_MATCH.EDIT_DISTANCE(SRC.DR, APP.DR)=1
				/*AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-360*2 AND SRC.REQ_DATE*/
			
      
      ) TAB
		WHERE TAB.F_POS=1 
      --исключаем девичьи фамилии
      AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL 
                /*AND SRC.MOBILE=APP.MOBILE*/)
			/*PostCheck: Проверка на родственников*/
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/;
    
	COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_P01" (v_request_id IN NUMBER) IS
-- НЕАКТУАЛЬНО! ========================================================================
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
	INSERT INTO SFF.FROD_RULE_DEMO (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
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
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
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
				AND NOT SRC.PASSPORT IS NULL AND NOT SRC.PASSPORT='-'
				AND NOT SRC.FIO IS NULL AND NOT APP.FIO IS NULL AND NOT SRC.FIO='-' AND NOT APP.FIO='-'
				/*AND UTL_MATCH.EDIT_DISTANCE(SRC.FIO, APP.FIO)+UTL_MATCH.EDIT_DISTANCE(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/;
	COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_H03" (v_request_id IN NUMBER) IS
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
				--,'Яма_3' as TYPE_REL
        ,'Псп1-Адрес0' as TYPE_REL
				,'У клиента совпадает паспорт с другим, не совпадает адрес проживания за последние 90 дней' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
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
				AND NOT SRC.PASSPORT IS NULL AND NOT SRC.PASSPORT='-'
				AND NOT SRC.LA_REGION='-' AND NOT APP.LA_REGION='-' AND NOT SRC.LA_REGION IS NULL AND NOT APP.LA_REGION IS NULL 
				AND NOT SRC.LA_APARTMENT='-' AND NOT APP.LA_APARTMENT='-' AND NOT SRC.LA_APARTMENT IS NULL AND NOT APP.LA_APARTMENT IS NULL
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
      --исключаем девичьи фамилии
      /*AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL )*/
			/*PostCheck: Проверка на родственников*/
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/;
	COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."TESTPRM" (vRID IN NUMBER)
IS
BEGIN
	NULL;
END TESTPRM;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_P02" (v_request_id IN NUMBER) IS
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
				--,'Фальсификация_паспорта_2' as TYPE_REL
        ,'Псп1-Другой регион клиента' as TYPE_REL
				,'Паспорт клиента совпадает с другим паспортом, регион не совпадает за последние 90 дней.' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
                  					OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID as PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Паспорт:'||SRC.PASSPORT as INFO_EQ
				,'Рег.прж:'||SRC.LA_REGION as INFO_NEQ
				,'Рег.прж:'||APP.LA_REGION as INFO_NEQ_REL
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
				AND NOT SRC.PASSPORT IS NULL AND NOT SRC.PASSPORT='-'
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
  CREATE OR REPLACE TRIGGER "SFF"."FROD_RULE_TRG" BEFORE INSERT ON SFF.FROD_RULE 
FOR EACH ROW 
BEGIN
  <<COLUMN_SEQUENCES>>
  BEGIN
    IF :NEW.FROD_RULE_ID IS NULL THEN
      SELECT FROD_RULE_SEQ.NEXTVAL INTO :NEW.FROD_RULE_ID FROM DUAL;
    END IF;
  END COLUMN_SEQUENCES;
  
  IF NOT :NEW.REQUEST_ID_REL IS NULL THEN
    --SELECT SFF.FN_STRIP_DUBL_IN_STR(:NEW.REQUEST_ID_REL) INTO :NEW.REQUEST_ID_REL FROM DUAL;
    :NEW.REQUEST_ID_REL := SFF.FN_DEDUBL_STR(:NEW.REQUEST_ID_REL);
  END IF;
END;
ALTER TRIGGER "SFF"."FROD_RULE_TRG" ENABLE"
"
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_RUN_ALL" (v_request_id IN NUMBER) as
-- ========================================================================
-- ==	ПРОЦЕДУРА		"ЗАПУСК ПРОВЕРКИ ПО ФРОД ПРАВИЛАМ"
-- ==	ОПИСАНИЕ:		В каждом вызове процедуры происходит вставка в целевую таблицу SFF.FROD_RULES
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	21.12.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN 
	--SFF.FROD_RULES_VERIFY_F01(v_request_id); 
	SFF.FROD_RULES_VERIFY_H01(v_request_id);
	SFF.FROD_RULES_VERIFY_H02(v_request_id);
	SFF.FROD_RULES_VERIFY_H03(v_request_id);
	SFF.FROD_RULES_VERIFY_H04(v_request_id);
	SFF.FROD_RULES_VERIFY_H05(v_request_id);
	SFF.FROD_RULES_VERIFY_H06(v_request_id);
  
	SFF.FROD_RULES_VERIFY_H07(v_request_id);
  
	SFF.FROD_RULES_VERIFY_H08(v_request_id);
	SFF.FROD_RULES_VERIFY_H09(v_request_id); 
	--SFF.FROD_RULES_VERIFY_P01(v_request_id); 
	SFF.FROD_RULES_VERIFY_P02(v_request_id);
  --SFF.FROD_RULES_VERIFY_P03(v_request_id);
  --SFF.FROD_RULES_VERIFY_P04(v_request_id);
  SFF.FROD_RULES_VERIFY_P1_F0(v_request_id);
  SFF.FROD_RULES_VERIFY_P1_F0_ERR(v_request_id);
  
  --UPDATE SFF.FROD_RULE SET REQUEST_ID_REL = SFF.FN_STRIP_DUBL_IN_STR(REQUEST_ID_REL) WHERE REQUEST_ID=v_request_id;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_H09" (v_request_id IN NUMBER) IS
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
				--,'Яма_9' as TYPE_REL
        ,'Псп1-Пмж0' as TYPE_REL
				,'У клиента совпадает паспорт с ранее указанным, не совпадает адрес регистрации за последние 90 дней' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
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
				AND NOT SRC.PASSPORT IS NULL AND NOT SRC.PASSPORT='-'
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
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_H02" (v_request_id IN NUMBER) IS
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
				--,'Яма_2' as TYPE_REL
        ,'Адрес 1-Псп0' as TYPE_REL
				,'У клиента совпадает адрес проживания с другим, не совпадает паспорт  за последние 90 дней., не родственники' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
                  					OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
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
				AND NOT SRC.PASSPORT IS NULL AND NOT APP.PASSPORT IS NULL AND NOT SRC.PASSPORT='-' AND NOT APP.PASSPORT='-'
				AND NOT SRC.PERSON_ID=APP.PERSON_ID
        --проверка адреса на в/ч
        AND NOT EXISTS(SELECT * FROM SFF.APPLICATIONS WHERE REQUEST_ID=SRC.REQUEST_ID
                        AND BA_REGION=SRC.LA_REGION
                        AND BA_DISTRICT=SRC.LA_DISTRICT
                        AND BA_CITY=SRC.LA_CITY
                        AND BA_STREET=SRC.LA_STREET
                        AND BA_HOUSE=SRC.LA_HOUSE
                        AND NVL(BA_BUILDING,'-')=NVL(SRC.LA_BUILDING,'-')
                        AND NVL(BA_APARTMENT,'-')=NVL(SRC.LA_APARTMENT,'-')
                        AND (WORK_ORG_NAME LIKE '%В/Ч%' OR WORK_ORG_NAME LIKE 'ВЧ%') )
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
      --исключаем девичьи фамилии
      AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL 
                /*AND SRC.MOBILE=APP.MOBILE*/)
      /* исключение однофамильцев */
			AND NOT UTL_MATCH.EDIT_DISTANCE(SUBSTR(FIO, 1, INSTR(FIO,' ')-1 ), SUBSTR(FIO_REL, 1, INSTR(FIO_REL,' ')-1 ))<3
			--PostCheck: Проверка на родственников
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/
			--PostCheck: Модифицированная проверка на родственников
      AND NOT SFF.FN_IS_FAMILY_REL(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1 
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID_REL, TAB.PERSON_ID)=1
                    ;
	COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_H01" (v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 04. Адрес 1-ФИО0"
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
				--,'Яма_1' as TYPE_REL
        ,'Адрес 1-ФИО0' as TYPE_REL
				,'У клиента совпадает адрес проживания с другим, не совпадает ФИО  за последние 90 дней, не родственники' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
                  					OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID as PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Адр.прж:'||SRC.LA_REGION||', '||SRC.LA_DISTRICT||', '||SRC.LA_CITY||', '||SRC.LA_STREET
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
						AND NVL(SRC.LA_BUILDING,'-')=NVL(APP.LA_BUILDING,'-')
						AND NVL(SRC.LA_APARTMENT,'-')=NVL(APP.LA_APARTMENT,'-')
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
				/*AND NOT SRC.LA_APARTMENT='-' AND NOT SRC.LA_APARTMENT IS NULL*/
				AND NOT SRC.FIO IS NULL AND NOT APP.FIO IS NULL AND NOT SRC.FIO='-' AND NOT APP.FIO='-'
				AND NOT SRC.DR IS NULL AND NOT APP.DR IS NULL AND NOT SRC.DR='-' AND NOT APP.DR='-'
				AND NOT SRC.PERSON_ID=APP.PERSON_ID
        --проверка адреса на в/ч
        AND NOT EXISTS(SELECT * FROM SFF.APPLICATIONS WHERE REQUEST_ID=SRC.REQUEST_ID
                        AND BA_REGION=SRC.LA_REGION
                        AND BA_DISTRICT=SRC.LA_DISTRICT
                        AND BA_CITY=SRC.LA_CITY
                        AND BA_STREET=SRC.LA_STREET
                        AND BA_HOUSE=SRC.LA_HOUSE
                        AND NVL(BA_BUILDING,'-')=NVL(SRC.LA_BUILDING,'-')
                        AND NVL(BA_APARTMENT,'-')=NVL(SRC.LA_APARTMENT,'-')
                        AND (WORK_ORG_NAME LIKE '%В/Ч%' OR WORK_ORG_NAME LIKE 'ВЧ%') )
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB 
		WHERE TAB.F_POS=1 
      /* исключение однофамильцев */
      AND NOT UTL_MATCH.EDIT_DISTANCE(SUBSTR(FIO, 1, INSTR(FIO,' ')-1 ), SUBSTR(FIO_REL, 1, INSTR(FIO_REL,' ')-1 ))<3
      --исключаем девичьи фамилии
      AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL 
                /*AND SRC.MOBILE=APP.MOBILE*/)
			--PostCheck: Проверка на родственников
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=TAB.PERSON_ID_REL
                     AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/
			--PostCheck: Модифицированная проверка на родственников
      AND NOT SFF.FN_IS_FAMILY_REL(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1 
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID_REL, TAB.PERSON_ID)=1
 ;
	COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_H04" (v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 07. Яма 4"
-- ==	ОПИСАНИЕ:		совпадает адрес места проживания
-- ==					, не совпадает номер мобильного телефона, период для сравнения 90 дней, Не родственники
-- ==					(АДРЕС ПРОЖ =, МОБ ТЕЛ ^= за 90 дней, нет связи в FAMILY_REL ) 
-- ==					Правка: увеличен период сравнения до 180 дней
-- ==         Правка 07.09.2015: добавлены фильтры для телефонов
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	07.09.2015 (ТРАХАЧЕВ В.В.)
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
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91*2 AND SRC.REQ_DATE-14
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.LA_REGION='-'AND NOT SRC.LA_REGION IS NULL AND NOT SRC.LA_APARTMENT='-' AND NOT SRC.LA_APARTMENT IS NULL
				AND NOT SRC.MOBILE IS NULL AND NOT APP.MOBILE IS NULL AND SRC.MOBILE>0 AND APP.MOBILE>0
        AND NOT MOD(SRC.MOBILE, 1000000)=0 AND NOT MOD(APP.MOBILE, 1000000)=0
        AND NOT TO_CHAR(SRC.MOBILE) LIKE '%999999%' AND NOT TO_CHAR(APP.MOBILE) LIKE '%999999%'
				AND NOT SRC.PERSON_ID=APP.PERSON_ID
        --проверка адреса на в/ч
        AND NOT EXISTS(SELECT * FROM SFF.APPLICATIONS WHERE REQUEST_ID=SRC.REQUEST_ID
                        AND BA_REGION=SRC.LA_REGION
                        AND BA_DISTRICT=SRC.LA_DISTRICT
                        AND BA_CITY=SRC.LA_CITY
                        AND BA_STREET=SRC.LA_STREET
                        AND BA_HOUSE=SRC.LA_HOUSE
                        AND NVL(BA_BUILDING,'-')=NVL(SRC.LA_BUILDING,'-')
                        AND NVL(BA_APARTMENT,'-')=NVL(SRC.LA_APARTMENT,'-')
                        AND (WORK_ORG_NAME LIKE '%В/Ч%' OR WORK_ORG_NAME LIKE 'ВЧ%') )
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
      --исключаем девичьи фамилии
      AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL 
                /*AND SRC.MOBILE=APP.MOBILE*/)
      /* исключение однофамильцев */
			AND NOT UTL_MATCH.EDIT_DISTANCE(SUBSTR(FIO, 1, INSTR(FIO,' ')-1 ), SUBSTR(FIO_REL, 1, INSTR(FIO_REL,' ')-1 ))<3
			--PostCheck: Проверка на родственников
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/
			--PostCheck: Модифицированная проверка на родственников
      AND NOT SFF.FN_IS_FAMILY_REL(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1 
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID_REL, TAB.PERSON_ID)=1;
	COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_H05" (v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 08. Яма 5"
-- ==	ОПИСАНИЕ:		совпадает мобильный телефон
-- ==					, но не совпадает адрес проживания, период для сравнения 90 дней. 
-- ==					(МОБ ТЕЛ =, АДР ПРОЖ ^= за 90 дней) 
-- ==					Правка: увеличен период сравнения до 180 дней
-- ==         Правка 07.09.2015: добавлены фильтры для телефонов
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
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
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
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91*1 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.MOBILE IS NULL AND SRC.MOBILE>0
				AND NOT SRC.LA_REGION='-'AND NOT SRC.LA_REGION IS NULL AND NOT SRC.LA_APARTMENT='-' AND NOT SRC.LA_APARTMENT IS NULL
				AND NOT APP.LA_REGION='-'AND NOT APP.LA_REGION IS NULL AND NOT APP.LA_APARTMENT='-' AND NOT APP.LA_APARTMENT IS NULL
        AND NOT MOD(SRC.MOBILE, 1000000)=0 AND NOT TO_CHAR(SRC.MOBILE) LIKE '%999999%'
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/
      AND EXISTS(SELECT * FROM CPD.PHONES@DBLINK_PKK 
                  WHERE (OBJECTS_ID=TAB.PERSON_ID OR OBJECTS_ID=TAB.PERSON_ID_REL ) AND OBJECTS_TYPE=2 
                      AND PHONE=SUBSTR(TAB.INFO_EQ, 9, 10)
                      AND PHONES_COMM = 'Телефон из РБО: Мобильный')
                    ;
	COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_H06" (v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 09. Яма 6"
-- ==	ОПИСАНИЕ:		совпадает номер мобильного телефона
-- ==					, не совпадает домашний телефон (фактическое место проживания). Одинаковый OBJECTS_ID
-- ==					, период для сравнения 90 дней. (МОБ ТЕЛ =, ДОМ ТЕЛ ^= за 90 дней) 
-- ==         Правка 07.09.2015: добавлены фильтры для телефонов
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	07.09.2015 (ТРАХАЧЕВ В.В.)
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
				,'У клиента совпадает сотовый, не совпадает домашний телефон, за последние 90 дней' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
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
				AND NOT SRC.MOBILE IS NULL AND SRC.MOBILE>0
        AND NOT MOD(SRC.MOBILE, 1000000)=0 AND NOT TO_CHAR(SRC.MOBILE) LIKE '%999999%'
				AND NOT SRC.HOME_PHONE IS NULL AND NOT APP.HOME_PHONE IS NULL AND SRC.HOME_PHONE>0 AND APP.HOME_PHONE>0
				AND SRC.PERSON_ID=APP.PERSON_ID
        AND NOT MOD(SRC.HOME_PHONE, 1000000)=0 AND NOT MOD(APP.HOME_PHONE, 1000000)=0
        AND NOT TO_CHAR(SRC.HOME_PHONE) LIKE '%999999%' AND NOT TO_CHAR(APP.HOME_PHONE) LIKE '%999999%'
        AND NOT SFF.FN_IS_POS_RID(SRC.REQUEST_ID)=1 AND NOT SFF.FN_IS_POS_RID(APP.REQUEST_ID)=1
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
      /*AND NOT SFF.FN_IS_POS_RID(TAB.REQUEST_ID)=1 AND NOT SFF.FN_IS_POS_RID(TAB.REQUEST_ID_REL)=1*/
			/*PostCheck: Проверка на родственников*/
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/
      AND EXISTS(SELECT * FROM CPD.PHONES@DBLINK_PKK 
                  WHERE (OBJECTS_ID=TAB.PERSON_ID OR OBJECTS_ID=TAB.PERSON_ID_REL ) AND OBJECTS_TYPE=2 
                      AND PHONE=SUBSTR(TAB.INFO_EQ, 9, 10)
                      AND PHONES_COMM = 'Телефон из РБО: Мобильный')  
        ;
	COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_H07" (v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 10. Яма 7"
-- ==	ОПИСАНИЕ:		совпадает домашний телефон, не совпадает личный телефон
-- ==					, период для сравнения 90 дней
-- ==					, клиенты не связаны родственной связью. 
-- ==					(ДОМ ТЕЛ =, МОБ ТЕЛ ^= за 90 дней, нет связи в FAMILY_REL) 
-- ==         Правка 07.09.2015: добавлены фильтры для телефонов
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	07.09.2015 (ТРАХАЧЕВ В.В.)
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
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
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
				AND NOT SRC.HOME_PHONE IS NULL AND SRC.HOME_PHONE>0
        AND NOT MOD(SRC.HOME_PHONE, 1000000)=0 AND NOT TO_CHAR(SRC.HOME_PHONE) LIKE '%999999%'
				AND NOT SRC.MOBILE IS NULL AND NOT APP.MOBILE IS NULL AND SRC.MOBILE>0 AND APP.MOBILE>0
        AND NOT MOD(SRC.MOBILE, 1000000)=0 AND NOT TO_CHAR(SRC.MOBILE) LIKE '%999999%'
        AND NOT MOD(APP.MOBILE, 1000000)=0 AND NOT TO_CHAR(APP.MOBILE) LIKE '%999999%'
				AND NOT SRC.PERSON_ID=APP.PERSON_ID
        AND NOT SFF.FN_IS_POS_RID(SRC.REQUEST_ID)=1 AND NOT SFF.FN_IS_POS_RID(APP.REQUEST_ID)=1
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
      --исключаем девичьи фамилии
      AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL 
                /*AND SRC.MOBILE=APP.MOBILE*/)
      /* исключение однофамильцев */
			AND NOT UTL_MATCH.EDIT_DISTANCE(SUBSTR(FIO, 1, INSTR(FIO,' ')-1 ), SUBSTR(FIO_REL, 1, INSTR(FIO_REL,' ')-1 ))<3
			--PostCheck: Проверка на родственников
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/
			--PostCheck: Модифицированная проверка на родственников
      AND NOT SFF.FN_IS_FAMILY_REL(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1 
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID_REL, TAB.PERSON_ID)=1
      ;
	COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_H08" (v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 11. Яма 8"
-- ==	ОПИСАНИЕ:		совпадает место проживания
-- ==					, не совпадает телефон по месту проживания, период для сравнения 90 дней. 
-- ==					, Не родственники. Разные OBJECTS_ID
-- ==					(АДР ПРОЖ =, ДОМ ТЕЛ ^= за 90 дней)
-- ==					Правка: увеличен период сравнения до 180 дней
-- ==         Правка 07.09.2015: добавлены фильтры для телефонов
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	07.09.2015 (ТРАХАЧЕВ В.В.)
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
				--,'Яма_8' as TYPE_REL
        ,'Фмж1-Дом телефон0' as TYPE_REL
				,'У клиента совпадает место проживания с другим, не совпадает домашний телефон, не родственники за последние 90 дней.' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
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
						AND SRC.LA_CITY = APP.LA_CITY
						AND SRC.LA_STREET = APP.LA_STREET
						AND SRC.LA_HOUSE=APP.LA_HOUSE
						AND NVL(SRC.LA_BUILDING, '-')=NVL(APP.LA_BUILDING, '-')
						AND NVL(SRC.LA_APARTMENT, '-')=NVL(APP.LA_APARTMENT, '-')
						AND NVL(SRC.HOME_PHONE, '-') ^= NVL(APP.HOME_PHONE, '-')
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91*2 AND SRC.REQ_DATE+1
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.LA_REGION='-' AND NOT SRC.LA_REGION IS NULL AND NOT SRC.LA_APARTMENT='-' AND NOT SRC.LA_APARTMENT IS NULL
				AND NOT SRC.HOME_PHONE IS NULL AND NOT APP.HOME_PHONE IS NULL AND SRC.HOME_PHONE>0 AND APP.HOME_PHONE>0
        AND NOT MOD(SRC.HOME_PHONE, 1000000)=0 AND NOT TO_CHAR(SRC.HOME_PHONE) LIKE '%999999%'
        AND NOT MOD(APP.HOME_PHONE, 1000000)=0 AND NOT TO_CHAR(APP.HOME_PHONE) LIKE '%999999%'
				AND NOT SRC.PERSON_ID=APP.PERSON_ID
        --проверка адреса на в/ч
        AND NOT EXISTS(SELECT * FROM SFF.APPLICATIONS WHERE REQUEST_ID=SRC.REQUEST_ID
                        AND BA_REGION=SRC.LA_REGION
                        AND BA_DISTRICT=SRC.LA_DISTRICT
                        AND BA_CITY=SRC.LA_CITY
                        AND BA_STREET=SRC.LA_STREET
                        AND BA_HOUSE=SRC.LA_HOUSE
                        AND NVL(BA_BUILDING,'-')=NVL(SRC.LA_BUILDING,'-')
                        AND NVL(BA_APARTMENT,'-')=NVL(SRC.LA_APARTMENT,'-')
                        AND (WORK_ORG_NAME LIKE '%В/Ч%' OR WORK_ORG_NAME LIKE 'ВЧ%') )
        AND NOT SFF.FN_IS_POS_RID(SRC.REQUEST_ID)=1 AND NOT SFF.FN_IS_POS_RID(APP.REQUEST_ID)=1
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
      --исключаем девичьи фамилии
      AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL 
                /*AND SRC.MOBILE=APP.MOBILE*/)
      /* исключение однофамильцев */
			AND NOT UTL_MATCH.EDIT_DISTANCE(SUBSTR(FIO, 1, INSTR(FIO,' ')-1 ), SUBSTR(FIO_REL, 1, INSTR(FIO_REL,' ')-1 ))<3
			--PostCheck: Проверка на родственников
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/
			--PostCheck: Модифицированная проверка на родственников
      AND NOT SFF.FN_IS_FAMILY_REL(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1 
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID_REL, TAB.PERSON_ID)=1
      /*AND NOT SFF.FN_IS_POS_RID(TAB.REQUEST_ID)=1 AND NOT SFF.FN_IS_POS_RID(TAB.REQUEST_ID_REL)=1*/
      ;
	COMMIT;
END;
  CREATE OR REPLACE FUNCTION "SFF"."FN_DEDUBL_STR" (inSTR IN VARCHAR2, inDelim IN VARCHAR2 DEFAULT ',') 
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
  CREATE OR REPLACE PROCEDURE "SFF"."PR_SCAN_FROD_FROM_MSG" (RID_start in NUMBER, RID_end in NUMBER) 
IS   
-- ========================================================================
-- ==	ПРОЦЕДУРА   "Прогон всех актуальных заявок которые попали в очередь через фрод правила"
-- == IN:         RID_start и RID_end указывающие диапазон необходимых REQUEST_ID
-- ==	ОПИСАНИЕ:		пополняет таблицу FROD_RULE со сработавшими алертами по фрод правилам
-- ========================================================================
-- ==	СОЗДАНИЕ:		26.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	05.11.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
CURSOR get_REQUEST
  IS 
		SELECT DISTINCT REQUEST_ID FROM SNAUSER.SNA_DEQUEUE_MSG@SNA sdm 
    WHERE REQUEST_ID BETWEEN RID_start AND RID_end
      /*AND NOT EXISTS(SELECT REQUEST_ID FROM KREDIT.C_REQUEST@DBLINK_PKK crid 
                                                    WHERE REQUEST_ID=sdm.REQUEST_ID AND CREATED_GROUP_ID IN(11455))*/
          ORDER BY REQUEST_ID;

	v_gt get_REQUEST%ROWTYPE;
BEGIN
	DBMS_OUTPUT.enable;
	OPEN get_REQUEST; 
  
  FETCH get_REQUEST INTO v_gt;	
	LOOP  
    SFF.FROD_RULES_RUN_ALL(v_gt.REQUEST_ID);
    IF MOD(get_REQUEST%ROWCOUNT,1000)=0 OR get_REQUEST%ROWCOUNT=1 THEN 
      DBMS_OUTPUT.put_line('Cur RID: '||TO_CHAR(get_REQUEST%ROWCOUNT)||'  '||TO_CHAR(v_gt.REQUEST_ID)
                                          ||' - '||TO_CHAR(systimestamp at time zone 'utc'));
    END IF; 
		FETCH get_REQUEST INTO v_gt;			
		EXIT WHEN get_REQUEST%NOTFOUND;
	END LOOP; 
  DBMS_OUTPUT.put_line('End RID: '||TO_CHAR(get_REQUEST%ROWCOUNT)||'  '||TO_CHAR(v_gt.REQUEST_ID)
                                          ||' - '||TO_CHAR(systimestamp at time zone 'utc'));
	CLOSE get_REQUEST;
  
  EXCEPTION
      WHEN OTHERS
      THEN NULL;
END;
  CREATE OR REPLACE FUNCTION "SFF"."FN_TEST_COL" (param1 IN NUMBER) RETURN VARCHAR2
IS

  TYPE col1 IS TABLE OF NUMBER;
  t1 col1;
BEGIN
  DBMS_OUTPUT.ENABLE;
  SELECT REQUEST_ID_REL BULK COLLECT INTO t1
    FROM FROD_RULE WHERE REQUEST_ID BETWEEN 51562532 AND 51562744;
    
  t1(1) := param1;
  DBMS_OUTPUT.PUT_LINE('count = '||TO_CHAR(t1.COUNT));

  RETURN 'Fun Completed';
END;
  CREATE OR REPLACE PROCEDURE "SNAUSER"."PR$SNA_LINKS_UPDATE_FROM_TMP" 
IS
-- Вспомогательная Процедура для обновления таблицы SNAUSER.SNA_LINKS
-- ======================================================================
-- ПРОЦЕДУРА "Вспомогательная. Для обновления таблицы SNAUSER.SNA_LINKS"
-- ОПИСАНИЕ:		Обновляет только отсутсвующие данные в SNAUSER.SNA_LINKS
--              Комментарий /*+ INDEX(SL I_SL_PERSON_ID) */ в запросе не удалять! 
-- ======================================================================
-- СОЗДАНИЕ:		03.09.2015 (ТРАХАЧЕВ В.В.)
-- МОДИФИКАЦИЯ:	03.09.2015 (ТРАХАЧЕВ В.В.)
-- ======================================================================
BEGIN

--удалить из добавляемой таблицы уже существующие данные. 
  DELETE FROM SNAUSER."SNA_LINKS_for_ins_tmp" srcins
						WHERE EXISTS(SELECT /*+ INDEX(SL I_SL_PERSON_ID) */ * 
								FROM SNAUSER.SNA_LINKS SL 
								WHERE SL.PERSON_ID=srcins.PERSON_ID 
									AND SL.LINK_TYPE=srcins.LINK_TYPE
									AND SL.LABEL=srcins.LABEL AND SL."GROUP"=srcins."GROUP");

  COMMIT;
  
  --добавляем записи
  INSERT INTO SNAUSER.SNA_LINKS
    SELECT * FROM SNAUSER."SNA_LINKS_for_ins_tmp";
    
  COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."UPD_APP_CONT" (v_request_id IN NUMBER)
AS    
  req$in number;
   
BEGIN

req$in := v_request_id;

delete from C_REQUEST_FULL_ST_RID;
delete from A_C_REQUEST_INF_PACK;
delete from A_C_REQUEST_CR_PACK;
delete from A_PHONES_FULL;
delete from A_PHONES_4SR_NEW;
delete from email;
delete from FIO4_HISTORY;
delete from ADDRESS_4SR;
delete from A_ADR_ORG_prev;
delete from A_ADR_ORG;
delete from A_DOCS;
/*delete from CONT_pre_full_1;
delete from CONT_pre_full_2;
delete from CONT_pre_full_12; 
delete from CONT_pre_full_3;*/
delete from CONT_pre_full_123;
--delete from applications_temp;


insert into C_REQUEST_FULL_ST_RID
select
REQUEST_ID  
,STATUS_ID 
,OBJECTS_ID as PERSON_ID
,CREATED_GROUP_ID 
,CREATED_DATE  

from C_REQUEST@dblink_pkk a
where a.request_id = v_request_id
;

insert into A_C_REQUEST_INF_PACK 
(select * from 
      (
      select
      REQUEST_INFO_ID 
      ,REQUEST_ID 
      ,FIO_ID 
      ,SEX_ID 
      ,BDATE 
      ,BADR_ID 
      ,MDOC_ID 
      ,MADR_ID 
      ,LADR_ID 
      ,DADR_ID
      ,EDUC
      ,coalesce(to_number(WORK),-1) as work_id
      ,DOHOD_CALC          
      ,DOHOD_DECL_ALL 
      ,FAMILY
      ,row_number() over(partition by REQUEST_ID order by REQUEST_ID desc, REQUEST_INFO_ID desc) as rnmbr  
      
      from C_REQUEST_INFO@dblink_pkk a
      where a.request_id = v_request_id
      ) 
where rnmbr=1
)
;


insert into A_C_REQUEST_CR_PACK 
(select * from 
      (
      select
      REQUEST_ID 
      ,REQUEST_CREDIT_ID  
      ,PRIVILEGE_ID 
      ,CURRENCY_ID
      ,SCHEMS_ID
      ,SUMMA
      ,TYPE_CREDIT_ID
      ,row_number() over(partition by REQUEST_ID order by REQUEST_ID desc, REQUEST_CREDIT_ID desc) as rnmbr 
      
      from C_REQUEST_CREDIT@dblink_pkk a
      where a.request_id = v_request_id
      )
where rnmbr=1
)
;

insert into a_phones_full 
select * from 
(select 
PHONES_ID 
,OBJECTS_ID 
,OBJECTS_TYPE 
,PHONES_COMM 
,PHONES_AKT 
,PHONES_CREATED
,MODIFICATION_DATE 
,PHONE 
,row_number() over(partition by OBJECTS_ID, OBJECTS_TYPE order by MODIFICATION_DATE desc) as rnmbr 

from  PHONES@dblink_pkk
where objects_id in (select person_id from C_REQUEST_FULL_ST_RID)
) a
/*where rnmbr=1*/
;


insert into A_PHONES_4SR_NEW 
(select
--DISTINCT 
creq.request_id
,creq.person_id as objects_id
,to_number(COALESCE(homphn_f.phone, homphn.phone))  as home_phone
,to_number(COALESCE(wphn.phone, phn_org.phone)) AS work_phone
,to_number(ph_mob.phone) AS mobile
        
        
FROM C_REQUEST_FULL_ST_RID  creq
    LEFT JOIN A_C_REQUEST_INF_PACK  reqinf 
		ON creq.REQUEST_ID= reqinf.REQUEST_ID
	
    LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full  homphn /*телефон по адресу регистрации*/
		ON reqinf.MADR_ID=homphn.objects_id 
    AND homphn.OBJECTS_TYPE=8 
		AND homphn.phone is not null
		AND homphn.phones_akt=1 
	
    LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full homphn_f /*телефон по адресу факт. проживания*/
    ON reqinf.LADR_ID=homphn_f.objects_id
    AND homphn_f.OBJECTS_TYPE=8
    AND homphn_f.phone is not null
    AND homphn_f.phones_akt=1 
	
	  LEFT JOIN WORKS@dblink_pkk  works /*добавлено исключение избыточных телефонов для Пенсионеров/безработных 24.04.2015*/
    ON reqinf.WORK_ID = works.works_id
		AND COALESCE(works.org_vid,-1) NOT IN (0,5,98,99,100,101,102,105) /*почему то когда значение имеет . то результат не джойнится*/
  
	  LEFT JOIN ORG@dblink_pkk  org /*добавлено исключение избыточных телефонов для Пенсионеров/безработных 24.04.2015*/
		on works.org_id = org.org_id
    and COALESCE(org.org_vid,-1) NOT IN (0,5,98,99,100,101,102,105)
    and org.org_id=67790
	
    LEFT JOIN  /*PHONES@dblink_pkk*/a_phones_full wphn 
		on wphn.objects_id = works.works_id 
    AND wphn.OBJECTS_TYPE=works.OBJECTS_TYPE 
    AND wphn.phone is not null
    AND wphn.phones_akt=1
	
    LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full  phn_org 
		on phn_org.objects_id = org.org_id 
    AND phn_org.OBJECTS_TYPE=org.OBJECTS_TYPE 
    AND phn_org.phone is not null
    AND phn_org.phones_akt=1
	
    LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full  ph_mob /*мобильные телефоны - новый вариант. 24.04.2015*/
		on creq.person_id=ph_mob.objects_id
    AND ph_mob.OBJECTS_TYPE=2
    AND ph_mob.phone is not null
    AND ph_mob.phones_akt=1

WHERE 
to_number(COALESCE(homphn_f.phone, homphn.phone)) is not null
or to_number(COALESCE(wphn.phone, phn_org.phone)) is not null
or to_number(ph_mob.phone) is not null
)
;

insert into email 
(
select
EMAIL_ID
,EMAIL
,OBJECTS_ID
,OBJECTS_TYPE
,EMAIL_AKT

from  EMAIL@dblink_pkk
where EMAIL_AKT=1
and instr(email,'@')>0
and OBJECTS_ID in (select person_id from C_REQUEST_FULL_ST_RID)
)
;

insert into FIO4_HISTORY 
select
FH.FIO_ID
,FH.OBJECTS_ID
,FH.OBJECTS_TYPE
,FH.FIO_AKT
,F.FIO4SEARCH
    
from FIO_HISTORY@dblink_pkk FH
    left join FIO@dblink_pkk F
    on FH.FIO_ID=F.FIO_ID
where FH.OBJECTS_ID in (select person_id from C_REQUEST_FULL_ST_RID)
;


insert into ADDRESS_4SR 
(
SELECT 
DISTINCT
adr.address_id /*"ID адреса"*/
,adr.GEO_ID
,GEO.ADDRESS_STR AS ADDRESS_STR
,GEO.QUALITY_CODE 
,KLADR_SIT.SHOTNAME as SHOTNAME_CIT /*"Тип НП ФМЖ"*/
,KLADR_STR.SHOTNAME as SHOTNAME_STR /*"Тип улицы ФМЖ"*/
,COUNTRIES.COUNTRIES_ISO2 /*"Страна"*/
,reg.REGIONS_NAMES /*"Регион ФМЖ"*/	
,area.AREAS_NAMES /*"Район ФМЖ"*/
,CIT.CITIES_NAMES /*"НП ФМЖ"*/
,STREET.STREETS_NAMES /*"Улица ФМЖ"*/
,adr.HOUSE /*"Дом ФМЖ"*/
,adr.BUILD /*"Корпус ФМЖ"*/
,adr.FLAT /*"Квартира ФМЖ"*/
,adr.POSTOFFICE /*"ПочтИндекс"*/

FROM ADDRESS@dblink_pkk  adr 
    LEFT JOIN COUNTRIES@dblink_pkk  c 
  		on adr.COUNTRIES_ID=c.COUNTRIES_ID
    LEFT JOIN REGIONS_NAMES@dblink_pkk  reg   
  		on adr.REGIONS_UID=reg.REGIONS_UID
    LEFT JOIN AREAS_NAMES@dblink_pkk  area 
      on adr.AREAS_UID=area.AREAS_UID
	  LEFT JOIN CITIES_NAMES@dblink_pkk  CIT 
      on adr.CITIES_UID=CIT.CITIES_UID
	  LEFT JOIN STREETS_NAMES@dblink_pkk  STREET 
      on adr.STREETS_UID=STREET.STREETS_UID
	  LEFT JOIN COUNTRIES@dblink_pkk  COUNTRIES 
      on adr.COUNTRIES_ID = COUNTRIES.COUNTRIES_ID
    LEFT JOIN KLADR_SOCR@dblink_pkk  KLADR_SIT 
      ON CIT.CITIES_TYPE = KLADR_SIT.SOCR_ID
    LEFT JOIN KLADR_SOCR@dblink_pkk  KLADR_STR 
      ON STREET.STREETS_TYPE = KLADR_STR.SOCR_ID
    LEFT JOIN GEOCOORDINATES@dblink_pkk GEO 
      ON adr.GEO_ID = GEO.ID
where adr.address_id in (select MADR_ID from A_C_REQUEST_INF_PACK
                                    union
                                    select LADR_ID as MADR_ID from A_C_REQUEST_INF_PACK)
)
;


insert into A_ADR_ORG_prev 
(
SELECT 
W.WORKS_ID
,W.ORG_ID
,VO.ORG_NAME
,ah.ADDRESS_ID

FROM WORKS@dblink_pkk  W
    
    LEFT JOIN ORG@dblink_pkk VO
    ON VO.ORG_ID=W.ORG_ID
    
    left join ADDRESS_HISTORY@dblink_pkk ah
    on VO.org_id=ah.objects_id
    and VO.OBJECTS_TYPE=ah.OBJECTS_TYPE
    and ah.address_akt=1

where W.WORKS_ID in (select work_id from A_C_REQUEST_INF_PACK)
)
;

insert into A_ADR_ORG
(
SELECT 
DISTINCT
t1.works_id
,t1.org_id
,t1.org_name
,case when instr(geo.QUALITY_CODE,'GOOD')>0 then 1
        when geo.QUALITY_CODE is null then 0
        else to_number(substr(geo.QUALITY_CODE, instr(geo.QUALITY_CODE,'_')+1, length(geo.QUALITY_CODE)-instr(geo.QUALITY_CODE,'_')))
end as  QUALITY_CODE_N

,adr.address_id /*"ID адреса"*/
,adr.GEO_ID
,GEO.ADDRESS_STR AS ADDRESS_STR
,GEO.QUALITY_CODE 
,KLADR_SIT.SHOTNAME as ba_settlement /*"Тип НП ФМЖ" SHOTNAME_CIT*/
,KLADR_STR.SHOTNAME as SHOTNAME_STR /*"Тип улицы ФМЖ"*/
,COUNTRIES.COUNTRIES_ISO2 as ba_country/*"Страна"*/
,reg.REGIONS_NAMES as ba_region/*"Регион ФМЖ"*/	
,area.AREAS_NAMES as ba_district/*"Район ФМЖ"*/
,CIT.CITIES_NAMES as ba_city/*"НП ФМЖ"*/
,STREET.STREETS_NAMES as ba_street/*"Улица ФМЖ"*/
,adr.HOUSE as ba_house/*"Дом ФМЖ"*/
,adr.BUILD as ba_building/*"Корпус ФМЖ"*/
,adr.FLAT as ba_apartment/*"Квартира ФМЖ"*/
,adr.POSTOFFICE as ba_index/*"ПочтИндекс"*/

FROM A_ADR_ORG_prev t1

    left join ADDRESS@dblink_pkk  adr 
      on t1.address_id=adr.address_id    
    LEFT JOIN COUNTRIES@dblink_pkk  c 
  		on adr.COUNTRIES_ID=c.COUNTRIES_ID
    LEFT JOIN REGIONS_NAMES@dblink_pkk  reg   
  		on adr.REGIONS_UID=reg.REGIONS_UID
    LEFT JOIN AREAS_NAMES@dblink_pkk  area 
      on adr.AREAS_UID=area.AREAS_UID
	  LEFT JOIN CITIES_NAMES@dblink_pkk  CIT 
      on adr.CITIES_UID=CIT.CITIES_UID
	  LEFT JOIN STREETS_NAMES@dblink_pkk  STREET 
      on adr.STREETS_UID=STREET.STREETS_UID
	  LEFT JOIN COUNTRIES@dblink_pkk  COUNTRIES 
      on adr.COUNTRIES_ID = COUNTRIES.COUNTRIES_ID
    LEFT JOIN KLADR_SOCR@dblink_pkk  KLADR_SIT 
      ON CIT.CITIES_TYPE = KLADR_SIT.SOCR_ID
    LEFT JOIN KLADR_SOCR@dblink_pkk  KLADR_STR 
      ON STREET.STREETS_TYPE = KLADR_STR.SOCR_ID
    LEFT JOIN GEOCOORDINATES@dblink_pkk GEO 
      ON adr.GEO_ID = GEO.ID
)
;


insert into A_DOCS 
(
SELECT 
DH.DOCUMENTS_ID 
,DH.OBJECTS_ID 
,DH.OBJECTS_TYPE 
,DH.DOCUMENTS_AKT 
,DH.DOCUMENTS_CREATED
,D.DOCUMENTS_SERIAL
,D.DOCUMENTS_NUMBER
,D.DOCUMENTS_TYPE
,D.DOCUMENTS_ORGS

FROM DOCUMENTS_HISTORY@dblink_pkk  DH
      LEFT OUTER JOIN DOCUMENTS@dblink_pkk  D
      ON DH.DOCUMENTS_ID=D.DOCUMENTS_ID

WHERE D.DOCUMENTS_TYPE IN (21) AND DH.DOCUMENTS_AKT<>0
and dh.DOCUMENTS_ID in (select MDOC_ID from A_C_REQUEST_INF_PACK)
)
;

insert into CONT_pre_full_123
(SELECT
--DISTINCT
t1.PERSON_ID
,case when ph.OBJECTS_TYPE=200 then t1.PERSON_ID else t1.CONTACT_PERSON_ID end as CONTACT_PERSON_ID
,case when ph.OBJECTS_TYPE=200 then 'Телефоны контактных лиц' else t1.CONTACT_RELATION end as CONTACT_RELATION
,case when ph.OBJECTS_TYPE=200 then ph.PHONES_COMM else fh.FIO4SEARCH end as CONTACT_FIO
,case when ph.OBJECTS_TYPE=200 then to_number(ph.PHONE) else to_number(ph.PHONE) end as CONTACT_PHONE   
FROM (SELECT
         --DISTINCT 
        contprs.OBJECTS_ID AS PERSON_ID
				,contprs.CONTACT_OBJECTS_ID as CONTACT_PERSON_ID
				,COALESCE(fam_rel.family_rel_name, 'NULL') AS CONTACT_RELATION
        
        FROM CONTACT_PERSON@dblink_pkk  contprs
              LEFT JOIN FAMILY_RELATIONS@dblink_pkk  fam_rel 
              ON fam_rel.family_rel=contprs.family_rel
        WHERE contprs.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)

		UNION

        SELECT 
        --DISTINCT
        fam.OBJECTS_ID AS PERSON_ID
        ,fam.OB_ID AS CONTACT_PERSON_ID
        ,COALESCE(fam_rel.family_rel_name, 'NULL') AS CONTACT_RELATION
        
        FROM FAMILY@dblink_pkk  fam
        
              LEFT JOIN FAMILY_RELATIONS@dblink_pkk  fam_rel 
              ON fam_rel.family_rel=fam.family_rel
              WHERE fam.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)
          )  t1
          
          LEFT JOIN FIO4_HISTORY  fh
          on fh.OBJECTS_ID = t1.CONTACT_PERSON_ID 
          and fh.fio_akt=1
          LEFT JOIN /*A_PHONES_FULL*/ PHONES@dblink_pkk  ph
          on t1.CONTACT_PERSON_ID = ph.OBJECTS_ID 
				  AND ph.OBJECTS_TYPE =2
          and ph.PHONES_AKT =1
          WHERE ph.PHONE is not null
  )
UNION
  (SELECT 
  --DISTINCT
  t1.PERSON_ID
  
  ,case when ph.OBJECTS_TYPE=200 then t1.PERSON_ID else t1.CONTACT_PERSON_ID end as CONTACT_PERSON_ID
  ,case when ph.OBJECTS_TYPE=200 then 'Телефоны контактных лиц' else t1.CONTACT_RELATION end as CONTACT_RELATION
  ,case when ph.OBJECTS_TYPE=200 then ph.PHONES_COMM else fh.FIO4SEARCH end as CONTACT_FIO
  ,case when ph.OBJECTS_TYPE=200 then to_number(ph.PHONE) else to_number(ph.PHONE) end as CONTACT_PHONE
  
  FROM (SELECT 
            --DISTINCT 
            contprs.OBJECTS_ID AS PERSON_ID
            ,contprs.CONTACT_OBJECTS_ID as CONTACT_PERSON_ID
            ,COALESCE(fam_rel.family_rel_name, 'NULL') AS CONTACT_RELATION
        
        FROM CONTACT_PERSON@dblink_pkk contprs
        LEFT JOIN FAMILY_RELATIONS@dblink_pkk fam_rel 
        ON fam_rel.family_rel=contprs.family_rel
        WHERE contprs.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)
        
      UNION
      
        SELECT 
        --DISTINCT 
        fam.OBJECTS_ID AS PERSON_ID
        ,fam.OB_ID AS CONTACT_PERSON_ID
        ,COALESCE(fam_rel.family_rel_name, 'NULL') AS CONTACT_RELATION
        
        FROM FAMILY@dblink_pkk fam
        LEFT JOIN FAMILY_RELATIONS@dblink_pkk fam_rel 
        ON fam_rel.family_rel=fam.family_rel
        
        WHERE fam.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)
        )  t1
        
        LEFT JOIN FIO4_HISTORY fh
        on fh.OBJECTS_ID = t1.CONTACT_PERSON_ID and fh.fio_akt=1
        LEFT JOIN PHONES@dblink_pkk /*a_phones_full*/  ph
        on ph.OBJECTS_ID = t1.PERSON_ID AND ph.OBJECTS_TYPE =200
    
    WHERE ph.PHONE IS NOT NULL
  )
UNION
  (SELECT
  --DISTINCT
  t1.PERSON_ID
  
  ,case when ph.OBJECTS_TYPE=200 then t1.PERSON_ID else t1.CONTACT_PERSON_ID end as CONTACT_PERSON_ID
  ,case when ph.OBJECTS_TYPE=200 then 'Телефоны контактных лиц' else t1.CONTACT_RELATION end as CONTACT_RELATION
  ,case when ph.OBJECTS_TYPE=200 then ph.PHONES_COMM else fh.FIO4SEARCH end as CONTACT_FIO
  ,case when ph.OBJECTS_TYPE=200 then to_number(ph.PHONE) else to_number(ph.PHONE) end as CONTACT_PHONE
  
  FROM (SELECT DISTINCT src_ph.OBJECTS_ID AS PERSON_ID
            ,src_ph.OBJECTS_ID AS CONTACT_PERSON_ID
            ,'Телефоны контактных лиц' AS CONTACT_RELATION
            
            FROM  /*PHONES@dblink_pkk*/a_phones_full src_ph
            WHERE /*src_ph.OBJECTS_ID NOT IN (SELECT PERSON_ID FROM CONT_pre_full_12)
            AND*/ src_ph.OBJECTS_TYPE=200
            AND src_ph.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)
            ) t1
            
            LEFT JOIN FIO4_HISTORY fh
            on fh.OBJECTS_ID = t1.CONTACT_PERSON_ID and fh.fio_akt=1
            LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full ph
            on ph.OBJECTS_ID = t1.PERSON_ID 
            AND ph.OBJECTS_TYPE =200
            WHERE ph.PHONE is not null
)
;


--В contacts нет уникального идентификатора, поэтому трем все записи с одним REQUEST_ID
--и заново вставляем все
--delete from sff.contacts_temp where REQUEST_ID=v_request_id;

--Тк merge работать не захотел
--delete from SFF.applications_temp where REQUEST_ID=v_request_id;

insert into contacts_temp
(
SELECT 
DISTINCT
C_REQ.REQUEST_ID
,PRE.PERSON_ID

,case when PRE.CONTACT_RELATION='Телефоны контактных лиц' then PRE.CONTACT_PHONE else PRE.CONTACT_PERSON_ID end as CONTACT_PERSON_ID
,case when PRE.CONTACT_RELATION='Телефоны контактных лиц' then 'Тел конт лиц' else PRE.CONTACT_RELATION end as CONTACT_RELATION


,UPPER(COALESCE(PRE.CONTACT_FIO, '-')) as CONTACT_FIO
,ltrim(rtrim(to_char(PRE.CONTACT_PHONE))) as CONTACT_PHONE
,1 AS IS_NEW_DATA

FROM C_REQUEST_FULL_ST_RID  C_REQ
      LEFT OUTER JOIN CONT_pre_full_123 PRE
      ON C_REQ.PERSON_ID=PRE.PERSON_ID

WHERE  PRE.CONTACT_PHONE is not null
AND ((C_REQ.CREATED_GROUP_ID^=11455 AND C_REQ.STATUS_ID^=14)
    OR C_REQ.STATUS_ID=14)
)
;



insert into SFF.applications_temp
(
SELECT 
DISTINCT 

f00_C_RQ.REQUEST_ID
,f00_C_RQ.PERSON_ID AS PERSON_ID
,f00_C_RQ.CREATED_DATE AS REQ_DATE
,f01_C_RQ_CR.SUMMA AS REQ_SUMM
,(CASE WHEN f01_C_RQ_CR.CURRENCY_ID=1 THEN 'R'
  WHEN f01_C_RQ_CR.CURRENCY_ID=2 THEN 'U'
  WHEN f01_C_RQ_CR.CURRENCY_ID=3 THEN 'E'
  WHEN f01_C_RQ_CR.CURRENCY_ID=4 THEN 'C'
  WHEN f01_C_RQ_CR.CURRENCY_ID=5 THEN 'J'
END) AS REQ_SUMM_CURR
,f02_C_RQ_INF.BDATE AS BIRTHDAY
,f03_FIO.FIO4SEARCH as FIO

/*адрес прописки*/
,COALESCE(f04_ADR_R.REGIONS_NAMES, '-') AS RA_REGION
,f04_ADR_R.AREAS_NAMES AS RA_DISTRICT
,f04_ADR_R.CITIES_NAMES AS RA_CITY
,f04_ADR_R.SHOTNAME_CIT as RA_SETTLEMENT 
,f04_ADR_R.STREETS_NAMES AS RA_STREET
,f04_ADR_R.HOUSE AS RA_HOUSE
,f04_ADR_R.BUILD AS RA_BUILDING
,f04_ADR_R.FLAT AS RA_APARTMENT
,f04_ADR_R.POSTOFFICE AS RA_INDEX

/*адрес проживания*/
,COALESCE(f04_ADR_L.REGIONS_NAMES, '-') AS LA_REGION
,f04_ADR_L.AREAS_NAMES AS LA_DISTRICT
,f04_ADR_L.CITIES_NAMES AS LA_CITY
,f04_ADR_L.SHOTNAME_CIT as LA_SETTLEMENT 
,f04_ADR_L.STREETS_NAMES AS LA_STREET
,f04_ADR_L.HOUSE AS LA_HOUSE
,f04_ADR_L.BUILD AS LA_BUILDING
,f04_ADR_L.FLAT AS LA_APARTMENT
,f04_ADR_L.POSTOFFICE AS LA_INDEX

/*адрес работы*/
,COALESCE(f13_ORG_ADR_NEW.BA_REGION, '-') AS BA_REGION
,f13_ORG_ADR_NEW.BA_DISTRICT AS BA_DISTRICT
,f13_ORG_ADR_NEW.BA_CITY AS BA_CITY
,COALESCE(f13_ORG_ADR_NEW.BA_SETTLEMENT, '-') as BA_SETTLEMENT 
,f13_ORG_ADR_NEW.BA_STREET AS BA_STREET
,f13_ORG_ADR_NEW.BA_HOUSE AS BA_HOUSE
,f13_ORG_ADR_NEW.BA_BUILDING AS BA_BUILDING
,f13_ORG_ADR_NEW.BA_APARTMENT AS BA_APARTMENT
,f13_ORG_ADR_NEW.BA_INDEX AS BA_INDEX
,f13_ORG_ADR_NEW.ORG_NAME AS WORK_ORG_NAME

,f05_C_SCH.SCHEMS_NAME AS PRODUCT_TYPE
,f06_GRP.GROUPS_NAME AS REQ_CREATED_BRANCH
/*,_08_WH_SALARY.SALARY_SUM AS SALARY
,_08_WH_SALARY.EDUCATION_VID_ID AS EDUCATION
,_08_WH_SALARY.PENSION_SUM AS IS_PENSION*/
,SUBSTR(f09_DOC.DOCUMENTS_SERIAL,1,4)||SUBSTR(f09_DOC.DOCUMENTS_NUMBER,1,6) as PASSPORT
,f10_EMAIL.EMAIL
,f11_PHONE.MOBILE as MOBILE
,f11_PHONE.WORK_PHONE as WORK_PHONE
,f11_PHONE.HOME_PHONE as HOME_PHONE
--,null AS IS_NEW_CLIENT
--,null AS IS_NEW_DATA
--,null AS WORK_TYPE 
      
FROM C_REQUEST_FULL_ST_RID f00_C_RQ
		
    LEFT OUTER JOIN a_c_request_cr_pack f01_C_RQ_CR
		ON f00_C_RQ.REQUEST_ID=f01_C_RQ_CR.REQUEST_ID
		
    LEFT OUTER JOIN a_c_request_inf_pack f02_C_RQ_INF
    ON f00_C_RQ.REQUEST_ID=f02_C_RQ_INF.REQUEST_ID
		
    LEFT OUTER JOIN fio4_history f03_FIO
    ON f00_C_RQ.PERSON_ID=f03_FIO.OBJECTS_ID  
    AND f03_FIO.FIO_AKT=1
		
    LEFT OUTER JOIN address_4sr f04_ADR_R
    ON f02_C_RQ_INF.MADR_ID=f04_ADR_R.ADDRESS_ID
		
    LEFT OUTER JOIN address_4sr f04_ADR_L
    ON f02_C_RQ_INF.LADR_ID=f04_ADR_L.ADDRESS_ID
		
    LEFT OUTER JOIN C_SCHEMS@dblink_pkk f05_C_SCH
		ON f01_C_RQ_CR.SCHEMS_ID=f05_C_SCH.SCHEMS_ID
		
    LEFT OUTER JOIN "GROUPS"@dblink_pkk f06_GRP
		ON f00_C_RQ.CREATED_GROUP_ID=f06_GRP.GROUPS_ID
		
    /*LEFT OUTER JOIN A_WH_PC_RD_ALL f08_WH_SALARY*/
			/*ON f00_C_RQ.REQUEST_ID=f08_WH_SALARY.REQUEST_ID*/
		
    LEFT OUTER JOIN A_DOCS f09_DOC
    ON f02_C_RQ_INF.MDOC_ID=f09_DOC.DOCUMENTS_ID
    AND DOCUMENTS_AKT=1
		
    LEFT OUTER JOIN EMAIL f10_EMAIL
    ON f00_C_RQ.PERSON_ID=f10_EMAIL.OBJECTS_ID
    AND f10_EMAIL.OBJECTS_TYPE = 2 AND f10_EMAIL.EMAIL_AKT = 1
		
    LEFT OUTER JOIN a_phones_4sr_new	f11_PHONE
    ON f00_C_RQ.REQUEST_ID=f11_PHONE.REQUEST_ID
    AND f00_C_RQ.PERSON_ID=f11_PHONE.OBJECTS_ID

		LEFT OUTER JOIN a_adr_org f13_ORG_ADR_NEW
    ON f02_C_RQ_INF.WORK_ID=f13_ORG_ADR_NEW.WORKS_ID

		WHERE 
    ((f00_C_RQ.CREATED_GROUP_ID^=11455 AND f00_C_RQ.STATUS_ID^=14)
						OR f00_C_RQ.STATUS_ID=14)
)
; 
EXCEPTION
    WHEN OTHERS
    THEN
       DBMS_OUTPUT.put_line(  'Trapped the error L2: '||TO_CHAR(v_request_id) );
    --RAISE;
    
--COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."PR$UPD_RP_BNK" 
-- ========================================================================
-- ==	ПРОЦЕДУРА "Обновление БНК данных для сети"
-- ==	ОПИСАНИЕ:		обновляет таблицу с заявками для кредитов по которым был срабатывали БНК
-- ========================================================================
-- ==	СОЗДАНИЕ:		26.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	27.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
IS
  --day_ago NUMBER := 5;
  max_RID NUMBER;
  --dRID NUMBER;
  
  last_ID NUMBER;
BEGIN 
  DBMS_OUTPUT.ENABLE;

  SELECT NVL(MAX(REQUEST_ID), 0)-100 INTO max_RID FROM SFF.RP_BNK ;
  --dRID := max_RID+500000;
  
  DBMS_OUTPUT.PUT_LINE('Start: '||TO_CHAR(systimestamp at time zone 'utc') || '. От REQUEST_ID = '||TO_CHAR(max_RID));
  DELETE FROM SFF."RP_BNK_TMP" ;
  
  SELECT NVL(MAX(ID), 0) INTO last_ID FROM SFF.RP_BNK_HISTORY_LOG ;
  
    INSERT INTO SFF.RP_BNK_HISTORY_LOG
    SELECT REQUEST_ID, REQUEST_REACT_ID, PERSON_ID
        , SYSDATE AS DATE_INS, BNK_CODE
        , BNK_TYPE
        , DATE_EXEC
        , ID
      FROM scoring.L_FLOW_PRICE_LEVEL@SPR WHERE ID>last_ID;
    
  -- Пополняем нашу историческую таблицу со всеми REQUEST_ID из СПР.L_FLOW_PRICE_LEVEL. 
  -- Для ускорения общего запроса обновления
    INSERT INTO SFF.RP_BNK_TMP 
    SELECT * FROM (SELECT DISTINCT LFPL.REQUEST_ID
        ,LFPL.PERSON_ID
        ,SYSDATE AS DATE_INS
        ,LFPL.BNK_CODE
        ,LFPL.BNK_TYPE
        /*,LFPL.DATE_EXEC*/
      FROM SFF.RP_BNK_HISTORY_LOG /*scoring.L_FLOW_PRICE_LEVEL@SPR*/ LFPL
      WHERE ID>last_ID /*REQUEST_ID > max_RID AND REQUEST_ID < dRID*/
        AND LFPL.BNK_CODE>0 
        AND NOT EXISTS(SELECT REQUEST_ID 
                      FROM SFF.RP_BNK_HISTORY_LOG /*scoring.L_FLOW_PRICE_LEVEL@SPR */
                      WHERE PERSON_ID=LFPL.PERSON_ID AND REQUEST_ID>LFPL.REQUEST_ID 
                              AND DECODE(BNK_CODE, 0, 99999, BNK_CODE)>LFPL.BNK_CODE)) ;
  
  /*удаляем то что обновилось*/
  DELETE FROM SFF.RP_BNK for_del 
    WHERE EXISTS(SELECT REQUEST_ID FROM SFF.RP_BNK_TMP WHERE REQUEST_ID=for_del.REQUEST_ID);
  
  /*добавляем обновленное и новое*/
  INSERT INTO SFF.RP_BNK
    SELECT * FROM SFF."RP_BNK_TMP";
    
  /*обновляем уже добавленные сработавшие REQUEST_ID. Если физик пришел снова и код БНК по нему не сработал
        , то удаляем такую запис*/
  -- весь прогон 14 минут
  DELETE FROM SFF.RP_BNK for_del WHERE EXISTS(SELECT REQUEST_ID 
                                                FROM SFF.RP_BNK_HISTORY_LOG 
                                                WHERE PERSON_ID=for_del.PERSON_ID AND REQUEST_ID>for_del.REQUEST_ID 
                                                  AND DECODE(BNK_CODE, 0, 99999, BNK_CODE)>for_del.BNK_CODE);
    
  DBMS_OUTPUT.PUT_LINE('End:   '||TO_CHAR(systimestamp at time zone 'utc') || '. До REQUEST_ID = '||TO_CHAR(last_ID));
  COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SNAUSER"."PR$QUEUE_PKK_READ_MSG" 
-- ========================================================================
-- ==	ПРОЦЕДУРА     "Считывание очереди"
-- ==	ОПИСАНИЕ:	    Считывание очередных сообщений в очереди. Альтернативный метод.
-- ==	              Сообщение из таблицы очередей удаляется с задержкой (~ 10 сек.)                
-- == Примечание:   За выходные может упасть 100-150 тыс. сообщений.
-- == Внимание!     Если запустить кол-во считываний больше чем есть сообщений для считывания, 
-- ==                  то процесс может зависнуть.
-- ==               Если запустить когда работает основной процесс считывания, то он зависнет в сессиях.
-- ========================================================================
-- ==	СОЗДАНИЕ:		  02.10.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	02.10.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
IS
	deq_options     DBMS_AQ.dequeue_options_t;
	msg_properties  DBMS_AQ.message_properties_t;
	msg_handle      RAW(16);
	msg             msg_PKK;
  
  cntI NUMBER;        -- счетчик всех
  cntI_HAND NUMBER;   -- счетчик ручных
  cntAll_MSG NUMBER;  -- кол-во всех сообщений. Определяется в теле процедуры
	BEGIN
  DBMS_OUTPUT.ENABLE;
  DBMS_OUTPUT.PUT_LINE('Старт считывания очереди: '||TO_CHAR(systimestamp));
  cntI := 0;   cntI_HAND := 0;

  --скорость примерно 5000 шт за 1 мин.
    /*статистика замеров:
      Считывание очереди: 11:50:28 - 11:50:47 (Всего RID:41/5000)
      Считывание очереди: 11:52:35 - 12:38:38 (Всего RID:13183/300000)
      Считывание очереди: 04:41:24 - 04:51:11 (Всего RID:4080/100000)
    */
  SELECT COUNT(*) INTO cntAll_MSG FROM SNAUSER.AQ_TAB;
  DBMS_OUTPUT.PUT_LINE('Сообщений до: '||TO_CHAR(cntAll_MSG));
  
	FOR i IN 1..91 LOOP
		deq_options.navigation := DBMS_AQ.FIRST_MESSAGE;
		deq_options.consumer_name := 'SNAUSER';
		DBMS_AQ.DEQUEUE(queue_name          =>     'QUEUE_PKK',
		   dequeue_options     =>     deq_options,
		   message_properties  =>     msg_properties,
		   payload             =>     msg,
		   msgid               =>     msg_handle);

		/*DBMS_OUTPUT.PUT_LINE('msg_date = '||msg.msg_date||'; request_id = '||msg.request_id
                        ||'; old_status_id = '||msg.old_status_id||'; v_new_status_id = '||msg.v_new_status_id);
    */
				
    --если заявка пошла через ручное рассмотрение, то записать ее в таблицу SNA_dequeue_msg
    IF SFF.FN_CHECK_HAND_RID(msg.request_id)=1 THEN   
    		--DBMS_OUTPUT.PUT_LINE('msg_Id: '|| msg.msg_Id||'; msg_date: '||msg.msg_date||'; request_id: '||msg.request_id);
        cntI_HAND := cntI_HAND + 1;
        INSERT INTO SNAUSER.SNA_dequeue_msg
          VALUES (msg.msg_Id,msg.msg_date, msg.request_id, msg.old_status_id, msg.v_new_status_id, systimestamp);
        COMMIT; --подтверждаем когда REQUEST_ID прошел проверку.
    ELSE --иначе добавляем в таблицу реквестов не прошедших проверку на ручное рассмотрение. 
        --Можно закомменить, если таблица не будет использоваться.
        INSERT INTO "SNAUSER"."SNA_DEQUEUE_MSG_NOAKT"
          VALUES (msg.msg_Id,msg.msg_date, msg.request_id, msg.old_status_id, msg.v_new_status_id, systimestamp);
    END IF;
    
    cntI := cntI + 1;
    IF MOD(cntI,1000)=0 THEN 
        COMMIT;
    END IF;
  END LOOP;

  COMMIT;
  
  SELECT COUNT(*) INTO cntAll_MSG FROM SNAUSER.AQ_TAB;
  DBMS_OUTPUT.PUT_LINE('Сообщений после: '||TO_CHAR(cntAll_MSG));
  
  --для мониторинга при проверке процедуры.
  DBMS_OUTPUT.PUT_LINE('Конец считывания очереди: '||TO_CHAR(systimestamp)||'(Всего RID:'||TO_CHAR(cntI_HAND)||'/'||TO_CHAR(cntI)||')');

END;
  CREATE OR REPLACE PROCEDURE "SFF"."PR$APP_FROD_UPDATE_FROM_TMP" 
IS
-- Вспомогательная Процедура для обновления таблицы SNAUSER.SNA_LINKS
-- ======================================================================
-- ПРОЦЕДУРА "Вспомогательная. Для обновления таблицы SNAUSER.SNA_LINKS"
-- ОПИСАНИЕ:		Обновляет только отсутсвующие данные в SNAUSER.SNA_LINKS
--              Комментарий /*+ INDEX(SL I_SL_PERSON_ID) */ в запросе не удалять! 
-- ======================================================================
-- СОЗДАНИЕ:		03.09.2015 (ТРАХАЧЕВ В.В.)
-- МОДИФИКАЦИЯ:	03.09.2015 (ТРАХАЧЕВ В.В.)
-- ======================================================================
BEGIN

--удалить из добавляемой таблицы уже существующие данные. 
  DELETE FROM SFF."APPLICATIONS_FROD_tmp" af_tmp
						WHERE EXISTS(SELECT /*+ INDEX(SL I_SL_PERSON_ID) */ * 
								FROM SFF.APPLICATIONS_FROD af 
								WHERE af.REQUEST_ID=af_tmp.REQUEST_ID
                      AND af.PERSON_ID=af_tmp.PERSON_ID
									AND af.MOBILE=af_tmp.MOBILE AND af.HOME_PHONE=af_tmp.HOME_PHONE );
  COMMIT;
  
  --добавляем записи
  INSERT INTO SFF.APPLICATIONS_FROD
    SELECT * FROM SFF."APPLICATIONS_FROD_tmp";
    
  COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_P03" (v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА   "ФРОД-ПРАВИЛО 03. Регион Пасп не равен региону ПМЖ"
-- ==	ОПИСАНИЕ:		Регион ОКВЭД (первые 2 цифры серии паспорта, согласно справочнику) не соответствуют региону ПМЖ клиента
-- ==					    MADR_ID^=PASP2_REG, (RA_REGION)
-- ========================================================================
-- ==	СОЗДАНИЕ:		  27.10.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	27.10.2015 (ТРАХАЧЕВ В.В.)
-- ======================================================================== 
BEGIN
	INSERT INTO SFF.FROD_RULE_DEMO1 (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
						FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
						TYPE_REL, TYPE_REL_DESC, /*DAY_BETWEEN,*/
						/*REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,*/
						/*FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,*/
						INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
			SELECT DISTINCT sysdate as FROD_RULE_DATE
        ,TAB.* FROM 
			(SELECT DISTINCT /*sysdate as FROD_RULE_DATE
				,*/SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				--,C_R.STATUS_ID
        ,14 as STATUS_ID
				,/*C_R.SCORE_TREE_ROUTE_ID*/ NULL as SCORE_TREE_ROUTE_ID
				,/*C_R.CREATED_GROUP_ID*/ NULL as CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Регион Паспорта не равен региону ПМЖ' as TYPE_REL
				,'Регион ОКВЭД (первые 2 цифры серии паспорта, согласно справочнику) не соответствуют региону ПМЖ клиента' as TYPE_REL_DESC
				/*,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN*/
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				/*,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL*/
				/*,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL*/
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Регион:'||SRC.RA_REGION as INFO_EQ
				,'Паспорт.:'||SRC.PASSPORT as INFO_NEQ
				,'Сер. по Рег.ПМЖ:'||rp.PASP2_CHAR||'xx' as INFO_NEQ_REL
				/*,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS*/
          ,1 as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN SFF.REGIONS_PASP rp --таблица поиска связей
			ON /*RULE: определяем фрод-правило поиска */
          SRC.RA_REGION=rp.REGIONS_NAME AND rp.PASP2_CHAR^=SUBSTR(SRC.PASSPORT,1,2)
        /*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
        AND rp.PASP2_CHAR^='-1' AND NOT SRC.PASSPORT IS NULL AND NOT SRC.PASSPORT='-'
			/*LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID*/
			WHERE SRC.REQUEST_ID=v_request_id
				/*AND UTL_MATCH.EDIT_DISTANCE(SRC.FIO, APP.FIO)+UTL_MATCH.EDIT_DISTANCE(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1;
	COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."PR$UPD_REQUEST_ACCRED" 
-- ========================================================================
-- ==	ПРОЦЕДУРА "Обновление списка заявок на акрредитацию"
-- ==	ОПИСАНИЕ:		обновляет таблицу SFF.REQUEST_ACCREDITATION
-- ========================================================================
-- ==	СОЗДАНИЕ:		29.09.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	29.09.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
IS
   max_RID NUMBER;
BEGIN 
  DBMS_OUTPUT.ENABLE;

  SELECT NVL(MAX(REQUEST_ID), 0) INTO max_RID FROM SFF.REQUEST_ACCREDITATION ;
  
  DBMS_OUTPUT.PUT_LINE('Start: '||TO_CHAR(systimestamp at time zone 'utc') || '. От REQUEST_ID = '||TO_CHAR(max_RID));
  
  -- проверка 10 млн. заявок - 4 мин.
  /*добавляем новое*/
  INSERT INTO SFF.REQUEST_ACCREDITATION
    SELECT cr.REQUEST_ID, cr.OBJECTS_ID, cr.OBJECTS_TYPE
      ,cr.CREATED_DATE
      ,cr.TYPE_REQUEST_ID, cr.STATUS_ID, cr.CREATED_USER_ID
      ,cr.CREATED_GROUP_ID
      ,gr.GROUPS_NAME
    FROM KREDIT.C_REQUEST@DBLINK_PKK cr
    LEFT OUTER JOIN KREDIT."GROUPS"@DBLINK_PKK gr ON cr.CREATED_GROUP_ID=gr.GROUPS_ID
    WHERE cr.REQUEST_ID>max_RID
      /*AND cr.REQUEST_ID>50000000 AND cr.REQUEST_ID<60000000 */
      AND cr.CREATED_GROUP_ID=8393
      AND NOT EXISTS(SELECT * FROM SFF.REQUEST_ACCREDITATION WHERE REQUEST_ID=cr.REQUEST_ID);

  SELECT NVL(MAX(REQUEST_ID), 0) INTO max_RID FROM SFF.REQUEST_ACCREDITATION ; 
  DBMS_OUTPUT.PUT_LINE('End:   '||TO_CHAR(systimestamp at time zone 'utc') || '. До REQUEST_ID = '||TO_CHAR(max_RID));
  
  COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."PR$UPD_RP_DEFOLT" 
-- ========================================================================
-- ==	ПРОЦЕДУРА "Обновление криминальных дефолтников"
-- ==	ОПИСАНИЕ:		обновляет таблицу с заявками для кредитов по которым был криминальный дефолт
-- ========================================================================
-- ==	СОЗДАНИЕ:		25.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	25.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
IS
  day_ago NUMBER := 10;
BEGIN 
  DELETE FROM SFF."RP_DEFOLT_tmp" ;
  
  INSERT INTO SFF."RP_DEFOLT_tmp"
    SELECT cci.REQUEST_ID
            ,crid.OBJECTS_ID AS PERSON_ID
            ,crid.CREATED_DATE
            ,cci.MODIFICATION_DATE 
            ,SUBSTR(cci.MOP_DELAY,1,5) as MOP_lst5
            ,NVL(SUBSTR(cci.MOP_DELAY,-5,5), cci.MOP_DELAY) as MOP_fst5
            ,NVL(SUBSTR(RTRIM(cci.MOP_DELAY,'0-'),-3,3), RTRIM(cci.MOP_DELAY,'0-')) AS MOP_LST_TRIM
            ,1 as DEFOLT
            ,crid.STATUS_ID
            ,cci.MOP_DELAY
            ,(SYSDATE) as RP_DEFOLT_DATE
      FROM KREDIT.C_CREDIT_INFO@DBLINK_PKK cci
      LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK crid
        ON cci.REQUEST_ID=crid.REQUEST_ID
      WHERE cci.MODIFICATION_DATE > sysdate-day_ago
          AND regexp_substr(NVL(SUBSTR(RTRIM(cci.MOP_DELAY,'0-'),-3,3), RTRIM(cci.MOP_DELAY,'0-')), '[2345789]', 1)>0
          AND NOT EXISTS(SELECT REQUEST_ID FROM SFF.RP_DEFOLT WHERE REQUEST_ID=cci.REQUEST_ID AND MOP_DELAY=cci.MOP_DELAY);
  
  /*удаляем то что обновилось*/
  DELETE FROM SFF.RP_DEFOLT for_del 
    WHERE EXISTS(SELECT REQUEST_ID FROM SFF."RP_DEFOLT_tmp" WHERE REQUEST_ID=for_del.REQUEST_ID);
  
  /*добавляем обновленное и новое*/
  INSERT INTO SFF.RP_DEFOLT
    SELECT * FROM SFF."RP_DEFOLT_tmp";
  
 
  COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."PR$UPD$BASE_SFF_TABLE" as
-- ========================================================================
-- ==	ПРОЦЕДУРА		"Запуск обновления базовых таблиц в схеме SFF"
-- ==	ОПИСАНИЕ:		Для постановки на планировщик.
-- ========================================================================
-- ==	СОЗДАНИЕ:		30.09.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	30.09.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
BEGIN
	SFF.PR$UPD_RP_BNK;
  SFF.PR$UPD_RP_DEFOLT;
	SFF.PR$UPD_REQUEST_ACCRED;
  --SFF."PR$UPD_SFF_C_R";
END;
  CREATE OR REPLACE FUNCTION "SFF"."FN_CHECK_HAND_RID" (inRID IN NUMBER) 
  RETURN NUMBER
  
IS
 FLAG_NUM NUMBER;
BEGIN
  DBMS_OUTPUT.ENABLE;
            
  FLAG_NUM :=0;
  SELECT 1 INTO FLAG_NUM FROM kredit.c_request@dblink_pkk cr
          WHERE REQUEST_ID=inRID 
              AND EXISTS(SELECT * FROM kredit.c_request_react@dblink_pkk 
                        where request_id=cr.request_id and request_new_status_id IN(2,3,4,35,163,24,25,27,79,80,90,87,88,89,97,141,173,255));
          
  IF FLAG_NUM>0 THEN
    --DBMS_OUTPUT.PUT_LINE(' РУЧНАЯ '||FLAG_NUM);
    RETURN 1;
  ELSE 
    --DBMS_OUTPUT.PUT_LINE(' не РУЧНАЯ '||FLAG_NUM);
    RETURN 0;
  END IF;


EXCEPTION
    WHEN OTHERS
    THEN /*DBMS_OUTPUT.PUT_LINE('REQUEST_ID ERROR (not exist)')*/ RETURN 0;

END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_P04" (v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА   "ФРОД-ПРАВИЛО 03. Регион Пасп не равен региону ФМЖ"
-- ==	ОПИСАНИЕ:		Регион ОКВЭД (первые 2 цифры серии паспорта, согласно справочнику) не соответствуют региону ПМЖ клиента
-- ==					    LADR_ID^=PASP2_REG, (LA_REGION)
-- ========================================================================
-- ==	СОЗДАНИЕ:		  27.10.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	27.10.2015 (ТРАХАЧЕВ В.В.)
-- ======================================================================== 
BEGIN
	INSERT INTO SFF.FROD_RULE_DEMO1 (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
						FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
						TYPE_REL, TYPE_REL_DESC, /*DAY_BETWEEN,*/
						/*REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,*/
						/*FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,*/
						INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
		SELECT DISTINCT sysdate as FROD_RULE_DATE
        ,TAB.* FROM 
			(SELECT DISTINCT /*sysdate as FROD_RULE_DATE
				,*/SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
        ,14 as STATUS_ID
				,/*C_R.SCORE_TREE_ROUTE_ID*/ NULL as SCORE_TREE_ROUTE_ID
				,/*C_R.CREATED_GROUP_ID*/ NULL as CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Регион Паспорта не равен региону ФМЖ' as TYPE_REL
				,'Регион ОКВЭД (первые 2 цифры серии паспорта, согласно справочнику) не соответствуют региону ФМЖ клиента' as TYPE_REL_DESC
				/*,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN*/
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				/*,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL*/
				/*,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL*/
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Регион:'||SRC.LA_REGION as INFO_EQ
				,'Паспорт.:'||SRC.PASSPORT as INFO_NEQ
				,'Сер. по Рег.ФМЖ:'||rp.PASP2_CHAR||'xx' as INFO_NEQ_REL
				/*,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS*/
          ,1 as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN SFF.REGIONS_PASP rp --таблица поиска связей
			ON /*RULE: определяем фрод-правило поиска */
          SRC.LA_REGION=rp.REGIONS_NAME AND rp.PASP2_CHAR^=SUBSTR(SRC.PASSPORT,1,2)
        /*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
        AND rp.PASP2_CHAR^='-1' AND NOT SRC.PASSPORT IS NULL AND NOT SRC.PASSPORT='-'
			/*LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID*/
			WHERE SRC.REQUEST_ID=v_request_id
				/*AND UTL_MATCH.EDIT_DISTANCE(SRC.FIO, APP.FIO)+UTL_MATCH.EDIT_DISTANCE(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1;
	COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."PR$UPD_APPLICATIONS_FROD" (vRID IN NUMBER)
-- ========================================================================
-- ==	ПРОЦЕДУРА     Заполнение таблицы для проверки фрод.
-- ==	ОПИСАНИЕ:	    Пополняем таблицу при условии что данные заявки нужны для проверки по фрод
-- ========================================================================
-- ==	СОЗДАНИЕ:		  16.10.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	16.10.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
AS    
	flag_Checked NUMBER;
BEGIN
  DBMS_OUTPUT.ENABLE;
	flag_Checked := 1;
  
  /*insert into SFF.APPLICATIONS (REQUEST_ID,PERSON_ID,REQ_DATE,REQ_SUMM,REQ_SUMM_CURR,BIRTHDAY,FIO
      ,RA_REGION,RA_DISTRICT,RA_CITY,RA_SETTLEMENT,RA_STREET,RA_HOUSE,RA_BUILDING,RA_APARTMENT,RA_INDEX
      ,LA_REGION,LA_DISTRICT,LA_CITY,LA_SETTLEMENT,LA_STREET,LA_HOUSE,LA_BUILDING,LA_APARTMENT,LA_INDEX
      ,BA_REGION,BA_DISTRICT,BA_CITY,BA_SETTLEMENT,BA_STREET,BA_HOUSE,BA_BUILDING,BA_APARTMENT,BA_INDEX
      ,WORK_ORG_NAME,PRODUCT_TYPE,REQ_CREATED_BRANCH,PASSPORT,EMAIL,MOBILE,WORK_PHONE,HOME_PHONE)
    SELECT REQUEST_ID,PERSON_ID,REQ_DATE,REQ_SUMM,REQ_SUMM_CURR,BIRTHDAY,FIO
      ,RA_REGION,RA_DISTRICT,RA_CITY,RA_SETTLEMENT,RA_STREET,RA_HOUSE,RA_BUILDING,RA_APARTMENT,RA_INDEX
      ,LA_REGION,LA_DISTRICT,LA_CITY,LA_SETTLEMENT,LA_STREET,LA_HOUSE,LA_BUILDING,LA_APARTMENT,LA_INDEX
      ,BA_REGION,BA_DISTRICT,BA_CITY,BA_SETTLEMENT,BA_STREET,BA_HOUSE,BA_BUILDING,BA_APARTMENT,BA_INDEX
      ,WORK_ORG_NAME,PRODUCT_TYPE,REQ_CREATED_BRANCH,PASSPORT,EMAIL,MOBILE,WORK_PHONE,HOME_PHONE 
    FROM snauser.applications@SNA WHERE REQUEST_ID=vRID; 

  INSERT INTO SFF.CONTACTS (REQUEST_ID, PERSON_ID, CONTACT_PERSON_ID, CONTACT_RELATION, CONTACT_FIO, CONTACT_PHONE, IS_NEW_DATA)
      SELECT REQUEST_ID, PERSON_ID, CONTACT_PERSON_ID, CONTACT_RELATION, CONTACT_FIO, CONTACT_PHONE, IS_NEW_DATA 
          FROM SNAUSER.CONTACTS@SNA WHERE REQUEST_ID=vRID;*/
        
	--vRID := 16149802 ;
	/*SELECT COUNT(*) INTO flag_Checked FROM dual
		WHERE NOT EXISTS(SELECT REQUEST_ID FROM KREDIT.C_REQUEST_REACT@DBLINK_PKK
					WHERE REQUEST_ID = vRID
						AND request_old_status_id = 200 and request_new_status_id = 7)*/
			/*AND NOT EXISTS(SELECT REQUEST_ID FROM KREDIT.C_REQUEST@DBLINK_PKK
					WHERE REQUEST_ID = vRID
						AND (CREATED_GROUP_ID IN(4862,4863,4865,4864,5736,5899,7674,11455,10165)
							--выбираем незавершенные заявки
							OR NOT (status_id IN(7,8,9,11,14,16,17,21))
							--исключить отказы по прескорингу
							OR (score_tree_route_id=6 and status_id in(7,8))
							--убираем мгновенные продажи
							OR (score_tree_route_id=1)) ) ;*/
          

	/*IF flag_Checked = 1 THEN */
    DBMS_OUTPUT.PUT_LINE(' Добавлена '||vRID);
    DELETE FROM SFF."APPLICATIONS_FROD_tmp" WHERE REQUEST_ID = vRID;
    
		INSERT INTO SFF."APPLICATIONS_FROD_tmp"
			(DR
        ,FIRST_L,FIRST_F,FIRST_M
        ,REQUEST_ID,PERSON_ID,REQ_DATE
        ,FIO
        ,RA_REGION,RA_DISTRICT,RA_CITY,RA_STREET,RA_HOUSE,RA_BUILDING,RA_APARTMENT
        ,LA_REGION,LA_DISTRICT,LA_CITY,LA_STREET,LA_HOUSE, LA_BUILDING,LA_APARTMENT
        ,PASSPORT,EMAIL,MOBILE,HOME_PHONE
        ,DATE_UPD)
			SELECT DISTINCT COALESCE(TO_CHAR(BIRTHDAY, 'dd.mm.yyyy'),'-') AS DR
        ,REGEXP_SUBSTR(FIO,'[А-Я]',1 ) AS FIRST_L
        ,SUBSTR(TRIM(REGEXP_SUBSTR(FIO ,' [А-Я]',1, 1)), 1,1) as FIRST_F
        ,SUBSTR(TRIM(REGEXP_SUBSTR(FIO ,' [А-Я]',1 , 2)), 1,1) AS FIRST_M
        ,REQUEST_ID, PERSON_ID, REQ_DATE
        ,COALESCE(FIO,'-') AS FIO
        ,COALESCE(RA_REGION,'-') AS RA_REGION,COALESCE(RA_DISTRICT,'-') AS RA_DISTRICT
        ,COALESCE(RA_CITY,'-') AS RA_CITY, COALESCE(RA_STREET,'-') AS RA_STREET
        ,COALESCE(RA_HOUSE,'-') AS RA_HOUSE, COALESCE(RA_BUILDING,'-') AS RA_BUILDING, COALESCE(RA_APARTMENT,'-') AS RA_APARTMENT
        ,COALESCE(LA_REGION,'-') AS LA_REGION, COALESCE(LA_DISTRICT,'-') AS LA_DISTRICT
        ,COALESCE(LA_CITY,'-') AS LA_CITY, COALESCE(LA_STREET,'-') AS LA_STREET
        ,COALESCE(LA_HOUSE,'-') AS LA_HOUSE, COALESCE(LA_BUILDING,'-') AS LA_BUILDING, COALESCE(LA_APARTMENT,'-') AS LA_APARTMENT
        ,COALESCE(PASSPORT,'-') as PASSPORT, COALESCE(EMAIL,'-') as EMAIL
        ,COALESCE(MOBILE,0) as MOBILE,COALESCE(HOME_PHONE,0) as HOME_PHONE
        ,sysdate AS DATE_UPD
      FROM SNAUSER.APPLICATIONS@SNA WHERE REQUEST_ID=vRID;
  /*END IF;*/
EXCEPTION
    WHEN OTHERS
    THEN NULL;
END;
  CREATE OR REPLACE TRIGGER "SFF"."FROD_RULE_DEMO_TRG" BEFORE INSERT ON SFF.FROD_RULE_DEMO 
FOR EACH ROW 
BEGIN
  <<COLUMN_SEQUENCES>>
  BEGIN
    IF :NEW.FROD_RULE_ID IS NULL THEN
      SELECT FROD_RULE_DEMO_SEQ.NEXTVAL INTO :NEW.FROD_RULE_ID FROM DUAL;
    END IF;
  END COLUMN_SEQUENCES;
  
  IF NOT :NEW.REQUEST_ID_REL IS NULL THEN
    --SELECT SFF.FN_STRIP_DUBL_IN_STR(:NEW.REQUEST_ID_REL) INTO :NEW.REQUEST_ID_REL FROM DUAL;
    :NEW.REQUEST_ID_REL := SFF.FN_DEDUBL_STR(:NEW.REQUEST_ID_REL);
  END IF;
END;
ALTER TRIGGER "SFF"."FROD_RULE_DEMO_TRG" ENABLE"
"
  CREATE OR REPLACE PROCEDURE "SFF"."PR$UPD_SFF_C_R" (nRID_ago IN NUMBER default 2000000)
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицу с данными из C_REQUEST
-- ==	ОПИСАНИЕ:	    Обновляем статусов и прочее для заявок
-- ========================================================================
-- ==	СОЗДАНИЕ:		  09.11.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	09.11.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
AS    
  last_RID_SFF NUMBER; -- последний существующий REQUEST_ID в SFF
  last_RID_PKK NUMBER; -- последний существующий REQUEST_ID в ПКК
  
  last_MODDT_SFF DATE; -- последний MODIFICATION_DATE в SFF
  last_MODDT_PKK DATE; -- последний MODIFICATION_DATE в ПКК
BEGIN
  DBMS_OUTPUT.ENABLE;
  
  --назад на 7 млн - около 10 мин.
  --назад на 2 млн - около 1 мин.
  --SELECT MAX(REQUEST_ID)-nRID_ago INTO last_RID_SFF FROM SFF.SFF_C_REQUEST;
  --SELECT MAX(REQUEST_ID) INTO last_RID_PKK FROM KREDIT.C_REQUEST@DBLINK_PKK;
  
  --SELECT MAX(MODIFICATION_DATE)-100 INTO last_MODDT_SFF FROM SFF.SFF_C_REQUEST;
  SELECT MAX(MODIFICATION_DATE) INTO last_MODDT_PKK FROM KREDIT.C_REQUEST@DBLINK_PKK;
  last_MODDT_SFF := last_MODDT_PKK-100;

  MERGE INTO SFF.SFF_C_REQUEST tar
  USING (SELECT REQUEST_ID, TYPE_REQUEST_ID, STATUS_ID, OBJECTS_ID, OBJECTS_TYPE
              , CREATED_USER_ID, CREATED_GROUP_ID, CREATED_DATE, MODIFICATION_DATE
              , REQUEST_REACT_ID_LAST, REQUEST_CREDIT_ID_LAST, REQUEST_INFO_ID_LAST, SCORE_TREE_ROUTE_ID, PARENT_ID
          FROM KREDIT.C_REQUEST@DBLINK_PKK 
          WHERE /*REQUEST_ID>=last_RID_SFF AND REQUEST_ID<last_RID_PKK*/
                MODIFICATION_DATE>=last_MODDT_SFF AND MODIFICATION_DATE<=last_MODDT_PKK ) src
    ON (src.REQUEST_ID=tar.REQUEST_ID )
  WHEN MATCHED THEN
    --закомменченые поля не нужно обновлять. FLAG_NEW_DATA=2 если обновились существующуие данные
    UPDATE SET /*tar.TYPE_REQUEST_ID=src.TYPE_REQUEST_ID,*/ tar.STATUS_ID=src.STATUS_ID/*, tar.OBJECTS_TYPE=src.OBJECTS_TYPE*/
                , tar.CREATED_USER_ID=src.CREATED_USER_ID, tar.CREATED_GROUP_ID=src.CREATED_GROUP_ID
                /*, tar.CREATED_DATE=src.CREATED_DATE*/, tar.MODIFICATION_DATE=src.MODIFICATION_DATE
                , tar.REQUEST_REACT_ID_LAST=src.REQUEST_REACT_ID_LAST, tar.REQUEST_CREDIT_ID_LAST=src.REQUEST_CREDIT_ID_LAST
                , tar.REQUEST_INFO_ID_LAST=src.REQUEST_INFO_ID_LAST, tar.SCORE_TREE_ROUTE_ID=src.SCORE_TREE_ROUTE_ID 
                ,tar.FLAG_NEW_DATA=(CASE WHEN tar.MODIFICATION_DATE^=src.MODIFICATION_DATE THEN 2 ELSE 0 END)
                ,tar.PARENT_ID=src.PARENT_ID
  WHEN NOT MATCHED THEN 
    --вставляем новое
    INSERT (tar.REQUEST_ID, tar.TYPE_REQUEST_ID, tar.STATUS_ID, tar.OBJECTS_ID, tar.OBJECTS_TYPE
          , tar.CREATED_USER_ID, tar.CREATED_GROUP_ID, tar.CREATED_DATE, tar.MODIFICATION_DATE
          , tar.REQUEST_REACT_ID_LAST, tar.REQUEST_CREDIT_ID_LAST, tar.REQUEST_INFO_ID_LAST, tar.SCORE_TREE_ROUTE_ID
          , tar.FLAG_NEW_DATA, tar.PARENT_ID) 
    VALUES (src.REQUEST_ID, src.TYPE_REQUEST_ID, src.STATUS_ID, src.OBJECTS_ID, src.OBJECTS_TYPE
          , src.CREATED_USER_ID, src.CREATED_GROUP_ID, src.CREATED_DATE, src.MODIFICATION_DATE
          , src.REQUEST_REACT_ID_LAST, src.REQUEST_CREDIT_ID_LAST, src.REQUEST_INFO_ID_LAST, src.SCORE_TREE_ROUTE_ID
          , 1, src.PARENT_ID)
    ;	
    --DBMS_OUTPUT.PUT_LINE('RID:'||TO_CHAR(last_RID_SFF)||' - '||TO_CHAR(last_RID_PKK));
    DBMS_OUTPUT.PUT_LINE('RID:'||TO_CHAR(last_MODDT_SFF)||' - '||TO_CHAR(last_MODDT_PKK));
EXCEPTION
    WHEN OTHERS
    THEN DBMS_OUTPUT.PUT_LINE('Ошибка выполения процедуры ');
END;
  CREATE OR REPLACE FUNCTION "SFF"."FN_IS_POS_RID" (inRID IN NUMBER) RETURN NUMBER
-- ========================================================================
-- ==	ФУНКЦИЯ       Проверка признака что заявка является POS (логика ПКК).
-- ==	ОПИСАНИЕ:   
-- == КОММЕНТАРИЙ:  Обращение к ПКК стендбаю
-- ========================================================================
-- ==	СОЗДАНИЕ:		  20.11.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.11.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
IS
 FLAG NUMBER;
BEGIN
  --DBMS_OUTPUT.ENABLE;
  -- для проверки 55000042, 55000155  ,55000279, 55000420   
  --старое решение от 20.11.2015
  /*SELECT count(*) INTO FLAG_POS FROM dual
      WHERE EXISTS(SELECT * FROM KREDIT.C_REQUEST_CREDIT@DBLINK_PKK crc
                            LEFT JOIN KREDIT.C_SCHEMS@DBLINK_PKK c_sch ON (crc.SCHEMS_ID = c_sch.SCHEMS_ID)
                            LEFT JOIN KREDIT.RETAIL_PRODUCT@DBLINK_PKK ret_prod 
                              ON (ret_prod.RETAIL_PRODUCT_ID = c_sch.RETAIL_PRODUCT_ID)
                            WHERE crc.REQUEST_ID=inRID AND ret_prod.RETAIL_PRODUCT_GROUPS_ID = 5 );*/
    --новое решение от 15.01.2016
    SELECT FIRST_VALUE(ret_prod.RETAIL_PRODUCT_GROUPS_ID) 
            OVER (PARTITION BY crc.REQUEST_ID ORDER BY crc.REQUEST_CREDIT_ID DESC) INTO FLAG
          /*LAST_VALUE(ret_prod.RETAIL_PRODUCT_GROUPS_ID)  
              OVER (PARTITION BY crc.REQUEST_ID order by crc.REQUEST_CREDIT_ID rows between current row and unbounded following)     */
                    FROM KREDIT.C_REQUEST_CREDIT@DBLINK_PKK crc
                            LEFT JOIN KREDIT.C_SCHEMS@DBLINK_PKK c_sch ON (crc.SCHEMS_ID = c_sch.SCHEMS_ID)
                            LEFT JOIN KREDIT.RETAIL_PRODUCT@DBLINK_PKK ret_prod 
                              ON (ret_prod.RETAIL_PRODUCT_ID = c_sch.RETAIL_PRODUCT_ID)
                            WHERE crc.REQUEST_ID=inRID /*AND ret_prod.RETAIL_PRODUCT_GROUPS_ID IN(6, 7, 32,39)*/
                                  and rownum=1;

  --IF FLAG_POS>0 THEN
  IF FLAG IN(5) THEN
    --DBMS_OUTPUT.PUT_LINE(' Пос кредит '||FLAG);
    RETURN 1;
  ELSE 
    --DBMS_OUTPUT.PUT_LINE(' Не пос кредит '||FLAG);
    RETURN 0;
  END IF;

  EXCEPTION
    WHEN OTHERS
     THEN 
        --DBMS_OUTPUT.PUT_LINE('REQUEST_ID ERROR (not exist)')
        RETURN -1;
END;
  CREATE OR REPLACE FUNCTION "SFF"."FN_IS_HAND_RID" (inRID IN NUMBER) RETURN NUMBER
-- ========================================================================
-- ==	ФУНКЦИЯ     Проверка была ли заявка на ручном рассмотрениии.
-- ==	ОПИСАНИЕ:   Обращение к ПКК стендбаю
-- ========================================================================
-- ==	СОЗДАНИЕ:		  20.11.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	20.11.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
IS
 FLAG_NUM NUMBER;
BEGIN
  DBMS_OUTPUT.ENABLE;
           
  SELECT count(*) INTO FLAG_NUM FROM dual
      WHERE EXISTS(SELECT * FROM kredit.c_request_react@dblink_pkk 
                    where request_id=inRID and request_new_status_id IN(2,3,4,35,163,24,25,27,79,80,90,87,88,89,97,141,173,255));
      
  IF FLAG_NUM>0 THEN
    --DBMS_OUTPUT.PUT_LINE(' РУЧНАЯ '||FLAG_NUM);
    RETURN 1;
  ELSE 
    --DBMS_OUTPUT.PUT_LINE(' не РУЧНАЯ '||FLAG_NUM);
    RETURN 0;
  END IF;

EXCEPTION
    WHEN OTHERS
    THEN 
        --DBMS_OUTPUT.PUT_LINE('REQUEST_ID ERROR (not exist)')
        RETURN -1;

END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_H10" (v_request_id IN NUMBER, fraud_flag OUT NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО -- Телефон1-Псп0"
-- ==	ОПИСАНИЕ:		Совпадает личный телефон
-- ==					, отличается паспорт и хотя бы 1 из параметров: Дата рождения или ФИО, за последние 90 дней. 
-- ==					(МОБ ТЕЛ =, ПАСП ^= и (ДР^= или ФИО^=) за 90 дней) 
-- ==         Правка 07.09.2015: добавлены фильтры для телефонов
-- ========================================================================
-- ==	СОЗДАНИЕ:		20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	02.02.2016 (ЛЯЛИН Н.В.)
-- ========================================================================

  CURSOR get_fraud(in_RID NUMBER) IS
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
				,'Телефон1-Псп0' as TYPE_REL
				,'Совпадает личный телефон, отличается паспорт и хотя бы 1 из параметров: Дата рождения или ФИО, за последние 90 дней.' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Тел.моб:'||TO_CHAR(SRC.MOBILE) as INFO_EQ
				,'Фио+Др.:'||SRC.FIO||' '||SRC.DR as INFO_NEQ
				,'Фио+Др.:'||APP.FIO||' '||APP.DR as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
				ON --RULE: определяем фрод-правило поиска 
					SRC.MOBILE = APP.MOBILE
						AND SRC.PASSPORT^=APP.PASSPORT 
						AND (SRC.FIO^=APP.FIO OR SRC.DR^=APP.DR)
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91*1 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся физиков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=in_RID
				--EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)
				AND NOT SRC.MOBILE IS NULL AND SRC.MOBILE>0
				AND NOT SRC.PASSPORT='-'AND NOT SRC.PASSPORT IS NULL AND NOT APP.PASSPORT='-'AND NOT APP.PASSPORT IS NULL 
        AND NOT SRC.FIO IS NULL AND NOT APP.FIO IS NULL AND NOT SRC.FIO='-' AND NOT APP.FIO='-'
        AND NOT SRC.DR='-' AND NOT SRC.DR IS NULL
        AND NOT MOD(SRC.MOBILE, 1000000)=0 AND NOT TO_CHAR(SRC.MOBILE) LIKE '%999999%'
				--AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1
        --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
      --исключаем девичьи фамилии
      AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL )
      -- исключение однофамильцев 
			AND NOT UTL_MATCH.EDIT_DISTANCE(SUBSTR(FIO, 1, INSTR(FIO,' ')-1 ), SUBSTR(FIO_REL, 1, INSTR(FIO_REL,' ')-1 ))<3
			--PostCheck: Модифицированная проверка на родственников
      AND NOT SFF.FN_IS_FAMILY_REL(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1 
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID_REL, TAB.PERSON_ID)=1
      --TYPE_REL IN('Телефон1-Псп0' , 'Яма_5' , 'Яма_6')
      AND EXISTS(SELECT * FROM CPD.PHONES@DBLINK_PKK 
                        WHERE (OBJECTS_ID=TAB.PERSON_ID OR OBJECTS_ID=TAB.PERSON_ID_REL ) AND OBJECTS_TYPE=2 
                            AND PHONE=SUBSTR(TAB.INFO_EQ, 9, 10)
                            AND PHONES_COMM = 'Телефон из РБО: Мобильный')
                    ;

  in_fraud get_fraud%ROWTYPE;

--TYPE is_Frod_table IS TABLE OF SFF.FROD_RULE_DEMO1%ROWTYPE	INDEX BY BINARY_INTEGER;
--cur_frod is_Frod_table;


BEGIN   
  DBMS_OUTPUT.ENABLE;
  DBMS_OUTPUT.PUT_LINE('fraud = '||fraud_flag);
  --fraud_flag := NULL;
  OPEN get_fraud(v_request_id);
	--FETCH get_fraud INTO in_fraud;
	LOOP 
    FETCH get_fraud INTO in_fraud;
		EXIT WHEN get_fraud%NOTFOUND;

    IF (fraud_flag IS NULL) THEN 
      --фрод найден, возвращаем 1
      fraud_flag := 1;   
    END IF; 
    
      INSERT INTO SFF.FROD_RULE_DEMO1(FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
                FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
                TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
                REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
                FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
                INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
        VALUES(
        in_fraud.FROD_RULE_DATE, in_fraud.REQUEST_ID, in_fraud.PERSON_ID, in_fraud.REQ_DATE,
                in_fraud.FIO, in_fraud.DR, in_fraud.STATUS_ID, in_fraud.SCORE_TREE_ROUTE_ID, in_fraud.CREATED_GROUP_ID,
                in_fraud.TYPE_REL, in_fraud.TYPE_REL_DESC, in_fraud.DAY_BETWEEN,
                in_fraud.REQUEST_ID_REL, in_fraud.PERSON_ID_REL, in_fraud.REQ_DATE_REL,
                in_fraud.FIO_REL, in_fraud.DR_REL, in_fraud.STATUS_ID_REL, in_fraud.SCORE_TREE_ROUTE_ID_REL, in_fraud.CREATED_GROUP_ID_REL,
                in_fraud.INFO_EQ, in_fraud.INFO_NEQ, in_fraud.INFO_NEQ_REL, in_fraud.F_POS);
    
    /*INSERT INTO SFF.FROD_RULE_DEMO1 (FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
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
          ,'Телефон1-Псп0' as TYPE_REL
          ,'Совпадает личный телефон, отличается паспорт и хотя бы 1 из параметров: Дата рождения или ФИО, за последние 90 дней.' as TYPE_REL_DESC
          ,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
          ,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
                                OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
          ,APP.PERSON_ID PERSON_ID_REL
          ,APP.REQ_DATE AS REQ_DATE_REL
          ,APP.FIO as FIO_REL, APP.DR as DR_REL
          ,C_R_REL.STATUS_ID as STATUS_ID_REL
          --,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
          --,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
          , NULL as SCORE_TREE_ROUTE_ID_REL
          , NULL as CREATED_GROUP_ID_REL
          --INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
          ,'Тел.моб:'||TO_CHAR(SRC.MOBILE) as INFO_EQ
          ,'Фио+Др.:'||SRC.FIO||' '||SRC.DR as INFO_NEQ
          ,'Фио+Др.:'||APP.FIO||' '||APP.DR as INFO_NEQ_REL
          ,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
                      ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
        FROM APPLICATIONS_FROD SRC
        INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
          ON --RULE: определяем фрод-правило поиска 
            SRC.MOBILE = APP.MOBILE
              AND SRC.PASSPORT^=APP.PASSPORT 
              AND (SRC.FIO^=APP.FIO OR SRC.DR^=APP.DR)
              AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91*1 AND SRC.REQ_DATE
              AND APP.REQUEST_ID<SRC.REQUEST_ID
        LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
          ON SRC.REQUEST_ID=C_R.REQUEST_ID
        LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся физиков
          ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
        WHERE SRC.REQUEST_ID=v_request_id
          --EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)
          AND NOT SRC.MOBILE IS NULL AND SRC.MOBILE>0
          AND NOT SRC.PASSPORT='-'AND NOT SRC.PASSPORT IS NULL AND NOT APP.PASSPORT='-'AND NOT APP.PASSPORT IS NULL 
          AND NOT SRC.FIO IS NULL AND NOT APP.FIO IS NULL AND NOT SRC.FIO='-' AND NOT APP.FIO='-'
          AND NOT SRC.DR='-' AND NOT SRC.DR IS NULL
          AND NOT MOD(SRC.MOBILE, 1000000)=0 AND NOT TO_CHAR(SRC.MOBILE) LIKE '%999999%'
          --AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1
          --если нужно будет удалить подобных
        ) TAB
      WHERE TAB.F_POS=1 
        --исключаем девичьи фамилии
        AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL )
        -- исключение однофамильцев 
        AND NOT UTL_MATCH.EDIT_DISTANCE(SUBSTR(FIO, 1, INSTR(FIO,' ')-1 ), SUBSTR(FIO_REL, 1, INSTR(FIO_REL,' ')-1 ))<3
        --PostCheck: Модифицированная проверка на родственников
        AND NOT SFF.FN_IS_FAMILY_REL(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1
        AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1 
        AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID_REL, TAB.PERSON_ID)=1
        --TYPE_REL IN('Телефон1-Псп0' , 'Яма_5' , 'Яма_6')
        AND EXISTS(SELECT * FROM CPD.PHONES@DBLINK_PKK 
                          WHERE (OBJECTS_ID=TAB.PERSON_ID OR OBJECTS_ID=TAB.PERSON_ID_REL ) AND OBJECTS_TYPE=2 
                              AND PHONE=SUBSTR(TAB.INFO_EQ, 9, 10)
                              AND PHONES_COMM = 'Телефон из РБО: Мобильный')
                      ;*/
        
        --пока случайное число 0 или 1
        --SELECT ROUND(dbms_random.value(0,1), 0) INTO fraud_flag FROM dual;
        --SELECT (SQL%ROWCOUNT) INTO fraud_flag FROM dual;
  END LOOP;

  IF (fraud_flag IS NULL) THEN fraud_flag := 0;
  END IF;
  
  CLOSE get_fraud;
  
	--COMMIT;
  EXCEPTION
    WHEN OTHERS
    THEN fraud_flag := -1;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_PKK_DOCUMENTS_HISTORY" 
-- ========================================================================
-- ==	ПРОЦЕДУРА    Обновление таблицы DOCUMENTS_HISTORY
-- ==	ОПИСАНИЕ:	   История документов по физикам
-- ========================================================================
-- ==	СОЗДАНИЕ:		  30.12.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	11.02.2016 (ТРАХАЧЕВ В.В.)
/*PKK_DOCUMENTS_HISTORY_INFO:21.01.16 04:31:03-03.03.16 14:13:04(372853). От 03.03.16 11:13:14 до 03.03.16 11:56:46
  
*/
-- ========================================================================
AS    
  last_DT_SFF DATE; -- последний MODIFICATION_DATE в SFF
  last_DT_PKK DATE; -- последний MODIFICATION_DATE в ПКК
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp;
  
  --ускоренная выборка максимальной даты
  SELECT MAX(DOCUMENTS_CREATED)-5/24 INTO last_DT_SFF FROM ODS.PKK_DOCUMENTS_HISTORY_INFO
      WHERE DOCUMENTS_HISTORY_ID>=(SELECT MAX(DOCUMENTS_HISTORY_ID)-1000 FROM ODS.PKK_DOCUMENTS_HISTORY_INFO);

  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_DOCUMENTS_HISTORY_INFO tar
	USING (SELECT * FROM ODS.VIEW_PKK_DOCUMENTS WHERE DOCUMENTS_CREATED > last_DT_SFF
          UNION 
         SELECT * FROM ODS.VIEW_PKK_DOCUMENTS WHERE MODIFICATION_DATE > last_DT_SFF 
		) src
    ON (src.DOCUMENTS_HISTORY_ID = tar.DOCUMENTS_HISTORY_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        tar.DOCUMENTS_ID=src.DOCUMENTS_ID
        ,tar.OBJECTS_ID=src.OBJECTS_ID
        /*,tar.OBJECTS_TYPE=src.OBJECTS_TYPE*/
        ,tar.DOCUMENTS_AKT=src.DOCUMENTS_AKT
        /*,tar.DOCUMENTS_CREATED=src.DOCUMENTS_CREATED
        ,tar.CREATED_SOURCE=src.CREATED_SOURCE 
        ,tar.CREATED_USER_ID=src.CREATED_USER_ID 
        ,tar.CREATED_GROUP_ID=src.CREATED_GROUP_ID 
        ,tar.CREATED_IPADR=src.CREATED_IPADR*/
        ,tar.MODIFICATION_DATE=src.MODIFICATION_DATE
        ,tar.MODIFICATION_SOURCE=src.MODIFICATION_SOURCE
        ,tar.MODIFICATION_USER_ID=src.MODIFICATION_USER_ID
        ,tar.MODIFICATION_GROUP_ID=src.MODIFICATION_GROUP_ID
        ,tar.MODIFICATION_IPADR=src.MODIFICATION_IPADR
        --,tar.DOCUMENTS_HISTORY_ID=src.DOCUMENTS_HISTORY_ID
        ,tar.DOCUMENTS_TYPE=src.DOCUMENTS_TYPE
        ,tar.DOCUMENTS_NAME=src.DOCUMENTS_NAME
        ,tar.DOCUMENTS_SERIAL=src.DOCUMENTS_SERIAL
        ,tar.DOCUMENTS_NUMBER=src.DOCUMENTS_NUMBER
        ,tar.DOCUMENTS_ORGS=src.DOCUMENTS_ORGS
        ,tar.DOCUMENTS_DATE=src.DOCUMENTS_DATE
        --,tar.OBJECTS_TYPE_DOC=src.OBJECTS_TYPE_DOC
        ,tar.DATE_UPD=SYSDATE
        ,tar.DOCUMENTS_RANK=1
        , tar.OBJECTS_RANK=1
      WHERE NOT ( NVL(tar.OBJECTS_ID, -1) = NVL(src.OBJECTS_ID, -1)
        AND NVL(tar.DOCUMENTS_AKT, -1) = NVL(src.DOCUMENTS_AKT, -1)
        --AND NVL(tar.MODIFICATION_DATE, TO_DATE('01-01-1900', 'dd-mm-yyyy')) = NVL(src.MODIFICATION_DATE, TO_DATE('01-01-1900', 'dd-mm-yyyy'))
        AND NVL(tar.DOCUMENTS_SERIAL, '-') = NVL(src.DOCUMENTS_SERIAL, '-')
        AND NVL(tar.DOCUMENTS_NUMBER, '-') = NVL(src.DOCUMENTS_NUMBER, '-')
        AND NVL(tar.DOCUMENTS_ORGS, -1) = NVL(src.DOCUMENTS_ORGS, -1)
        )
	WHEN NOT MATCHED THEN 
	--вставляем новое 
	INSERT (	tar.DOCUMENTS_ID
            ,tar.OBJECTS_ID, tar.OBJECTS_TYPE
            ,tar.DOCUMENTS_AKT, tar.DOCUMENTS_CREATED
            ,tar.CREATED_SOURCE, tar.CREATED_USER_ID, tar.CREATED_GROUP_ID, tar.CREATED_IPADR
            ,tar.MODIFICATION_DATE
            ,tar.MODIFICATION_SOURCE, tar.MODIFICATION_USER_ID, tar.MODIFICATION_GROUP_ID, tar.MODIFICATION_IPADR
            ,tar.DOCUMENTS_HISTORY_ID
            ,tar.DOCUMENTS_TYPE, tar.DOCUMENTS_NAME
            ,tar.DOCUMENTS_SERIAL, tar.DOCUMENTS_NUMBER
            ,tar.DOCUMENTS_ORGS, tar.DOCUMENTS_DATE, tar.OBJECTS_TYPE_DOC
            ,tar.DATE_UPD
            ,tar.DOCUMENTS_RANK, tar.OBJECTS_RANK
          )
	VALUES (src.DOCUMENTS_ID
            ,src.OBJECTS_ID, src.OBJECTS_TYPE
            ,src.DOCUMENTS_AKT, src.DOCUMENTS_CREATED
            ,src.CREATED_SOURCE, src.CREATED_USER_ID, src.CREATED_GROUP_ID, src.CREATED_IPADR
            ,src.MODIFICATION_DATE
            ,src.MODIFICATION_SOURCE, src.MODIFICATION_USER_ID, src.MODIFICATION_GROUP_ID, src.MODIFICATION_IPADR
            ,src.DOCUMENTS_HISTORY_ID
            ,src.DOCUMENTS_TYPE, src.DOCUMENTS_NAME
            ,src.DOCUMENTS_SERIAL, src.DOCUMENTS_NUMBER
            ,src.DOCUMENTS_ORGS, src.DOCUMENTS_DATE, src.OBJECTS_TYPE_DOC
            ,SYSDATE
            ,1, 1)
	;

  cnt_MODIF := SQL%ROWCOUNT;
  SELECT MAX(DOCUMENTS_CREATED) INTO last_DT_PKK FROM ODS.PKK_DOCUMENTS_HISTORY_INFO
       WHERE DOCUMENTS_HISTORY_ID>=(SELECT MAX(DOCUMENTS_HISTORY_ID)-1000 FROM ODS.PKK_DOCUMENTS_HISTORY_INFO);
       
  p_ADD_INFO := 'Период обновления: '||TO_CHAR(last_DT_SFF)||' до '||TO_CHAR(last_DT_PKK);
  ODS.PR$INS_LOG ('PR$UPD_PKK_DOCUMENTS_HISTORY', start_time, 'ODS.PKK_DOCUMENTS_HISTORY_INFO', 'OK', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
  
  COMMIT;

EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_PKK_DOCUMENTS_HISTORY', start_time, 'ODS.PKK_DOCUMENTS_HISTORY_INFO', 'ERR', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
END;
  CREATE OR REPLACE FUNCTION "SFF"."FN_IS_FAMILY_REL" (inPID IN NUMBER, inPID_REL IN NUMBER) RETURN NUMBER
-- ========================================================================
-- ==	ФУНКЦИЯ     Проверка состоят ли PERSON_ID и PERSON_ID_REL в семейных отношениях.
-- ==	ОПИСАНИЕ:   Проверка в прямой таблице семейных связей
-- ========================================================================
-- ==	СОЗДАНИЕ:		  23.11.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	23.11.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
IS
 FLAG_FAMILY NUMBER;
BEGIN
  DBMS_OUTPUT.ENABLE;
           
  SELECT count(*) INTO FLAG_FAMILY FROM dual
      WHERE EXISTS(SELECT * FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=inPID AND fam.OB_ID=inPID_REL
										/*AND fam.FAMILY_AKT=1*/ /*AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2*/);
      
  IF FLAG_FAMILY>0 THEN
    RETURN 1;
  ELSE 
    RETURN 0;
  END IF;

EXCEPTION
    WHEN OTHERS
    THEN 
        RETURN -1;

END;
  CREATE OR REPLACE PROCEDURE "SNAUSER"."DATA_PKK_MOD" (v_request_id IN NUMBER)
-- ========================================================================
-- ==	ПРОЦЕДУРА     "Подтягивание данных из ПКК"
-- ==	ОПИСАНИЕ:	    Более правильный скрипт (но еще не идеальный) для подтягивания данных из ПКК.
-- ========================================================================
-- ==	СОЗДАНИЕ:		  08.2015     (ТРАХАЧЕВ В.В., ТИМОНИН И.Е.)
-- ==	МОДИФИКАЦИЯ:	16.11.2015  (ТРАХАЧЕВ В.В.)
-- ========================================================================
AS    
  req$in number;
   
BEGIN

req$in := v_request_id;

delete from C_REQUEST_FULL_ST_RID;
delete from A_C_REQUEST_INF_PACK;
delete from A_C_REQUEST_CR_PACK;
delete from A_PHONES_FULL;
delete from A_PHONES_4SR_NEW;
delete from email;
delete from FIO4_HISTORY;
delete from ADDRESS_4SR;
delete from A_ADR_ORG_prev;
delete from A_ADR_ORG;
delete from A_DOCS;
/*delete from CONT_pre_full_1;
delete from CONT_pre_full_2;
delete from CONT_pre_full_12;
delete from CONT_pre_full_3;*/
delete from CONT_pre_full_123;
--delete from applications_temp;


insert into C_REQUEST_FULL_ST_RID
select
REQUEST_ID 
,STATUS_ID 
,OBJECTS_ID as PERSON_ID
,CREATED_GROUP_ID 
,CREATED_DATE 

from C_REQUEST@dblink_pkk a
where a.request_id = v_request_id
;

insert into A_C_REQUEST_INF_PACK 
(select * from 
      (
      select
      REQUEST_INFO_ID 
      ,REQUEST_ID 
      ,FIO_ID 
      ,SEX_ID 
      ,BDATE 
      ,BADR_ID 
      ,MDOC_ID 
      ,MADR_ID 
      ,LADR_ID 
      ,DADR_ID
      ,EDUC
      /*,coalesce(to_number(WORK),-1) as work_id*/
      ,to_number(SUBSTR(WORK, 1, INSTR(WORK||' ', ' '))) AS WORK_ID
      ,DOHOD_CALC          
      ,DOHOD_DECL_ALL 
      ,FAMILY
      ,row_number() over(partition by REQUEST_ID order by REQUEST_ID desc, REQUEST_INFO_ID desc) as rnmbr  
      
      from C_REQUEST_INFO@dblink_pkk a
      where a.request_id = v_request_id
      ) 
where rnmbr=1
)
;


insert into A_C_REQUEST_CR_PACK 
(select * from 
      (
      select
      REQUEST_ID 
      ,REQUEST_CREDIT_ID  
      ,PRIVILEGE_ID 
      ,CURRENCY_ID
      ,SCHEMS_ID
      ,SUMMA
      ,TYPE_CREDIT_ID
      ,row_number() over(partition by REQUEST_ID order by REQUEST_ID desc, REQUEST_CREDIT_ID desc) as rnmbr 
      
      from C_REQUEST_CREDIT@dblink_pkk a
      where a.request_id = v_request_id
      )
where rnmbr=1
)
;

insert into a_phones_full 
select * from 
(select 
PHONES_ID 
,OBJECTS_ID 
,OBJECTS_TYPE 
,PHONES_COMM 
,PHONES_AKT 
,PHONES_CREATED
,MODIFICATION_DATE 
,PHONE 
,row_number() over(partition by OBJECTS_ID, OBJECTS_TYPE order by MODIFICATION_DATE desc) as rnmbr 

from  PHONES@dblink_pkk
where objects_id in (select person_id from C_REQUEST_FULL_ST_RID)
) a
/*where rnmbr=1*/
;


insert into A_PHONES_4SR_NEW 
(select
--DISTINCT 
creq.request_id
,creq.person_id as objects_id
,to_number(COALESCE(homphn_f.phone, homphn.phone))  as home_phone
,to_number(COALESCE(wphn.phone, phn_org.phone)) AS work_phone
,to_number(ph_mob.phone) AS mobile
        
        
FROM C_REQUEST_FULL_ST_RID  creq
    LEFT JOIN A_C_REQUEST_INF_PACK  reqinf 
		ON creq.REQUEST_ID= reqinf.REQUEST_ID
	
    LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full  homphn /*телефон по адресу регистрации*/
		ON reqinf.MADR_ID=homphn.objects_id 
    AND homphn.OBJECTS_TYPE=8 
		AND homphn.phone is not null
		AND homphn.phones_akt=1 
	
    LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full homphn_f /*телефон по адресу факт. проживания*/
    ON reqinf.LADR_ID=homphn_f.objects_id
    AND homphn_f.OBJECTS_TYPE=8
    AND homphn_f.phone is not null
    AND homphn_f.phones_akt=1 
	
	  LEFT JOIN WORKS@dblink_pkk  works /*добавлено исключение избыточных телефонов для Пенсионеров/безработных 24.04.2015*/
    ON reqinf.WORK_ID = works.works_id
		AND COALESCE(works.org_vid,-1) NOT IN (0,5,98,99,100,101,102,105) /*почему то когда значение имеет . то результат не джойнится*/
  
	  LEFT JOIN ORG@dblink_pkk  org /*добавлено исключение избыточных телефонов для Пенсионеров/безработных 24.04.2015*/
		on works.org_id = org.org_id
    and COALESCE(org.org_vid,-1) NOT IN (0,5,98,99,100,101,102,105)
    and org.org_id=67790
	
    LEFT JOIN  /*PHONES@dblink_pkk*/a_phones_full wphn 
		on wphn.objects_id = works.works_id 
    AND wphn.OBJECTS_TYPE=works.OBJECTS_TYPE 
    AND wphn.phone is not null
    AND wphn.phones_akt=1
	
    LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full  phn_org 
		on phn_org.objects_id = org.org_id 
    AND phn_org.OBJECTS_TYPE=org.OBJECTS_TYPE 
    AND phn_org.phone is not null
    AND phn_org.phones_akt=1
	
    LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full  ph_mob /*мобильные телефоны - новый вариант. 24.04.2015*/
		on creq.person_id=ph_mob.objects_id
    AND ph_mob.OBJECTS_TYPE=2
    AND ph_mob.phone is not null
    AND ph_mob.phones_akt=1

WHERE 
to_number(COALESCE(homphn_f.phone, homphn.phone)) is not null
or to_number(COALESCE(wphn.phone, phn_org.phone)) is not null
or to_number(ph_mob.phone) is not null
)
;

insert into email 
(
select
EMAIL_ID
,EMAIL
,OBJECTS_ID
,OBJECTS_TYPE
,EMAIL_AKT

from  EMAIL@dblink_pkk
where EMAIL_AKT=1
and instr(email,'@')>0
and OBJECTS_ID in (select person_id from C_REQUEST_FULL_ST_RID)
)
;

insert into FIO4_HISTORY 
select
FH.FIO_ID
,FH.OBJECTS_ID
,FH.OBJECTS_TYPE
,FH.FIO_AKT
,F.FIO4SEARCH
    
from FIO_HISTORY@dblink_pkk FH
    left join FIO@dblink_pkk F
    on FH.FIO_ID=F.FIO_ID
where FH.OBJECTS_ID in (select person_id from C_REQUEST_FULL_ST_RID)
;


insert into ADDRESS_4SR 
(
SELECT 
DISTINCT
adr.address_id /*"ID адреса"*/
,adr.GEO_ID
,GEO.ADDRESS_STR AS ADDRESS_STR
,GEO.QUALITY_CODE 
,KLADR_SIT.SHOTNAME as SHOTNAME_CIT /*"Тип НП ФМЖ"*/
,KLADR_STR.SHOTNAME as SHOTNAME_STR /*"Тип улицы ФМЖ"*/
,COUNTRIES.COUNTRIES_ISO2 /*"Страна"*/
,reg.REGIONS_NAMES /*"Регион ФМЖ"*/	
,area.AREAS_NAMES /*"Район ФМЖ"*/
,CIT.CITIES_NAMES /*"НП ФМЖ"*/
,STREET.STREETS_NAMES /*"Улица ФМЖ"*/
,adr.HOUSE /*"Дом ФМЖ"*/
,adr.BUILD /*"Корпус ФМЖ"*/
,adr.FLAT /*"Квартира ФМЖ"*/
,adr.POSTOFFICE /*"ПочтИндекс"*/

FROM ADDRESS@dblink_pkk  adr 
    LEFT JOIN COUNTRIES@dblink_pkk  c 
  		on adr.COUNTRIES_ID=c.COUNTRIES_ID
    LEFT JOIN REGIONS_NAMES@dblink_pkk  reg   
  		on adr.REGIONS_UID=reg.REGIONS_UID
    LEFT JOIN AREAS_NAMES@dblink_pkk  area 
      on adr.AREAS_UID=area.AREAS_UID
	  LEFT JOIN CITIES_NAMES@dblink_pkk  CIT 
      on adr.CITIES_UID=CIT.CITIES_UID
	  LEFT JOIN STREETS_NAMES@dblink_pkk  STREET 
      on adr.STREETS_UID=STREET.STREETS_UID
	  LEFT JOIN COUNTRIES@dblink_pkk  COUNTRIES 
      on adr.COUNTRIES_ID = COUNTRIES.COUNTRIES_ID
    LEFT JOIN KLADR_SOCR@dblink_pkk  KLADR_SIT 
      ON CIT.CITIES_TYPE = KLADR_SIT.SOCR_ID
    LEFT JOIN KLADR_SOCR@dblink_pkk  KLADR_STR 
      ON STREET.STREETS_TYPE = KLADR_STR.SOCR_ID
    LEFT JOIN GEOCOORDINATES@dblink_pkk GEO 
      ON adr.GEO_ID = GEO.ID
where adr.address_id in (select MADR_ID from A_C_REQUEST_INF_PACK
                                    union
                                    select LADR_ID as MADR_ID from A_C_REQUEST_INF_PACK)
)
;


insert into A_ADR_ORG_prev 
(
SELECT 
W.WORKS_ID
,W.ORG_ID
,VO.ORG_NAME
,ah.ADDRESS_ID

FROM WORKS@dblink_pkk  W
    
    LEFT JOIN ORG@dblink_pkk VO
    ON VO.ORG_ID=W.ORG_ID
    
    left join ADDRESS_HISTORY@dblink_pkk ah
    on VO.org_id=ah.objects_id
    and VO.OBJECTS_TYPE=ah.OBJECTS_TYPE
    and ah.address_akt=1

where W.WORKS_ID in (select work_id from A_C_REQUEST_INF_PACK)
)
;

insert into A_ADR_ORG
(
SELECT 
DISTINCT
t1.works_id
,t1.org_id
,t1.org_name
,case when instr(geo.QUALITY_CODE,'GOOD')>0 then 1
        when geo.QUALITY_CODE is null then 0
        else to_number(substr(geo.QUALITY_CODE, instr(geo.QUALITY_CODE,'_')+1, length(geo.QUALITY_CODE)-instr(geo.QUALITY_CODE,'_')))
end as  QUALITY_CODE_N

,adr.address_id /*"ID адреса"*/
,adr.GEO_ID
,GEO.ADDRESS_STR AS ADDRESS_STR
,GEO.QUALITY_CODE 
,KLADR_SIT.SHOTNAME as ba_settlement /*"Тип НП ФМЖ" SHOTNAME_CIT*/
,KLADR_STR.SHOTNAME as SHOTNAME_STR /*"Тип улицы ФМЖ"*/
,COUNTRIES.COUNTRIES_ISO2 as ba_country/*"Страна"*/
,reg.REGIONS_NAMES as ba_region/*"Регион ФМЖ"*/	
,area.AREAS_NAMES as ba_district/*"Район ФМЖ"*/
,CIT.CITIES_NAMES as ba_city/*"НП ФМЖ"*/
,STREET.STREETS_NAMES as ba_street/*"Улица ФМЖ"*/
,adr.HOUSE as ba_house/*"Дом ФМЖ"*/
,adr.BUILD as ba_building/*"Корпус ФМЖ"*/
,adr.FLAT as ba_apartment/*"Квартира ФМЖ"*/
,adr.POSTOFFICE as ba_index/*"ПочтИндекс"*/

FROM A_ADR_ORG_prev t1

    left join ADDRESS@dblink_pkk  adr 
      on t1.address_id=adr.address_id    
    LEFT JOIN COUNTRIES@dblink_pkk  c 
  		on adr.COUNTRIES_ID=c.COUNTRIES_ID
    LEFT JOIN REGIONS_NAMES@dblink_pkk  reg   
  		on adr.REGIONS_UID=reg.REGIONS_UID
    LEFT JOIN AREAS_NAMES@dblink_pkk  area 
      on adr.AREAS_UID=area.AREAS_UID
	  LEFT JOIN CITIES_NAMES@dblink_pkk  CIT 
      on adr.CITIES_UID=CIT.CITIES_UID
	  LEFT JOIN STREETS_NAMES@dblink_pkk  STREET 
      on adr.STREETS_UID=STREET.STREETS_UID
	  LEFT JOIN COUNTRIES@dblink_pkk  COUNTRIES 
      on adr.COUNTRIES_ID = COUNTRIES.COUNTRIES_ID
    LEFT JOIN KLADR_SOCR@dblink_pkk  KLADR_SIT 
      ON CIT.CITIES_TYPE = KLADR_SIT.SOCR_ID
    LEFT JOIN KLADR_SOCR@dblink_pkk  KLADR_STR 
      ON STREET.STREETS_TYPE = KLADR_STR.SOCR_ID
    LEFT JOIN GEOCOORDINATES@dblink_pkk GEO 
      ON adr.GEO_ID = GEO.ID
)
;


insert into A_DOCS 
(
SELECT 
DH.DOCUMENTS_ID 
,DH.OBJECTS_ID 
,DH.OBJECTS_TYPE 
,DH.DOCUMENTS_AKT 
,DH.DOCUMENTS_CREATED
,D.DOCUMENTS_SERIAL
,D.DOCUMENTS_NUMBER
,D.DOCUMENTS_TYPE
,D.DOCUMENTS_ORGS

FROM DOCUMENTS_HISTORY@dblink_pkk  DH
      LEFT OUTER JOIN DOCUMENTS@dblink_pkk  D
      ON DH.DOCUMENTS_ID=D.DOCUMENTS_ID

WHERE D.DOCUMENTS_TYPE IN (21) AND DH.DOCUMENTS_AKT<>0
and dh.DOCUMENTS_ID in (select MDOC_ID from A_C_REQUEST_INF_PACK)
)
;

insert into CONT_pre_full_123
(SELECT
--DISTINCT
t1.PERSON_ID
,case when ph.OBJECTS_TYPE=200 then t1.PERSON_ID else t1.CONTACT_PERSON_ID end as CONTACT_PERSON_ID
,case when ph.OBJECTS_TYPE=200 then 'Телефоны контактных лиц' else t1.CONTACT_RELATION end as CONTACT_RELATION
,case when ph.OBJECTS_TYPE=200 then ph.PHONES_COMM else fh.FIO4SEARCH end as CONTACT_FIO
,case when ph.OBJECTS_TYPE=200 then to_number(ph.PHONE) else to_number(ph.PHONE) end as CONTACT_PHONE   
FROM (SELECT
         --DISTINCT 
        contprs.OBJECTS_ID AS PERSON_ID
				,contprs.CONTACT_OBJECTS_ID as CONTACT_PERSON_ID
				,COALESCE(fam_rel.family_rel_name, 'NULL') AS CONTACT_RELATION
        
        FROM CONTACT_PERSON@dblink_pkk  contprs
              LEFT JOIN FAMILY_RELATIONS@dblink_pkk  fam_rel 
              ON fam_rel.family_rel=contprs.family_rel
        WHERE contprs.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)

		UNION

        SELECT 
        --DISTINCT
        fam.OBJECTS_ID AS PERSON_ID
        ,fam.OB_ID AS CONTACT_PERSON_ID
        ,COALESCE(fam_rel.family_rel_name, 'NULL') AS CONTACT_RELATION
        
        FROM FAMILY@dblink_pkk  fam
        
              LEFT JOIN FAMILY_RELATIONS@dblink_pkk  fam_rel 
              ON fam_rel.family_rel=fam.family_rel
              WHERE fam.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)
          )  t1
          
          LEFT JOIN FIO4_HISTORY  fh
          on fh.OBJECTS_ID = t1.CONTACT_PERSON_ID 
          and fh.fio_akt=1
          LEFT JOIN /*A_PHONES_FULL*/ PHONES@dblink_pkk  ph
          on t1.CONTACT_PERSON_ID = ph.OBJECTS_ID 
				  AND ph.OBJECTS_TYPE =2
          and ph.PHONES_AKT =1
          WHERE ph.PHONE is not null
  )
UNION
  (SELECT 
  --DISTINCT
  t1.PERSON_ID
  
  ,case when ph.OBJECTS_TYPE=200 then t1.PERSON_ID else t1.CONTACT_PERSON_ID end as CONTACT_PERSON_ID
  ,case when ph.OBJECTS_TYPE=200 then 'Телефоны контактных лиц' else t1.CONTACT_RELATION end as CONTACT_RELATION
  ,case when ph.OBJECTS_TYPE=200 then ph.PHONES_COMM else fh.FIO4SEARCH end as CONTACT_FIO
  ,case when ph.OBJECTS_TYPE=200 then to_number(ph.PHONE) else to_number(ph.PHONE) end as CONTACT_PHONE
  
  FROM (SELECT 
            --DISTINCT 
            contprs.OBJECTS_ID AS PERSON_ID
            ,contprs.CONTACT_OBJECTS_ID as CONTACT_PERSON_ID
            ,COALESCE(fam_rel.family_rel_name, 'NULL') AS CONTACT_RELATION
        
        FROM CONTACT_PERSON@dblink_pkk contprs
        LEFT JOIN FAMILY_RELATIONS@dblink_pkk fam_rel 
        ON fam_rel.family_rel=contprs.family_rel
        WHERE contprs.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)
        
      UNION
      
        SELECT 
        --DISTINCT 
        fam.OBJECTS_ID AS PERSON_ID
        ,fam.OB_ID AS CONTACT_PERSON_ID
        ,COALESCE(fam_rel.family_rel_name, 'NULL') AS CONTACT_RELATION
        
        FROM FAMILY@dblink_pkk fam
        LEFT JOIN FAMILY_RELATIONS@dblink_pkk fam_rel 
        ON fam_rel.family_rel=fam.family_rel
        
        WHERE fam.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)
        )  t1
        
        LEFT JOIN FIO4_HISTORY fh
        on fh.OBJECTS_ID = t1.CONTACT_PERSON_ID and fh.fio_akt=1
        LEFT JOIN PHONES@dblink_pkk /*a_phones_full*/  ph
        on ph.OBJECTS_ID = t1.PERSON_ID AND ph.OBJECTS_TYPE =200
    
    WHERE ph.PHONE IS NOT NULL
  )
UNION
  (SELECT
  --DISTINCT
  t1.PERSON_ID
  
  ,case when ph.OBJECTS_TYPE=200 then t1.PERSON_ID else t1.CONTACT_PERSON_ID end as CONTACT_PERSON_ID
  ,case when ph.OBJECTS_TYPE=200 then 'Телефоны контактных лиц' else t1.CONTACT_RELATION end as CONTACT_RELATION
  ,case when ph.OBJECTS_TYPE=200 then ph.PHONES_COMM else fh.FIO4SEARCH end as CONTACT_FIO
  ,case when ph.OBJECTS_TYPE=200 then to_number(ph.PHONE) else to_number(ph.PHONE) end as CONTACT_PHONE
  
  FROM (SELECT DISTINCT src_ph.OBJECTS_ID AS PERSON_ID
            ,src_ph.OBJECTS_ID AS CONTACT_PERSON_ID
            ,'Телефоны контактных лиц' AS CONTACT_RELATION
            
            FROM  /*PHONES@dblink_pkk*/a_phones_full src_ph
            WHERE /*src_ph.OBJECTS_ID NOT IN (SELECT PERSON_ID FROM CONT_pre_full_12)
            AND*/ src_ph.OBJECTS_TYPE=200
            AND src_ph.OBJECTS_ID IN (SELECT PERSON_ID FROM C_REQUEST_FULL_ST_RID)
            ) t1
            
            LEFT JOIN FIO4_HISTORY fh
            on fh.OBJECTS_ID = t1.CONTACT_PERSON_ID and fh.fio_akt=1
            LEFT JOIN /*PHONES@dblink_pkk*/a_phones_full ph
            on ph.OBJECTS_ID = t1.PERSON_ID 
            AND ph.OBJECTS_TYPE =200
            WHERE ph.PHONE is not null
)
;


--В contacts нет уникального идентификатора, поэтому трем все записи с одним REQUEST_ID
--и заново вставляем все, т.к merge работать не захотел
delete from SNAUSER.CONTACTS where REQUEST_ID=v_request_id;
delete from SNAUSER.APPLICATIONS where REQUEST_ID=v_request_id;

insert into SNAUSER.CONTACTS
(
SELECT 
DISTINCT
C_REQ.REQUEST_ID
,PRE.PERSON_ID
,case when PRE.CONTACT_RELATION='Телефоны контактных лиц' then PRE.CONTACT_PHONE else PRE.CONTACT_PERSON_ID end as CONTACT_PERSON_ID
,case when PRE.CONTACT_RELATION='Телефоны контактных лиц' then 'Тел конт лиц' else PRE.CONTACT_RELATION end as CONTACT_RELATION

,UPPER(COALESCE(PRE.CONTACT_FIO, '-')) as CONTACT_FIO
,ltrim(rtrim(to_char(PRE.CONTACT_PHONE))) as CONTACT_PHONE
,1 AS IS_NEW_DATA

FROM C_REQUEST_FULL_ST_RID  C_REQ
      LEFT OUTER JOIN CONT_pre_full_123 PRE
      ON C_REQ.PERSON_ID=PRE.PERSON_ID

/*WHERE ((C_REQ.CREATED_GROUP_ID^=11455 AND C_REQ.STATUS_ID^=14)
    OR C_REQ.STATUS_ID=14)*/
)
;



insert into SNAUSER.APPLICATIONS
(
SELECT 
DISTINCT 

f00_C_RQ.REQUEST_ID
,f00_C_RQ.PERSON_ID AS PERSON_ID
,f00_C_RQ.CREATED_DATE AS REQ_DATE
,f01_C_RQ_CR.SUMMA AS REQ_SUMM
,(CASE WHEN f01_C_RQ_CR.CURRENCY_ID=1 THEN 'R'
  WHEN f01_C_RQ_CR.CURRENCY_ID=2 THEN 'U'
  WHEN f01_C_RQ_CR.CURRENCY_ID=3 THEN 'E'
  WHEN f01_C_RQ_CR.CURRENCY_ID=4 THEN 'C'
  WHEN f01_C_RQ_CR.CURRENCY_ID=5 THEN 'J'
END) AS REQ_SUMM_CURR
,f02_C_RQ_INF.BDATE AS BIRTHDAY
,f03_FIO.FIO4SEARCH as FIO

/*адрес прописки*/
,COALESCE(f04_ADR_R.REGIONS_NAMES, '-') AS RA_REGION
,f04_ADR_R.AREAS_NAMES AS RA_DISTRICT
,f04_ADR_R.CITIES_NAMES AS RA_CITY
,f04_ADR_R.SHOTNAME_CIT as RA_SETTLEMENT 
,f04_ADR_R.STREETS_NAMES AS RA_STREET
,f04_ADR_R.HOUSE AS RA_HOUSE
,f04_ADR_R.BUILD AS RA_BUILDING
,f04_ADR_R.FLAT AS RA_APARTMENT
,f04_ADR_R.POSTOFFICE AS RA_INDEX

/*адрес проживания*/
,COALESCE(f04_ADR_L.REGIONS_NAMES, '-') AS LA_REGION
,f04_ADR_L.AREAS_NAMES AS LA_DISTRICT
,f04_ADR_L.CITIES_NAMES AS LA_CITY
,f04_ADR_L.SHOTNAME_CIT as LA_SETTLEMENT 
,f04_ADR_L.STREETS_NAMES AS LA_STREET
,f04_ADR_L.HOUSE AS LA_HOUSE
,f04_ADR_L.BUILD AS LA_BUILDING
,f04_ADR_L.FLAT AS LA_APARTMENT
,f04_ADR_L.POSTOFFICE AS LA_INDEX

/*адрес работы*/
,COALESCE(f13_ORG_ADR_NEW.BA_REGION, '-') AS BA_REGION
,f13_ORG_ADR_NEW.BA_DISTRICT AS BA_DISTRICT
,f13_ORG_ADR_NEW.BA_CITY AS BA_CITY
,COALESCE(f13_ORG_ADR_NEW.BA_SETTLEMENT, '-') as BA_SETTLEMENT 
,f13_ORG_ADR_NEW.BA_STREET AS BA_STREET
,f13_ORG_ADR_NEW.BA_HOUSE AS BA_HOUSE
,f13_ORG_ADR_NEW.BA_BUILDING AS BA_BUILDING
,f13_ORG_ADR_NEW.BA_APARTMENT AS BA_APARTMENT
,f13_ORG_ADR_NEW.BA_INDEX AS BA_INDEX
,f13_ORG_ADR_NEW.ORG_NAME AS WORK_ORG_NAME

,f05_C_SCH.SCHEMS_NAME AS PRODUCT_TYPE
,f06_GRP.GROUPS_NAME AS REQ_CREATED_BRANCH
/*,_08_WH_SALARY.SALARY_SUM AS SALARY
,_08_WH_SALARY.EDUCATION_VID_ID AS EDUCATION
,_08_WH_SALARY.PENSION_SUM AS IS_PENSION*/
,SUBSTR(f09_DOC.DOCUMENTS_SERIAL,1,4)||SUBSTR(f09_DOC.DOCUMENTS_NUMBER,1,6) as PASSPORT
,f10_EMAIL.EMAIL
,f11_PHONE.MOBILE as MOBILE
,f11_PHONE.WORK_PHONE as WORK_PHONE
,f11_PHONE.HOME_PHONE as HOME_PHONE
--,null AS IS_NEW_CLIENT
--,null AS IS_NEW_DATA
--,null AS WORK_TYPE 
      
FROM C_REQUEST_FULL_ST_RID f00_C_RQ
		
    LEFT OUTER JOIN a_c_request_cr_pack f01_C_RQ_CR
		ON f00_C_RQ.REQUEST_ID=f01_C_RQ_CR.REQUEST_ID
		
    LEFT OUTER JOIN a_c_request_inf_pack f02_C_RQ_INF
    ON f00_C_RQ.REQUEST_ID=f02_C_RQ_INF.REQUEST_ID
		
    LEFT OUTER JOIN fio4_history f03_FIO
    ON f00_C_RQ.PERSON_ID=f03_FIO.OBJECTS_ID  
    AND f03_FIO.FIO_AKT=1
		
    LEFT OUTER JOIN address_4sr f04_ADR_R
    ON f02_C_RQ_INF.MADR_ID=f04_ADR_R.ADDRESS_ID
		
    LEFT OUTER JOIN address_4sr f04_ADR_L
    ON f02_C_RQ_INF.LADR_ID=f04_ADR_L.ADDRESS_ID
		
    LEFT OUTER JOIN C_SCHEMS@dblink_pkk f05_C_SCH
		ON f01_C_RQ_CR.SCHEMS_ID=f05_C_SCH.SCHEMS_ID
		
    LEFT OUTER JOIN "GROUPS"@dblink_pkk f06_GRP
		ON f00_C_RQ.CREATED_GROUP_ID=f06_GRP.GROUPS_ID
		
    /*LEFT OUTER JOIN A_WH_PC_RD_ALL f08_WH_SALARY*/
			/*ON f00_C_RQ.REQUEST_ID=f08_WH_SALARY.REQUEST_ID*/
		
    LEFT OUTER JOIN A_DOCS f09_DOC
    ON f02_C_RQ_INF.MDOC_ID=f09_DOC.DOCUMENTS_ID
    AND DOCUMENTS_AKT=1
		
    LEFT OUTER JOIN EMAIL f10_EMAIL
    ON f00_C_RQ.PERSON_ID=f10_EMAIL.OBJECTS_ID
    AND f10_EMAIL.OBJECTS_TYPE = 2 AND f10_EMAIL.EMAIL_AKT = 1
		
    LEFT OUTER JOIN a_phones_4sr_new	f11_PHONE
    ON f00_C_RQ.REQUEST_ID=f11_PHONE.REQUEST_ID
    AND f00_C_RQ.PERSON_ID=f11_PHONE.OBJECTS_ID

		LEFT OUTER JOIN a_adr_org f13_ORG_ADR_NEW
    ON f02_C_RQ_INF.WORK_ID=f13_ORG_ADR_NEW.WORKS_ID

		/*WHERE 
    ((f00_C_RQ.CREATED_GROUP_ID^=11455 AND f00_C_RQ.STATUS_ID^=14)
						OR f00_C_RQ.STATUS_ID=14)*/
)
;

EXCEPTION
    WHEN OTHERS
    THEN
       DBMS_OUTPUT.put_line(  'Trapped the error L2: '||TO_CHAR(v_request_id) );
    --RAISE;
    
--COMMIT;
END;
  CREATE OR REPLACE FUNCTION "SFF"."FN_IS_FAMILY_CONT" (inPID IN NUMBER, inPID_REL IN NUMBER) RETURN NUMBER
-- ========================================================================
-- ==	ФУНКЦИЯ     Проверка состоят ли PERSON_ID и PERSON_ID_REL в семейных отношениях.
-- ==	ОПИСАНИЕ:   Проверка явных (уровень 1) и неявных (уровень 2) семейных связей.
-- ==             В общем понимаю что не оптимально, но нагрузка планируется не очень большая
-- ==               , а правильные оптимизации будут очень затратны... Мб когда нибудь кто-нибудь доведет.
-- ========================================================================
-- ==	СОЗДАНИЕ:		  23.11.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	23.11.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
IS
 FLAG_FAMILY_CONT NUMBER;
BEGIN
  DBMS_OUTPUT.ENABLE;
           
  SELECT count(*) INTO FLAG_FAMILY_CONT FROM dual
      WHERE EXISTS(SELECT ph.*
                  /*,UPPER(TRIM(ph.PHONES_COMM)) as fio_200
                  ,f.FIO4SEARCH as FIO_2
                  ,UTL_MATCH.EDIT_DISTANCE(UPPER(TRIM(ph.PHONES_COMM)), f.FIO4SEARCH) as lev_Fio
                  ,UTL_MATCH.EDIT_DISTANCE(SUBSTR(UPPER(TRIM(ph.PHONES_COMM)), 1, INSTR(UPPER(TRIM(ph.PHONES_COMM)),' ')-1 )
                                          , SUBSTR(f.FIO4SEARCH, 1, INSTR(f.FIO4SEARCH,' ')-1 )) as lev_Fam*/
                  /*,fh.FIO_AKT 
                  ,ph.FAMILY_REL*/
  FROM CPD.PHONES@DBLINK_PKK ph 
  LEFT JOIN CPD.FIO_HISTORY@DBLINK_PKK fh 
    ON fh.OBJECTS_ID = inPID_REL AND NOT fh.OBJECTS_ID IS NULL
  LEFT JOIN CPD.FIO@DBLINK_PKK f
    ON f.FIO_ID=fh.FIO_ID AND NOT f.FIO_ID IS NULL
          
  WHERE  ph.OBJECTS_ID = inPID AND ph.OBJECTS_TYPE=200 
    AND ph.FAMILY_REL>0 AND NOT ph.FAMILY_REL IN(35, 44, 106) AND inPID ^= inPID_REL  
    -- 1 уровень - проверка на родственные связи в пределах допустимой погрешности
    AND ( (UTL_MATCH.EDIT_DISTANCE(UPPER(TRIM(ph.PHONES_COMM)), f.FIO4SEARCH) BETWEEN 0 AND 5)
    -- 2 уровень - проверка на одинаковость фамилии в пределах допустимой погрешности
        OR  UTL_MATCH.EDIT_DISTANCE(SUBSTR(UPPER(TRIM(ph.PHONES_COMM)), 1, INSTR(UPPER(TRIM(ph.PHONES_COMM)),' ')-1 )
                                  , SUBSTR(f.FIO4SEARCH, 1, INSTR(f.FIO4SEARCH,' ')-1 ))
              /LENGTH(SUBSTR(UPPER(TRIM(ph.PHONES_COMM)), 1, INSTR(UPPER(TRIM(ph.PHONES_COMM)),' ')-1 )) BETWEEN 0 AND 0.35 )
  );
      
  IF FLAG_FAMILY_CONT>0 THEN
    RETURN 1;
  ELSE 
    RETURN 0;
  END IF;

EXCEPTION
    WHEN OTHERS
    THEN 
        RETURN -1;

END;
  CREATE OR REPLACE FUNCTION "SFF"."FN_CNT_PAYMENT_KI" (str_KI IN VARCHAR2) RETURN NUMBER
-- ========================================================================
-- ==	ФУНКЦИЯ     Подсчет кол-ва платежей на основе строки кредитной истории
-- ==	ОПИСАНИЕ:   Предполагаемый подсчет, из того что есть в ПКК
-- ==             str_KI строка кредитной истории (пример '111B1111111110') 
-- ========================================================================
-- ==	СОЗДАНИЕ:		  14.12.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	14.12.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================
IS
  --str_KI VARCHAR2(400) := '111B1111111110';
  cnt_pay_in_VBKI NUMBER := 0;
  cnt_pay_in_VBKI_last NUMBER := 0;
BEGIN
  --DBMS_OUTPUT.ENABLE;
--432AAA0 432AB0  5432A-0 112A432A15432A2A1BBA11B1110 32ABB2AB1A111B1B1BBABBA0
 SELECT COUNT(*), NVL(MAX(num_char)-(DECODE(SUBSTR(str_KI, 1, 1), '1', 0, '0', 0, 1) ), 0)-REGEXP_COUNT(str_KI, '-', 1)
    INTO cnt_pay_in_VBKI, cnt_pay_in_VBKI_last
  FROM (
   SELECT SUBSTR(str_KI, LEVEL, 1) as cur_char
        ,(CASE SUBSTR(str_KI, LEVEL, 1)
                      WHEN '-' THEN 0
                      WHEN 'X' THEN 0
                      WHEN '0' THEN 0
                      WHEN '1' THEN 10
                      WHEN 'B' THEN 16
                      WHEN 'A' THEN 30
                      WHEN '2' THEN 60
                      WHEN '3' THEN 80
                      WHEN '4' THEN 90
                      ELSE 100 END) as f_key
                  , (CASE SUBSTR(str_KI, LEVEL + 1, 1)
                      WHEN '-' THEN 0
                      WHEN 'X' THEN 0
                      WHEN '0' THEN 0
                      WHEN '1' THEN 10
                      WHEN 'B' THEN 16
                      WHEN 'A' THEN 30
                      WHEN '2' THEN 60
                      WHEN '3' THEN 80
                      WHEN '4' THEN 90
                      ELSE 100 END) as f_next
                  , LEVEL
                  ,LENGTH(str_KI)-LEVEL as num_char
               FROM DUAL
         CONNECT BY LEVEL < LENGTH(str_KI)
           ORDER BY LEVEL DESC
    ) WHERE f_next-f_key>=0 /*AND f_next^=100*/  AND NOT cur_char=' '
    ;
    RETURN cnt_pay_in_VBKI_last;
    --DBMS_OUTPUT.PUT_LINE(cnt_pay_in_VBKI);
    --DBMS_OUTPUT.PUT_LINE(cnt_pay_in_VBKI_last);

    EXCEPTION
        WHEN OTHERS
        THEN 
            --DBMS_OUTPUT.PUT_LINE('REQUEST_ID ERROR (not exist)')
            RETURN 0;
  
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_PKK_ADDRESS" 
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицу с данными из C_REQUEST
-- ==	ОПИСАНИЕ:	    Обновляем статусов и прочее для заявок
-- ========================================================================
-- ==	СОЗДАНИЕ:		  09.11.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	25.01.2016 (ТРАХАЧЕВ В.В.)
-- ==
/*PKK_ADDRESS:45147121 - 45180956. От :25.01.16 06:01:19,117174000 UTC до: 25.01.16 06:11:52,391406000 UTC
  PKK_ADDRESS:45179956 - 45181029. От :25.01.16 06:26:27,650414000 UTC до: 25.01.16 06:26:33,475282000 UTC
  PKK_ADDRESS:45180029 - 45181031. От :25.01.16 06:27:10,728925000 UTC до: 25.01.16 06:27:15,561455000 UTC
  PKK_ADDRESS:45180031 - 45181041. От  25.01.16 06:31:27,625945000 UTC до  25.01.16 06:31:32,745738000 UTC
  PKK_ADDRESS:45171041-45223898(52649). От 11.02.16 02:49:31,756795000 UTC до 11.02.16 02:55:52,778584000 UTC
  PKK_ADDRESS:45223798-45223905(108). От 11.02.16 02:57:32,505665000 UTC до 11.02.16 02:57:35,696665000 UTC
  PKK_ADDRESS:45222905-45223905(994). От 11.02.16 02:58:04,738978000 UTC до 11.02.16 02:58:09,354902000 UTC
  PKK_ADDRESS:45222905-45227280(4360). От 12.02.16 07:35:29,271767000 UTC до 12.02.16 07:36:54,258192000 UTC
  PKK_ADDRESS:45271881-45277951(6057). От 02.03.16 06:25:58,410337000 +03:00 до 02.03.16 06:28:24,000454000 +03:00
*/
-- ========================================================================
AS    
  last_ID_SFF NUMBER; -- последний существующий REQUEST_ID в SFF
  last_ID_PKK NUMBER; -- последний существующий REQUEST_ID в ПКК
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  start_time := systimestamp;
  
  SELECT MAX(ADDRESS_ID)-1000 INTO last_ID_SFF FROM ODS.PKK_ADDRESS;
  SELECT MAX(ADDRESS_ID) INTO last_ID_PKK FROM CPD.ADDRESS@DBLINK_PKK;

  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_ADDRESS tar
	USING (SELECT
			adr.ADDRESS_ID
			,cntr.COUNTRIES_ISO2 as COUNTRY		--Страна
			,adr.REGIONS_UID
			,reg.REGIONS_NAMES					--Регион
			,adr.AREAS_UID    
			,are.AREAS_NAMES					--Район 
			,adr.CITIES_UID						--Id нас. пункта
			,cit.CITIES_NAMES					--НП
			,cit.CITIES_TYPE					--Тип НП ID
			,KLADR_SIT.SHOTNAME as SHOTNAME_CIT	--Тип НП 
			,adr.STREETS_UID					--ID типа улицы
			,str.STREETS_NAMES					--Улица 
			,str.STREETS_TYPE					--Тип улицы ID
			,KLADR_STR.SHOTNAME as SHOTNAME_STR	--Тип улицы 
			,adr.HOUSE							--Дом 
			,adr.BUILD							--Корпус 
			,adr.FLAT							--Квартира
			,adr.POSTOFFICE						--Индекс
			,adr.GEO_ID							--ID геоданных
			,geo.QUALITY_CODE					--Код качества при преобразовании исходных адресных данных для получения геокоординат
			,geo.GEO_LAT						--Широта
			,geo.GEO_LNG						--Долгота
			,geo.GEO_QC							--Точность определения преобразования адреса для получения координат
			,geo.ADDRESS_STR AS GEO_ADR			--Преобразованный адрес, по которому цеплялись координаты
			,geo.CREATED_DATE AS GEO_CREATED	--Дата создания кординат
		FROM CPD.ADDRESS@dblink_pkk adr 
		LEFT JOIN CPD.REGIONS_NAMES@dblink_pkk reg ON adr.REGIONS_UID = reg.REGIONS_UID
		LEFT JOIN CPD.AREAS_NAMES@dblink_pkk are ON adr.AREAS_UID = are.AREAS_UID
		LEFT JOIN CPD.CITIES_NAMES@dblink_pkk cit ON adr.CITIES_UID = cit.CITIES_UID
		LEFT JOIN CPD.STREETS_NAMES@dblink_pkk str ON adr.STREETS_UID = str.STREETS_UID
		LEFT JOIN CPD.COUNTRIES@dblink_pkk cntr ON adr.COUNTRIES_ID = cntr.COUNTRIES_ID
		LEFT JOIN CPD.KLADR_SOCR@dblink_pkk KLADR_SIT ON cit.CITIES_TYPE = KLADR_SIT.SOCR_ID
		LEFT JOIN CPD.KLADR_SOCR@dblink_pkk KLADR_STR ON str.STREETS_TYPE = KLADR_STR.SOCR_ID
		LEFT JOIN CPD.GEOCOORDINATES@dblink_pkk geo ON adr.GEO_ID = geo.ID
		WHERE 
      adr.ADDRESS_ID BETWEEN last_ID_SFF AND last_ID_PKK
		) src
    ON (src.ADDRESS_ID = tar.ADDRESS_ID )
	WHEN MATCHED THEN
		--клюевые и неизменяемые поля не нужно обновлять. 
		UPDATE SET
			--tar.ADDRESS_ID=src.ADDRESS_ID
			tar.COUNTRY=src.COUNTRY
			,tar.REGIONS_UID=src.REGIONS_UID
			,tar.REGIONS_NAMES=src.REGIONS_NAMES
			,tar.AREAS_UID=src.AREAS_UID
			,tar.AREAS_NAMES=src.AREAS_NAMES
			,tar.CITIES_UID=src.CITIES_UID
			,tar.CITIES_NAMES=src.CITIES_NAMES
			,tar.CITIES_TYPE=src.CITIES_TYPE
			,tar.SHOTNAME_CIT=src.SHOTNAME_CIT
			,tar.STREETS_UID=src.STREETS_UID
			,tar.STREETS_NAMES=src.STREETS_NAMES
			,tar.STREETS_TYPE=src.STREETS_TYPE
			,tar.SHOTNAME_STR=src.SHOTNAME_STR
			,tar.HOUSE=src.HOUSE
			,tar.BUILD=src.BUILD
			,tar.FLAT=src.FLAT
			,tar.POSTOFFICE=src.POSTOFFICE
			,tar.GEO_ID=src.GEO_ID
			,tar.QUALITY_CODE=src.QUALITY_CODE
			,tar.GEO_LAT=src.GEO_LAT
			,tar.GEO_LNG=src.GEO_LNG
			,tar.GEO_QC=src.GEO_QC
			,tar.GEO_ADR=src.GEO_ADR
			,tar.GEO_CREATED=src.GEO_CREATED
      ,tar.DATE_UPD=SYSDATE
	WHEN NOT MATCHED THEN 
	--вставляем новое
	INSERT (tar.ADDRESS_ID
			,tar.COUNTRY
			,tar.REGIONS_UID, tar.REGIONS_NAMES, tar.AREAS_UID, tar.AREAS_NAMES
			,tar.CITIES_UID, tar.CITIES_NAMES, tar.CITIES_TYPE, tar.SHOTNAME_CIT
			,tar.STREETS_UID, tar.STREETS_NAMES, tar.STREETS_TYPE, tar.SHOTNAME_STR
			,tar.HOUSE, tar.BUILD, tar.FLAT
			,tar.POSTOFFICE
			,tar.GEO_ID, tar.QUALITY_CODE, tar.GEO_LAT, tar.GEO_LNG
			,tar.GEO_QC, tar.GEO_ADR, tar.GEO_CREATED
      ,tar.DATE_UPD)
	VALUES (src.ADDRESS_ID
			,src.COUNTRY
			,src.REGIONS_UID, src.REGIONS_NAMES, src.AREAS_UID, src.AREAS_NAMES
			,src.CITIES_UID, src.CITIES_NAMES, src.CITIES_TYPE, src.SHOTNAME_CIT
			,src.STREETS_UID, src.STREETS_NAMES, src.STREETS_TYPE, src.SHOTNAME_STR
			,src.HOUSE, src.BUILD, src.FLAT
			,src.POSTOFFICE
			,src.GEO_ID, src.QUALITY_CODE, src.GEO_LAT, src.GEO_LNG
			,src.GEO_QC, src.GEO_ADR, src.GEO_CREATED
      ,SYSDATE)
	;	

  --информация для вывода при ручном обновлении
  cnt_MODIF := SQL%ROWCOUNT;
  p_ADD_INFO := 'Период обновления: '||TO_CHAR(last_ID_SFF)||' до '||TO_CHAR(last_ID_PKK);
  
  ODS.PR$INS_LOG ('PR$UPD_PKK_ADDRESS', start_time, 'ODS.PKK_ADDRESS', 'OK', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_PKK_ADDRESS', start_time, 'ODS.PKK_ADDRESS', 'ERR', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
END;
  CREATE OR REPLACE PROCEDURE "SFF"."PR$UPD_SFF_PHONES" (nPHID_ago IN NUMBER default 10000, nID_ADD IN NUMBER DEFAULT 10000)
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицы с данными из PHONES
-- ==	ОПИСАНИЕ:	    Обновляем статусов и прочее для заявок
-- ========================================================================
-- ==	СОЗДАНИЕ:		  09.11.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	09.11.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
AS    
  last_PHID_SFF NUMBER; -- последний существующий REQUEST_ID в SFF
  last_PHID_PKK NUMBER; -- последний существующий REQUEST_ID в ПКК
  
  last_MODDT_SFF DATE; -- последний MODIFICATION_DATE в SFF
  last_MODDT_PKK DATE; -- последний MODIFICATION_DATE в ПКК
BEGIN
  DBMS_OUTPUT.ENABLE;  
  
  DBMS_OUTPUT.PUT_LINE('START:'||TO_CHAR(systimestamp at time zone 'utc'));
  SELECT MAX(PHONES_ID)-nPHID_ago INTO last_PHID_SFF FROM SFF.SFF_PHONES;
  last_PHID_SFF := nPHID_ago;
  last_PHID_PKK := last_PHID_SFF + nID_ADD;
  --SELECT MAX(REQUEST_ID) INTO last_PHID_PKK FROM CPD.PHONES@DBLINK_PKK;
  
  --определение максимумов - не так критично по времени когда таблица без индексов.
  --  поэтому считаю проставление индексов нецелесообразным
  SELECT MAX(PHONES_CREATED)-15 INTO last_MODDT_SFF FROM SFF.SFF_PHONES;
  DBMS_OUTPUT.PUT_LINE(' --- 1_date:'||TO_CHAR(systimestamp at time zone 'utc'));
  SELECT MAX(PHONES_CREATED) INTO last_MODDT_PKK FROM CPD.PHONES@DBLINK_PKK;
  DBMS_OUTPUT.PUT_LINE(' --- 2_date:'||TO_CHAR(systimestamp at time zone 'utc'));
  --last_MODDT_SFF := last_MODDT_PKK-100;

  --УДАЛЕНИЕ 
  DELETE FROM SFF.SFF_PHONES sff_ph 
    WHERE EXISTS(SELECT PHONES_ID FROM CPD.PHONES_DEL_LOG@DBLINK_PKK WHERE PHONES_ID=sff_ph.PHONES_ID);
  
  --ОБНОВЛЕНИЕ
  MERGE INTO SFF.SFF_PHONES tar
  USING (SELECT PHONES_ID, OBJECTS_ID,OBJECTS_TYPE, PHONES_AKT, PHONES_COMM, PHONES_CREATED
                ,CREATED_USER_ID, CREATED_GROUP_ID, MODIFICATION_DATE, PHONES_LAST, FAMILY_REL 
                ,PHONE, ADDRESS_ID
          FROM CPD.PHONES@DBLINK_PKK 
          WHERE 
                --PHONES_ID BETWEEN last_PHID_SFF AND last_PHID_PKK
                COALESCE(MODIFICATION_DATE, PHONES_CREATED) BETWEEN last_MODDT_SFF AND last_MODDT_PKK 
                ) src
    ON (src.PHONES_ID=tar.PHONES_ID )
  WHEN MATCHED THEN
    --закомменченые поля не нужно обновлять. FLAG_NEW_DATA=2 если обновились существующуие данные
    UPDATE SET tar.OBJECTS_ID=src.OBJECTS_ID
                ,tar.OBJECTS_TYPE=src.OBJECTS_TYPE, tar.PHONES_AKT=src.PHONES_AKT, tar.PHONES_COMM=src.PHONES_COMM
                ,tar.PHONES_CREATED=src.PHONES_CREATED,tar.CREATED_USER_ID=src.CREATED_USER_ID
                ,tar.CREATED_GROUP_ID=src.CREATED_GROUP_ID,tar.MODIFICATION_DATE=src.MODIFICATION_DATE
                ,tar.PHONES_LAST=src.PHONES_LAST,tar.FAMILY_REL=src.FAMILY_REL, tar.PHONE=src.PHONE
                ,tar.ADDRESS_ID=src.ADDRESS_ID
  WHEN NOT MATCHED THEN 
    --вставляем новое
    INSERT (tar.PHONES_ID,tar.OBJECTS_ID,tar.OBJECTS_TYPE, tar.PHONES_AKT, tar.PHONES_COMM, tar.PHONES_CREATED
                ,tar.CREATED_USER_ID,tar.CREATED_GROUP_ID,tar.MODIFICATION_DATE,tar.PHONES_LAST,tar.FAMILY_REL 
                ,tar.PHONE, tar.ADDRESS_ID)
    VALUES (src.PHONES_ID,src.OBJECTS_ID,src.OBJECTS_TYPE, src.PHONES_AKT, src.PHONES_COMM, src.PHONES_CREATED
                ,src.CREATED_USER_ID,src.CREATED_GROUP_ID,src.MODIFICATION_DATE,src.PHONES_LAST,src.FAMILY_REL 
                ,src.PHONE, src.ADDRESS_ID)
    ;	
    --DBMS_OUTPUT.PUT_LINE('RID:'||TO_CHAR(last_PHID_SFF)||' - '||TO_CHAR(last_PHID_PKK));
    DBMS_OUTPUT.PUT_LINE('END:'||TO_CHAR(last_MODDT_SFF)||' - '||TO_CHAR(last_MODDT_PKK)
                          ||'. Dttm:'||TO_CHAR(systimestamp at time zone 'utc'));
/*EXCEPTION
    WHEN OTHERS
    THEN DBMS_OUTPUT.PUT_LINE('Ошибка выполения процедуры ');*/
END;
  CREATE OR REPLACE PROCEDURE "SFF"."PR$UPD_SNA_DATA" (nRID_beg IN NUMBER, nRID_end IN NUMBER)
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицу APPLICATIONS
-- ==	ОПИСАНИЕ:	    Новый оптимизированный скрипт оновления подготовленнхы данных для SNA
-- ========================================================================
-- ==	СОЗДАНИЕ:		  30.11.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	30.11.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
AS    
  cnt_before NUMBER;
  cnt_after NUMBER;
  --last_RID_SFF NUMBER; -- последний существующий REQUEST_ID в SFF
  --last_RID_PKK NUMBER; -- последний существующий REQUEST_ID в ПКК
BEGIN
  DBMS_OUTPUT.ENABLE;
  --SELECT COUNT(*) INTO cnt_before FROM SFF.APPLICATIONS_TEST;
  --УДАЛЯЕМ ПРИ НАЛИЧИИ
  --DELETE FROM SFF.APPLICATIONS_TEST WHERE REQUEST_ID=nRID_beg;
/*--== 26.11.2015 - ОБНОВЛЕНИЕ РЕСТРУКТУРИЗАЦИЯ И РЕФАКТОРИНГ СКРИПТА НАКАТА для SNA - старая версия*/
INSERT INTO SFF.APPLICATIONS_TEST
(REQUEST_ID,PERSON_ID, REQ_DATE, 
	REQ_SUMM , REQ_SUMM_CURR
	,SEX
  ,BIRTHDAY 
  ,FIO 
	,PRODUCT_TYPE, REQ_CREATED_BRANCH
  ,SALARY
  ,PASSPORT, EMAIL, 
	HOME_PHONE, 
  MOBILE, 
  WORK_PHONE 
  --,RA_COUNTRY
  , RA_REGION, RA_DISTRICT, RA_CITY, RA_SETTLEMENT, RA_STREET, RA_HOUSE, RA_BUILDING, RA_APARTMENT, RA_INDEX 
	--,LA_COUNTRY
  , LA_REGION, LA_DISTRICT, LA_CITY, LA_SETTLEMENT, LA_STREET, LA_HOUSE, LA_BUILDING, LA_APARTMENT, LA_INDEX
  --,BA_COUNTRY
  , BA_REGION, BA_DISTRICT, BA_CITY,BA_SETTLEMENT,BA_STREET,BA_HOUSE,BA_BUILDING, BA_APARTMENT, BA_INDEX
  ,WORK_ORG_NAME
	--,EDUCATION
  --, IS_PENSION
  --, IS_NEW_CLIENT
  --,IS_NEW_DATA
  --WORK_TYPE, 
	)
SELECT DISTINCT * FROM (
	SELECT cr.REQUEST_ID, cr.OBJECTS_ID as PERSON_ID, cr.CREATED_DATE as REQ_DATE
    ,crc.SUMMA AS REQ_SUMM
    ,DECODE(crc.CURRENCY_ID, 1,'R', 2,'U', 3,'E', 4,'C', 5,'J', '-')  AS REQ_SUMM_CURR
		/*,cr.MODIFICATION_DATE, cr.CREATED_GROUP_ID, cr.STATUS_ID*/
		,cri.SEX_ID as SEX
    , cri.BDATE as BIRTHDAY
    ,f.FIO4SEARCH as FIO
		/*,cri.MDOC_ID 
		,cri.MADR_ID 
		,cri.LADR_ID */
		--,cri.DADR_ID
		/*,cri.EDUC*/
		--,cri.WORK
		--,to_number(SUBSTR(cri.WORK, 1, INSTR(cri.WORK||' ', ' '))) AS WORK_ID
		/*,crc.SCHEMS_ID*/
		,c_sch.SCHEMS_NAME as PRODUCT_TYPE
		,gr.GROUPS_NAME AS REQ_CREATED_BRANCH
    ,works.WORKS_SALARY AS SALARY
		/*,v_pers.FIO4SEARCH*/
		--,cri.FIO_ID 

		,D.DOCUMENTS_SERIAL||D.DOCUMENTS_NUMBER as PASSPORT
    --,eml.EMAIL
    ,FIRST_VALUE(eml.EMAIL) OVER (PARTITION BY cr.REQUEST_ID ORDER BY eml.EMAIL_ID) as EMAIL
		,TO_NUMBER(homph.PHONE) as HOME_PHONE
		--,homph.CREATED_USER_ID as us_ID
    ,TO_NUMBER(ph_mob.PHONE) as MOBILE
    --,ph_mob.PHONES_AKT as mob_akt
    --,ph_mob.CREATED_USER_ID as us_ID
    --,ph_mob.PHONES_COMM
    ,TO_NUMBER(COALESCE(ph_work.phone, phn_org.phone)) AS WORK_PHONE
  --АДРЕС ПРОПИСКИ (RA_ - MADR_ID)
    /*,ra_adr.GEO_ID AS ra_GEO_ID
    ,ra_geo.ADDRESS_STR AS la_ADDRESS_STR
    ,ra_geo.QUALITY_CODE as ra_QUALITY_CODE
    ,ra_KLADR_SIT.SHOTNAME as ra_SHOTNAME_CIT --Тип НП ФМЖ
    ,ra_KLADR_STR.SHOTNAME as ra_SHOTNAME_STR --Тип улицы ФМЖ
    ,ra_cntr.COUNTRIES_ISO2 as RA_COUNTRY --Страна*/
    --,''                   AS RA_COUNTRY
    ,ra_reg.REGIONS_NAMES AS RA_REGION      --Регион
    ,ra_are.AREAS_NAMES   AS RA_DISTRICT    --Район 
    ,ra_cit.CITIES_NAMES  AS RA_CITY        --НП 
    ,''                   AS RA_SETTLEMENT
    ,ra_str.STREETS_NAMES AS RA_STREET      --Улица 
    ,ra_adr.HOUSE         AS RA_HOUSE       --Дом 
    ,ra_adr.BUILD         AS RA_BUILDING    --Корпус 
    ,ra_adr.FLAT          AS RA_APPARTMENT  --Квартира
    ,ra_adr.POSTOFFICE    AS RA_INDEX       --Индекс
  --АДРЕС ПРОЖИВАНИЯ (LA_ - LADR_ID)
    /*,la_adr.GEO_ID AS la_GEO_ID
    ,la_geo.ADDRESS_STR AS la_ADDRESS_STR
    ,la_geo.QUALITY_CODE as la_QUALITY_CODE
    ,la_KLADR_SIT.SHOTNAME as la_SHOTNAME_CIT --Тип НП ФМЖ
    ,la_KLADR_STR.SHOTNAME as la_SHOTNAME_STR --Тип улицы ФМЖ
    ,la_cntr.COUNTRIES_ISO2 as LA_COUNTRY--Страна*/
    --,''                   AS LA_COUNTRY
    ,la_reg.REGIONS_NAMES AS LA_REGION      --Регион
    ,la_are.AREAS_NAMES   AS LA_DISTRICT    --Район 
    ,la_cit.CITIES_NAMES  AS LA_CITY        --НП 
    ,''                   AS LA_SETTLEMENT
    ,la_str.STREETS_NAMES AS LA_STREET      --Улица 
    ,la_adr.HOUSE         AS LA_HOUSE       --Дом 
    ,la_adr.BUILD         AS LA_BUILDING    --Корпус 
    ,la_adr.FLAT          AS LA_APPARTMENT  --Квартира
    ,la_adr.POSTOFFICE    AS LA_INDEX       --Индекс
  --АДРЕС РАБОТЫ 
    --,''                   AS BA_COUNTRY
    ,ba_reg.REGIONS_NAMES AS BA_REGION      --Регион
    ,ba_are.AREAS_NAMES   AS BA_DISTRICT    --Район 
    ,ba_cit.CITIES_NAMES  AS BA_CITY        --НП 
    ,''                   AS BA_SETTLEMENT
    ,ba_str.STREETS_NAMES AS BA_STREET      --Улица 
    ,ba_adr.HOUSE         AS BA_HOUSE       --Дом 
    ,ba_adr.BUILD         AS BA_BUILDING    --Корпус 
    ,ba_adr.FLAT          AS BA_APPARTMENT  --Квартира
    ,''                   AS BA_INDEX       --Индекс
    ,org.ORG_NAME as WORK_ORG_NAME    
    --,works.ORG_ID
    --,works.WORKS_POST
    /*,w_post.WORKS_POST_NAME
    ,org_a.ORG_ID as ORG_ID_BA
    ,org_a.ORG_NAME as ORG_NAME_BA*/
    --,ah.ADDRESS_ID
    /*,wh.CREATED_USER_ID as cui
    ,wh.CREATED_GROUP_ID as cgi
    ,wh.WORKS_CREATED as w_cr*/
    
    --,LAST_VALUE(ph_mob.WORKS_ID) OVER (PARTITION BY cr.REQUEST_ID ORDER BY ph_mob.PHONES_ID) as pr_mob2
		--,(CASE WHEN cr.REQUEST_INFO_ID_LAST=cri.REQUEST_INFO_ID THEN 1 ELSE 0 END) as RRID_eq
		--,DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID ORDER BY rownum) as n_prior
    --,DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID ORDER BY ph_mob.PHONES_ID) as pr_mob
    --,LAST_VALUE(ph_mob.PHONE) OVER (PARTITION BY cr.REQUEST_ID ORDER BY ph_mob.PHONES_ID) as pr_mob2
    
    --,NULL as EDUCATION
    --,NULL AS IS_PENSION
    --,NULL AS IS_NEW_CLIENT
    --,NULL AS IS_NEW_DATA
	FROM KREDIT.C_REQUEST@DBLINK_PKK cr
	RIGHT JOIN KREDIT.C_REQUEST_INFO@DBLINK_PKK cri
		ON cri.REQUEST_INFO_ID = NVL(cr.REQUEST_INFO_ID_LAST, -1)
	LEFT JOIN KREDIT.C_REQUEST_CREDIT@DBLINK_PKK crc
		ON crc.REQUEST_CREDIT_ID = NVL(cr.REQUEST_CREDIT_ID_LAST, -1)
	LEFT JOIN CPD.FIO@DBLINK_PKK f
		ON f.FIO_ID = cri.FIO_ID
	LEFT JOIN KREDIT.C_SCHEMS@dblink_pkk c_sch
		ON c_sch.SCHEMS_ID = crc.SCHEMS_ID
	LEFT JOIN KREDIT."GROUPS"@DBLINK_PKK gr
		ON gr.GROUPS_ID=cr.CREATED_GROUP_ID
	/*LEFT JOIN KREDIT."VIEW_PERSONS"@DBLINK_PKK v_pers
		ON v_pers.PERSON_ID=cr.OBJECTS_ID*/
	LEFT JOIN CPD.DOCUMENTS@DBLINK_PKK D
		ON D.DOCUMENTS_ID = cri.MDOC_ID AND D.DOCUMENTS_TYPE=21 
    --AND DH.DOCUMENTS_AKT<>0
  LEFT JOIN CPD.EMAIL@dblink_pkk eml
    ON eml.OBJECTS_ID=cr.OBJECTS_ID AND eml.EMAIL_AKT=1 and eml.EMAIL LIKE '%@%'
  --АДРЕС прописки RA_ (MADR_ID)
  LEFT JOIN CPD.ADDRESS@dblink_pkk ra_adr ON ra_adr.address_id = NVL(cri.MADR_ID, -1)
  LEFT JOIN CPD.REGIONS_NAMES@dblink_pkk ra_reg ON ra_adr.REGIONS_UID = ra_reg.REGIONS_UID
  LEFT JOIN CPD.AREAS_NAMES@dblink_pkk ra_are ON ra_adr.AREAS_UID = ra_are.AREAS_UID
  LEFT JOIN CPD.CITIES_NAMES@dblink_pkk ra_cit ON ra_adr.CITIES_UID = ra_cit.CITIES_UID
  LEFT JOIN CPD.STREETS_NAMES@dblink_pkk ra_str ON ra_adr.STREETS_UID = ra_str.STREETS_UID
  --LEFT JOIN COUNTRIES@dblink_pkk ra_cntr ON adr.COUNTRIES_ID = ra_cntr.COUNTRIES_ID
  --LEFT JOIN KLADR_SOCR@dblink_pkk ra_KLADR_SIT ON ra_cit.CITIES_TYPE = ra_KLADR_SIT.SOCR_ID
  --LEFT JOIN KLADR_SOCR@dblink_pkk ra_KLADR_STR ON ra_str.STREETS_TYPE = ra_KLADR_STR.SOCR_ID
  --LEFT JOIN GEOCOORDINATES@dblink_pkk ra_geo ON ra_adr.GEO_ID = ra_geo.ID
  
  --АДРЕС проживания LA_ (LADR_ID)
  LEFT JOIN CPD.ADDRESS@dblink_pkk la_adr ON la_adr.address_id = NVL(cri.LADR_ID, -1)
  LEFT JOIN CPD.REGIONS_NAMES@dblink_pkk la_reg ON la_adr.REGIONS_UID = la_reg.REGIONS_UID
  LEFT JOIN CPD.AREAS_NAMES@dblink_pkk la_are ON la_adr.AREAS_UID = la_are.AREAS_UID
  LEFT JOIN CPD.CITIES_NAMES@dblink_pkk la_cit ON la_adr.CITIES_UID = la_cit.CITIES_UID
  LEFT JOIN CPD.STREETS_NAMES@dblink_pkk la_str ON la_adr.STREETS_UID = la_str.STREETS_UID
  
  --LEFT JOIN COUNTRIES@dblink_pkk la_cntr ON adr.COUNTRIES_ID = la_cntr.COUNTRIES_ID
  --LEFT JOIN KLADR_SOCR@dblink_pkk la_KLADR_SIT ON ra_cit.CITIES_TYPE = la_KLADR_SIT.SOCR_ID
  --LEFT JOIN KLADR_SOCR@dblink_pkk la_KLADR_STR ON ra_str.STREETS_TYPE = la_KLADR_STR.SOCR_ID
  --LEFT JOIN GEOCOORDINATES@dblink_pkk la_geo ON la_adr.GEO_ID = la_geo.ID
 
	--ТЕЛЕФОН по адресу проживания (регистрации). LADR_ID-проживание, MADR_ID-прописки
	LEFT JOIN  
        CPD.PHONES@dblink_pkk 
        --SFF.SFF_PHONES
       homph 
		ON homph.objects_id=COALESCE(cri.LADR_ID, cri.MADR_ID, -1) AND homph.OBJECTS_TYPE=8 
      AND homph.phones_akt=1 
      AND homph.phone is not null 
      AND homph.PHONES_CREATED<cr.MODIFICATION_DATE
	--МОБИЛЬНЫЙ ТЕЛЕФОН клиента
  LEFT JOIN  
        CPD.PHONES@dblink_pkk 
        --SFF.SFF_PHONES
       ph_mob
    on ph_mob.OBJECTS_ID=cr.OBJECTS_ID AND ph_mob.OBJECTS_TYPE=2 
      AND ph_mob.phones_akt=1
      AND ph_mob.phone is not null 
      AND ph_mob.PHONES_CREATED<cr.MODIFICATION_DATE 
      --AND ph_mob.CREATED_USER_ID^=1512142 --не автоподгруженный телефон
  --РАБОЧИЙ ТЕЛЕФОН клиента
	LEFT JOIN CPD.WORKS@DBLINK_PKK works 
		ON works.works_id = TO_NUMBER(NVL(regexp_substr(cri.WORK, '[0-9]+ ?'), '-1'))
            --TO_NUMBER(SUBSTR(NVL(cri.WORK, '-1')||' ', 0, INSTR(NVL(cri.WORK, '-1')||' ', ' ')))
			AND works.org_vid NOT IN (0,5,98,99,100,101,102,105) 
  LEFT JOIN CPD.ORG@DBLINK_PKK  org -- через эту таблицу подтягивается только последний WORKS_ID
    ON org.ORG_ID = works.ORG_ID
      AND org.org_vid NOT IN (0,5,98,99,100,101,102,105)
      /*AND org.org_id=67790*/
  LEFT JOIN  
        CPD.PHONES@dblink_pkk 
        --SFF.SFF_PHONES
       ph_work 
    ON ph_work.objects_id = works.works_id AND ph_work.OBJECTS_TYPE=works.OBJECTS_TYPE 
      AND ph_work.phones_akt=1
      AND ph_work.phone IS NOT NULL
  LEFT JOIN 
        CPD.PHONES@dblink_pkk 
        --SFF.SFF_PHONES
      phn_org 
    ON phn_org.objects_id = org.org_id AND phn_org.OBJECTS_TYPE=org.OBJECTS_TYPE 
      AND phn_org.phones_akt=1
      AND phn_org.phone is not null
  --АДРЕС РАБОТЫ (BA_) РАСКОММЕНТИРОВАТЬ НА БУДУЩЕЙ НОВОЙ ВЕРСИИ РАЗДЕЛЕННОЙ СТРУТКРУЫ.
	/*LEFT JOIN CPD.WORKS_HISTORY@DBLINK_PKK wh 
		ON wh.OBJECTS_ID = cr.OBJECTS_ID AND wh.works_akt=1 AND wh.works_created<cr.MODIFICATION_DATE
  INNER JOIN works@DBLINK_PKK w
    ON wh.works_id = w.works_id
  LEFT JOIN CPD.WORKS_POST@DBLINK_PKK w_post
    ON w_post.WORKS_POST = w.WORKS_POST
  LEFT JOIN CPD.ORG@DBLINK_PKK org_a
    ON org_a.ORG_ID = w.ORG_ID*/
     
  LEFT JOIN CPD.ADDRESS_HISTORY@dblink_pkk ah 
    ON ah.objects_id=NVL(org.org_id, -1) AND ah.OBJECTS_TYPE=3 and ah.address_akt=1
  LEFT JOIN CPD.ADDRESS@dblink_pkk ba_adr ON ba_adr.address_id = ah.ADDRESS_ID
  LEFT JOIN CPD.REGIONS_NAMES@dblink_pkk ba_reg ON ba_adr.REGIONS_UID = ba_reg.REGIONS_UID
  LEFT JOIN CPD.AREAS_NAMES@dblink_pkk ba_are ON ba_adr.AREAS_UID = ba_are.AREAS_UID
  LEFT JOIN CPD.CITIES_NAMES@dblink_pkk ba_cit ON ba_adr.CITIES_UID = ba_cit.CITIES_UID
  LEFT JOIN CPD.STREETS_NAMES@dblink_pkk ba_str ON ba_adr.STREETS_UID = ba_str.STREETS_UID
  
  --LEFT JOIN COUNTRIES@dblink_pkk la_cntr ON adr.COUNTRIES_ID = la_cntr.COUNTRIES_ID
  --LEFT JOIN KLADR_SOCR@dblink_pkk la_KLADR_SIT ON ra_cit.CITIES_TYPE = la_KLADR_SIT.SOCR_ID
  --LEFT JOIN KLADR_SOCR@dblink_pkk la_KLADR_STR ON ra_str.STREETS_TYPE = la_KLADR_STR.SOCR_ID
  --LEFT JOIN GEOCOORDINATES@dblink_pkk la_geo ON la_adr.GEO_ID = la_geo.ID  
	WHERE cr.REQUEST_ID /*=nRID_beg*/ BETWEEN nRID_beg AND nRID_end
    --AND cr.CREATED_GROUP_ID^=11455
    --AND ((cr.CREATED_GROUP_ID^=11455 AND cr.STATUS_ID^=14) OR cr.STATUS_ID=14)
    --AND ((cr.CREATED_GROUP_ID=11455 AND cr.STATUS_ID IN(13, 14, 225)) OR cr.CREATED_GROUP_ID^=11455)
    /*AND cri.REQUEST_ID BETWEEN nRID_beg AND nRID_end*/
    --cr.OBJECTS_ID IN(1526664,22477470,512490 )
    --AND NOT cri.WORK IS NULL
		/*AND cr.MODIFICATION_DATE>TO_DATE('26-11-2015','dd-mm-yyyy')*/
	/*ORDER BY cr.REQUEST_ID, cr.REQUEST_INFO_ID_LAST, cri.REQUEST_INFO_ID*/
  )
	/*WHERE n_prior=1 */
	--ORDER BY PERSON_ID, REQUEST_ID DESC
  ;
  
  --SELECT COUNT(*) INTO cnt_after FROM SFF.APPLICATIONS_TEST;
  
  --DBMS_OUTPUT.PUT_LINE('cnt до:'||TO_CHAR(cnt_before)||', после: '||TO_CHAR(cnt_after)||'. Insert'||TO_CHAR(cnt_after-cnt_before));
EXCEPTION
    WHEN OTHERS
    THEN DBMS_OUTPUT.PUT_LINE('Ошибка выполения процедуры ');
END;
  CREATE OR REPLACE FUNCTION "SFF"."FN_DIST_HAMM" (
  as_src_i                         IN VARCHAR2
, as_trg_i                         IN VARCHAR2
)
RETURN NUMBER
DETERMINISTIC
AS
/* Самописное расстояние Хемминга для нечеткого поиска */
  ln_src_len                       PLS_INTEGER := NVL(LENGTH(as_src_i), 0);
  ln_trg_len                       PLS_INTEGER := NVL(LENGTH(as_trg_i), 0);
  ln_distance                      PLS_INTEGER := 0;
BEGIN
  IF (ln_src_len <> ln_trg_len)
  THEN
    RETURN NULL;
  END IF;

  IF (ln_src_len = 0)
  THEN
    RETURN ln_src_len;
  END IF;

  FOR i IN 1..ln_src_len
  LOOP
    IF (SUBSTR(as_src_i, i, 1) <> SUBSTR(as_trg_i, i, 1))
    THEN
      ln_distance := ln_distance + 1;
    END IF;
  END LOOP;
  RETURN ln_distance;
END FN_DIST_HAMM;
  CREATE OR REPLACE FUNCTION "SFF"."FN_DIST_LEV" (
	as_src_i		IN VARCHAR2
	, as_trg_i		IN VARCHAR2
	, porog_ln IN NUMBER DEFAULT 1000
)
RETURN NUMBER
DETERMINISTIC
AS
/* Самописное расстояние Левенштейна (кол-во унарных операций вставки/удаления/замены символа) для нечеткого сравнения */
	ln_src_len				PLS_INTEGER := NVL(LENGTH(as_src_i), 0);
	ln_trg_len				PLS_INTEGER := NVL(LENGTH(as_trg_i), 0);
	ln_hlen					PLS_INTEGER;
	ln_cost					PLS_INTEGER;
	TYPE t_numtbl IS TABLE OF PLS_INTEGER INDEX BY BINARY_INTEGER;
	la_ldmatrix				t_numtbl;
BEGIN
	IF (ln_src_len = 0)
	THEN
		RETURN ln_trg_len;
	ELSIF (ln_trg_len = 0)
	THEN
		RETURN ln_src_len;
	END IF;
	IF ABS(ln_src_len-ln_trg_len)>porog_ln THEN --если разница длин больше установленной то прерывать
    RETURN NULL;
  END IF;

	ln_hlen := ln_src_len + 1;
	FOR h IN 0 .. ln_src_len
	LOOP
		la_ldmatrix(h) := h;
	END LOOP;

	FOR v IN 0 .. ln_trg_len
	LOOP
		la_ldmatrix(v * ln_hlen) := v;
	END LOOP;

	FOR h IN 1 .. ln_src_len
	LOOP
		FOR v IN 1 .. ln_trg_len
		LOOP
			IF (SUBSTR(as_src_i, h, 1) = SUBSTR(as_trg_i, v, 1))
		THEN
			ln_cost := 0;
		ELSE
			ln_cost := 1;
		END IF;
			la_ldmatrix(v * ln_hlen + h) :=
				LEAST(
					la_ldmatrix((v - 1) * ln_hlen + h    ) + 1
					, la_ldmatrix( v      * ln_hlen + h - 1) + 1
					, la_ldmatrix((v - 1) * ln_hlen + h - 1) + ln_cost
				);
		END LOOP;
	END LOOP;
	RETURN la_ldmatrix(ln_trg_len * ln_hlen + ln_src_len);
END FN_DIST_LEV ;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_P1_F0" (v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО. Разные ФиоДр - Паспорт равен. Разл_ФИО больше 1"
-- ==	ОПИСАНИЕ:		Совпадает серия и номер паспорта. ФИО или ДР - другие (различие больше чем в 1 символе) 
-- ==					(SER+NUM = , ФИО+ДР^= в > чем 1 симв.)
-- ========================================================================
-- ==	СОЗДАНИЕ:		03.11.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	24.11.2015 (ТРАХАЧЕВ В.В.)
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
				--,'Паспорт1_ФиоДр0' as TYPE_REL
        ,'Разные ФиоДр - Паспорт равен' as TYPE_REL
				,'Совпадает серия и номер паспорта. ФИО или ДР - другие (различие больше чем в 1 символе)' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Паспорт:'||SRC.PASSPORT
              /*||' - '||TO_CHAR(SFF.FN_DIST_LEV(SRC.FIO, APP.FIO)+SFF.FN_DIST_LEV(SRC.DR, APP.DR))||'симв.'*/ as INFO_EQ
				,'Фио+Др.:'||SRC.FIO||' '||SRC.DR as INFO_NEQ
				,'Фио+Др.:'||APP.FIO||' '||APP.DR as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
			ON /*RULE: определяем фрод-правило поиска */
				SRC.PASSPORT = APP.PASSPORT AND (SRC.FIO ^= APP.FIO OR SRC.DR ^= APP.DR)
					AND SRC.PERSON_ID^=APP.PERSON_ID 
					/*AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-121 AND SRC.REQ_DATE*/
					AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.PASSPORT IS NULL AND NOT SRC.PASSPORT='-'
				AND NOT SRC.FIO IS NULL AND NOT APP.FIO IS NULL AND NOT SRC.FIO='-' AND NOT APP.FIO='-'
        --исключаем девичьи фамилии
        /*AND NOT (SUBSTR(SRC.FIO, INSTR(SRC.FIO,' ')-1, 254 )=SUBSTR(APP.FIO, INSTR(APP.FIO,' ')-1, 254 )
                    AND SRC.DR=APP.DR )*/
        --если нужно будет удалить подобных в нечетком сравнении
				/*AND UTL_MATCH.EDIT_DISTANCE(SRC.FIO, APP.FIO)+UTL_MATCH.EDIT_DISTANCE(SRC.DR, APP.DR)>1*/
				/*AND SFF.FN_DIST_LEV(SRC.FIO, APP.FIO)+SFF.FN_DIST_LEV(SRC.DR, APP.DR)>1*/
        AND SFF.FN_DIST_LEV(SRC.FIO||SRC.DR, APP.FIO||APP.DR)>1
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/;
	COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_P1_F0_ERR" (v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО. Паспорт равен. Разл. ФИО в 1 симв"
-- ==	ОПИСАНИЕ:		Совпадает серия и номер паспорта. ФИО или ДР - отличие в 1 символе 
-- ==					(SER+NUM = , ФИО+ДР^= в 1 симв.)
-- ========================================================================
-- ==	СОЗДАНИЕ:		03.11.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	24.11.2015 (ТРАХАЧЕВ В.В.)
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
				--,'Паспорт1_ФиоДр_Ошиб' as TYPE_REL
        ,'Ошибка в ФиоДр - Паспорт равен' as TYPE_REL
				,'Совпадает серия и номер паспорта. ФИО или ДР - отличие в 1 символе' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID)
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,C_R_REL.STATUS_ID as STATUS_ID_REL
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Паспорт:'||SRC.PASSPORT
              /*||' - '||TO_CHAR(SFF.FN_DIST_LEV(SRC.FIO, APP.FIO)+SFF.FN_DIST_LEV(SRC.DR, APP.DR))||'симв.'*/ as INFO_EQ
				,'Фио+Др.:'||SRC.FIO||' '||SRC.DR as INFO_NEQ
				,'Фио+Др.:'||APP.FIO||' '||APP.DR as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
			ON /*RULE: определяем фрод-правило поиска */
				SRC.PASSPORT = APP.PASSPORT AND (SRC.FIO ^= APP.FIO OR SRC.DR ^= APP.DR)
					AND SRC.PERSON_ID^=APP.PERSON_ID 
					/*AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-121 AND SRC.REQ_DATE*/
					AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.PASSPORT IS NULL AND NOT SRC.PASSPORT='-'
				AND NOT SRC.FIO IS NULL AND NOT APP.FIO IS NULL AND NOT SRC.FIO='-' AND NOT APP.FIO='-'
        --исключаем девичьи фамилии
        /*AND NOT (SUBSTR(SRC.FIO, INSTR(SRC.FIO,' ')-1, 254 )=SUBSTR(APP.FIO, INSTR(APP.FIO,' ')-1, 254 )
                    AND SRC.DR=APP.DR )*/
        --если нужно будет удалить подобных в нечетком сравнении
				/*AND UTL_MATCH.EDIT_DISTANCE(SRC.FIO, APP.FIO)+UTL_MATCH.EDIT_DISTANCE(SRC.DR, APP.DR)=1*/
				/*AND SFF.FN_DIST_LEV(SRC.FIO, APP.FIO)+SFF.FN_DIST_LEV(SRC.DR, APP.DR)=1*/
        AND SFF.FN_DIST_LEV(SRC.FIO||SRC.DR, APP.FIO||APP.DR)=1
			) TAB
		WHERE TAB.F_POS=1 
			/*PostCheck: Проверка на родственников*/
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/;
	COMMIT;
END;
  CREATE OR REPLACE TRIGGER "SFF"."FROD_RULE_DEMO_TRG_1" BEFORE INSERT ON SFF.FROD_RULE_DEMO1 
FOR EACH ROW 
BEGIN
  <<COLUMN_SEQUENCES>>
  BEGIN
    IF :NEW.FROD_RULE_ID IS NULL THEN
      SELECT FROD_RULE_DEMO1_SEQ.NEXTVAL INTO :NEW.FROD_RULE_ID FROM DUAL;
    END IF;
  END COLUMN_SEQUENCES;
  
  IF NOT :NEW.REQUEST_ID_REL IS NULL THEN
    --SELECT SFF.FN_STRIP_DUBL_IN_STR(:NEW.REQUEST_ID_REL) INTO :NEW.REQUEST_ID_REL FROM DUAL;
    :NEW.REQUEST_ID_REL := SFF.FN_DEDUBL_STR(:NEW.REQUEST_ID_REL);
  END IF;
END;
ALTER TRIGGER "SFF"."FROD_RULE_DEMO_TRG_1" ENABLE"
"
  CREATE OR REPLACE PROCEDURE "SFF"."PR_SCAN_FROD_FROM_MSG_DEMO" (RID_start in NUMBER, RID_end in NUMBER) 
IS   
CURSOR get_REQUEST
  IS 
		SELECT DISTINCT REQUEST_ID FROM SFF.APPLICATIONS_FROD/*KREDIT.C_REQUEST@DBLINK_PKK*/ sdm 
    WHERE REQUEST_ID BETWEEN RID_start AND RID_end /*and status_id=14 and created_user_id^=1512142
      /*AND NOT EXISTS(SELECT REQUEST_ID FROM KREDIT.C_REQUEST@DBLINK_PKK crid 
                                                    WHERE REQUEST_ID=sdm.REQUEST_ID AND CREATED_GROUP_ID IN(11455))*/
          ORDER BY REQUEST_ID;

	v_gt get_REQUEST%ROWTYPE;
  fraud_flag  NUMBER;
BEGIN
	DBMS_OUTPUT.enable;
	OPEN get_REQUEST;
  
  FETCH get_REQUEST INTO v_gt;	
	LOOP  
    --SFF.FROD_RULES_RUN_ALL(v_gt.REQUEST_ID);
    --тестовое правило
    --SFF.FROD_RULES_VERIFY_H10(v_gt.REQUEST_ID);
    --SFF.FROD_RULES_VERIFY_P03(v_gt.REQUEST_ID);
    --SFF.FROD_RULES_VERIFY_P04(v_gt.REQUEST_ID);
    SFF.FRAUD_RULE_M01(v_gt.REQUEST_ID, fraud_flag);
     SFF.FRAUD_RULE_P1_F0(v_gt.REQUEST_ID, fraud_flag);
      SFF.FRAUD_RULE_P1_F0_ERR(v_gt.REQUEST_ID, fraud_flag);
      
    IF MOD(get_REQUEST%ROWCOUNT,10000)=0 OR get_REQUEST%ROWCOUNT=1 THEN 
      DBMS_OUTPUT.put_line('Cur RID: '||TO_CHAR(get_REQUEST%ROWCOUNT)||'  '||TO_CHAR(v_gt.REQUEST_ID)
                                          ||' - '||TO_CHAR(systimestamp at time zone 'utc'));
    END IF;
		FETCH get_REQUEST INTO v_gt;			
		EXIT WHEN get_REQUEST%NOTFOUND;
	END LOOP; 
  DBMS_OUTPUT.put_line('End RID: '||TO_CHAR(get_REQUEST%ROWCOUNT)||'  '||TO_CHAR(v_gt.REQUEST_ID)
                                          ||' - '||TO_CHAR(systimestamp at time zone 'utc'));
	CLOSE get_REQUEST;
  
  EXCEPTION
      WHEN OTHERS
      THEN NULL;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_PKK_ADDRESS_HISTORY" 
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицы с адресной историей
-- ==	ОПИСАНИЕ:	   
-- ========================================================================
-- ==	СОЗДАНИЕ:		  24.12.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	12.02.2016 (ТРАХАЧЕВ В.В.)
/*PKK_ADDRESS_HISTORY:11.01.16 07:03:56-10.02.16 07:03:53(645058). От 02.03.16 06:30:33 до 02.03.16 10:11:09

*/
-- ========================================================================
AS    
  last_DT_SFF DATE; -- последний MODIFICATION_DATE в SFF
  last_DT_PKK DATE; -- последний MODIFICATION_DATE в ПКК
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  
  start_time := systimestamp;
  
  SELECT MAX(ADDRESS_CREATED)-5/24 INTO last_DT_SFF FROM ODS.PKK_ADDRESS_HISTORY
       WHERE ADDRESS_HISTORY_ID>=(SELECT MAX(ADDRESS_HISTORY_ID)-1000 FROM ODS.PKK_ADDRESS_HISTORY);

  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_ADDRESS_HISTORY tar
	USING (SELECT ADDRESS_ID
          ,OBJECTS_ID
          ,OBJECTS_TYPE --
          ,ADDRESS_AKT
          ,ADDRESS_CREATED
          ,CREATED_SOURCE
          ,CREATED_USER_ID
          ,CREATED_GROUP_ID
          ,CREATED_IPADR
          ,MODIFICATION_DATE
          ,MODIFICATION_SOURCE
          ,MODIFICATION_USER_ID
          ,MODIFICATION_GROUP_ID
          ,MODIFICATION_IPADR
          ,ADDRESS_HISTORY_ID
          ,ADDRESS_COMM FROM ODS.VIEW_PKK_ADDRESS_HISTORY WHERE ADDRESS_CREATED > last_DT_SFF
          UNION 
         SELECT ADDRESS_ID
          ,OBJECTS_ID
          ,OBJECTS_TYPE --
          ,ADDRESS_AKT
          ,ADDRESS_CREATED
          ,CREATED_SOURCE
          ,CREATED_USER_ID
          ,CREATED_GROUP_ID
          ,CREATED_IPADR
          ,MODIFICATION_DATE
          ,MODIFICATION_SOURCE
          ,MODIFICATION_USER_ID
          ,MODIFICATION_GROUP_ID
          ,MODIFICATION_IPADR
          ,ADDRESS_HISTORY_ID
          ,ADDRESS_COMM FROM ODS.VIEW_PKK_ADDRESS_HISTORY WHERE MODIFICATION_DATE > last_DT_SFF
		) src
    ON (src.ADDRESS_HISTORY_ID = tar.ADDRESS_HISTORY_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        tar.ADDRESS_ID=src.ADDRESS_ID
        ,tar.OBJECTS_ID=src.OBJECTS_ID
        --,tar.OBJECTS_TYPE=src.OBJECTS_TYPE
        ,tar.ADDRESS_AKT=src.ADDRESS_AKT
        --,tar.ADDRESS_CREATED=src.ADDRESS_CREATED
        --,tar.CREATED_SOURCE=src.CREATED_SOURCE
        --,tar.CREATED_USER_ID=src.CREATED_USER_ID
        --,tar.CREATED_GROUP_ID=src.CREATED_GROUP_ID
        --,tar.CREATED_IPADR=src.CREATED_IPADR
        ,tar.MODIFICATION_DATE=src.MODIFICATION_DATE
        ,tar.MODIFICATION_SOURCE=src.MODIFICATION_SOURCE
        ,tar.MODIFICATION_USER_ID=src.MODIFICATION_USER_ID
        ,tar.MODIFICATION_GROUP_ID=src.MODIFICATION_GROUP_ID
        ,tar.MODIFICATION_IPADR=src.MODIFICATION_IPADR
        --,tar.ADDRESS_HISTORY_ID=src.ADDRESS_HISTORY_ID
        --,tar.ADDRESS_COMM=src.ADDRESS_COMM
        ,tar.DATE_UPD=SYSDATE
        ,tar.ADDRESS_RANK=1
        ,tar.OBJECTS_RANK=1
      WHERE NOT ( NVL(tar.ADDRESS_ID, -1) = NVL(src.ADDRESS_ID, -1)
        AND NVL(tar.ADDRESS_AKT, -1) = NVL(src.ADDRESS_AKT, -1)
        AND NVL(tar.MODIFICATION_DATE, TO_DATE('01-01-1900', 'dd-mm-yyyy')) = NVL(src.MODIFICATION_DATE, TO_DATE('01-01-1900', 'dd-mm-yyyy'))
        AND NVL(tar.MODIFICATION_USER_ID, -1) = NVL(src.MODIFICATION_USER_ID, -1)
        AND NVL(tar.MODIFICATION_GROUP_ID, -1) = NVL(src.MODIFICATION_GROUP_ID, -1)
        AND NVL(tar.MODIFICATION_IPADR, -1) = NVL(src.MODIFICATION_IPADR, -1)
        )
	WHEN NOT MATCHED THEN 
	--вставляем новое
	INSERT (	tar.ADDRESS_ID
            ,tar.OBJECTS_ID, tar.OBJECTS_TYPE
            ,tar.ADDRESS_AKT, tar.ADDRESS_CREATED
            ,tar.CREATED_SOURCE, tar.CREATED_USER_ID,tar.CREATED_GROUP_ID,tar.CREATED_IPADR
            ,tar.MODIFICATION_DATE, tar.MODIFICATION_SOURCE, tar.MODIFICATION_USER_ID, tar.MODIFICATION_GROUP_ID
            ,tar.MODIFICATION_IPADR, tar.ADDRESS_HISTORY_ID, tar.ADDRESS_COMM
            ,tar.DATE_UPD
            ,tar.ADDRESS_RANK, tar.OBJECTS_RANK)
	VALUES (src.ADDRESS_ID
            ,src.OBJECTS_ID, src.OBJECTS_TYPE
            ,src.ADDRESS_AKT, src.ADDRESS_CREATED
            ,src.CREATED_SOURCE, src.CREATED_USER_ID, src.CREATED_GROUP_ID, src.CREATED_IPADR
            ,src.MODIFICATION_DATE, src.MODIFICATION_SOURCE, src.MODIFICATION_USER_ID, src.MODIFICATION_GROUP_ID
            ,src.MODIFICATION_IPADR, src.ADDRESS_HISTORY_ID, src.ADDRESS_COMM
            ,SYSDATE
            ,1, 1)
	;	
  
  
  cnt_MODIF := SQL%ROWCOUNT;
  SELECT MAX(ADDRESS_CREATED) INTO last_DT_PKK FROM ODS.PKK_ADDRESS_HISTORY
       WHERE ADDRESS_HISTORY_ID>=(SELECT MAX(ADDRESS_HISTORY_ID)-1000 FROM ODS.PKK_ADDRESS_HISTORY);
       
  p_ADD_INFO := 'Период обновления: '||TO_CHAR(last_DT_SFF)||' до '||TO_CHAR(last_DT_PKK);
  ODS.PR$INS_LOG ('PR$UPD_PKK_ADDRESS_HISTORY', start_time, 'ODS.PKK_ADDRESS_HISTORY', 'OK', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
  
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_PKK_ADDRESS_HISTORY', start_time, 'ODS.PKK_ADDRESS_HISTORY', 'ERR', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_PKK_EMAIL" 
-- ========================================================================
-- ==	ПРОЦЕДУРА    Обновление таблицы EMAIL
-- ==	ОПИСАНИЕ:	   История Емайл адресов
-- ========================================================================
-- ==	СОЗДАНИЕ:		  31.12.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	11.02.2016 (ТРАХАЧЕВ В.В.)
/*PKK_EMAIL:20.01.16 23:18:06-11.02.16 04:30:43(13268). От 10.02.16 22:31:39,765712000 до 11.02.16 01:32:27
  PKK_EMAIL:10.02.16 20:30:43-11.02.16 04:30:43(222).   От 10.02.16 22:45:05,482114000 до 11.02.16 01:45:06
  PKK_EMAIL:12.02.16 04:21:27-01.03.16 05:32:00(11910). От 01.03.16 02:35:21,308047000 до 01.03.16 02:36:16 
  PKK_EMAIL:29.02.16 21:32:00-09.03.16 04:15:02(5667). От 09.03.16 01:24:14 до 09.03.16 01:25:27
  
*/
-- ========================================================================
AS    
  last_DT_SFF DATE; -- последний MODIFICATION_DATE в SFF
  last_DT_PKK DATE; -- последний MODIFICATION_DATE в ПКК
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  start_time := systimestamp;

  SELECT MAX(EMAIL_CREATED)-5/24 INTO last_DT_SFF FROM ODS.PKK_EMAIL
      WHERE EMAIL_ID>=(SELECT MAX(EMAIL_ID)-1000 FROM ODS.PKK_EMAIL);
      
  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_EMAIL tar
	USING (SELECT * FROM ODS.VIEW_PKK_EMAIL WHERE EMAIL_CREATED > last_DT_SFF
          UNION 
         SELECT * FROM ODS.VIEW_PKK_EMAIL WHERE MODIFICATION_DATE > last_DT_SFF
		) src
    ON (src.EMAIL_ID = tar.EMAIL_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        --tar.EMAIL_ID=src.EMAIL_ID
        tar.OBJECTS_ID=src.OBJECTS_ID
        /*,tar.OBJECTS_TYPE=src.OBJECTS_TYPE*/
        ,tar.EMAIL_AKT=src.EMAIL_AKT
        /*,tar.EMAIL_CREATED=src.EMAIL_CREATED
        ,tar.CREATED_SOURCE=src.CREATED_SOURCE
        ,tar.CREATED_USER_ID=src.CREATED_USER_ID
        ,tar.CREATED_GROUP_ID=src.CREATED_GROUP_ID
        ,tar.CREATED_IPADR=src.CREATED_IPADR*/
        ,tar.MODIFICATION_DATE=src.MODIFICATION_DATE
        /*,tar.MODIFICATION_SOURCE=src.MODIFICATION_SOURCE
        ,tar.MODIFICATION_USER_ID=src.MODIFICATION_USER_ID
        ,tar.MODIFICATION_GROUP_ID=src.MODIFICATION_GROUP_ID
        ,tar.MODIFICATION_IPADR=src.MODIFICATION_IPADR*/
        ,tar.DATE_UPD=SYSDATE
        ,tar.EMAIL_BCK=src.EMAIL
	WHEN NOT MATCHED THEN 
	--вставляем новое 
	INSERT (	tar.EMAIL_ID
            ,tar.OBJECTS_ID, tar.OBJECTS_TYPE
            ,tar.EMAIL
            ,tar.EMAIL_AKT, tar.EMAIL_CREATED
            ,tar.CREATED_SOURCE, tar.CREATED_USER_ID, tar.CREATED_GROUP_ID, tar.CREATED_IPADR
            ,tar.MODIFICATION_DATE
            ,tar.DATE_UPD
            ,tar.EMAIL_BCK
          )
	VALUES (src.EMAIL_ID
            ,src.OBJECTS_ID, src.OBJECTS_TYPE
            ,src.EMAIL
            ,src.EMAIL_AKT, src.EMAIL_CREATED
            ,src.CREATED_SOURCE, src.CREATED_USER_ID, src.CREATED_GROUP_ID, src.CREATED_IPADR
            ,src.MODIFICATION_DATE
            , SYSDATE
            ,src.EMAIL)
	;	
  
  --информация для вывода при ручном обновлении
  cnt_MODIF := SQL%ROWCOUNT;
  SELECT MAX(EMAIL_CREATED) INTO last_DT_PKK FROM ODS.PKK_EMAIL
       WHERE EMAIL_ID>=(SELECT MAX(EMAIL_ID)-1000 FROM ODS.PKK_EMAIL);
  p_ADD_INFO := 'Период обновления: '||TO_CHAR(last_DT_SFF)||' до '||TO_CHAR(last_DT_PKK);
  ODS.PR$INS_LOG ('PR$UPD_PKK_EMAIL', start_time, 'ODS.PKK_EMAIL', 'OK', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
  
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_PKK_EMAIL', start_time, 'ODS.PKK_EMAIL', 'ERR', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
END;
  CREATE OR REPLACE FUNCTION "SFF"."FN_IS_KK_RID" (inRID IN NUMBER) RETURN NUMBER
-- ========================================================================
-- ==	ФУНКЦИЯ       Проверка признака что заявка на КРЕД. КАРТУ или ОВЕРДРАФТ
-- ==	ОПИСАНИЕ:   
-- ========================================================================
-- ==	СОЗДАНИЕ:		  14.12.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	14.12.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================
IS
 FLAG NUMBER;
BEGIN
  --DBMS_OUTPUT.ENABLE;
  -- для проверки 55000042, 55000155  ,55000279, 55000420   
    SELECT FIRST_VALUE(ret_prod.RETAIL_PRODUCT_GROUPS_ID) 
            OVER (PARTITION BY crc.REQUEST_ID ORDER BY crc.REQUEST_CREDIT_ID DESC) INTO FLAG
          /*LAST_VALUE(ret_prod.RETAIL_PRODUCT_GROUPS_ID)  
              OVER (PARTITION BY crc.REQUEST_ID order by crc.REQUEST_CREDIT_ID rows between current row and unbounded following)     */
                    FROM KREDIT.C_REQUEST_CREDIT@DBLINK_PKK crc
                            LEFT JOIN KREDIT.C_SCHEMS@DBLINK_PKK c_sch ON (crc.SCHEMS_ID = c_sch.SCHEMS_ID)
                            LEFT JOIN KREDIT.RETAIL_PRODUCT@DBLINK_PKK ret_prod 
                              ON (ret_prod.RETAIL_PRODUCT_ID = c_sch.RETAIL_PRODUCT_ID)
                            WHERE crc.REQUEST_ID=inRID /*AND ret_prod.RETAIL_PRODUCT_GROUPS_ID IN(6, 7, 32,39)*/
                                  and rownum=1;

  IF FLAG IN(6, 7 /*, 32,39*/) THEN --елси КК или Овердрафт то вернуть 1
    --DBMS_OUTPUT.PUT_LINE(' Пос кредит '||FLAG_POS);
    RETURN 1;
  ELSE 
    --DBMS_OUTPUT.PUT_LINE(' Не пос кредит '||FLAG_POS);
    RETURN 0;
  END IF;

  /*EXCEPTION
    WHEN OTHERS
     THEN 
        --DBMS_OUTPUT.PUT_LINE('REQUEST_ID ERROR (not exist)')
        RETURN -1;*/
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_PKK_PERSON_INFO" 
-- ========================================================================
-- ==	ПРОЦЕДУРА    Обновление таблицы PERSON_INFO 
-- ==	ОПИСАНИЕ:	   Основные данные о физике (ФИО + ДР)
-- ========================================================================
-- ==	СОЗДАНИЕ:		  18.01.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	11.02.2016 (ТРАХАЧЕВ В.В.)
/*PKK_PERSON_INFO:20.01.16 05:22:37-01.03.16 13:07:37(799633). От 01.03.16 10:07:40 до 01.03.16 12:50:59
  PKK_PERSON_INFO:01.03.16 10:07:37-09.03.16 04:53:37(176868). От 09.03.16 01:56:11 до 09.03.16 02:51:57
  PKK_PERSON_INFO:09.03.16 01:53:37-09.03.16 05:58:20(123). От 09.03.16 02:58:52 до 09.03.16 03:02:39
  PKK_PERSON_INFO:09.03.16 02:58:20-09.03.16 06:07:58(366). От 09.03.16 03:07:58 до 09.03.16 03:10:46

*/
-- ========================================================================
AS    
  last_DT_SFF DATE; -- последний MODIFICATION_DATE в SFF
  last_DT_PKK DATE; -- последний MODIFICATION_DATE в ПКК
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp; 
  
  SELECT MAX(FIO_CREATED)-5/24 INTO last_DT_SFF FROM ODS.PKK_PERSON_INFO
      WHERE FIO_HISTORY_PK>=(SELECT MAX(FIO_HISTORY_PK)-1000 FROM ODS.PKK_PERSON_INFO);

  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_PERSON_INFO tar
	USING (SELECT FIO_ID, OBJECTS_ID
              ,FIO_AKT
              ,FIO_CREATED
              ,FIO_HISTORY_PK
              ,FIO4SEARCH
              ,BIRTH
              ,MODIFICATION_DATE 
            FROM ODS.VIEW_PKK_PERSON_INFO WHERE FIO_CREATED > last_DT_SFF
        UNION 
         SELECT FIO_ID, OBJECTS_ID
              ,FIO_AKT
              ,FIO_CREATED
              ,FIO_HISTORY_PK
              ,FIO4SEARCH
              ,BIRTH
              ,MODIFICATION_DATE  
            FROM ODS.VIEW_PKK_PERSON_INFO WHERE MODIFICATION_DATE > last_DT_SFF
		) src
    ON (src.FIO_HISTORY_PK = tar.FIO_HISTORY_PK )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        --tar.EMAIL_ID=src.EMAIL_ID
        tar.FIO_ID=src.FIO_ID
        ,tar.OBJECTS_ID=src.OBJECTS_ID
        ,tar.FIO_AKT=src.FIO_AKT
        --,tar.FIO_CREATED=src.FIO_CREATED
        --,tar.FIO_HISTORY_PK=src.FIO_HISTORY_PK
        ,tar.FIO4SEARCH=src.FIO4SEARCH
        ,tar.BIRTH=src.BIRTH
        ,tar.MODIFICATION_DATE=src.MODIFICATION_DATE
        ,tar.DATE_UPD=SYSDATE
        ,tar.FIO_RANK=1
        WHERE NOT (NVL(tar.FIO4SEARCH, '-')=NVL(src.FIO4SEARCH, '-') 
              AND NVL(tar.BIRTH, TO_DATE('01-01-1900', 'dd-mm-yyyy'))=NVL(src.BIRTH , TO_DATE('01-01-1900', 'dd-mm-yyyy'))
              AND NVL(tar.FIO_AKT, -1)=NVL(src.FIO_AKT, -1) 
              AND NVL(tar.OBJECTS_ID, -1)=NVL(src.OBJECTS_ID, -1) AND NVL(tar.FIO_ID, -1)=NVL(src.FIO_ID, -1))
	WHEN NOT MATCHED THEN 
	--вставляем новое 
	INSERT ( tar.FIO_ID
            ,tar.OBJECTS_ID
            ,tar.FIO_AKT
            ,tar.FIO_CREATED
            ,tar.FIO_HISTORY_PK
            ,tar.FIO4SEARCH
            ,tar.BIRTH
            ,tar.MODIFICATION_DATE
            ,tar.DATE_UPD
            ,tar.FIO_RANK)
	VALUES (src.FIO_ID
            ,src.OBJECTS_ID
            ,src.FIO_AKT
            ,src.FIO_CREATED
            ,src.FIO_HISTORY_PK
            ,src.FIO4SEARCH
            ,src.BIRTH
            ,src.MODIFICATION_DATE
            ,SYSDATE
            ,1)
	;	
  
  cnt_MODIF := SQL%ROWCOUNT;
  SELECT MAX(FIO_CREATED) INTO last_DT_PKK FROM ODS.PKK_PERSON_INFO
       WHERE FIO_HISTORY_PK>=(SELECT MAX(FIO_HISTORY_PK)-1000 FROM ODS.PKK_PERSON_INFO);
  p_ADD_INFO := 'Период обновления: '||TO_CHAR(last_DT_SFF)||' до '||TO_CHAR(last_DT_PKK);
  ODS.PR$INS_LOG ('PR$UPD_PKK_PERSON_INFO', start_time, 'ODS.PKK_PERSON_INFO', 'OK', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
  
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_PKK_PERSON_INFO', start_time, 'ODS.PKK_PERSON_INFO', 'ERR', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
END;
  CREATE OR REPLACE FUNCTION "SFF"."FN$FRAUD_RULE_M01" (v_request_id IN NUMBER)
RETURN NUMBER
-- ========================================================================
-- ==	ФУНКЦИЯ:      ФРОД-ПРАВИЛО -- Телефон1-Псп0"
-- ==	ОПИСАНИЕ:		  Совпадает личный телефон
-- ==					      , отличается паспорт и хотя бы 1 из параметров: Дата рождения или ФИО, за последние 90 дней. 
-- ==					      (МОБ ТЕЛ =, ПАСП ^= и (ДР^= или ФИО^=) за 90 дней) 
-- ==               Правка 07.09.2015: добавлены фильтры для телефонов
-- ========================================================================
-- ==	СОЗДАНИЕ:		  20.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	16.12.2015 (ЛЯЛИН Н.В.)
-- ======================================================================== 
IS 
  fl_FROD NUMBER := 0 ;
BEGIN 
	SELECT 1 INTO fl_FROD
  FROM dual
  WHERE exists(SELECT SRC.REQUEST_ID

				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				/*,'Телефон1-Псп0' as TYPE_REL
				,'Совпадает личный телефон, отличается паспорт и хотя бы 1 из параметров: Дата рождения или ФИО, за последние 90 дней.' as TYPE_REL_DESC
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS*/
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
				ON /*RULE: определяем фрод-правило поиска */
					SRC.MOBILE = APP.MOBILE
						AND SRC.PASSPORT^=APP.PASSPORT 
						AND (SRC.FIO^=APP.FIO OR SRC.DR^=APP.DR)
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91*1 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.MOBILE IS NULL AND SRC.MOBILE>0
				AND NOT SRC.PASSPORT='-'AND NOT SRC.PASSPORT IS NULL AND NOT APP.PASSPORT='-'AND NOT APP.PASSPORT IS NULL 
        AND NOT SRC.FIO IS NULL AND NOT APP.FIO IS NULL AND NOT SRC.FIO='-' AND NOT APP.FIO='-'
        AND NOT SRC.DR='-' AND NOT SRC.DR IS NULL
        AND NOT MOD(SRC.MOBILE, 1000000)=0 AND NOT TO_CHAR(SRC.MOBILE) LIKE '%999999%'
        --исключаем девичьи фамилии 
        AND NOT (SUBSTR(SRC.FIO, INSTR(SRC.FIO,' '), 254 )=SUBSTR(APP.FIO, INSTR(APP.FIO,' '), 254 ) AND SRC.DR=APP.DR )
        /* исключение однофамильцев */
        AND NOT UTL_MATCH.EDIT_DISTANCE(SUBSTR(SRC.FIO, 1, INSTR(SRC.FIO,' ')-1 ), SUBSTR(APP.FIO, 1, INSTR(APP.FIO,' ')-1 ))<3
        --PostCheck: Модифицированная проверка на родственников
        AND NOT SFF.FN_IS_FAMILY_REL(SRC.PERSON_ID, APP.PERSON_ID)=1
        AND NOT SFF.FN_IS_FAMILY_CONT(SRC.PERSON_ID, APP.PERSON_ID)=1 
        AND NOT SFF.FN_IS_FAMILY_CONT(SRC.PERSON_ID, APP.PERSON_ID)=1
        
        AND EXISTS(SELECT * FROM CPD.PHONES@DBLINK_PKK 
                          WHERE (OBJECTS_ID=SRC.PERSON_ID OR OBJECTS_ID=APP.PERSON_ID ) AND OBJECTS_TYPE=2 
                              AND PHONE=TO_CHAR(SRC.MOBILE)
                              AND PHONES_COMM = 'Телефон из РБО: Мобильный')
                    );

  RETURN fl_FROD;

	EXCEPTION
    WHEN OTHERS
      THEN RETURN -1;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_PKK_C_REQUEST_SNA" 
-- ========================================================================
-- ==	ПРОЦЕДУРА    Обновление таблицы PKK_C_REQUEST_SAS
-- ==	ОПИСАНИЕ:	   Основные данные по заявке (последние)
-- ========================================================================
-- ==	СОЗДАНИЕ:		  21.01.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	11.02.2016 (ТРАХАЧЕВ В.В.)
/*PKK_C_REQUEST_SNA:29.01.16 21:22:08-02.03.16 13:21:55(1491284). От 02.03.16 10:21:59 до 02.03.16 11:48:54
  
*/
-- ========================================================================
AS    
  last_DT_SFF DATE; -- последний MODIFICATION_DATE в SFF
  last_DT_PKK DATE; -- последний MODIFICATION_DATE в ПКК
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  
  start_time := systimestamp;
  
  SELECT MAX(CREATED_DATE)-5/24 INTO last_DT_SFF FROM ODS.PKK_C_REQUEST_SNA
      WHERE REQUEST_ID>=(SELECT MAX(REQUEST_ID)-1000 FROM ODS.PKK_C_REQUEST_SNA);
      
  --ОБНОВЛЕНИЕ
MERGE INTO ODS.PKK_C_REQUEST_SNA tar
	USING (SELECT * FROM ODS.VIEW_PKK_C_REQUEST_SNA WHERE CREATED_DATE > last_DT_SFF
          UNION 
         SELECT * FROM ODS.VIEW_PKK_C_REQUEST_SNA WHERE MODIFICATION_DATE_REQUEST > last_DT_SFF 
  
  /*SELECT 
          cr.REQUEST_ID
          ,cr.PARENT_ID
          ,cr.TYPE_REQUEST_ID
          ,cr.STATUS_ID
          ,cr.OBJECTS_ID
          ,cr.OBJECTS_TYPE
          ,cr.CREATED_USER_ID
          ,cr.CREATED_GROUP_ID
          ,cr.CREATED_DATE
          ,cr.MODIFICATION_DATE AS MODIFICATION_DATE_REQUEST
          ,cr.REQUEST_UNIQUE_CODE
          ,cr.SCORE_TREE_ROUTE_ID
          ,cr.REQUEST_REACT_ID_MT
          ,cr.REQUEST_REACT_ID_CK
          ,cr.REQUEST_REACT_ID_LAST
          ,cr.REQUEST_CREDIT_ID_LAST
          ,cr.REQUEST_INFO_ID_LAST
          ,cri.MODIFICATION_DATE AS MODIFICATION_DATE_INFO
          ,cri.DECLARANT_TYPE
          ,cri.FIO_ID
          ,cri.SEX_ID
          ,cri.BDATE
          ,cri.BADR_ID
          ,cri.MDOC_ID
          ,cri.MADR_ID
          ,cri.LADR_ID
          ,cri.DADR_ID
          ,crr.REACT_DATE
          --,crr.REACT_COMMENT
          ,SUBSTR(crr.REACT_COMMENT, 1, 4000) as REACT_COMMENT
          ,crr.REQUEST_START_PATH
          ,crr.CANCEL_CODE
          ,crr.REQUEST_OLD_STATUS_ID
          ,crr.REACT_USER_ID
          ,crc.MODIFICATION_DATE as MODIFICATION_DATE_CREDIT
          ,crc.SCHEMS_ID
          ,crc.TYPE_CREDIT_ID
          ,crc.ORG_PARTNER_ID
          ,crc.ORG_PARTNER_CODE
          ,crc.CURRENCY_ID
          ,crc.PERIOD
          ,crc.PERCENT
          ,crc.PATH_CODE
          ,crc.SUMMA
          ,crc.SUMMA_DECL
          ,crc.SUMMA_ANN
          ,crc.SUMMA_FULL
          ,crc.RETAIL_PRODUCT_GROUPS_ID#NEED as RETAIL_PRODUCT_GROUPS_ID
          --,cci.BASE_DOLG
          --,cci.SUMMA_BASE_DOLG
          --,cci.CONTRACT_STATUS_CODE
          ,cc.CREDIT_JUR_CONTRACT
          ,cc.CREDIT_ID
          ,cc.DAY_PAY
        FROM KREDIT.C_REQUEST@DBLINK_PKK cr
        LEFT JOIN KREDIT.C_REQUEST_INFO@DBLINK_PKK cri
          ON cr.REQUEST_ID=cri.REQUEST_ID AND NVL(cr.REQUEST_INFO_ID_LAST, -1)=cri.REQUEST_INFO_ID
        LEFT JOIN KREDIT.C_REQUEST_REACT@DBLINK_PKK crr
          ON cr.REQUEST_ID=crr.REQUEST_ID AND NVL(cr.REQUEST_REACT_ID_LAST, -1)=crr.REQUEST_REACT_ID
        LEFT JOIN KREDIT.C_REQUEST_CREDIT@DBLINK_PKK crc
          ON cr.REQUEST_ID=crc.REQUEST_ID AND NVL(cr.REQUEST_CREDIT_ID_LAST, -1)=crc.REQUEST_CREDIT_ID

        --LEFT OUTER JOIN KREDIT.C_CREDIT_INFO@DBLINK_PKK cci ON cr.REQUEST_ID=cci.REQUEST_ID
        LEFT JOIN KREDIT.C_CREDIT@DBLINK_PKK cc
          ON cr.REQUEST_ID=cc.REQUEST_ID
  WHERE 
      --вариант с обновлением по частям
      cr.CREATED_DATE >= last_DT_SFF */
		) src
    ON (src.REQUEST_ID = tar.REQUEST_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        --tar.REQUEST_ID=tar.REQUEST_ID
        --tar.PARENT_ID=src.PARENT_ID
        tar.TYPE_REQUEST_ID=src.TYPE_REQUEST_ID
        ,tar.STATUS_ID=src.STATUS_ID
        ,tar.OBJECTS_ID=src.OBJECTS_ID
        ,tar.OBJECTS_TYPE=src.OBJECTS_TYPE
        --,tar.CREATED_USER_ID=src.CREATED_USER_ID
        --,tar.CREATED_GROUP_ID=src.CREATED_GROUP_ID
        --,tar.CREATED_DATE=src.CREATED_DATE
        ,tar.MODIFICATION_DATE_REQUEST=src.MODIFICATION_DATE_REQUEST
        --,tar.REQUEST_UNIQUE_CODE=src.REQUEST_UNIQUE_CODE
        ,tar.SCORE_TREE_ROUTE_ID=src.SCORE_TREE_ROUTE_ID
        ,tar.REQUEST_REACT_ID_MT=src.REQUEST_REACT_ID_MT
        ,tar.REQUEST_REACT_ID_CK=src.REQUEST_REACT_ID_CK
        ,tar.REQUEST_REACT_ID_LAST=src.REQUEST_REACT_ID_LAST
        ,tar.REQUEST_CREDIT_ID_LAST=src.REQUEST_CREDIT_ID_LAST
        ,tar.REQUEST_INFO_ID_LAST=src.REQUEST_INFO_ID_LAST
        ,tar.MODIFICATION_DATE_INFO=src.MODIFICATION_DATE_INFO
        ,tar.DECLARANT_TYPE=src.DECLARANT_TYPE
        ,tar.FIO_ID=src.FIO_ID
        ,tar.SEX_ID=src.SEX_ID
        ,tar.BDATE=src.BDATE
        ,tar.BADR_ID=src.BADR_ID
        ,tar.MDOC_ID=src.MDOC_ID
        ,tar.MADR_ID=src.MADR_ID
        ,tar.LADR_ID=src.LADR_ID
        ,tar.DADR_ID=src.DADR_ID
        ,tar.REACT_DATE=src.REACT_DATE
        ,tar.REACT_COMMENT=src.REACT_COMMENT
        ,tar.REQUEST_START_PATH=src.REQUEST_START_PATH
        ,tar.CANCEL_CODE=src.CANCEL_CODE
        ,tar.REQUEST_OLD_STATUS_ID=src.REQUEST_OLD_STATUS_ID
        ,tar.REACT_USER_ID=src.REACT_USER_ID
        ,tar.MODIFICATION_DATE_CREDIT=src.MODIFICATION_DATE_CREDIT
        ,tar.SCHEMS_ID=src.SCHEMS_ID
        ,tar.TYPE_CREDIT_ID=src.TYPE_CREDIT_ID
        ,tar.ORG_PARTNER_ID=src.ORG_PARTNER_ID
        ,tar.ORG_PARTNER_CODE=src.ORG_PARTNER_CODE
        --,tar.CURRENCY_ID=src.CURRENCY_ID
        ,tar.PERIOD=src.PERIOD
        ,tar.PERCENT=src.PERCENT
        ,tar.PATH_CODE=src.PATH_CODE
        ,tar.SUMMA=src.SUMMA
        ,tar.SUMMA_DECL=src.SUMMA_DECL
        ,tar.SUMMA_ANN=src.SUMMA_ANN
        ,tar.SUMMA_FULL=src.SUMMA_FULL
        ,tar.RETAIL_PRODUCT_GROUPS_ID=src.RETAIL_PRODUCT_GROUPS_ID
        ,tar.CREDIT_JUR_CONTRACT=src.CREDIT_JUR_CONTRACT
        ,tar.CREDIT_ID=src.CREDIT_ID
        ,tar.DAY_PAY=src.DAY_PAY
        ,tar.DATE_UPD=SYSDATE
	WHEN NOT MATCHED THEN 
	--вставляем новое 
	INSERT ( tar.REQUEST_ID
          ,tar.PARENT_ID
          ,tar.TYPE_REQUEST_ID,tar.STATUS_ID
          ,tar.OBJECTS_ID,tar.OBJECTS_TYPE
          ,tar.CREATED_USER_ID,tar.CREATED_GROUP_ID
          ,tar.CREATED_DATE,tar.MODIFICATION_DATE_REQUEST
          ,tar.REQUEST_UNIQUE_CODE
          ,tar.SCORE_TREE_ROUTE_ID
          ,tar.REQUEST_REACT_ID_MT,tar.REQUEST_REACT_ID_CK,tar.REQUEST_REACT_ID_LAST
          ,tar.REQUEST_CREDIT_ID_LAST,tar.REQUEST_INFO_ID_LAST,tar.MODIFICATION_DATE_INFO
          ,tar.DECLARANT_TYPE
          ,tar.FIO_ID,tar.SEX_ID,tar.BDATE
          ,tar.BADR_ID,tar.MDOC_ID,tar.MADR_ID,tar.LADR_ID,tar.DADR_ID
          ,tar.REACT_DATE
          ,tar.REACT_COMMENT
          ,tar.REQUEST_START_PATH
          ,tar.CANCEL_CODE
          ,tar.REQUEST_OLD_STATUS_ID
          ,tar.REACT_USER_ID
          ,tar.MODIFICATION_DATE_CREDIT
          ,tar.SCHEMS_ID,tar.TYPE_CREDIT_ID
          ,tar.ORG_PARTNER_ID,tar.ORG_PARTNER_CODE
          ,tar.CURRENCY_ID,tar.PERIOD,tar.PERCENT
          ,tar.PATH_CODE
          ,tar.SUMMA,tar.SUMMA_DECL,tar.SUMMA_ANN,tar.SUMMA_FULL
          ,tar.RETAIL_PRODUCT_GROUPS_ID
          ,tar.CREDIT_JUR_CONTRACT,tar.CREDIT_ID,tar.DAY_PAY
          ,tar.DATE_UPD
          )
	VALUES (src.REQUEST_ID
          ,src.PARENT_ID
          ,src.TYPE_REQUEST_ID,src.STATUS_ID
          ,src.OBJECTS_ID,src.OBJECTS_TYPE
          ,src.CREATED_USER_ID,src.CREATED_GROUP_ID
          ,src.CREATED_DATE,src.MODIFICATION_DATE_REQUEST
          ,src.REQUEST_UNIQUE_CODE
          ,src.SCORE_TREE_ROUTE_ID
          ,src.REQUEST_REACT_ID_MT,src.REQUEST_REACT_ID_CK,src.REQUEST_REACT_ID_LAST
          ,src.REQUEST_CREDIT_ID_LAST,src.REQUEST_INFO_ID_LAST,src.MODIFICATION_DATE_INFO
          ,src.DECLARANT_TYPE
          ,src.FIO_ID,src.SEX_ID,src.BDATE
          ,src.BADR_ID,src.MDOC_ID,src.MADR_ID,src.LADR_ID,src.DADR_ID
          ,src.REACT_DATE
          ,src.REACT_COMMENT
          ,src.REQUEST_START_PATH
          ,src.CANCEL_CODE
          ,src.REQUEST_OLD_STATUS_ID
          ,src.REACT_USER_ID
          ,src.MODIFICATION_DATE_CREDIT
          ,src.SCHEMS_ID,src.TYPE_CREDIT_ID
          ,src.ORG_PARTNER_ID,src.ORG_PARTNER_CODE
          ,src.CURRENCY_ID,src.PERIOD,src.PERCENT
          ,src.PATH_CODE
          ,src.SUMMA,src.SUMMA_DECL,src.SUMMA_ANN,src.SUMMA_FULL
          ,src.RETAIL_PRODUCT_GROUPS_ID
          ,src.CREDIT_JUR_CONTRACT,src.CREDIT_ID,src.DAY_PAY
          ,SYSDATE)
	;	
  
  cnt_MODIF := SQL%ROWCOUNT;
  SELECT MAX(CREATED_DATE) INTO last_DT_PKK FROM ODS.PKK_C_REQUEST_SNA
      WHERE REQUEST_ID>=(SELECT MAX(REQUEST_ID)-1000 FROM ODS.PKK_C_REQUEST_SNA);
  
  p_ADD_INFO := 'Период обновления: '||TO_CHAR(last_DT_SFF)||' до '||TO_CHAR(last_DT_PKK);
  ODS.PR$INS_LOG ('PR$UPD_PKK_C_REQUEST_SNA', start_time, 'ODS.PKK_C_REQUEST_SNA', 'OK', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);

  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_PKK_C_REQUEST_SNA', start_time, 'ODS.PKK_C_REQUEST_SNA', 'ERR', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FRAUD_RULE_M01" (v_request_id IN NUMBER, fraud_flag OUT NUMBER)
  IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО -- Телефон1-Псп0"
-- ==	ОПИСАНИЕ:		Совпадает личный телефон
-- ==					, отличается паспорт и хотя бы 1 из параметров: Дата рождения или ФИО, за последние 90 дней. 
-- ==					(МОБ ТЕЛ =, ПАСП ^= и (ДР^= или ФИО^=) за 90 дней) 
-- ==         Правка 07.09.2015: добавлены фильтры для телефонов
-- ========================================================================
-- ==	СОЗДАНИЕ:		02.02.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	02.02.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================

  CURSOR get_fraud(in_RID NUMBER) IS
    SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,NULL AS STATUS_ID
				,NULL AS SCORE_TREE_ROUTE_ID
				,NULL AS CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				,'Телефон1-Псп0' as TYPE_REL
				,'Совпадает личный телефон, отличается паспорт и хотя бы 1 из параметров: Дата рождения или ФИО, за последние 90 дней.' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,NULL as STATUS_ID_REL
				--,C_R_REL.SCORE_TREE_ROUTE_ID as SCORE_TREE_ROUTE_ID_REL
				--,C_R_REL.CREATED_GROUP_ID as CREATED_GROUP_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Тел.моб:'||TO_CHAR(SRC.MOBILE) as INFO_EQ
				,'Фио+Др.:'||SRC.FIO||' '||SRC.DR as INFO_NEQ
				,'Фио+Др.:'||APP.FIO||' '||APP.DR as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
				ON --RULE: определяем фрод-правило поиска 
					SRC.MOBILE = APP.MOBILE
						AND SRC.PASSPORT^=APP.PASSPORT 
						AND (SRC.FIO^=APP.FIO OR SRC.DR^=APP.DR)
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-91*1 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			/*LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся физиков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID*/
			WHERE SRC.REQUEST_ID=in_RID
				--EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)
				AND NOT SRC.MOBILE IS NULL AND SRC.MOBILE>0
				AND NOT SRC.PASSPORT='-'AND NOT SRC.PASSPORT IS NULL AND NOT APP.PASSPORT='-'AND NOT APP.PASSPORT IS NULL 
        AND NOT SRC.FIO IS NULL AND NOT APP.FIO IS NULL AND NOT SRC.FIO='-' AND NOT APP.FIO='-'
        AND NOT SRC.DR='-' AND NOT SRC.DR IS NULL
        AND NOT MOD(SRC.MOBILE, 1000000)=0 AND NOT TO_CHAR(SRC.MOBILE) LIKE '%999999%'
				--AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1
        --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
      --исключаем девичьи фамилии
      AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL )
      -- исключение однофамильцев 
			AND NOT UTL_MATCH.EDIT_DISTANCE(SUBSTR(FIO, 1, INSTR(FIO,' ')-1 ), SUBSTR(FIO_REL, 1, INSTR(FIO_REL,' ')-1 ))<3
			--PostCheck: Модифицированная проверка на родственников
      AND NOT SFF.FN_IS_FAMILY_REL(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1 
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID_REL, TAB.PERSON_ID)=1
      --TYPE_REL IN('Телефон1-Псп0' , 'Яма_5' , 'Яма_6')
      AND EXISTS(SELECT * FROM CPD.PHONES@DBLINK_PKK 
                        WHERE (OBJECTS_ID=TAB.PERSON_ID OR OBJECTS_ID=TAB.PERSON_ID_REL ) AND OBJECTS_TYPE=2 
                            AND PHONE=SUBSTR(TAB.INFO_EQ, 9, 10)
                            AND PHONES_COMM = 'Телефон из РБО: Мобильный');

  in_fraud get_fraud%ROWTYPE;
  flag_exists_rid NUMBER;
BEGIN   
  DBMS_OUTPUT.ENABLE;
  SELECT COUNT(*) INTO flag_exists_rid FROM dual 
    WHERE EXISTS(SELECT REQUEST_ID FROM SFF.APPLICATIONS_FROD WHERE REQUEST_ID=v_request_id);
  IF flag_exists_rid=0 THEN 
    fraud_flag := 9;
    DBMS_OUTPUT.PUT_LINE('fraud = '||fraud_flag);
  END IF;
  
  OPEN get_fraud(v_request_id);
	LOOP 
    FETCH get_fraud INTO in_fraud;
		EXIT WHEN get_fraud%NOTFOUND;

    IF (fraud_flag IS NULL) THEN 
      --фрод найден, возвращаем 1
      fraud_flag := 1;   
    END IF; 
    
    INSERT INTO SFF.FROD_RULE_DEMO1(FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
               FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
               TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
               REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
               FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
               INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
       VALUES(
       in_fraud.FROD_RULE_DATE, in_fraud.REQUEST_ID, in_fraud.PERSON_ID, in_fraud.REQ_DATE,
               in_fraud.FIO, in_fraud.DR, in_fraud.STATUS_ID, in_fraud.SCORE_TREE_ROUTE_ID, in_fraud.CREATED_GROUP_ID,
               in_fraud.TYPE_REL, in_fraud.TYPE_REL_DESC, in_fraud.DAY_BETWEEN,
               in_fraud.REQUEST_ID_REL, in_fraud.PERSON_ID_REL, in_fraud.REQ_DATE_REL,
               in_fraud.FIO_REL, in_fraud.DR_REL, in_fraud.STATUS_ID_REL, in_fraud.SCORE_TREE_ROUTE_ID_REL, in_fraud.CREATED_GROUP_ID_REL,
               in_fraud.INFO_EQ, in_fraud.INFO_NEQ, in_fraud.INFO_NEQ_REL, in_fraud.F_POS);
  END LOOP;
  CLOSE get_fraud;
  
  IF (fraud_flag IS NULL) THEN 
    fraud_flag := 0;
  END IF;
  
  EXCEPTION
    WHEN OTHERS
    THEN fraud_flag := -1;
  END;
  CREATE OR REPLACE FUNCTION "SFF"."FN$FRAUD_RULE_P1_F0" (v_request_id IN NUMBER)
RETURN NUMBER
-- ========================================================================
-- ==	ФУНКЦИЯ:      ФРОД-ПРАВИЛО. Разные ФиоДр - Паспорт равен. Разл_ФИО больше 1"
-- ==	ОПИСАНИЕ:		  Совпадает серия и номер паспорта. ФИО или ДР - другие (различие больше чем в 1 символе) 
-- ==					      (SER+NUM = , ФИО+ДР^= в > чем 1 симв.)
-- ========================================================================
-- ==	СОЗДАНИЕ:		  02.02.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	02.02.2016 (ТРАХАЧЕВ В.В.)
-- ======================================================================== 
IS 
  fl_FROD NUMBER ;
BEGIN 
  fl_FROD := 0;
	SELECT COUNT(*) INTO fl_FROD
  FROM dual
  WHERE exists(SELECT SRC.REQUEST_ID
				/* ,SRC.PERSON_ID */
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
        /*,'Разные ФиоДр - Паспорт равен' as TYPE_REL
				,'Совпадает серия и номер паспорта. ФИО или ДР - другие (различие больше чем в 1 символе)' as TYPE_REL_DESC*/
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
			ON 
        /*RULE: определяем фрод-правило поиска */
				SRC.PASSPORT = APP.PASSPORT
			WHERE SRC.REQUEST_ID=v_request_id
        AND (SRC.FIO ^= APP.FIO OR SRC.DR ^= APP.DR)
				AND SRC.PERSON_ID^=APP.PERSON_ID 
				/*AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-121 AND SRC.REQ_DATE*/
				AND APP.REQUEST_ID<SRC.REQUEST_ID
        
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.PASSPORT IS NULL AND NOT SRC.PASSPORT='-'
				AND NOT SRC.FIO IS NULL AND NOT APP.FIO IS NULL AND NOT SRC.FIO='-' AND NOT APP.FIO='-'
        --исключаем девичьи фамилии
        /*AND NOT (SUBSTR(SRC.FIO, INSTR(SRC.FIO,' ')-1, 254 )=SUBSTR(APP.FIO, INSTR(APP.FIO,' ')-1, 254 )
                    AND SRC.DR=APP.DR )*/
        --если нужно будет удалить подобных в нечетком сравнении
				/*AND UTL_MATCH.EDIT_DISTANCE(SRC.FIO, APP.FIO)+UTL_MATCH.EDIT_DISTANCE(SRC.DR, APP.DR)>1*/
				/*AND SFF.FN_DIST_LEV(SRC.FIO, APP.FIO)+SFF.FN_DIST_LEV(SRC.DR, APP.DR)>1*/
        AND SFF.FN_DIST_LEV(SRC.FIO||SRC.DR, APP.FIO||APP.DR)>1
			);
  RETURN NVL(fl_FROD, 0);
  
	EXCEPTION
    WHEN OTHERS
      THEN RETURN -1;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_AFMG01" (v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 07. Адрес1-Телефон0 (один клиент)"
-- ==	ОПИСАНИЕ:		совпадает адрес места проживания
-- ==					, Совпадает адрес места проживания, не совпадает номер мобильного телефона. Один и тот же клиент сменил номер. 
-- ==         Повторное обращение в интервале 14 дней
-- ==					(АДРЕС ПРОЖ =, МОБ ТЕЛ^=, < 14 дней, Objects_Id = ) 
-- ========================================================================
-- ==	СОЗДАНИЕ:		26.01.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	26.01.2016 (ТРАХАЧЕВ В.В.)
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
				,'Адрес1-Телефон0 (один клиент)' as TYPE_REL
				,'Совпадает адрес места проживания, не совпадает номер мобильного телефона. Один и тот же клиент сменил номер. Повторное обращение в интервале 14 дней' as TYPE_REL_DESC
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
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-13 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.LA_REGION='-'AND NOT SRC.LA_REGION IS NULL AND NOT SRC.LA_APARTMENT='-' AND NOT SRC.LA_APARTMENT IS NULL
				AND NOT SRC.MOBILE IS NULL AND NOT APP.MOBILE IS NULL AND SRC.MOBILE>0 AND APP.MOBILE>0
        AND NOT MOD(SRC.MOBILE, 1000000)=0 AND NOT MOD(APP.MOBILE, 1000000)=0
        AND NOT TO_CHAR(SRC.MOBILE) LIKE '%999999%' AND NOT TO_CHAR(APP.MOBILE) LIKE '%999999%'
				AND SRC.PERSON_ID=APP.PERSON_ID
        --проверка адреса на в/ч
        AND NOT EXISTS(SELECT * FROM SFF.APPLICATIONS WHERE REQUEST_ID=SRC.REQUEST_ID
                        AND BA_REGION=SRC.LA_REGION
                        AND BA_DISTRICT=SRC.LA_DISTRICT
                        AND BA_CITY=SRC.LA_CITY
                        AND BA_STREET=SRC.LA_STREET
                        AND BA_HOUSE=SRC.LA_HOUSE
                        AND NVL(BA_BUILDING,'-')=NVL(SRC.LA_BUILDING,'-')
                        AND NVL(BA_APARTMENT,'-')=NVL(SRC.LA_APARTMENT,'-')
                        AND (WORK_ORG_NAME LIKE '%В/Ч%' OR WORK_ORG_NAME LIKE 'ВЧ%') )
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO)+utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
      --исключаем девичьи фамилии
      AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL 
                /*AND SRC.MOBILE=APP.MOBILE*/)
      /* исключение однофамильцев */
			AND NOT UTL_MATCH.EDIT_DISTANCE(SUBSTR(FIO, 1, INSTR(FIO,' ')-1 ), SUBSTR(FIO_REL, 1, INSTR(FIO_REL,' ')-1 ))<3
			--PostCheck: Проверка на родственников
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/
			--PostCheck: Модифицированная проверка на родственников
      AND NOT SFF.FN_IS_FAMILY_REL(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1 
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID_REL, TAB.PERSON_ID)=1;
	COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FROD_RULES_VERIFY_AFMG02" (v_request_id IN NUMBER) IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО 07. Адрес1-Телефон0 (др клиент<14дн)"
-- ==	ОПИСАНИЕ:		Совпадает адрес места проживания, не совпадает номер мобильного телефона. 
-- ==             Разные клиенты не родственники. < 14дн.
-- ==					(АДРЕС ПРОЖ =, МОБ ТЕЛ^=, < 14 дней, Objects_Id ^=, не родственники) 
-- ========================================================================
-- ==	СОЗДАНИЕ:		26.01.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	26.01.2016 (ТРАХАЧЕВ В.В.)
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
				,'Адрес1-Телефон0 (др клиент<14дн)' as TYPE_REL
				,'Совпадает адрес места проживания, не совпадает номер мобильного телефона. Разные клиенты не родственники.' as TYPE_REL_DESC
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
						AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-13 AND SRC.REQ_DATE
						AND APP.REQUEST_ID<SRC.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R -- доп. информация для целевого физика
				ON SRC.REQUEST_ID=C_R.REQUEST_ID
			LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK C_R_REL -- доп. информация для привязавшихся фзииков
				ON APP.REQUEST_ID=C_R_REL.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.LA_REGION='-'AND NOT SRC.LA_REGION IS NULL AND NOT SRC.LA_APARTMENT='-' AND NOT SRC.LA_APARTMENT IS NULL
				AND NOT SRC.MOBILE IS NULL AND NOT APP.MOBILE IS NULL AND SRC.MOBILE>0 AND APP.MOBILE>0
        AND NOT MOD(SRC.MOBILE, 1000000)=0 AND NOT MOD(APP.MOBILE, 1000000)=0
        AND NOT TO_CHAR(SRC.MOBILE) LIKE '%999999%' AND NOT TO_CHAR(APP.MOBILE) LIKE '%999999%'
				AND NOT SRC.PERSON_ID=APP.PERSON_ID
        --проверка адреса на в/ч
        AND NOT EXISTS(SELECT * FROM SFF.APPLICATIONS WHERE REQUEST_ID=SRC.REQUEST_ID
                        AND BA_REGION=SRC.LA_REGION
                        AND BA_DISTRICT=SRC.LA_DISTRICT
                        AND BA_CITY=SRC.LA_CITY
                        AND BA_STREET=SRC.LA_STREET
                        AND BA_HOUSE=SRC.LA_HOUSE
                        AND NVL(BA_BUILDING,'-')=NVL(SRC.LA_BUILDING,'-')
                        AND NVL(BA_APARTMENT,'-')=NVL(SRC.LA_APARTMENT,'-')
                        AND (WORK_ORG_NAME LIKE '%В/Ч%' OR WORK_ORG_NAME LIKE 'ВЧ%') )
				/*AND utl_match.edit_distance(SRC.FIO, APP.FIO) + utl_match.edit_distance(SRC.DR, APP.DR)>1*/ --если нужно будет удалить подобных
			) TAB
		WHERE TAB.F_POS=1 
      --исключаем девичьи фамилии
      AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL 
                /*AND SRC.MOBILE=APP.MOBILE*/)
      /* исключение однофамильцев */
			AND NOT UTL_MATCH.EDIT_DISTANCE(SUBSTR(FIO, 1, INSTR(FIO,' ')-1 ), SUBSTR(FIO_REL, 1, INSTR(FIO_REL,' ')-1 ))<3
			--PostCheck: Проверка на родственников
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/
			--PostCheck: Модифицированная проверка на родственников
      AND NOT SFF.FN_IS_FAMILY_REL(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID, TAB.PERSON_ID_REL)=1 
      AND NOT SFF.FN_IS_FAMILY_CONT(TAB.PERSON_ID_REL, TAB.PERSON_ID)=1;
	COMMIT;
END;
  CREATE OR REPLACE FUNCTION "SFF"."FN$FRAUD_RULE_P1_F0_ERR" (v_request_id IN NUMBER)
RETURN NUMBER
-- ========================================================================
-- ==	ФУНКЦИЯ:      ФРОД-ПРАВИЛО. Паспорт равен. Разл. ФИО в 1 симв
-- ==	ОПИСАНИЕ:		  Совпадает серия и номер паспорта. ФИО или ДР - отличие в 1 символе 
-- ==					      (SER+NUM = , ФИО+ДР^= в 1 симв.)
-- ========================================================================
-- ==	СОЗДАНИЕ:		  02.02.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	02.02.2016 (ТРАХАЧЕВ В.В.)
-- ======================================================================== 
IS 
  fl_FROD NUMBER := 0 ;
BEGIN 
	SELECT COUNT(*) INTO fl_FROD
  FROM dual
  WHERE exists(SELECT SRC.REQUEST_ID
        --,'Ошибка в ФиоДр - Паспорт равен' as TYPE_REL
				--,'Совпадает серия и номер паспорта. ФИО или ДР - отличие в 1 символе' as TYPE_REL_DESC
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
			ON /*RULE: определяем фрод-правило поиска */
				SRC.PASSPORT = APP.PASSPORT AND (SRC.FIO ^= APP.FIO OR SRC.DR ^= APP.DR)
					AND SRC.PERSON_ID^=APP.PERSON_ID 
					/*AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-121 AND SRC.REQ_DATE*/
					AND APP.REQUEST_ID<SRC.REQUEST_ID
			WHERE SRC.REQUEST_ID=v_request_id
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.PASSPORT IS NULL AND NOT SRC.PASSPORT='-'
				AND NOT SRC.FIO IS NULL AND NOT APP.FIO IS NULL AND NOT SRC.FIO='-' AND NOT APP.FIO='-'
        --исключаем девичьи фамилии
        /*AND NOT (SUBSTR(SRC.FIO, INSTR(SRC.FIO,' ')-1, 254 )=SUBSTR(APP.FIO, INSTR(APP.FIO,' ')-1, 254 )
                    AND SRC.DR=APP.DR )*/
        --если нужно будет удалить подобных в нечетком сравнении
        AND SFF.FN_DIST_LEV(SRC.FIO||SRC.DR, APP.FIO||APP.DR)=1
			)
			/*PostCheck: Проверка на родственников*/
			/*AND NOT EXISTS(SELECT OBJECTS_ID FROM CPD.FAMILY@DBLINK_PKK fam
									WHERE fam.OBJECTS_ID=TAB.PERSON_ID AND fam.OB_ID=PERSON_ID_REL
										AND fam.FAMILY_AKT=1 AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2)*/;

  RETURN NVL(fl_FROD, 0);
  
	EXCEPTION
    WHEN OTHERS
      THEN RETURN -1;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_PKK_WORKS_INFO" 
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицы с историей работы
-- ==	ОПИСАНИЕ:	   
-- ========================================================================
-- ==	СОЗДАНИЕ:		  08.02.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	11.02.2016 (ТРАХАЧЕВ В.В.)
/*PKK_WORKS_INFO:04.02.16 16:09:27 - 14.02.16 16:09:27. От 08.02.16 00:56:25,939728000 UTC до 08.02.16 01:04:33,759377000 UTC
  PKK_WORKS_INFO:07.02.16 06:56:11 - 17.02.16 06:56:11. От 08.02.16 01:05:40,315728000 UTC до 08.02.16 01:05:41,670367000 UTC
  PKK_WORKS_INFO:07.02.16 07:15:17 - 17.02.16 07:15:17(2172). От 08.02.16 01:29:32,255336000 UTC до 08.02.16 01:29:37,050470000 UTC
  PKK_WORKS_INFO:06.02.16 07:29:48 - 16.02.16 07:29:48(7739). От 08.02.16 01:30:19,973645000 UTC до 08.02.16 01:30:22,523433000 UTC
  --далее с использованием MODIFICATION_DATE
  PKK_WORKS_INFO:07.02.16 07:46:20 - 17.02.16 07:46:20(2329). От 08.02.16 01:55:55 UTC до 08.02.16 02:08:23 UTC
  PKK_WORKS_INFO:04.02.16 07:55:28(22864).                  От 08.02.16 02:14:07 UTC до 08.02.16 02:23:44 UTC
  PKK_WORKS_INFO:07.02.16 08:14:00-08.02.16 08:35:18(1).    От 08.02.16 02:35:29 UTC до 08.02.16 02:48:37
  PKK_WORKS_INFO:07.02.16 09:07:57-08.02.16 09:22:08(2983). От 08.02.16 06:22:09 до 08.02.16 06:30:57
  PKK_WORKS_INFO:07.02.16 09:22:08-08.02.16 09:32:43(3044). От 08.02.16 06:33:02 до 08.02.16 06:42:33 (3 индекса)
  PKK_WORKS_INFO:07.02.16 09:32:43-08.02.16 10:03:48(3296). От 08.02.16 07:03:50 до 08.02.16 07:16:06 (4 индекса)
  PKK_WORKS_INFO:08.02.16 02:03:48-08.02.16 10:32:56(2095). От 08.02.16 07:33:00 до 08.02.16 07:43:57 (4 инд, -8 часов)
  PKK_WORKS_INFO:08.02.16 09:44:49-08.02.16 10:59:55(715). От 08.02.16 08:00:16 до 08.02.16 08:10:21 (4 инд, -1 час)
  PKK_WORKS_INFO:08.02.16 02:59:55-08.02.16 11:38:05(2645). От 08.02.16 08:38:22 до 08.02.16 08:47:20
  PKK_WORKS_INFO:08.02.16 03:38:05-09.02.16 08:21:07(8402). От 09.02.16 05:21:07 до 09.02.16 05:34:10
  PKK_WORKS_INFO:09.02.16 00:21:07-09.02.16 08:35:07(956). От 09.02.16 05:35:14 до 09.02.16 05:47:42
  PKK_WORKS_INFO:12.02.16 06:59:02-09.03.16 08:24:07(176602). От 09.03.16 05:24:39 до 09.03.16 06:47:38
  PKK_WORKS_INFO:09.03.16 03:24:07-09.03.16 09:49:30(722). От 09.03.16 06:49:36 до 09.03.16 07:03:54
*/
-- ========================================================================
AS    
  last_DT_SFF DATE; -- последний MODIFICATION_DATE в SFF
  last_DT_PKK DATE; -- последний MODIFICATION_DATE в ПКК
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp;

  SELECT MAX(WORKS_CREATED)-5/24 INTO last_DT_SFF FROM ODS.PKK_WORKS_INFO
       WHERE WORKS_HISTORY_ID>=(SELECT MAX(WORKS_HISTORY_ID)-1000 FROM ODS.PKK_WORKS_INFO);

  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_WORKS_INFO tar
	USING (SELECT * FROM ODS.VIEW_PKK_WORKS_INFO WHERE WORKS_CREATED > last_DT_SFF
          UNION 
         SELECT * FROM ODS.VIEW_PKK_WORKS_INFO WHERE MODIFICATION_DATE > last_DT_SFF
		) src
    ON (src.WORKS_HISTORY_ID = tar.WORKS_HISTORY_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        tar.OBJECTS_ID=src.OBJECTS_ID
        ,tar.works_id=src.works_id
        --,tar.WORKS_HISTORY_ID=src.WORKS_HISTORY_ID
        ,tar.WORKS_SALARY=src.WORKS_SALARY
        ,tar.WORKS_STAG=src.WORKS_STAG
        ,tar.works_akt=src.works_akt
        ,tar.works_last=src.works_last
        ,tar.ORG_ID=src.ORG_ID
        ,tar.ORG_NAME=src.ORG_NAME
        ,tar.WORKS_POST=src.WORKS_POST
        ,tar.WORKS_POST_NAME=src.WORKS_POST_NAME
        ,tar.RELATION_PDL=src.RELATION_PDL
        --,tar.WORKS_CREATED=src.WORKS_CREATED
        ,tar.MODIFICATION_DATE=src.MODIFICATION_DATE
        /*,tar.phone_work=src.phone_work
        ,tar.phone_org=src.phone_org*/
        ,tar.DATE_UPD=SYSDATE
        ,tar.WORKS_RANK=1
        , tar.OBJECTS_RANK = 1
      WHERE NOT (NVL(tar.OBJECTS_ID, -1)=NVL(src.OBJECTS_ID, -1)
        AND NVL(tar.works_id, -1)=NVL(src.works_id, -1)
        AND NVL(tar.WORKS_SALARY, -1)=NVL(src.WORKS_SALARY, -1)
        AND NVL(tar.WORKS_STAG, '-')=NVL(src.WORKS_STAG, '-')
        AND NVL(tar.works_akt, -1)=NVL(src.works_akt, -1)
        AND NVL(tar.works_last, -1)=NVL(src.works_last, -1)
        AND NVL(tar.ORG_ID, -1)=NVL(src.ORG_ID, -1)
        AND NVL(tar.ORG_NAME, '-')=NVL(src.ORG_NAME, '-')
        AND NVL(tar.WORKS_POST, -1)=NVL(src.WORKS_POST, -1)
        AND NVL(tar.WORKS_POST_NAME, '-')=NVL(src.WORKS_POST_NAME, '-')
        --AND NVL(tar.RELATION_PDL, -1)=NVL(src.RELATION_PDL, -1)
        /*AND NVL(tar.phone_work, '-')=NVL(src.phone_work, '-')
        AND NVL(tar.phone_org, '-')=NVL(src.phone_org, '-')*/ )
	WHEN NOT MATCHED THEN 
	--вставляем новое
	INSERT ( tar.OBJECTS_ID, tar.works_id, tar.WORKS_HISTORY_ID
          ,tar.WORKS_SALARY, tar.WORKS_STAG,tar.works_akt,tar.works_last
          ,tar.ORG_ID,tar.ORG_NAME
          ,tar.WORKS_POST, tar.WORKS_POST_NAME,tar.RELATION_PDL
          ,tar.WORKS_CREATED,tar.MODIFICATION_DATE
          --,tar.phone_work,tar.phone_org
          ,tar.DATE_UPD
          ,tar.WORKS_RANK, tar.OBJECTS_RANK)
	VALUES (src.OBJECTS_ID, src.works_id, src.WORKS_HISTORY_ID
          ,src.WORKS_SALARY, src.WORKS_STAG,src.works_akt,src.works_last
          ,src.ORG_ID,src.ORG_NAME
          ,src.WORKS_POST, src.WORKS_POST_NAME,src.RELATION_PDL
          ,src.WORKS_CREATED,src.MODIFICATION_DATE
          --,src.phone_work,src.phone_org
          , SYSDATE
          ,1, 1) 
	;	
  
  cnt_MODIF := SQL%ROWCOUNT;
  SELECT MAX(WORKS_CREATED) INTO last_DT_PKK FROM ODS.PKK_WORKS_INFO
       WHERE WORKS_HISTORY_ID>=(SELECT MAX(WORKS_HISTORY_ID)-1000 FROM ODS.PKK_WORKS_INFO);
  p_ADD_INFO := 'Период обновления: '||TO_CHAR(last_DT_SFF)||' до '||TO_CHAR(last_DT_PKK);
  ODS.PR$INS_LOG ('PR$UPD_PKK_WORKS_INFO', start_time, 'ODS.PKK_WORKS_INFO', 'OK', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
  
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_PKK_WORKS_INFO', start_time, 'ODS.PKK_WORKS_INFO', 'ERR', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
END;
  CREATE OR REPLACE PROCEDURE "SFF"."FRAUD_RULE_P1_F0" (v_request_id IN NUMBER, fraud_flag OUT NUMBER)
  IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО -- Телефон1-Псп0"
-- ==	ОПИСАНИЕ:		Совпадает личный телефон
-- ==					, отличается паспорт и хотя бы 1 из параметров: Дата рождения или ФИО, за последние 90 дней. 
-- ==					(МОБ ТЕЛ =, ПАСП ^= и (ДР^= или ФИО^=) за 90 дней) 
-- ==         Правка 07.09.2015: добавлены фильтры для телефонов
-- ========================================================================
-- ==	СОЗДАНИЕ:		02.02.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	02.02.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================

  CURSOR get_fraud(in_RID NUMBER) IS
    SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,NULL as STATUS_ID
				,NULL as SCORE_TREE_ROUTE_ID
				,NULL as CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				--,'Паспорт1_ФиоДр0' as TYPE_REL
        ,'Разные ФиоДр - Паспорт равен' as TYPE_REL
				,'Совпадает серия и номер паспорта. ФИО или ДР - другие (различие больше чем в 1 символе)' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID) 
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				,NULL as STATUS_ID_REL
        ,NULL as SCORE_TREE_ROUTE_ID_REL
        ,NULL as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Паспорт:'||SRC.PASSPORT
              /*||' - '||TO_CHAR(SFF.FN_DIST_LEV(SRC.FIO, APP.FIO)+SFF.FN_DIST_LEV(SRC.DR, APP.DR))||'симв.'*/ as INFO_EQ
				,'Фио+Др.:'||SRC.FIO||' '||SRC.DR as INFO_NEQ
				,'Фио+Др.:'||APP.FIO||' '||APP.DR as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
			ON /*RULE: определяем фрод-правило поиска */
				SRC.PASSPORT = APP.PASSPORT AND (SRC.FIO ^= APP.FIO OR SRC.DR ^= APP.DR)
					AND SRC.PERSON_ID^=APP.PERSON_ID 
					/*AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-121 AND SRC.REQ_DATE*/
					AND APP.REQUEST_ID<SRC.REQUEST_ID
      WHERE SRC.REQUEST_ID=in_RID
				/*EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)*/
				AND NOT SRC.PASSPORT IS NULL AND NOT SRC.PASSPORT='-'
				AND NOT SRC.FIO IS NULL AND NOT APP.FIO IS NULL AND NOT SRC.FIO='-' AND NOT APP.FIO='-'
        AND SFF.FN_DIST_LEV(SRC.FIO||SRC.DR, APP.FIO||APP.DR)>1
			) TAB
		WHERE TAB.F_POS=1 ;

  in_fraud get_fraud%ROWTYPE;
  flag_exists_rid NUMBER;
BEGIN   
  DBMS_OUTPUT.ENABLE;
  SELECT COUNT(*) INTO flag_exists_rid FROM dual where EXISTS(SELECT REQUEST_ID FROM SFF.APPLICATIONS_FROD);
  IF flag_exists_rid=0 THEN 
    fraud_flag := 9;
  END IF;
  
  OPEN get_fraud(v_request_id);
	LOOP 
    FETCH get_fraud INTO in_fraud;
		EXIT WHEN get_fraud%NOTFOUND;

    IF (fraud_flag IS NULL) THEN 
      --фрод найден, возвращаем 1
      fraud_flag := 1;   
    END IF; 
    
    INSERT INTO SFF.FROD_RULE_DEMO1(FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
               FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
               TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
               REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
               FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
               INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
       VALUES(
       in_fraud.FROD_RULE_DATE, in_fraud.REQUEST_ID, in_fraud.PERSON_ID, in_fraud.REQ_DATE,
               in_fraud.FIO, in_fraud.DR, in_fraud.STATUS_ID, in_fraud.SCORE_TREE_ROUTE_ID, in_fraud.CREATED_GROUP_ID,
               in_fraud.TYPE_REL, in_fraud.TYPE_REL_DESC, in_fraud.DAY_BETWEEN,
               in_fraud.REQUEST_ID_REL, in_fraud.PERSON_ID_REL, in_fraud.REQ_DATE_REL,
               in_fraud.FIO_REL, in_fraud.DR_REL, in_fraud.STATUS_ID_REL, in_fraud.SCORE_TREE_ROUTE_ID_REL, in_fraud.CREATED_GROUP_ID_REL,
               in_fraud.INFO_EQ, in_fraud.INFO_NEQ, in_fraud.INFO_NEQ_REL, in_fraud.F_POS);
  END LOOP;
  CLOSE get_fraud;
  
  IF (fraud_flag IS NULL) THEN 
    fraud_flag := 0;
  END IF;
  
  EXCEPTION
    WHEN OTHERS
    THEN fraud_flag := -1;
  END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_PKK_C_CREDIT_INFO" 
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицы с адресной историей
-- ==	ОПИСАНИЕ:	   
-- ========================================================================
-- ==	СОЗДАНИЕ:		  26.01.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	26.01.2016 (ТРАХАЧЕВ В.В.)
/*    PKK_C_CREDIT_INFO:24.12.06.          От 26.01.16 07:20:00,463908000 UTC до 26.01.16 07:25:02,700445000 UTC
      PKK_C_CREDIT_INFO:27.12.06.          От 27.01.16 01:25:08,570565000 UTC до 27.01.16 01:26:15,846299000 UTC
      PKK_C_CREDIT_INFO:27.12.06-27.03.07. От 27.01.16 01:28:10,962313000 UTC до 27.01.16 01:32:57,735540000 UTC
      PKK_C_CREDIT_INFO:26.03.07-26.03.08. От 27.01.16 01:50:24,783232000 UTC до 27.01.16 02:03:05,311283000 UTC
      PKK_C_CREDIT_INFO:04.04.07-29.12.09. От 27.01.16 02:06:31,738509000 UTC до 27.01.16 03:32:31,226826000 UTC
      PKK_C_CREDIT_INFO:24.12.09-19.09.12. От 27.01.16 03:35:27,077796000 UTC до 27.01.16 04:42:21,999533000 UTC
      PKK_C_CREDIT_INFO:15.09.12-12.06.15. От 27.01.16 04:49:50,880758000 UTC до 27.01.16 07:15:47,634665000 UTC
      PKK_C_CREDIT_INFO:11.06.15-06.04.16. От 27.01.16 08:09:35,034905000 UTC до 27.01.16 11:25:39,611323000 UTC
      PKK_C_CREDIT_INFO:26.01.16-05.02.16. От 27.01.16 23:35:34,872914000 UTC до 28.01.16 04:49:11,373125000 UTC
      PKK_C_CREDIT_INFO:27.01.16-30.01.16. От 28.01.16 05:39:37,504957000 UTC до 28.01.16 08:12:29,366184000 UTC
      PKK_C_CREDIT_INFO:05.02.16 11:14:57-02.03.16 14:46:04(2119591). От 02.03.16 11:50:34 до 02.03.16 15:03:19
*/
-- ========================================================================

AS    
  last_DT_SFF DATE; -- последний MODIFICATION_DATE в SFF
  last_DT_PKK DATE; -- последний MODIFICATION_DATE в ПКК
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp;
   
  SELECT MAX(MODIFICATION_DATE)-4/24 INTO last_DT_SFF FROM ODS.PKK_C_CREDIT_INFO_SNA
       WHERE REQUEST_ID>=(SELECT MAX(REQUEST_ID)-1000 FROM ODS.PKK_C_CREDIT_INFO_SNA);

  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_C_CREDIT_INFO_SNA tar
	USING (SELECT DISTINCT cci.REQUEST_ID
			,crid.OBJECTS_ID AS PERSON_ID
			,crid.CREATED_DATE
			,crid.TYPE_REQUEST_ID
			,cci.MODIFICATION_DATE 
			,cci.SUMMA_DELAY --сумма просрочки (для определения ПЗ >= 400 р)
			,cci.BASE_DOLG --Остаток основного долга
			,cci.SUMMA_BASE_DOLG --доп. поле
			,cci.UNUSED_LIMIT -- неиспользованные лимит кредита
			,cci.CNT_DELAY_15 --Количество просрочек до 15 дней
			,cci.CNT_DELAY_30 --Количество просрочек от 15 до 30 дней
			,cci.CNT_DELAY_60 --Количество просрочек от 30 до 60 дней
			,cci.CNT_DELAY_90 --Количество просрочек от 60 до 90 дней
			,cci.CNT_DELAY_180 --Количество просрочек от 90 до 180 дней
			,cci.CNT_DELAY_365 --Количество просрочек от 180 до 365 дней
			,cci.CNT_DELAY_MORE --Количество просрочек свыше 365 дней
			,cci.CONTRACT_STATUS_CODE --Статус кредитной истории
			,cci.DATE_CLOSE --Дата фактического закрытия кредита РБО.
			,cci.DEFOLT
			,cci.DELAY_OLD -- 0 - ранее просрочек не было, 1 - ранее были просрочки
			,cci.PRDAY -- Количество дней просрочки
			,SUBSTR(cci.MOP_DELAY,1,24) as MOP_lst24 --строка для 24 месяцев
			,SUBSTR(cci.MOP_DELAY,1,6) as MOP_lst6 --строка для 6 месяцев
			,cci.MOP_DELAY --Строка кредитной истории
			,regexp_substr(cci.MOP_DELAY, '[45789]', 1) as pr_day_90_more --просрочка 90> за все время
			,regexp_substr(SUBSTR(cci.MOP_DELAY,1, 24), '[3]', 1) as pr_day_60_89 --просрочка 60-89 за последние 24 мес
			,regexp_substr(SUBSTR(cci.MOP_DELAY,1, 6), '[2]', 1) as pr_day_30_59 --просрочка 30-59 за последние 6 мес
			--,(CASE WHEN SUMMA_DELAY>=400 THEN 1 ELSE 0 END) as flag_PROSR --флаг просрочки >400 руб
			,(CASE WHEN SUMMA_DELAY>=200000 AND regexp_count(cci.MOP_DELAY, '1', 1) BETWEEN 0 AND 3 THEN 1 ELSE 0 END) as flag_PROSR --флаг просрочки >=200 тыс. руб
			,LAST_VALUE(cr_rs.REQUEST_RESTRUCTION_ID) OVER (PARTITION BY cr_rs.REQUEST_ID) as REQUEST_RESTRUCTION_ID_LAST --последний реквест который подлежал РС
			--,(CASE WHEN cci.BASE_DOLG>0 THEN regexp_count(cci.MOP_DELAY, '1', 1) ELSE -1 END) as cnt_payment_veb --кол-во платежей по текущей строке КИ
      ,regexp_count(cci.MOP_DELAY, '1', 1) as CNT_PAYMENT_VEB --кол-во платежей по текущей строке КИ
			--,SFF.FN_IS_KK_RID(crid.REQUEST_ID) as KK -- флаг кредитной карты или овердрафта
			--,(SYSDATE) as RP_DEFOLT_DATE -- дата логирования
	FROM KREDIT.C_REQUEST@DBLINK_PKK crid 
	INNER JOIN KREDIT.C_CREDIT_INFO@DBLINK_PKK cci
		ON cci.REQUEST_ID=crid.REQUEST_ID
	LEFT OUTER JOIN KREDIT.C_REQUEST_RESTRUCTION@DBLINK_PKK cr_rs
		ON cci.REQUEST_ID=cr_rs.REQUEST_ID
	/*LEFT OUTER JOIN KREDIT.C_REQUEST_CREDIT@DBLINK_PKK cr_cr
		ON cci.REQUEST_ID=cr_cr.REQUEST_ID*/
	/*LEFT OUTER JOIN KREDIT.C_SCHEMS@DBLINK_PKK c_sch
		ON cr_cr.SCHEMS_ID=c_sch.SCHEMS_ID*/
	/*LEFT OUTER JOIN KREDIT.C_TYPE_CREDIT@DBLINK_PKK c_tp_cr --по факту название кредита здесь
		ON cr_cr.TYPE_CREDIT_ID=c_tp_cr.TYPE_CREDIT_ID*/
	WHERE 
      --вариант с обновлением по частям
    --cci.MODIFICATION_DATE BETWEEN last_DT_SFF AND last_DT_SFF + 3
    cci.MODIFICATION_DATE BETWEEN last_DT_SFF AND last_DT_SFF+5
      AND NOT EXISTS(SELECT REQUEST_ID FROM ODS.PKK_C_CREDIT_INFO_SNA WHERE REQUEST_ID=cci.REQUEST_ID
                    /*AND MODIFICATION_DATE=cci.MODIFICATION_DATE*/
                    AND SUMMA_DELAY =cci.SUMMA_DELAY
                    AND BASE_DOLG =cci.BASE_DOLG
                    AND SUMMA_BASE_DOLG=cci.SUMMA_BASE_DOLG
                    --AND DELAY_OLD=cci.DELAY_OLD
                    --AND PRDAY=cci.PRDAY
                    AND MOP_DELAY=cci.MOP_DELAY)
		--AND NOT cci.MOP_DELAY LIKE '%1%'
		--cci.MODIFICATION_DATE > sysdate-10 AND
		--regexp_substr(NVL(SUBSTR(RTRIM(cci.MOP_DELAY,'0-'),-3,3), RTRIM(cci.MOP_DELAY,'0-')), '[2345789]', 1)>0
	--ORDER BY cci.REQUEST_ID
	
		) src
    ON (src.REQUEST_ID = tar.REQUEST_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        --tar.REQUEST_ID=src.REQUEST_ID
        tar.PERSON_ID=src.PERSON_ID
        --,tar.CREATED_DATE=src.CREATED_DATE
        --,tar.TYPE_REQUEST_ID=src.TYPE_REQUEST_ID
        ,tar.MODIFICATION_DATE =src.MODIFICATION_DATE
        ,tar.SUMMA_DELAY =src.SUMMA_DELAY
        ,tar.BASE_DOLG =src.BASE_DOLG
        ,tar.SUMMA_BASE_DOLG=src.SUMMA_BASE_DOLG
        ,tar.UNUSED_LIMIT =src.UNUSED_LIMIT
        /*,tar.CNT_DELAY_15 =src.CNT_DELAY_15
        ,tar.CNT_DELAY_30=src.CNT_DELAY_30
        ,tar.CNT_DELAY_60=src.CNT_DELAY_60
        ,tar.CNT_DELAY_90=src.CNT_DELAY_90
        ,tar.CNT_DELAY_180=src.CNT_DELAY_180
        ,tar.CNT_DELAY_365 =src.CNT_DELAY_365
        ,tar.CNT_DELAY_MORE=src.CNT_DELAY_MORE*/
        ,tar.CONTRACT_STATUS_CODE =src.CONTRACT_STATUS_CODE
        ,tar.DATE_CLOSE =src.DATE_CLOSE
        ,tar.DEFOLT=src.DEFOLT
        ,tar.DELAY_OLD=src.DELAY_OLD
        ,tar.PRDAY=src.PRDAY
        ,tar.MOP_lst24 =src.MOP_lst24
        ,tar.MOP_lst6=src.MOP_lst6
        ,tar.MOP_DELAY=src.MOP_DELAY
        ,tar.pr_day_90_more=src.pr_day_90_more
        ,tar.pr_day_60_89=src.pr_day_60_89
        ,tar.pr_day_30_59=src.pr_day_30_59
        ,tar.flag_PROSR=src.flag_PROSR
        ,tar.REQUEST_RESTRUCTION_ID_LAST =src.REQUEST_RESTRUCTION_ID_LAST
        ,tar.cnt_payment_veb =src.cnt_payment_veb
        --,tar.KK =src.KK
        ,tar.DATE_UPD=SYSDATE
      WHERE NOT (NVL(tar.SUMMA_DELAY, -1) = NVL(src.SUMMA_DELAY , -1)
            AND NVL(tar.BASE_DOLG, -1) = NVL(src.BASE_DOLG, -1) AND NVL(tar.SUMMA_BASE_DOLG, -1)= NVL(src.SUMMA_BASE_DOLG, -1)
            --AND tar.DELAY_OLD=cci.DELAY_OLD
            AND NVL(tar.PRDAY, -1)=NVL(src.PRDAY, -1) AND NVL(tar.MOP_DELAY, '-')=NVL(src.MOP_DELAY, '-'))
	WHEN NOT MATCHED THEN 
	--вставляем новое
	INSERT (tar.REQUEST_ID,tar.PERSON_ID
          ,tar.CREATED_DATE,tar.TYPE_REQUEST_ID
          ,tar.MODIFICATION_DATE,tar.SUMMA_DELAY 
          ,tar.BASE_DOLG,tar.SUMMA_BASE_DOLG
          ,tar.UNUSED_LIMIT 
          ,tar.CNT_DELAY_15,tar.CNT_DELAY_30,tar.CNT_DELAY_60,tar.CNT_DELAY_90,tar.CNT_DELAY_180
          ,tar.CNT_DELAY_365,tar.CNT_DELAY_MORE,tar.CONTRACT_STATUS_CODE,tar.DATE_CLOSE 
          ,tar.DEFOLT,tar.DELAY_OLD,tar.PRDAY
          ,tar.MOP_lst24,tar.MOP_lst6,tar.MOP_DELAY
          ,tar.pr_day_90_more,tar.pr_day_60_89,tar.pr_day_30_59,tar.flag_PROSR
          ,tar.REQUEST_RESTRUCTION_ID_LAST,tar.CNT_PAYMENT_VEB --,tar.KK 
          ,tar.DATE_UPD)
	VALUES (src.REQUEST_ID,src.PERSON_ID
          ,src.CREATED_DATE,src.TYPE_REQUEST_ID
          ,src.MODIFICATION_DATE,src.SUMMA_DELAY 
          ,src.BASE_DOLG,src.SUMMA_BASE_DOLG
          ,src.UNUSED_LIMIT 
          ,src.CNT_DELAY_15,src.CNT_DELAY_30,src.CNT_DELAY_60,src.CNT_DELAY_90,src.CNT_DELAY_180
          ,src.CNT_DELAY_365,src.CNT_DELAY_MORE,src.CONTRACT_STATUS_CODE,src.DATE_CLOSE 
          ,src.DEFOLT,src.DELAY_OLD,src.PRDAY
          ,src.MOP_lst24,src.MOP_lst6,src.MOP_DELAY
          ,src.pr_day_90_more,src.pr_day_60_89,src.pr_day_30_59,src.flag_PROSR
          ,src.REQUEST_RESTRUCTION_ID_LAST,src.CNT_PAYMENT_VEB --,src.KK 
          ,SYSDATE)
	;	
  
  --информация для вывода при ручном обновлении
  cnt_MODIF := SQL%ROWCOUNT;
  SELECT MAX(CREATED_DATE) INTO last_DT_PKK FROM ODS.PKK_C_CREDIT_INFO_SNA
       WHERE REQUEST_ID>=(SELECT MAX(REQUEST_ID)-1000 FROM ODS.PKK_C_CREDIT_INFO_SNA);

  p_ADD_INFO := 'Период обновления: '||TO_CHAR(last_DT_SFF)||' до '||TO_CHAR(last_DT_PKK);
  ODS.PR$INS_LOG ('PR$UPD_PKK_C_CREDIT_INFO', start_time, 'ODS.PKK_C_CREDIT_INFO', 'OK', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
    
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_PKK_C_CREDIT_INFO', start_time, 'ODS.PKK_C_CREDIT_INFO', 'ERR', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_PKK_CONTACTS" 
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицы с историей контактов
-- ==	ОПИСАНИЕ:	   
-- ========================================================================
-- ==	СОЗДАНИЕ:		  09.02.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	09.02.2016 (ТРАХАЧЕВ В.В.)
/*PKK_CONTACTS:09.02.16 02:50:28-09.02.16 07:50:28(1693). От 09.02.16 09:57:45 до 09.02.16 09:58:51 (1 индекс)
  PKK_CONTACTS:09.02.16 02:50:28-09.02.16 07:50:28(1839). От 09.02.16 10:33:37 до 09.02.16 10:35:17 (3 индекса)
  PKK_CONTACTS:09.02.16 03:50:28-09.02.16 07:50:28(1857). От 09.02.16 10:40:19 до 09.02.16 10:40:55
  PKK_CONTACTS:09.02.16 05:50:28-09.02.16 07:50:28(1791). От 09.02.16 10:42:02 до 09.02.16 10:42:38
  PKK_CONTACTS:09.02.16 04:50:28-10.02.16 06:00:32(2816). От 10.02.16 03:28:29 до 10.02.16 03:32:14
  PKK_CONTACTS:10.02.16 05:00:32-10.02.16 06:00:32(109). От 10.02.16 03:33:05 до 10.02.16 03:36:14
  PKK_CONTACTS:10.02.16 05:00:32-03.03.16 13:40:20(901344). От 03.03.16 10:40:33 до 03.03.16 11:04:45
*/
-- ========================================================================
AS    
  last_DT_SFF DATE; -- последний MODIFICATION_DATE в SFF
  last_DT_PKK DATE; -- последний MODIFICATION_DATE в ПКК
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp;

  SELECT MAX(CONTACT_CREATED)-5/24 INTO last_DT_SFF FROM ODS.PKK_CONTACTS
       WHERE CONTACT_ID>=(SELECT MAX(CONTACT_ID)-1000 FROM ODS.PKK_CONTACTS);

  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_CONTACTS tar
	USING (SELECT * FROM ODS.VIEW_PKK_CONTACTS WHERE CONTACT_CREATED > last_DT_SFF
          UNION 
         SELECT * FROM ODS.VIEW_PKK_CONTACTS WHERE CONTACT_MODIFICATION > last_DT_SFF
  
    /*SELECT cp.CONTACT_PERSON_ID as CONTACT_ID--первичный ключ вместе с флагом источника
    ,cp.OBJECTS_ID
    ,cp.CONTACT_OBJECTS_ID as OB_ID
    ,cp.CONTACT_AKT
    ,cp.CONTACT_COMMENT
    ,cp.CONTANT_CREATED as CONTACT_CREATED
    ,cp.MODIFICATION_DATE as CONTACT_MODIFICATION
    ,cp.FAMILY_REL
    ,fr.FAMILY_REL_NAME --наименование родственной связи
    ,fr.FAMILY_REL_STATUS --флаг что родство есть
    ,fr.FAMILY_SEQ_ID --степень родства
FROM CPD.CONTACT_PERSON@DBLINK_PKK cp
LEFT JOIN CPD.FAMILY_RELATIONS@DBLINK_PKK fr
  ON fr.family_rel=cp.family_rel
WHERE COALESCE(cp.MODIFICATION_DATE, cp.CONTANT_CREATED)>last_DT_SFF
    --cp.CONTANT_CREATED BETWEEN last_DT_SFF AND last_DT_SFF + 5*/
		) src
    ON (src.CONTACT_ID = tar.CONTACT_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET --tar.CONTACT_ID=src.CONTACT_ID
        tar.OBJECTS_ID=src.OBJECTS_ID
        ,tar.OB_ID=src.OB_ID
        ,tar.CONTACT_AKT=src.CONTACT_AKT
        ,tar.CONTACT_COMMENT=src.CONTACT_COMMENT
        --,tar.CONTACT_CREATED=src.CONTACT_CREATED
        ,tar.CONTACT_MODIFICATION=src.CONTACT_MODIFICATION
        ,tar.FAMILY_REL=src.FAMILY_REL
        ,tar.FAMILY_REL_NAME=src.FAMILY_REL_NAME
        ,tar.FAMILY_REL_STATUS=src.FAMILY_REL_STATUS
        ,tar.FAMILY_SEQ_ID=src.FAMILY_SEQ_ID
        ,tar.DATE_UPD=SYSDATE
	WHEN NOT MATCHED THEN 
	--вставляем новое
	INSERT ( tar.CONTACT_ID
          ,tar.OBJECTS_ID,tar.OB_ID,tar.CONTACT_AKT
          ,tar.CONTACT_COMMENT, tar.CONTACT_CREATED, tar.CONTACT_MODIFICATION
          ,tar.FAMILY_REL, tar.FAMILY_REL_NAME, tar.FAMILY_REL_STATUS, tar.FAMILY_SEQ_ID, DATE_UPD)
	VALUES (src.CONTACT_ID
          ,src.OBJECTS_ID, src.OB_ID, src.CONTACT_AKT
          ,src.CONTACT_COMMENT, src.CONTACT_CREATED, src.CONTACT_MODIFICATION
          ,src.FAMILY_REL, src.FAMILY_REL_NAME, src.FAMILY_REL_STATUS, src.FAMILY_SEQ_ID, SYSDATE)
	;	
  
  --информация для вывода при ручном обновлении
  cnt_MODIF := SQL%ROWCOUNT;
  SELECT MAX(CONTACT_CREATED) INTO last_DT_PKK FROM ODS.PKK_CONTACTS
       WHERE CONTACT_ID>=(SELECT MAX(CONTACT_ID)-1000 FROM ODS.PKK_CONTACTS);
       
  p_ADD_INFO := 'Период обновления: '||TO_CHAR(last_DT_SFF)||' до '||TO_CHAR(last_DT_PKK);
  ODS.PR$INS_LOG ('PR$UPD_PKK_CONTACTS', start_time, 'ODS.PKK_CONTACTS', 'OK', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);

  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_PKK_CONTACTS', start_time, 'ODS.PKK_CONTACTS', 'ERR', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);

END;
  CREATE OR REPLACE PROCEDURE "SFF"."FRAUD_RULE_P1_F0_ERR" (v_request_id IN NUMBER, fraud_flag OUT NUMBER)
  IS
-- ========================================================================
-- ==	ПРОЦЕДУРА "ФРОД-ПРАВИЛО -- Телефон1-Псп0"
-- ==	ОПИСАНИЕ:		Совпадает личный телефон
-- ==					, отличается паспорт и хотя бы 1 из параметров: Дата рождения или ФИО, за последние 90 дней. 
-- ==					(МОБ ТЕЛ =, ПАСП ^= и (ДР^= или ФИО^=) за 90 дней) 
-- ==         Правка 07.09.2015: добавлены фильтры для телефонов
-- ========================================================================
-- ==	СОЗДАНИЕ:		02.02.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	02.02.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================

  CURSOR get_fraud(in_RID NUMBER) IS
    SELECT * FROM 
			(SELECT sysdate as FROD_RULE_DATE
				,SRC.REQUEST_ID
				,SRC.PERSON_ID
				,SRC.REQ_DATE
				,SRC.FIO as FIO
				,SRC.DR as DR
				,NULL as STATUS_ID
				,NULL as SCORE_TREE_ROUTE_ID
				,NULL as CREATED_GROUP_ID
				-- при необходимости переобзоначить Тип связи и его описание для сработавшего правила
				--,'Паспорт1_ФиоДр_Ошиб' as TYPE_REL
        ,'Ошибка в ФиоДр - Паспорт равен' as TYPE_REL
				,'Совпадает серия и номер паспорта. ФИО или ДР - отличие в 1 символе' as TYPE_REL_DESC
				,ABS(ROUND(SRC.REQ_DATE-APP.REQ_DATE, 0)) as DAY_BETWEEN
				/*,TO_CHAR(APP.REQUEST_ID) as REQUEST_ID_REL*/
				,LISTAGG(TO_CHAR(APP.REQUEST_ID),',') WITHIN GROUP (ORDER BY SRC.REQUEST_ID,APP.PERSON_ID)
															OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID) REQUEST_ID_REL
				,APP.PERSON_ID PERSON_ID_REL
				,APP.REQ_DATE AS REQ_DATE_REL
				,APP.FIO as FIO_REL, APP.DR as DR_REL
				, NULL as STATUS_ID_REL
        , NULL as SCORE_TREE_ROUTE_ID_REL
        , NULL as CREATED_GROUP_ID_REL
				--INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
				,'Паспорт:'||SRC.PASSPORT as INFO_EQ
				,'Фио+Др.:'||SRC.FIO||' '||SRC.DR as INFO_NEQ
				,'Фио+Др.:'||APP.FIO||' '||APP.DR as INFO_NEQ_REL
				,ROW_NUMBER() OVER(PARTITION BY SRC.REQUEST_ID,APP.PERSON_ID 
										ORDER BY SRC.REQUEST_ID,APP.PERSON_ID DESC, APP.REQ_DATE DESC) as F_POS
			FROM APPLICATIONS_FROD SRC
			INNER JOIN APPLICATIONS_FROD APP --таблица поиска связей
			ON --RULE: определяем фрод-правило поиска 
				SRC.PASSPORT = APP.PASSPORT AND (SRC.FIO ^= APP.FIO OR SRC.DR ^= APP.DR)
					AND SRC.PERSON_ID^=APP.PERSON_ID 
					--AND APP.REQ_DATE BETWEEN SRC.REQ_DATE-121 AND SRC.REQ_DATE
					AND APP.REQUEST_ID<SRC.REQUEST_ID
			WHERE SRC.REQUEST_ID = in_RID
				--EXCEPT: определяем доп. исключения по фрод-правилу (исключение пустых, разные физики и т.п.)
				AND NOT SRC.PASSPORT IS NULL AND NOT SRC.PASSPORT='-'
				AND NOT SRC.FIO IS NULL AND NOT APP.FIO IS NULL AND NOT SRC.FIO='-' AND NOT APP.FIO='-'
        AND SFF.FN_DIST_LEV(SRC.FIO||SRC.DR, APP.FIO||APP.DR)=1
			) TAB
		WHERE TAB.F_POS=1 ;

  in_fraud get_fraud%ROWTYPE;
  flag_exists_rid NUMBER;
BEGIN   
  DBMS_OUTPUT.ENABLE;
  SELECT COUNT(*) INTO flag_exists_rid FROM dual where EXISTS(SELECT REQUEST_ID FROM SFF.APPLICATIONS_FROD);
  IF flag_exists_rid=0 THEN 
    fraud_flag := 9;
  END IF;
  
  OPEN get_fraud(v_request_id);
	LOOP 
    FETCH get_fraud INTO in_fraud;
		EXIT WHEN get_fraud%NOTFOUND;

    IF (fraud_flag IS NULL) THEN 
      --фрод найден, возвращаем 1
      fraud_flag := 1;   
    END IF; 
    
    INSERT INTO SFF.FROD_RULE_DEMO1(FROD_RULE_DATE, REQUEST_ID, PERSON_ID, REQ_DATE,
               FIO, DR, STATUS_ID, SCORE_TREE_ROUTE_ID, CREATED_GROUP_ID,
               TYPE_REL, TYPE_REL_DESC, DAY_BETWEEN,
               REQUEST_ID_REL, PERSON_ID_REL, REQ_DATE_REL,
               FIO_REL, DR_REL, STATUS_ID_REL, SCORE_TREE_ROUTE_ID_REL, CREATED_GROUP_ID_REL,
               INFO_EQ, INFO_NEQ, INFO_NEQ_REL, F_POS)
       VALUES(
       in_fraud.FROD_RULE_DATE, in_fraud.REQUEST_ID, in_fraud.PERSON_ID, in_fraud.REQ_DATE,
               in_fraud.FIO, in_fraud.DR, in_fraud.STATUS_ID, in_fraud.SCORE_TREE_ROUTE_ID, in_fraud.CREATED_GROUP_ID,
               in_fraud.TYPE_REL, in_fraud.TYPE_REL_DESC, in_fraud.DAY_BETWEEN,
               in_fraud.REQUEST_ID_REL, in_fraud.PERSON_ID_REL, in_fraud.REQ_DATE_REL,
               in_fraud.FIO_REL, in_fraud.DR_REL, in_fraud.STATUS_ID_REL, in_fraud.SCORE_TREE_ROUTE_ID_REL, in_fraud.CREATED_GROUP_ID_REL,
               in_fraud.INFO_EQ, in_fraud.INFO_NEQ, in_fraud.INFO_NEQ_REL, in_fraud.F_POS);
  END LOOP;
  CLOSE get_fraud;
  
  IF (fraud_flag IS NULL) THEN 
    fraud_flag := 0;
  END IF;
  
  EXCEPTION
    WHEN OTHERS
    THEN fraud_flag := -1;
  END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_PKK_FAMILY" 
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицы с историей семейных связей
-- ==	ОПИСАНИЕ:	   
-- ========================================================================
-- ==	СОЗДАНИЕ:		  09.02.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	09.02.2016 (ТРАХАЧЕВ В.В.)
/*PKK_FAMILY:09.02.16 07:06:17-09.02.16 07:50:28(7742). От 09.02.16 11:07:12 до 09.02.16 11:12:17
  PKK_FAMILY:09.02.16 11:06:45-10.02.16 06:00:32(11104). От 10.02.16 05:18:44 до 10.02.16 05:21:39
  PKK_FAMILY:10.02.16 05:16:53-03.03.16 13:40:20(369349). От 09.03.16 01:26:55 до 09.03.16 01:43:55
*/
-- ========================================================================
AS    
  last_DT_SFF DATE; -- последний MODIFICATION_DATE в SFF
  last_DT_PKK DATE; -- последний MODIFICATION_DATE в ПКК
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp;

  SELECT MAX(CONTACT_CREATED)-5/24 INTO last_DT_SFF FROM ODS.PKK_FAMILY
       WHERE CONTACT_ID>=(SELECT MAX(CONTACT_ID)-1000 FROM ODS.PKK_FAMILY);

  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_FAMILY tar 
	USING (SELECT * FROM ODS.VIEW_PKK_FAMILY WHERE CONTACT_CREATED > last_DT_SFF
          UNION 
         SELECT * FROM ODS.VIEW_PKK_FAMILY WHERE CONTACT_MODIFICATION > last_DT_SFF
      ) src
      ON (src.CONTACT_ID = tar.CONTACT_ID )
    WHEN MATCHED THEN
      --Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
      UPDATE SET --tar.CONTACT_ID=src.CONTACT_ID
          tar.OBJECTS_ID=src.OBJECTS_ID
          ,tar.OB_ID=src.OB_ID
          ,tar.CONTACT_AKT=src.CONTACT_AKT
          --,tar.CONTACT_CREATED=src.CONTACT_CREATED
          ,tar.CONTACT_MODIFICATION=src.CONTACT_MODIFICATION
          ,tar.FAMILY_REL=src.FAMILY_REL
          ,tar.FAMILY_REL_NAME=src.FAMILY_REL_NAME
          ,tar.FAMILY_REL_STATUS=src.FAMILY_REL_STATUS
          ,tar.FAMILY_SEQ_ID=src.FAMILY_SEQ_ID
          ,tar.DATE_UPD=SYSDATE
    WHEN NOT MATCHED THEN 
    --вставляем новое
    INSERT ( tar.CONTACT_ID
            ,tar.OBJECTS_ID,tar.OB_ID,tar.CONTACT_AKT
            , tar.CONTACT_CREATED, tar.CONTACT_MODIFICATION
            ,tar.FAMILY_REL, tar.FAMILY_REL_NAME, tar.FAMILY_REL_STATUS, tar.FAMILY_SEQ_ID, DATE_UPD)
    VALUES (src.CONTACT_ID
            ,src.OBJECTS_ID, src.OB_ID, src.CONTACT_AKT
            , src.CONTACT_CREATED, src.CONTACT_MODIFICATION
            ,src.FAMILY_REL, src.FAMILY_REL_NAME, src.FAMILY_REL_STATUS, src.FAMILY_SEQ_ID, SYSDATE)
    ;	
  
  --информация для вывода при ручном обновлении
  cnt_MODIF := SQL%ROWCOUNT;
  SELECT MAX(CONTACT_CREATED) INTO last_DT_PKK FROM ODS.PKK_CONTACTS
       WHERE CONTACT_ID>=(SELECT MAX(CONTACT_ID)-1000 FROM ODS.PKK_CONTACTS);
  
  p_ADD_INFO := 'Период обновления: '||TO_CHAR(last_DT_SFF)||' до '||TO_CHAR(last_DT_PKK);
  ODS.PR$INS_LOG ('PR$UPD_PKK_FAMILY', start_time, 'ODS.PKK_FAMILY', 'OK', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
  
	--DBMS_OUTPUT.PUT_LINE('PKK_FAMILY:'||TO_CHAR(last_DT_SFF)||'-'||last_DT_PKK||'('||cnt_MODIF||'). '||p_ADD_INFO);

  COMMIT;
                
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_PKK_FAMILY', start_time, 'ODS.PKK_FAMILY', 'ERR', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);

END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_P_PKK_PERSON_INFO" (v_REQUEST_ID NUMBER DEFAULT -999, v_OBJECTS_ID IN NUMBER)
-- ========================================================================
-- ==	ПРОЦЕДУРА    Обновление таблицы PERSON_INFO 
-- ==	ОПИСАНИЕ:	   Основные данные о физике (ФИО + ДР)
-- ========================================================================
-- ==	СОЗДАНИЕ:		  18.01.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	12.02.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================
AS    
  p_OBJECTS_ID NUMBER := v_OBJECTS_ID ;
  f_Exist_REQUEST_ID NUMBER;
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER; 
  
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp;
  
  IF v_REQUEST_ID^=-999 AND v_OBJECTS_ID IS NULL THEN --если параметр передали, ищем нужный OBJECTS_ID
    SELECT COUNT(*) INTO f_Exist_REQUEST_ID FROM dual 
      WHERE EXISTS(SELECT OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID);
    IF f_Exist_REQUEST_ID=1 THEN 
      SELECT NVL(OBJECTS_ID, -999) INTO p_OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID;
    END IF;
  END IF;
  
  --ОБНОВЛЕНИЕ 
  MERGE INTO ODS.PKK_PERSON_INFO tar
	USING (SELECT FIO_ID, OBJECTS_ID
              ,FIO_AKT
              ,FIO_CREATED
              ,FIO_HISTORY_PK
              ,FIO4SEARCH
              ,BIRTH
              ,MODIFICATION_DATE 
            FROM ODS.VIEW_PKK_PERSON_INFO WHERE OBJECTS_ID = p_OBJECTS_ID
                AND COALESCE(MODIFICATION_DATE, FIO_CREATED) > SYSDATE-7
		) src
    ON (src.FIO_HISTORY_PK = tar.FIO_HISTORY_PK )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        --tar.EMAIL_ID=src.EMAIL_ID
        tar.FIO_ID=src.FIO_ID
        ,tar.OBJECTS_ID=src.OBJECTS_ID
        ,tar.FIO_AKT=src.FIO_AKT
        --,tar.FIO_CREATED=src.FIO_CREATED
        --,tar.FIO_HISTORY_PK=src.FIO_HISTORY_PK
        ,tar.FIO4SEARCH=src.FIO4SEARCH
        ,tar.BIRTH=src.BIRTH
        ,tar.MODIFICATION_DATE=src.MODIFICATION_DATE
        ,tar.DATE_UPD=SYSDATE
        ,tar.FIO_RANK = 1
        WHERE NOT (NVL(tar.FIO4SEARCH, '-')=NVL(src.FIO4SEARCH, '-') 
              AND NVL(tar.BIRTH, TO_DATE('01-01-1900', 'dd-mm-yyyy'))=NVL(src.BIRTH , TO_DATE('01-01-1900', 'dd-mm-yyyy'))
              AND NVL(tar.FIO_AKT, -1)=NVL(src.FIO_AKT, -1) 
              AND NVL(tar.OBJECTS_ID, -1)=NVL(src.OBJECTS_ID, -1) AND NVL(tar.FIO_ID, -1)=NVL(src.FIO_ID, -1))
	WHEN NOT MATCHED THEN 
	--вставляем новое 
	INSERT ( tar.FIO_ID
            ,tar.OBJECTS_ID
            ,tar.FIO_AKT
            ,tar.FIO_CREATED
            ,tar.FIO_HISTORY_PK
            ,tar.FIO4SEARCH
            ,tar.BIRTH
            ,tar.MODIFICATION_DATE
            ,tar.DATE_UPD
            ,tar.FIO_RANK)
	VALUES (src.FIO_ID
            ,src.OBJECTS_ID
            ,src.FIO_AKT
            ,src.FIO_CREATED
            ,src.FIO_HISTORY_PK
            ,src.FIO4SEARCH
            ,src.BIRTH
            ,src.MODIFICATION_DATE
            ,SYSDATE
            ,1)
	;	
  
  cnt_MODIF := SQL%ROWCOUNT;
  
  ODS.PR$INS_LOG ('PR$UPD_P_PKK_PERSON_INFO', start_time, 'ODS.PKK_PERSON_INFO', 'OK', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_P_PKK_PERSON_INFO', start_time, 'ODS.PKK_PERSON_INFO', 'ERR', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR_RUN_UPD_TEST" 
IS

BEGIN
  DBMS_OUTPUT.ENABLE;
  FOR i IN 1195610..1195620 LOOP 
    --ODS.PR$UPD_PKK_EMAIL(i);
    UPD_PKK.PR$UPD_PKK_EMAIL(i);
    DBMS_OUTPUT.PUT_LINE('Время '||TO_CHAR(systimestamp at time zone 'utc')||' - '||i);
  END LOOP;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_P_PKK_ADDRESS_HISTORY" (v_REQUEST_ID IN NUMBER DEFAULT -999, v_OBJECTS_ID IN NUMBER)
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицы с адресной историей
-- ==	ОПИСАНИЕ:	   
-- ========================================================================
-- ==	СОЗДАНИЕ:		  24.12.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	12.02.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================
AS    
  p_OBJECTS_ID NUMBER := v_OBJECTS_ID;
  f_Exist_REQUEST_ID NUMBER;

  start_time TIMESTAMP; 
  cnt_MODIF NUMBER;
  
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  
  start_time := systimestamp;
  
  IF v_REQUEST_ID^=-999 AND v_OBJECTS_ID IS NULL THEN --если параметр передали, ищем нужный OBJECTS_ID
    SELECT COUNT(*) INTO f_Exist_REQUEST_ID FROM dual 
      WHERE EXISTS(SELECT OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID);
    IF f_Exist_REQUEST_ID=1 THEN 
      SELECT NVL(OBJECTS_ID, -999) INTO p_OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID;
    END IF;
  END IF;
  
  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_ADDRESS_HISTORY tar
	USING (SELECT ADDRESS_ID
            ,OBJECTS_ID
            ,OBJECTS_TYPE 
            ,ADDRESS_AKT
            ,ADDRESS_CREATED
            ,CREATED_SOURCE
            ,CREATED_USER_ID
            ,CREATED_GROUP_ID
            ,CREATED_IPADR
            ,MODIFICATION_DATE
            ,MODIFICATION_SOURCE
            ,MODIFICATION_USER_ID
            ,MODIFICATION_GROUP_ID
            ,MODIFICATION_IPADR
            ,ADDRESS_HISTORY_ID
            ,ADDRESS_COMM 
          FROM ODS.VIEW_PKK_ADDRESS_HISTORY WHERE OBJECTS_ID=p_OBJECTS_ID 
            AND COALESCE(MODIFICATION_DATE, ADDRESS_CREATED) > SYSDATE-7
		) src
    ON (src.ADDRESS_HISTORY_ID = tar.ADDRESS_HISTORY_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        tar.ADDRESS_ID=src.ADDRESS_ID
        ,tar.OBJECTS_ID=src.OBJECTS_ID
        --,tar.OBJECTS_TYPE=src.OBJECTS_TYPE
        ,tar.ADDRESS_AKT=src.ADDRESS_AKT
        --,tar.ADDRESS_CREATED=src.ADDRESS_CREATED
        --,tar.CREATED_SOURCE=src.CREATED_SOURCE
        --,tar.CREATED_USER_ID=src.CREATED_USER_ID
        --,tar.CREATED_GROUP_ID=src.CREATED_GROUP_ID
        --,tar.CREATED_IPADR=src.CREATED_IPADR
        ,tar.MODIFICATION_DATE=src.MODIFICATION_DATE
        ,tar.MODIFICATION_SOURCE=src.MODIFICATION_SOURCE
        ,tar.MODIFICATION_USER_ID=src.MODIFICATION_USER_ID
        ,tar.MODIFICATION_GROUP_ID=src.MODIFICATION_GROUP_ID
        ,tar.MODIFICATION_IPADR=src.MODIFICATION_IPADR
        --,tar.ADDRESS_HISTORY_ID=src.ADDRESS_HISTORY_ID
        --,tar.ADDRESS_COMM=src.ADDRESS_COMM
        ,tar.DATE_UPD=SYSDATE
        ,tar.ADDRESS_RANK=1
        ,tar.OBJECTS_RANK=1
      WHERE NOT ( NVL(tar.ADDRESS_ID, -1) = NVL(src.ADDRESS_ID, -1)
        AND NVL(tar.ADDRESS_AKT, -1) = NVL(src.ADDRESS_AKT, -1)
        AND NVL(tar.MODIFICATION_DATE, TO_DATE('01-01-1900', 'dd-mm-yyyy')) = NVL(src.MODIFICATION_DATE, TO_DATE('01-01-1900', 'dd-mm-yyyy'))
        AND NVL(tar.MODIFICATION_USER_ID, -1) = NVL(src.MODIFICATION_USER_ID, -1)
        AND NVL(tar.MODIFICATION_GROUP_ID, -1) = NVL(src.MODIFICATION_GROUP_ID, -1)
        AND NVL(tar.MODIFICATION_IPADR, -1) = NVL(src.MODIFICATION_IPADR, -1)
        )
	WHEN NOT MATCHED THEN 
	--вставляем новое
	INSERT (	tar.ADDRESS_ID
            ,tar.OBJECTS_ID, tar.OBJECTS_TYPE
            ,tar.ADDRESS_AKT, tar.ADDRESS_CREATED
            ,tar.CREATED_SOURCE, tar.CREATED_USER_ID,tar.CREATED_GROUP_ID,tar.CREATED_IPADR
            ,tar.MODIFICATION_DATE, tar.MODIFICATION_SOURCE, tar.MODIFICATION_USER_ID, tar.MODIFICATION_GROUP_ID
            ,tar.MODIFICATION_IPADR, tar.ADDRESS_HISTORY_ID, tar.ADDRESS_COMM
            ,tar.ADDRESS_RANK, tar.OBJECTS_RANK
            ,tar.DATE_UPD)
	VALUES (src.ADDRESS_ID
            ,src.OBJECTS_ID, src.OBJECTS_TYPE
            ,src.ADDRESS_AKT, src.ADDRESS_CREATED
            ,src.CREATED_SOURCE, src.CREATED_USER_ID, src.CREATED_GROUP_ID, src.CREATED_IPADR
            ,src.MODIFICATION_DATE, src.MODIFICATION_SOURCE, src.MODIFICATION_USER_ID, src.MODIFICATION_GROUP_ID
            ,src.MODIFICATION_IPADR, src.ADDRESS_HISTORY_ID, src.ADDRESS_COMM
            ,1, 1
            ,SYSDATE)
	;	
  
  cnt_MODIF := SQL%ROWCOUNT;
  
  ODS.PR$INS_LOG ('PR$UPD_P_PKK_ADDRESS_HISTORY', start_time, 'ODS.PKK_ADDRESS_HISTORY', 'OK', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_P_PKK_ADDRESS_HISTORY', start_time, 'ODS.PKK_ADDRESS_HISTORY', 'ERR', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_P_PKK_C_CREDIT_INFO" (v_REQUEST_ID NUMBER DEFAULT -999, v_OBJECTS_ID IN NUMBER)
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицы с адресной историей
-- ==	ОПИСАНИЕ:	   
-- ========================================================================
-- ==	СОЗДАНИЕ:		  26.01.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	26.01.2016 (ТРАХАЧЕВ В.В.)
/*    PKK_C_CREDIT_INFO:24.12.06.          От 26.01.16 07:20:00,463908000 UTC до 26.01.16 07:25:02,700445000 UTC
      PKK_C_CREDIT_INFO:27.12.06.          От 27.01.16 01:25:08,570565000 UTC до 27.01.16 01:26:15,846299000 UTC
      PKK_C_CREDIT_INFO:27.12.06-27.03.07. От 27.01.16 01:28:10,962313000 UTC до 27.01.16 01:32:57,735540000 UTC
      PKK_C_CREDIT_INFO:26.03.07-26.03.08. От 27.01.16 01:50:24,783232000 UTC до 27.01.16 02:03:05,311283000 UTC
      PKK_C_CREDIT_INFO:04.04.07-29.12.09. От 27.01.16 02:06:31,738509000 UTC до 27.01.16 03:32:31,226826000 UTC
      PKK_C_CREDIT_INFO:24.12.09-19.09.12. От 27.01.16 03:35:27,077796000 UTC до 27.01.16 04:42:21,999533000 UTC
      PKK_C_CREDIT_INFO:15.09.12-12.06.15. От 27.01.16 04:49:50,880758000 UTC до 27.01.16 07:15:47,634665000 UTC
      PKK_C_CREDIT_INFO:11.06.15-06.04.16. От 27.01.16 08:09:35,034905000 UTC до 27.01.16 11:25:39,611323000 UTC
      PKK_C_CREDIT_INFO:26.01.16-05.02.16. От 27.01.16 23:35:34,872914000 UTC до 28.01.16 04:49:11,373125000 UTC
      PKK_C_CREDIT_INFO:27.01.16-30.01.16. От 28.01.16 05:39:37,504957000 UTC до 28.01.16 08:12:29,366184000 UTC
*/ 
-- ========================================================================

AS    
  p_OBJECTS_ID NUMBER := v_OBJECTS_ID;
  f_Exist_REQUEST_ID NUMBER;
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp;
  
  IF v_REQUEST_ID^=-999 AND v_OBJECTS_ID IS NULL THEN --если параметр передали, ищем нужный OBJECTS_ID
    SELECT COUNT(*) INTO f_Exist_REQUEST_ID FROM dual 
      WHERE EXISTS(SELECT OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID);
    IF f_Exist_REQUEST_ID=1 THEN 
      SELECT NVL(OBJECTS_ID, -999) INTO p_OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID;
    END IF;
  END IF;
   
  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_C_CREDIT_INFO_SNA tar
	USING (SELECT DISTINCT cci.REQUEST_ID
			,crid.OBJECTS_ID AS PERSON_ID
			,crid.CREATED_DATE
			,crid.TYPE_REQUEST_ID
			,cci.MODIFICATION_DATE 
			,cci.SUMMA_DELAY --сумма просрочки (для определения ПЗ >= 400 р)
			,cci.BASE_DOLG --Остаток основного долга
			,cci.SUMMA_BASE_DOLG --доп. поле
			,cci.UNUSED_LIMIT -- неиспользованные лимит кредита
			,cci.CNT_DELAY_15 --Количество просрочек до 15 дней
			,cci.CNT_DELAY_30 --Количество просрочек от 15 до 30 дней
			,cci.CNT_DELAY_60 --Количество просрочек от 30 до 60 дней
			,cci.CNT_DELAY_90 --Количество просрочек от 60 до 90 дней
			,cci.CNT_DELAY_180 --Количество просрочек от 90 до 180 дней
			,cci.CNT_DELAY_365 --Количество просрочек от 180 до 365 дней
			,cci.CNT_DELAY_MORE --Количество просрочек свыше 365 дней
			,cci.CONTRACT_STATUS_CODE --Статус кредитной истории
			,cci.DATE_CLOSE --Дата фактического закрытия кредита РБО.
			,cci.DEFOLT
			,cci.DELAY_OLD -- 0 - ранее просрочек не было, 1 - ранее были просрочки
			,cci.PRDAY -- Количество дней просрочки
			,SUBSTR(cci.MOP_DELAY,1,24) as MOP_lst24 --строка для 24 месяцев
			,SUBSTR(cci.MOP_DELAY,1,6) as MOP_lst6 --строка для 6 месяцев
			,cci.MOP_DELAY --Строка кредитной истории
			,regexp_substr(cci.MOP_DELAY, '[45789]', 1) as pr_day_90_more --просрочка 90> за все время
			,regexp_substr(SUBSTR(cci.MOP_DELAY,1, 24), '[3]', 1) as pr_day_60_89 --просрочка 60-89 за последние 24 мес
			,regexp_substr(SUBSTR(cci.MOP_DELAY,1, 6), '[2]', 1) as pr_day_30_59 --просрочка 30-59 за последние 6 мес
			--,(CASE WHEN SUMMA_DELAY>=400 THEN 1 ELSE 0 END) as flag_PROSR --флаг просрочки >400 руб
			,(CASE WHEN SUMMA_DELAY>=200000 AND regexp_count(cci.MOP_DELAY, '1', 1) BETWEEN 0 AND 3 THEN 1 ELSE 0 END) as flag_PROSR --флаг просрочки >=200 тыс. руб
			,LAST_VALUE(cr_rs.REQUEST_RESTRUCTION_ID) OVER (PARTITION BY cr_rs.REQUEST_ID) as REQUEST_RESTRUCTION_ID_LAST --последний реквест который подлежал РС
			--,(CASE WHEN cci.BASE_DOLG>0 THEN regexp_count(cci.MOP_DELAY, '1', 1) ELSE -1 END) as cnt_payment_veb --кол-во платежей по текущей строке КИ
      ,regexp_count(cci.MOP_DELAY, '1', 1) as CNT_PAYMENT_VEB --кол-во платежей по текущей строке КИ
			,SFF.FN_IS_KK_RID(crid.REQUEST_ID) as KK -- флаг кредитной карты или овердрафта
			--,(SYSDATE) as RP_DEFOLT_DATE -- дата логирования
	FROM KREDIT.C_REQUEST@DBLINK_PKK crid 
	INNER JOIN KREDIT.C_CREDIT_INFO@DBLINK_PKK cci
		ON cci.REQUEST_ID=crid.REQUEST_ID
	LEFT OUTER JOIN KREDIT.C_REQUEST_RESTRUCTION@DBLINK_PKK cr_rs
		ON cci.REQUEST_ID=cr_rs.REQUEST_ID
	/*LEFT OUTER JOIN KREDIT.C_REQUEST_CREDIT@DBLINK_PKK cr_cr
		ON cci.REQUEST_ID=cr_cr.REQUEST_ID*/
	/*LEFT OUTER JOIN KREDIT.C_SCHEMS@DBLINK_PKK c_sch
		ON cr_cr.SCHEMS_ID=c_sch.SCHEMS_ID*/
	/*LEFT OUTER JOIN KREDIT.C_TYPE_CREDIT@DBLINK_PKK c_tp_cr --по факту название кредита здесь
		ON cr_cr.TYPE_CREDIT_ID=c_tp_cr.TYPE_CREDIT_ID*/
	WHERE 
    crid.OBJECTS_ID = p_OBJECTS_ID
      AND NOT EXISTS(SELECT REQUEST_ID FROM ODS.PKK_C_CREDIT_INFO_SNA WHERE REQUEST_ID=cci.REQUEST_ID
                    /*AND MODIFICATION_DATE=cci.MODIFICATION_DATE*/
                    AND SUMMA_DELAY =cci.SUMMA_DELAY
                    AND BASE_DOLG =cci.BASE_DOLG
                    AND SUMMA_BASE_DOLG=cci.SUMMA_BASE_DOLG
                    --AND DELAY_OLD=cci.DELAY_OLD
                    --AND PRDAY=cci.PRDAY
                    AND MOP_DELAY=cci.MOP_DELAY)
    AND COALESCE(cci.MODIFICATION_DATE, crid.CREATED_DATE) > SYSDATE-7
    --AND COALESCE(cci.MODIFICATION_DATE, crid.CREATED_DATE) BETWEEN TO_DATE('01-03-2016','dd-mm-yyyy') AND TO_DATE('10-03-2016','dd-mm-yyyy')
		--cci.REQUEST_ID BETWEEN 1 AND 10
		--AND NOT cci.MOP_DELAY LIKE '%1%'
		--cci.MODIFICATION_DATE > sysdate-10 AND
		--regexp_substr(NVL(SUBSTR(RTRIM(cci.MOP_DELAY,'0-'),-3,3), RTRIM(cci.MOP_DELAY,'0-')), '[2345789]', 1)>0
	--ORDER BY cci.REQUEST_ID
	
		) src
    ON (src.REQUEST_ID = tar.REQUEST_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        --tar.REQUEST_ID=src.REQUEST_ID
        --tar.PERSON_ID=src.PERSON_ID
        --,tar.CREATED_DATE=src.CREATED_DATE
        --,tar.TYPE_REQUEST_ID=src.TYPE_REQUEST_ID
        tar.MODIFICATION_DATE =src.MODIFICATION_DATE
        ,tar.SUMMA_DELAY =src.SUMMA_DELAY
        ,tar.BASE_DOLG =src.BASE_DOLG
        ,tar.SUMMA_BASE_DOLG=src.SUMMA_BASE_DOLG
        ,tar.UNUSED_LIMIT =src.UNUSED_LIMIT
        /*,tar.CNT_DELAY_15 =src.CNT_DELAY_15
        ,tar.CNT_DELAY_30=src.CNT_DELAY_30
        ,tar.CNT_DELAY_60=src.CNT_DELAY_60
        ,tar.CNT_DELAY_90=src.CNT_DELAY_90
        ,tar.CNT_DELAY_180=src.CNT_DELAY_180
        ,tar.CNT_DELAY_365 =src.CNT_DELAY_365
        ,tar.CNT_DELAY_MORE=src.CNT_DELAY_MORE*/
        ,tar.CONTRACT_STATUS_CODE =src.CONTRACT_STATUS_CODE
        ,tar.DATE_CLOSE =src.DATE_CLOSE
        ,tar.DEFOLT=src.DEFOLT
        ,tar.DELAY_OLD=src.DELAY_OLD
        ,tar.PRDAY=src.PRDAY
        ,tar.MOP_lst24 =src.MOP_lst24
        ,tar.MOP_lst6=src.MOP_lst6
        ,tar.MOP_DELAY=src.MOP_DELAY
        ,tar.pr_day_90_more=src.pr_day_90_more
        ,tar.pr_day_60_89=src.pr_day_60_89
        ,tar.pr_day_30_59=src.pr_day_30_59
        ,tar.flag_PROSR=src.flag_PROSR
        ,tar.REQUEST_RESTRUCTION_ID_LAST =src.REQUEST_RESTRUCTION_ID_LAST
        ,tar.cnt_payment_veb =src.cnt_payment_veb
        ,tar.KK =src.KK
        ,tar.DATE_UPD=SYSDATE
      WHERE NOT (NVL(tar.SUMMA_DELAY, -1) = NVL(src.SUMMA_DELAY , -1)
            AND NVL(tar.BASE_DOLG, -1) = NVL(src.BASE_DOLG, -1) AND NVL(tar.SUMMA_BASE_DOLG, -1)= NVL(src.SUMMA_BASE_DOLG, -1)
            --AND tar.DELAY_OLD=cci.DELAY_OLD
            AND NVL(tar.PRDAY, -1)=NVL(src.PRDAY, -1) AND NVL(tar.MOP_DELAY, '-')=NVL(src.MOP_DELAY, '-'))
	WHEN NOT MATCHED THEN 
	--вставляем новое
	INSERT (tar.REQUEST_ID,tar.PERSON_ID
          ,tar.CREATED_DATE,tar.TYPE_REQUEST_ID
          ,tar.MODIFICATION_DATE,tar.SUMMA_DELAY 
          ,tar.BASE_DOLG,tar.SUMMA_BASE_DOLG
          ,tar.UNUSED_LIMIT 
          ,tar.CNT_DELAY_15,tar.CNT_DELAY_30,tar.CNT_DELAY_60,tar.CNT_DELAY_90,tar.CNT_DELAY_180
          ,tar.CNT_DELAY_365,tar.CNT_DELAY_MORE,tar.CONTRACT_STATUS_CODE,tar.DATE_CLOSE 
          ,tar.DEFOLT,tar.DELAY_OLD,tar.PRDAY
          ,tar.MOP_lst24,tar.MOP_lst6,tar.MOP_DELAY
          ,tar.pr_day_90_more,tar.pr_day_60_89,tar.pr_day_30_59,tar.flag_PROSR
          ,tar.REQUEST_RESTRUCTION_ID_LAST,tar.CNT_PAYMENT_VEB ,tar.KK 
          ,tar.DATE_UPD)
	VALUES (src.REQUEST_ID,src.PERSON_ID
          ,src.CREATED_DATE,src.TYPE_REQUEST_ID
          ,src.MODIFICATION_DATE,src.SUMMA_DELAY 
          ,src.BASE_DOLG,src.SUMMA_BASE_DOLG
          ,src.UNUSED_LIMIT 
          ,src.CNT_DELAY_15,src.CNT_DELAY_30,src.CNT_DELAY_60,src.CNT_DELAY_90,src.CNT_DELAY_180
          ,src.CNT_DELAY_365,src.CNT_DELAY_MORE,src.CONTRACT_STATUS_CODE,src.DATE_CLOSE 
          ,src.DEFOLT,src.DELAY_OLD,src.PRDAY
          ,src.MOP_lst24,src.MOP_lst6,src.MOP_DELAY
          ,src.pr_day_90_more,src.pr_day_60_89,src.pr_day_30_59,src.flag_PROSR
          ,src.REQUEST_RESTRUCTION_ID_LAST,src.CNT_PAYMENT_VEB ,src.KK 
          ,SYSDATE)
	;	
  
  --информация для вывода при ручном обновлении
  cnt_MODIF := SQL%ROWCOUNT;
       
  ODS.PR$INS_LOG ('PR$UPD_P_PKK_C_CREDIT_INFO', start_time, 'ODS.PKK_C_CREDIT_INFO', 'OK', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
  COMMIT;

EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_P_PKK_C_CREDIT_INFO', start_time, 'ODS.PKK_C_CREDIT_INFO', 'ERR', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
        COMMIT; 
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_P_PKK_CONTACTS" (v_REQUEST_ID NUMBER DEFAULT -999, v_OBJECTS_ID IN NUMBER)
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицы с историей контактов
-- ==	ОПИСАНИЕ:	   
-- ========================================================================
-- ==	СОЗДАНИЕ:		  09.02.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	09.02.2016 (ТРАХАЧЕВ В.В.)
/*PKK_CONTACTS:09.02.16 02:50:28-09.02.16 07:50:28(1693). От 09.02.16 09:57:45 до 09.02.16 09:58:51 (1 индекс)
  PKK_CONTACTS:09.02.16 02:50:28-09.02.16 07:50:28(1839). От 09.02.16 10:33:37 до 09.02.16 10:35:17 (3 индекса)
  PKK_CONTACTS:09.02.16 03:50:28-09.02.16 07:50:28(1857). От 09.02.16 10:40:19 до 09.02.16 10:40:55
  PKK_CONTACTS:09.02.16 05:50:28-09.02.16 07:50:28(1791). От 09.02.16 10:42:02 до 09.02.16 10:42:38
  PKK_CONTACTS:09.02.16 04:50:28-10.02.16 06:00:32(2816). От 10.02.16 03:28:29 до 10.02.16 03:32:14
  PKK_CONTACTS:10.02.16 05:00:32-10.02.16 06:00:32(109). От 10.02.16 03:33:05 до 10.02.16 03:36:14
*/
-- ========================================================================
AS    
  p_OBJECTS_ID NUMBER := v_OBJECTS_ID;
  f_Exist_REQUEST_ID NUMBER;
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;  
  
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp;
  
  IF v_REQUEST_ID^=-999 AND v_OBJECTS_ID IS NULL THEN --если параметр передали, ищем нужный OBJECTS_ID
    SELECT COUNT(*) INTO f_Exist_REQUEST_ID FROM dual 
      WHERE EXISTS(SELECT OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID);
    IF f_Exist_REQUEST_ID=1 THEN 
      SELECT NVL(OBJECTS_ID, -999) INTO p_OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID;
    END IF;
  END IF;

  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_CONTACTS tar
	USING (SELECT * FROM ODS.VIEW_PKK_CONTACTS WHERE OBJECTS_ID = p_OBJECTS_ID
            AND COALESCE(CONTACT_CREATED, CONTACT_MODIFICATION) > SYSDATE-7
        ) src
    ON (src.CONTACT_ID = tar.CONTACT_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET --tar.CONTACT_ID=src.CONTACT_ID
        tar.OBJECTS_ID=src.OBJECTS_ID
        ,tar.OB_ID=src.OB_ID
        ,tar.CONTACT_AKT=src.CONTACT_AKT
        ,tar.CONTACT_COMMENT=src.CONTACT_COMMENT
        --,tar.CONTACT_CREATED=src.CONTACT_CREATED
        ,tar.CONTACT_MODIFICATION=src.CONTACT_MODIFICATION
        ,tar.FAMILY_REL=src.FAMILY_REL
        ,tar.FAMILY_REL_NAME=src.FAMILY_REL_NAME
        ,tar.FAMILY_REL_STATUS=src.FAMILY_REL_STATUS
        ,tar.FAMILY_SEQ_ID=src.FAMILY_SEQ_ID
        ,tar.DATE_UPD=SYSDATE
      WHERE NOT ( NVL(tar.OBJECTS_ID, -1) = NVL(src.OBJECTS_ID, -1)
        AND NVL(tar.OB_ID, -1) = NVL(src.OB_ID, -1)
        --AND NVL(tar.MODIFICATION_DATE, TO_DATE('01-01-1900', 'dd-mm-yyyy')) = NVL(src.MODIFICATION_DATE, TO_DATE('01-01-1900', 'dd-mm-yyyy'))
        AND NVL(tar.FAMILY_REL, -1) = NVL(src.FAMILY_REL, -1)
        AND NVL(tar.CONTACT_COMMENT, '-') = NVL(src.CONTACT_COMMENT, '-')
        AND NVL(tar.CONTACT_AKT, -1) = NVL(src.CONTACT_AKT, -1) )
	WHEN NOT MATCHED THEN 
	--вставляем новое
	INSERT ( tar.CONTACT_ID
          ,tar.OBJECTS_ID,tar.OB_ID,tar.CONTACT_AKT
          ,tar.CONTACT_COMMENT, tar.CONTACT_CREATED, tar.CONTACT_MODIFICATION
          ,tar.FAMILY_REL, tar.FAMILY_REL_NAME, tar.FAMILY_REL_STATUS, tar.FAMILY_SEQ_ID, DATE_UPD)
	VALUES (src.CONTACT_ID
          ,src.OBJECTS_ID, src.OB_ID, src.CONTACT_AKT
          ,src.CONTACT_COMMENT, src.CONTACT_CREATED, src.CONTACT_MODIFICATION
          ,src.FAMILY_REL, src.FAMILY_REL_NAME, src.FAMILY_REL_STATUS, src.FAMILY_SEQ_ID, SYSDATE)
	;	
  
  --информация для вывода при ручном обновлении
  cnt_MODIF := SQL%ROWCOUNT;
  
  ODS.PR$INS_LOG ('PR$UPD_P_PKK_CONTACTS', start_time, 'ODS.PKK_CONTACTS', 'OK', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
  COMMIT;

EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_P_PKK_CONTACTS', start_time, 'ODS.PKK_CONTACTS', 'ERR', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
        COMMIT;  
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_P_PKK_C_REQUEST_SNA" (v_REQUEST_ID NUMBER DEFAULT -999)
-- ========================================================================
-- ==	ПРОЦЕДУРА    Обновление таблицы PKK_C_REQUEST_SAS
-- ==	ОПИСАНИЕ:	   Основные данные по заявке (последние)
-- ========================================================================
-- ==	СОЗДАНИЕ:		  21.01.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	12.02.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================
AS    
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  
  start_time := systimestamp;
  
  /*IF v_REQUEST_ID^=-999 THEN --если параметр передали, ищем нужный OBJECTS_ID
    SELECT COUNT(*) INTO f_Exist_REQUEST_ID FROM dual 
      WHERE EXISTS(SELECT OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID);
    IF f_Exist_REQUEST_ID=1 THEN 
      SELECT NVL(OBJECTS_ID, -999) INTO v_OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID;
    END IF;
  END IF;*/
     
  --ОБНОВЛЕНИЕ
MERGE INTO ODS.PKK_C_REQUEST_SNA tar
	USING (SELECT * FROM ODS.VIEW_PKK_C_REQUEST_SNA WHERE REQUEST_ID = v_REQUEST_ID
		) src
    ON (src.REQUEST_ID = tar.REQUEST_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        --tar.REQUEST_ID=tar.REQUEST_ID
        --tar.PARENT_ID=src.PARENT_ID
        tar.TYPE_REQUEST_ID=src.TYPE_REQUEST_ID
        ,tar.STATUS_ID=src.STATUS_ID
        ,tar.OBJECTS_ID=src.OBJECTS_ID
        ,tar.OBJECTS_TYPE=src.OBJECTS_TYPE
        --,tar.CREATED_USER_ID=src.CREATED_USER_ID
        --,tar.CREATED_GROUP_ID=src.CREATED_GROUP_ID
        --,tar.CREATED_DATE=src.CREATED_DATE
        ,tar.MODIFICATION_DATE_REQUEST=src.MODIFICATION_DATE_REQUEST
        --,tar.REQUEST_UNIQUE_CODE=src.REQUEST_UNIQUE_CODE
        ,tar.SCORE_TREE_ROUTE_ID=src.SCORE_TREE_ROUTE_ID
        ,tar.REQUEST_REACT_ID_MT=src.REQUEST_REACT_ID_MT
        ,tar.REQUEST_REACT_ID_CK=src.REQUEST_REACT_ID_CK
        ,tar.REQUEST_REACT_ID_LAST=src.REQUEST_REACT_ID_LAST
        ,tar.REQUEST_CREDIT_ID_LAST=src.REQUEST_CREDIT_ID_LAST
        ,tar.REQUEST_INFO_ID_LAST=src.REQUEST_INFO_ID_LAST
        ,tar.MODIFICATION_DATE_INFO=src.MODIFICATION_DATE_INFO
        ,tar.DECLARANT_TYPE=src.DECLARANT_TYPE
        ,tar.FIO_ID=src.FIO_ID
        ,tar.SEX_ID=src.SEX_ID
        ,tar.BDATE=src.BDATE
        ,tar.BADR_ID=src.BADR_ID
        ,tar.MDOC_ID=src.MDOC_ID
        ,tar.MADR_ID=src.MADR_ID
        ,tar.LADR_ID=src.LADR_ID
        ,tar.DADR_ID=src.DADR_ID
        ,tar.REACT_DATE=src.REACT_DATE
        ,tar.REACT_COMMENT=src.REACT_COMMENT
        ,tar.REQUEST_START_PATH=src.REQUEST_START_PATH
        ,tar.CANCEL_CODE=src.CANCEL_CODE
        ,tar.REQUEST_OLD_STATUS_ID=src.REQUEST_OLD_STATUS_ID
        ,tar.REACT_USER_ID=src.REACT_USER_ID
        ,tar.MODIFICATION_DATE_CREDIT=src.MODIFICATION_DATE_CREDIT
        ,tar.SCHEMS_ID=src.SCHEMS_ID
        ,tar.TYPE_CREDIT_ID=src.TYPE_CREDIT_ID
        ,tar.ORG_PARTNER_ID=src.ORG_PARTNER_ID
        ,tar.ORG_PARTNER_CODE=src.ORG_PARTNER_CODE
        --,tar.CURRENCY_ID=src.CURRENCY_ID
        ,tar.PERIOD=src.PERIOD
        ,tar.PERCENT=src.PERCENT
        ,tar.PATH_CODE=src.PATH_CODE
        ,tar.SUMMA=src.SUMMA
        ,tar.SUMMA_DECL=src.SUMMA_DECL
        ,tar.SUMMA_ANN=src.SUMMA_ANN
        ,tar.SUMMA_FULL=src.SUMMA_FULL
        ,tar.RETAIL_PRODUCT_GROUPS_ID=src.RETAIL_PRODUCT_GROUPS_ID
        ,tar.CREDIT_JUR_CONTRACT=src.CREDIT_JUR_CONTRACT
        ,tar.CREDIT_ID=src.CREDIT_ID
        ,tar.DAY_PAY=src.DAY_PAY
        ,tar.DATE_UPD=SYSDATE
	WHEN NOT MATCHED THEN 
	--вставляем новое 
	INSERT ( tar.REQUEST_ID
          ,tar.PARENT_ID
          ,tar.TYPE_REQUEST_ID,tar.STATUS_ID
          ,tar.OBJECTS_ID,tar.OBJECTS_TYPE
          ,tar.CREATED_USER_ID,tar.CREATED_GROUP_ID
          ,tar.CREATED_DATE,tar.MODIFICATION_DATE_REQUEST
          ,tar.REQUEST_UNIQUE_CODE
          ,tar.SCORE_TREE_ROUTE_ID
          ,tar.REQUEST_REACT_ID_MT,tar.REQUEST_REACT_ID_CK,tar.REQUEST_REACT_ID_LAST
          ,tar.REQUEST_CREDIT_ID_LAST,tar.REQUEST_INFO_ID_LAST,tar.MODIFICATION_DATE_INFO
          ,tar.DECLARANT_TYPE
          ,tar.FIO_ID,tar.SEX_ID,tar.BDATE
          ,tar.BADR_ID,tar.MDOC_ID,tar.MADR_ID,tar.LADR_ID,tar.DADR_ID
          ,tar.REACT_DATE
          ,tar.REACT_COMMENT
          ,tar.REQUEST_START_PATH
          ,tar.CANCEL_CODE
          ,tar.REQUEST_OLD_STATUS_ID
          ,tar.REACT_USER_ID
          ,tar.MODIFICATION_DATE_CREDIT
          ,tar.SCHEMS_ID,tar.TYPE_CREDIT_ID
          ,tar.ORG_PARTNER_ID,tar.ORG_PARTNER_CODE
          ,tar.CURRENCY_ID,tar.PERIOD,tar.PERCENT
          ,tar.PATH_CODE
          ,tar.SUMMA,tar.SUMMA_DECL,tar.SUMMA_ANN,tar.SUMMA_FULL
          ,tar.RETAIL_PRODUCT_GROUPS_ID
          ,tar.CREDIT_JUR_CONTRACT,tar.CREDIT_ID,tar.DAY_PAY
          ,tar.DATE_UPD
          )
	VALUES (src.REQUEST_ID
          ,src.PARENT_ID
          ,src.TYPE_REQUEST_ID,src.STATUS_ID
          ,src.OBJECTS_ID,src.OBJECTS_TYPE
          ,src.CREATED_USER_ID,src.CREATED_GROUP_ID
          ,src.CREATED_DATE,src.MODIFICATION_DATE_REQUEST
          ,src.REQUEST_UNIQUE_CODE
          ,src.SCORE_TREE_ROUTE_ID
          ,src.REQUEST_REACT_ID_MT,src.REQUEST_REACT_ID_CK,src.REQUEST_REACT_ID_LAST
          ,src.REQUEST_CREDIT_ID_LAST,src.REQUEST_INFO_ID_LAST,src.MODIFICATION_DATE_INFO
          ,src.DECLARANT_TYPE
          ,src.FIO_ID,src.SEX_ID,src.BDATE
          ,src.BADR_ID,src.MDOC_ID,src.MADR_ID,src.LADR_ID,src.DADR_ID
          ,src.REACT_DATE
          ,src.REACT_COMMENT
          ,src.REQUEST_START_PATH
          ,src.CANCEL_CODE
          ,src.REQUEST_OLD_STATUS_ID
          ,src.REACT_USER_ID
          ,src.MODIFICATION_DATE_CREDIT
          ,src.SCHEMS_ID,src.TYPE_CREDIT_ID
          ,src.ORG_PARTNER_ID,src.ORG_PARTNER_CODE
          ,src.CURRENCY_ID,src.PERIOD,src.PERCENT
          ,src.PATH_CODE
          ,src.SUMMA,src.SUMMA_DECL,src.SUMMA_ANN,src.SUMMA_FULL
          ,src.RETAIL_PRODUCT_GROUPS_ID
          ,src.CREDIT_JUR_CONTRACT,src.CREDIT_ID,src.DAY_PAY
          ,SYSDATE)
	;	
  
  cnt_MODIF := SQL%ROWCOUNT;

  ODS.PR$INS_LOG ('PR$UPD_P_PKK_C_REQUEST_SNA', start_time, 'ODS.PKK_C_REQUEST_SNA', 'OK', SQLERRM, cnt_MODIF, v_REQUEST_ID, 'REQUEST_ID');
  COMMIT;

EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_P_PKK_C_REQUEST_SNA', start_time, 'ODS.PKK_C_REQUEST_SNA', 'ERR', SQLERRM, cnt_MODIF, v_REQUEST_ID, 'REQUEST_ID');
        COMMIT; 
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_P_PKK_DOCUMENTS_HISTORY" (v_REQUEST_ID NUMBER DEFAULT -999, v_OBJECTS_ID IN NUMBER)
-- ========================================================================
-- ==	ПРОЦЕДУРА    Обновление таблицы DOCUMENTS_HISTORY
-- ==	ОПИСАНИЕ:	   История документов по физикам
-- ========================================================================
-- ==	СОЗДАНИЕ:		  30.12.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	12.02.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================
AS    
  p_OBJECTS_ID NUMBER := v_OBJECTS_ID;
  f_Exist_REQUEST_ID NUMBER;
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;  
  
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp;

  IF v_REQUEST_ID^=-999 AND v_OBJECTS_ID IS NULL THEN --если параметр передали, ищем нужный OBJECTS_ID
    SELECT COUNT(*) INTO f_Exist_REQUEST_ID FROM dual 
      WHERE EXISTS(SELECT OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID);
    IF f_Exist_REQUEST_ID=1 THEN 
      SELECT NVL(OBJECTS_ID, -999) INTO p_OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID;
    END IF;
  END IF;

  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_DOCUMENTS_HISTORY_INFO tar
	USING (SELECT * FROM ODS.VIEW_PKK_DOCUMENTS WHERE OBJECTS_ID = p_OBJECTS_ID
              AND COALESCE(MODIFICATION_DATE, DOCUMENTS_CREATED) > SYSDATE-7
		) src
    ON (src.DOCUMENTS_HISTORY_ID = tar.DOCUMENTS_HISTORY_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        tar.DOCUMENTS_ID=src.DOCUMENTS_ID
        ,tar.OBJECTS_ID=src.OBJECTS_ID
        /*,tar.OBJECTS_TYPE=src.OBJECTS_TYPE
        ,tar.DOCUMENTS_AKT=src.DOCUMENTS_AKT*/
        /*,tar.DOCUMENTS_CREATED=src.DOCUMENTS_CREATED
        ,tar.CREATED_SOURCE=src.CREATED_SOURCE 
        ,tar.CREATED_USER_ID=src.CREATED_USER_ID 
        ,tar.CREATED_GROUP_ID=src.CREATED_GROUP_ID 
        ,tar.CREATED_IPADR=src.CREATED_IPADR*/
        ,tar.MODIFICATION_DATE=src.MODIFICATION_DATE
        ,tar.MODIFICATION_SOURCE=src.MODIFICATION_SOURCE
        ,tar.MODIFICATION_USER_ID=src.MODIFICATION_USER_ID
        ,tar.MODIFICATION_GROUP_ID=src.MODIFICATION_GROUP_ID
        ,tar.MODIFICATION_IPADR=src.MODIFICATION_IPADR
        --,tar.DOCUMENTS_HISTORY_ID=src.DOCUMENTS_HISTORY_ID
        ,tar.DOCUMENTS_TYPE=src.DOCUMENTS_TYPE
        ,tar.DOCUMENTS_NAME=src.DOCUMENTS_NAME
        ,tar.DOCUMENTS_SERIAL=src.DOCUMENTS_SERIAL
        ,tar.DOCUMENTS_NUMBER=src.DOCUMENTS_NUMBER
        --,tar.DOCUMENTS_ORGS=src.DOCUMENTS_ORGS
        ,tar.DOCUMENTS_DATE=src.DOCUMENTS_DATE
        --,tar.OBJECTS_TYPE_DOC=src.OBJECTS_TYPE_DOC
        ,tar.DATE_UPD=SYSDATE
        ,tar.DOCUMENTS_RANK=1
        ,tar.OBJECTS_RANK=1
      WHERE NOT ( NVL(tar.OBJECTS_ID, -1) = NVL(src.OBJECTS_ID, -1)
        AND NVL(tar.DOCUMENTS_AKT, -1) = NVL(src.DOCUMENTS_AKT, -1)
        --AND NVL(tar.MODIFICATION_DATE, TO_DATE('01-01-1900', 'dd-mm-yyyy')) = NVL(src.MODIFICATION_DATE, TO_DATE('01-01-1900', 'dd-mm-yyyy'))
        AND NVL(tar.DOCUMENTS_SERIAL, '-') = NVL(src.DOCUMENTS_SERIAL, '-')
        AND NVL(tar.DOCUMENTS_NUMBER, '-') = NVL(src.DOCUMENTS_NUMBER, '-')
        AND NVL(tar.DOCUMENTS_ORGS, -1) = NVL(src.DOCUMENTS_ORGS, -1)
        )
	WHEN NOT MATCHED THEN 
	--вставляем новое 
	INSERT (	tar.DOCUMENTS_ID
            ,tar.OBJECTS_ID, tar.OBJECTS_TYPE
            ,tar.DOCUMENTS_AKT, tar.DOCUMENTS_CREATED
            ,tar.CREATED_SOURCE, tar.CREATED_USER_ID, tar.CREATED_GROUP_ID, tar.CREATED_IPADR
            ,tar.MODIFICATION_DATE
            ,tar.MODIFICATION_SOURCE, tar.MODIFICATION_USER_ID, tar.MODIFICATION_GROUP_ID, tar.MODIFICATION_IPADR
            ,tar.DOCUMENTS_HISTORY_ID
            ,tar.DOCUMENTS_TYPE, tar.DOCUMENTS_NAME
            ,tar.DOCUMENTS_SERIAL, tar.DOCUMENTS_NUMBER
            ,tar.DOCUMENTS_ORGS, tar.DOCUMENTS_DATE, tar.OBJECTS_TYPE_DOC
            ,tar.DATE_UPD
            ,tar.DOCUMENTS_RANK, tar.OBJECTS_RANK
          )
	VALUES (src.DOCUMENTS_ID
            ,src.OBJECTS_ID, src.OBJECTS_TYPE
            ,src.DOCUMENTS_AKT, src.DOCUMENTS_CREATED
            ,src.CREATED_SOURCE, src.CREATED_USER_ID, src.CREATED_GROUP_ID, src.CREATED_IPADR
            ,src.MODIFICATION_DATE
            ,src.MODIFICATION_SOURCE, src.MODIFICATION_USER_ID, src.MODIFICATION_GROUP_ID, src.MODIFICATION_IPADR
            ,src.DOCUMENTS_HISTORY_ID
            ,src.DOCUMENTS_TYPE, src.DOCUMENTS_NAME
            ,src.DOCUMENTS_SERIAL, src.DOCUMENTS_NUMBER
            ,src.DOCUMENTS_ORGS, src.DOCUMENTS_DATE, src.OBJECTS_TYPE_DOC
            ,SYSDATE
            ,1, 1)
	;

  cnt_MODIF := SQL%ROWCOUNT;
  
  ODS.PR$INS_LOG ('PR$UPD_P_PKK_DOCUMENTS_HISTORY', start_time, 'ODS.PKK_DOCUMENTS_HISTORY_INFO', 'OK', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
  COMMIT;

EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_P_PKK_DOCUMENTS_HISTORY', start_time, 'ODS.PKK_DOCUMENTS_HISTORY_INFO', 'ERR', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_P_PKK_EMAIL" (v_REQUEST_ID NUMBER DEFAULT -999, v_OBJECTS_ID IN NUMBER)
-- ========================================================================
-- ==	ПРОЦЕДУРА    Обновление таблицы EMAIL
-- ==	ОПИСАНИЕ:	   История Емайл адресов
-- ========================================================================
-- ==	СОЗДАНИЕ:		  31.12.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	11.02.2016 (ТРАХАЧЕВ В.В.)
/*PKK_EMAIL:20.01.16 23:18:06-11.02.16 04:30:43(13268). От 10.02.16 22:31:39,765712000 UTC до 11.02.16 01:32:27
  PKK_EMAIL:10.02.16 20:30:43-11.02.16 04:30:43(222). От 10.02.16 22:45:05,482114000 UTC до 11.02.16 01:45:06
  
*/
-- ========================================================================
AS    
  p_OBJECTS_ID NUMBER := v_OBJECTS_ID ;
  f_Exist_REQUEST_ID NUMBER;
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER; 
  
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp;
  
  IF v_REQUEST_ID^=-999 AND v_OBJECTS_ID IS NULL THEN --если параметр передали, ищем нужный OBJECTS_ID
    SELECT COUNT(*) INTO f_Exist_REQUEST_ID FROM dual 
      WHERE EXISTS(SELECT OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID);
    IF f_Exist_REQUEST_ID=1 THEN 
      SELECT NVL(OBJECTS_ID, -999) INTO p_OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID;
    END IF;
  END IF;
 
      
  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_EMAIL tar
	USING (SELECT * FROM ODS.VIEW_PKK_EMAIL WHERE OBJECTS_ID = p_OBJECTS_ID
		) src
    ON (src.EMAIL_ID = tar.EMAIL_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        --tar.EMAIL_ID=src.EMAIL_ID
        tar.OBJECTS_ID=src.OBJECTS_ID
        /*,tar.OBJECTS_TYPE=src.OBJECTS_TYPE*/
        ,tar.EMAIL_AKT=src.EMAIL_AKT
        /*,tar.EMAIL_CREATED=src.EMAIL_CREATED
        ,tar.CREATED_SOURCE=src.CREATED_SOURCE
        ,tar.CREATED_USER_ID=src.CREATED_USER_ID
        ,tar.CREATED_GROUP_ID=src.CREATED_GROUP_ID
        ,tar.CREATED_IPADR=src.CREATED_IPADR*/
        ,tar.MODIFICATION_DATE=src.MODIFICATION_DATE
        /*,tar.MODIFICATION_SOURCE=src.MODIFICATION_SOURCE
        ,tar.MODIFICATION_USER_ID=src.MODIFICATION_USER_ID
        ,tar.MODIFICATION_GROUP_ID=src.MODIFICATION_GROUP_ID
        ,tar.MODIFICATION_IPADR=src.MODIFICATION_IPADR*/
        ,tar.DATE_UPD=SYSDATE
	WHEN NOT MATCHED THEN 
	--вставляем новое 
	INSERT (	tar.EMAIL_ID
            ,tar.OBJECTS_ID, tar.OBJECTS_TYPE
            ,tar.EMAIL
            ,tar.EMAIL_AKT, tar.EMAIL_CREATED
            ,tar.CREATED_SOURCE, tar.CREATED_USER_ID, tar.CREATED_GROUP_ID, tar.CREATED_IPADR
            ,tar.MODIFICATION_DATE
            ,tar.DATE_UPD
          )
	VALUES (src.EMAIL_ID
            ,src.OBJECTS_ID, src.OBJECTS_TYPE
            ,src.EMAIL
            ,src.EMAIL_AKT, src.EMAIL_CREATED
            ,src.CREATED_SOURCE, src.CREATED_USER_ID, src.CREATED_GROUP_ID, src.CREATED_IPADR
            ,src.MODIFICATION_DATE
            , SYSDATE)
	;

  --информация для вывода при ручном обновлении
  cnt_MODIF := SQL%ROWCOUNT;

  ODS.PR$INS_LOG ('PR$UPD_P_PKK_EMAIL', start_time, 'ODS.PKK_EMAIL', 'OK', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
  COMMIT;

EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_P_PKK_EMAIL', start_time, 'ODS.PKK_EMAIL', 'ERR', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_P_PKK_FAMILY" (v_REQUEST_ID NUMBER DEFAULT -999, v_OBJECTS_ID IN NUMBER)
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицы с историей семейных связей
-- ==	ОПИСАНИЕ:	   
-- ========================================================================
-- ==	СОЗДАНИЕ:		  09.02.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	09.02.2016 (ТРАХАЧЕВ В.В.)
/*PKK_FAMILY:09.02.16 07:06:17-09.02.16 07:50:28(7742). От 09.02.16 11:07:12 до 09.02.16 11:12:17
  PKK_FAMILY:09.02.16 11:06:45-10.02.16 06:00:32(11104). От 10.02.16 05:18:44 до 10.02.16 05:21:39
*/
-- ========================================================================
AS    
  p_OBJECTS_ID NUMBER := v_OBJECTS_ID ;
  f_Exist_REQUEST_ID NUMBER;
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER; 
  
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp;
  
  IF v_REQUEST_ID^=-999 AND v_OBJECTS_ID IS NULL THEN --если параметр передали, ищем нужный OBJECTS_ID
    SELECT COUNT(*) INTO f_Exist_REQUEST_ID FROM dual 
      WHERE EXISTS(SELECT OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID);
    IF f_Exist_REQUEST_ID=1 THEN 
      SELECT NVL(OBJECTS_ID, -999) INTO p_OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID;
    END IF;
  END IF;

  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_FAMILY tar 
	USING (SELECT * FROM ODS.VIEW_PKK_FAMILY WHERE OBJECTS_ID = p_OBJECTS_ID
            AND COALESCE(CONTACT_CREATED, CONTACT_MODIFICATION) > SYSDATE-7
      ) src
      ON (src.CONTACT_ID = tar.CONTACT_ID )
    WHEN MATCHED THEN
      --Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
      UPDATE SET --tar.CONTACT_ID=src.CONTACT_ID
          tar.OBJECTS_ID=src.OBJECTS_ID
          ,tar.OB_ID=src.OB_ID
          ,tar.CONTACT_AKT=src.CONTACT_AKT
          --,tar.CONTACT_CREATED=src.CONTACT_CREATED
          ,tar.CONTACT_MODIFICATION=src.CONTACT_MODIFICATION
          ,tar.FAMILY_REL=src.FAMILY_REL
          ,tar.FAMILY_REL_NAME=src.FAMILY_REL_NAME
          ,tar.FAMILY_REL_STATUS=src.FAMILY_REL_STATUS
          ,tar.FAMILY_SEQ_ID=src.FAMILY_SEQ_ID
          ,tar.DATE_UPD=SYSDATE
      WHERE NOT ( /*NVL(tar.OBJECTS_ID, -1) = NVL(src.OBJECTS_ID, -1)
        AND NVL(tar.OB_ID, -1) = NVL(src.OB_ID, -1)
        AND NVL(tar.CONTACT_MODIFICATION, TO_DATE('01-01-1900', 'dd-mm-yyyy')) = NVL(src.CONTACT_MODIFICATION, TO_DATE('01-01-1900', 'dd-mm-yyyy'))
        AND*/ NVL(tar.FAMILY_REL, -1) = NVL(src.FAMILY_REL, -1)
        AND NVL(tar.CONTACT_AKT, -1) = NVL(src.CONTACT_AKT, -1) )
    WHEN NOT MATCHED THEN 
    --вставляем новое
    INSERT ( tar.CONTACT_ID
            ,tar.OBJECTS_ID,tar.OB_ID,tar.CONTACT_AKT
            , tar.CONTACT_CREATED, tar.CONTACT_MODIFICATION
            ,tar.FAMILY_REL, tar.FAMILY_REL_NAME, tar.FAMILY_REL_STATUS, tar.FAMILY_SEQ_ID, DATE_UPD)
    VALUES (src.CONTACT_ID
            ,src.OBJECTS_ID, src.OB_ID, src.CONTACT_AKT
            , src.CONTACT_CREATED, src.CONTACT_MODIFICATION
            ,src.FAMILY_REL, src.FAMILY_REL_NAME, src.FAMILY_REL_STATUS, src.FAMILY_SEQ_ID, SYSDATE)
    ;	
  
  --информация для вывода при ручном обновлении
  cnt_MODIF := SQL%ROWCOUNT;
               
  ODS.PR$INS_LOG ('PR$UPD_P_PKK_FAMILY', start_time, 'ODS.PKK_FAMILY', 'OK', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_P_PKK_FAMILY', start_time, 'ODS.PKK_FAMILY', 'ERR', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_P_PKK_ADDRESS" (v_REQUEST_ID NUMBER DEFAULT -999)
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицу с данными из C_REQUEST
-- ==	ОПИСАНИЕ:	    Обновляем статусов и прочее для заявок
-- ========================================================================
-- ==	СОЗДАНИЕ:		  09.11.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	15.02.2016 (ТРАХАЧЕВ В.В.)
-- ==
/*
*/
-- ========================================================================
AS    
  last_ID_SFF NUMBER; -- последний существующий REQUEST_ID в SFF
  last_ID_PKK NUMBER; -- последний существующий REQUEST_ID в ПКК
  
  v_LADR_ID NUMBER := -999; -- ID адрес проживания
  v_MADR_ID NUMBER := -999; -- ID адрес пропиcки
  f_Exist_REQUEST_ID NUMBER;
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  
  start_time := systimestamp;

  IF v_REQUEST_ID^=-999 THEN --если параметр передали, ищем нужный OBJECTS_ID
    SELECT COUNT(*) INTO f_Exist_REQUEST_ID FROM dual 
      WHERE EXISTS(SELECT REQUEST_ID FROM KREDIT.C_REQUEST_INFO@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID);
 
     IF f_Exist_REQUEST_ID=1 THEN 
        SELECT NVL(LADR_ID, -999), NVL(MADR_ID, -999) 
            INTO v_LADR_ID,        v_MADR_ID 
            FROM KREDIT.C_REQUEST_INFO@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID
             AND REQUEST_INFO_ID=(SELECT MAX(REQUEST_INFO_ID) FROM KREDIT.C_REQUEST_INFO@DBLINK_PKK 
                                    WHERE REQUEST_ID = v_REQUEST_ID); 
    END IF;
  END IF;
  
  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_ADDRESS tar
	USING (SELECT
			adr.ADDRESS_ID
			,cntr.COUNTRIES_ISO2 as COUNTRY		--Страна
			,adr.REGIONS_UID
			,reg.REGIONS_NAMES					--Регион
			,adr.AREAS_UID    
			,are.AREAS_NAMES					--Район 
			,adr.CITIES_UID						--Id нас. пункта
			,cit.CITIES_NAMES					--НП
			,cit.CITIES_TYPE					--Тип НП ID
			,KLADR_SIT.SHOTNAME as SHOTNAME_CIT	--Тип НП 
			,adr.STREETS_UID					--ID типа улицы
			,str.STREETS_NAMES					--Улица 
			,str.STREETS_TYPE					--Тип улицы ID
			,KLADR_STR.SHOTNAME as SHOTNAME_STR	--Тип улицы 
			,adr.HOUSE							--Дом 
			,adr.BUILD							--Корпус 
			,adr.FLAT							--Квартира
			,adr.POSTOFFICE						--Индекс
			,adr.GEO_ID							--ID геоданных
			,geo.QUALITY_CODE					--Код качества при преобразовании исходных адресных данных для получения геокоординат
			,geo.GEO_LAT						--Широта
			,geo.GEO_LNG						--Долгота
			,geo.GEO_QC							--Точность определения преобразования адреса для получения координат
			,geo.ADDRESS_STR AS GEO_ADR			--Преобразованный адрес, по которому цеплялись координаты
			,geo.CREATED_DATE AS GEO_CREATED	--Дата создания кординат
		FROM CPD.ADDRESS@dblink_pkk adr 
		LEFT JOIN CPD.REGIONS_NAMES@dblink_pkk reg ON adr.REGIONS_UID = reg.REGIONS_UID
		LEFT JOIN CPD.AREAS_NAMES@dblink_pkk are ON adr.AREAS_UID = are.AREAS_UID
		LEFT JOIN CPD.CITIES_NAMES@dblink_pkk cit ON adr.CITIES_UID = cit.CITIES_UID
		LEFT JOIN CPD.STREETS_NAMES@dblink_pkk str ON adr.STREETS_UID = str.STREETS_UID
		LEFT JOIN CPD.COUNTRIES@dblink_pkk cntr ON adr.COUNTRIES_ID = cntr.COUNTRIES_ID
		LEFT JOIN CPD.KLADR_SOCR@dblink_pkk KLADR_SIT ON cit.CITIES_TYPE = KLADR_SIT.SOCR_ID
		LEFT JOIN CPD.KLADR_SOCR@dblink_pkk KLADR_STR ON str.STREETS_TYPE = KLADR_STR.SOCR_ID
		LEFT JOIN CPD.GEOCOORDINATES@dblink_pkk geo ON adr.GEO_ID = geo.ID
		WHERE 
      adr.ADDRESS_ID IN (v_LADR_ID, v_MADR_ID)
		) src
    ON (src.ADDRESS_ID = tar.ADDRESS_ID )
	WHEN MATCHED THEN
		--клюевые и неизменяемые поля не нужно обновлять. 
		UPDATE SET
			--tar.ADDRESS_ID=src.ADDRESS_ID
			tar.COUNTRY=src.COUNTRY
			,tar.REGIONS_UID=src.REGIONS_UID
			,tar.REGIONS_NAMES=src.REGIONS_NAMES
			,tar.AREAS_UID=src.AREAS_UID
			,tar.AREAS_NAMES=src.AREAS_NAMES
			,tar.CITIES_UID=src.CITIES_UID
			,tar.CITIES_NAMES=src.CITIES_NAMES
			,tar.CITIES_TYPE=src.CITIES_TYPE
			,tar.SHOTNAME_CIT=src.SHOTNAME_CIT
			,tar.STREETS_UID=src.STREETS_UID
			,tar.STREETS_NAMES=src.STREETS_NAMES
			,tar.STREETS_TYPE=src.STREETS_TYPE
			,tar.SHOTNAME_STR=src.SHOTNAME_STR
			,tar.HOUSE=src.HOUSE
			,tar.BUILD=src.BUILD
			,tar.FLAT=src.FLAT
			,tar.POSTOFFICE=src.POSTOFFICE
			,tar.GEO_ID=src.GEO_ID
			,tar.QUALITY_CODE=src.QUALITY_CODE
			,tar.GEO_LAT=src.GEO_LAT
			,tar.GEO_LNG=src.GEO_LNG
			,tar.GEO_QC=src.GEO_QC
			,tar.GEO_ADR=src.GEO_ADR
			,tar.GEO_CREATED=src.GEO_CREATED
      ,tar.DATE_UPD=SYSDATE
      WHERE NOT (tar.REGIONS_NAMES=src.REGIONS_NAMES AND tar.AREAS_NAMES=src.AREAS_NAMES 
            AND tar.CITIES_NAMES=src.CITIES_NAMES AND tar.STREETS_NAMES=src.STREETS_NAMES
            AND NVL(tar.HOUSE, '-')=NVL(src.HOUSE, '-') AND NVL(tar.BUILD, '-')=NVL(src.BUILD, '-') 
            AND NVL(tar.FLAT, '-')=NVL(src.FLAT, '-')
            AND tar.GEO_ID=src.GEO_ID AND tar.GEO_LAT=src.GEO_LAT AND tar.GEO_LNG=src.GEO_LNG)
	WHEN NOT MATCHED THEN 
	--вставляем новое
	INSERT (tar.ADDRESS_ID
			,tar.COUNTRY
			,tar.REGIONS_UID, tar.REGIONS_NAMES, tar.AREAS_UID, tar.AREAS_NAMES
			,tar.CITIES_UID, tar.CITIES_NAMES, tar.CITIES_TYPE, tar.SHOTNAME_CIT
			,tar.STREETS_UID, tar.STREETS_NAMES, tar.STREETS_TYPE, tar.SHOTNAME_STR
			,tar.HOUSE, tar.BUILD, tar.FLAT
			,tar.POSTOFFICE
			,tar.GEO_ID, tar.QUALITY_CODE, tar.GEO_LAT, tar.GEO_LNG
			,tar.GEO_QC, tar.GEO_ADR, tar.GEO_CREATED
      ,tar.DATE_UPD)
	VALUES (src.ADDRESS_ID
			,src.COUNTRY
			,src.REGIONS_UID, src.REGIONS_NAMES, src.AREAS_UID, src.AREAS_NAMES
			,src.CITIES_UID, src.CITIES_NAMES, src.CITIES_TYPE, src.SHOTNAME_CIT
			,src.STREETS_UID, src.STREETS_NAMES, src.STREETS_TYPE, src.SHOTNAME_STR
			,src.HOUSE, src.BUILD, src.FLAT
			,src.POSTOFFICE
			,src.GEO_ID, src.QUALITY_CODE, src.GEO_LAT, src.GEO_LNG
			,src.GEO_QC, src.GEO_ADR, src.GEO_CREATED
      ,SYSDATE)
	;	

  --информация для вывода при ручном обновлении
  cnt_MODIF := SQL%ROWCOUNT;
  
  p_ADD_INFO := 'ADDRESS_ID='||TO_CHAR(v_LADR_ID)||', '||TO_CHAR(v_MADR_ID);  
  ODS.PR$INS_LOG ('PR$UPD_P_PKK_ADDRESS', start_time, 'ODS.PKK_ADDRESS', 'OK', SQLERRM, cnt_MODIF, v_REQUEST_ID, p_ADD_INFO);
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_P_PKK_ADDRESS', start_time, 'ODS.PKK_ADDRESS', 'ERR', SQLERRM, cnt_MODIF, v_REQUEST_ID, p_ADD_INFO);
    COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_P_PKK_WORKS_INFO" (v_REQUEST_ID NUMBER DEFAULT -999, v_OBJECTS_ID IN NUMBER)
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицы с историей работы
-- ==	ОПИСАНИЕ:	   
-- ========================================================================
-- ==	СОЗДАНИЕ:		  08.02.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	12.02.2016 (ТРАХАЧЕВ В.В.)
/*

*/
-- ========================================================================
AS    
  p_OBJECTS_ID NUMBER := v_OBJECTS_ID ;
  f_Exist_REQUEST_ID NUMBER;
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;  
  
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp;
  
  IF v_REQUEST_ID^=-999 AND v_OBJECTS_ID IS NULL THEN --если параметр передали, ищем нужный OBJECTS_ID
    SELECT COUNT(*) INTO f_Exist_REQUEST_ID FROM dual 
      WHERE EXISTS(SELECT OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID);
    IF f_Exist_REQUEST_ID=1 THEN 
      SELECT NVL(OBJECTS_ID, -999) INTO p_OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID;
    END IF;
  END IF;

  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_WORKS_INFO tar
	USING (SELECT * FROM ODS.VIEW_PKK_WORKS_INFO WHERE OBJECTS_ID = p_OBJECTS_ID
              AND COALESCE(MODIFICATION_DATE, WORKS_CREATED) > SYSDATE-7
		) src
    ON (src.WORKS_HISTORY_ID = tar.WORKS_HISTORY_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        --tar.OBJECTS_ID=src.OBJECTS_ID
        tar.works_id=src.works_id
        --,tar.WORKS_HISTORY_ID=src.WORKS_HISTORY_ID
        ,tar.WORKS_SALARY=src.WORKS_SALARY
        ,tar.WORKS_STAG=src.WORKS_STAG
        ,tar.works_akt=src.works_akt
        ,tar.works_last=src.works_last
        --,tar.ORG_ID=src.ORG_ID
        ,tar.ORG_NAME=src.ORG_NAME
        ,tar.WORKS_POST=src.WORKS_POST
        ,tar.WORKS_POST_NAME=src.WORKS_POST_NAME
        ,tar.RELATION_PDL=src.RELATION_PDL
        --,tar.WORKS_CREATED=src.WORKS_CREATED
        ,tar.MODIFICATION_DATE=src.MODIFICATION_DATE
        --,tar.phone_work=src.phone_work, tar.phone_org=src.phone_org
        ,tar.DATE_UPD=SYSDATE
        ,tar.WORKS_RANK=1
        ,tar.OBJECTS_RANK=1
      WHERE NOT (NVL(tar.OBJECTS_ID, -1)=NVL(src.OBJECTS_ID, -1)
        AND NVL(tar.works_id, -1)=NVL(src.works_id, -1)
        AND NVL(tar.WORKS_SALARY, -1)=NVL(src.WORKS_SALARY, -1)
        AND NVL(tar.WORKS_STAG, '-')=NVL(src.WORKS_STAG, '-')
        AND NVL(tar.works_akt, -1)=NVL(src.works_akt, -1)
        AND NVL(tar.works_last, -1)=NVL(src.works_last, -1)
        AND NVL(tar.ORG_ID, -1)=NVL(src.ORG_ID, -1)
        AND NVL(tar.ORG_NAME, '-')=NVL(src.ORG_NAME, '-')
        AND NVL(tar.WORKS_POST, -1)=NVL(src.WORKS_POST, -1)
        AND NVL(tar.WORKS_POST_NAME, '-')=NVL(src.WORKS_POST_NAME, '-')
        --AND NVL(tar.RELATION_PDL, -1)=NVL(src.RELATION_PDL, -1)
        /*AND NVL(tar.phone_work, '-')=NVL(src.phone_work, '-')
        AND NVL(tar.phone_org, '-')=NVL(src.phone_org, '-')*/ )
	WHEN NOT MATCHED THEN 
	--вставляем новое
	INSERT ( tar.OBJECTS_ID, tar.works_id, tar.WORKS_HISTORY_ID
          ,tar.WORKS_SALARY, tar.WORKS_STAG,tar.works_akt,tar.works_last
          ,tar.ORG_ID,tar.ORG_NAME
          ,tar.WORKS_POST, tar.WORKS_POST_NAME,tar.RELATION_PDL
          ,tar.WORKS_CREATED,tar.MODIFICATION_DATE
          --,tar.phone_work,tar.phone_org
          , tar.DATE_UPD
          , tar.WORKS_RANK, tar.OBJECTS_RANK )
	VALUES (src.OBJECTS_ID, src.works_id, src.WORKS_HISTORY_ID
          ,src.WORKS_SALARY, src.WORKS_STAG,src.works_akt,src.works_last
          ,src.ORG_ID,src.ORG_NAME
          ,src.WORKS_POST, src.WORKS_POST_NAME,src.RELATION_PDL
          ,src.WORKS_CREATED,src.MODIFICATION_DATE
          --,src.phone_work,src.phone_org
          ,SYSDATE
          ,1, 1)
	;	
  
  cnt_MODIF := SQL%ROWCOUNT;
  
  ODS.PR$INS_LOG ('PR$UPD_P_PKK_WORKS_INFO', start_time, 'ODS.PKK_WORKS_INFO', 'OK', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_P_PKK_WORKS_INFO', start_time, 'ODS.PKK_WORKS_INFO', 'ERR', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
        COMMIT;
END;
  CREATE OR REPLACE TRIGGER "SNAUSER"."SNA_REQUEST_HISTORY_TRG_INS" BEFORE INSERT ON SNA_REQUEST_HISTORY 
--
FOR EACH ROW 
DECLARE

BEGIN
 
  <<COLUMN_SEQUENCES>>
  BEGIN
    IF :NEW.SNA_ID IS NULL THEN 
      SELECT SNA_REQUEST_HISTORY_SEQ.NEXTVAL INTO :NEW.SNA_ID FROM DUAL;
    END IF; 
  END COLUMN_SEQUENCES; 
  
  SNAUSER.PR$SNA_LOAD_INFO(:new.SNA_ID, :new.REQUEST_ID, :new.LOGIN, :new.ALERT_SERIES
        ,:new.SNA_DATE_BEGIN
        ,:new.SNA_DATE_END
        ,:new.SNA_TIME_LOAD
        ,:new.FLAG_INFO_EXISTS
        ,:new.OBJECTS_ID
        ,:new.CNT_NODE
        ,:new.CNT_LINK
        ,:new.CNT_OBJECTS_ID
        ,:new.CNT_FROD
        ,:new.CNT_BNK
        ,:new.CNT_DEFOLT
        ,:new.CNT_ALERT_PERSON
        ,:new.RESULT_MESSAGE);
  --COMMIT;
  
END;
ALTER TRIGGER "SNAUSER"."SNA_REQUEST_HISTORY_TRG_INS" ENABLE"
"
  CREATE OR REPLACE PROCEDURE "SNAUSER"."PR$SNA_LOAD_INFO" (v_SNA_ID IN NUMBER, v_REQUEST_ID IN NUMBER
            ,v_LOGIN IN VARCHAR2, v_ALERT_SERIES IN VARCHAR2
            ,v_SNA_DATE_BEGIN IN OUT NOCOPY DATE
            ,v_SNA_DATE_END IN OUT NOCOPY DATE
            ,v_SNA_TIME_LOAD IN OUT NOCOPY NUMBER
            ,v_FLAG_INFO_EXISTS IN OUT NOCOPY NUMBER
            ,v_OBJECTS_ID IN OUT NOCOPY NUMBER
            ,v_CNT_NODE IN OUT NOCOPY NUMBER
            ,v_CNT_LINK IN OUT NOCOPY NUMBER
            ,v_CNT_OBJECTS_ID IN OUT NOCOPY NUMBER
            ,v_CNT_FROD IN OUT NOCOPY NUMBER
            ,v_CNT_BNK IN OUT NOCOPY NUMBER
            ,v_CNT_DEFOLT IN OUT NOCOPY NUMBER
            ,v_CNT_ALERT_PERSON IN OUT NOCOPY NUMBER
            ,v_RESULT_MESSAGE IN OUT NOCOPY VARCHAR2)
----  ПРОЦЕДУРА сбора инфомрации для прорисовки сети по заявке
--==  
--==  
--==  
--=======================================================
  
IS 

  pragma autonomous_transaction; 
    
 /* p_FIODR VARCHAR2(380); -- ФИО и ДР физика для временного хранения при выборке персональных данных
  p_FIODR_AKT NUMBER(2);    -- актуальность физика для временного хранения при выборке персональных данных
  p_FIODR_DT DATE;
  p_DATE_RID DATE;        -- Дата заявки. Для отсечения будущих данных  */
  
  p_cnt_Level NUMBER := 0; --для идентификации уровня подтягивания информации
  
  --курсор с основной информацией по физику - модифицированный без таблицы PKK_PERSON_INFO    
  CURSOR cur_PERSON_NODE(p_cur_OBJECTS_ID NUMBER, p_cur_LEVEL NUMBER) IS
--INFO
SELECT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
      ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
      ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
      ,ROUTE_GRAPH, ATTRIBUTES
       FROM (
    SELECT DISTINCT 
      ph.OBJECTS_ID as OBJECTS_ID
      ,ph.OBJECTS_ID AS OBJECTS_ID_REL
      ,DECODE(ph.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
      ,DECODE(ph.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
      --Node 1
      ,snr.N1    AS N1
      ,snr.T1   AS T1
      ,snr.AKT1 AS AKT1
      ,snr.DT1 AS DT1
      --Node 2
      ,ph.PHONE AS N2
      ,'PHONE_MOBILE' AS T2
      ,ph.PHONES_AKT AS AKT2
      ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT2
      ,ph.PHONES_COMM
      --Node 3
      ,NULL       AS N3
      ,NULL       AS T3
      ,NULL       AS AKT3
      ,NULL       AS DT3
      --Node 4
      ,NULL     AS N4
      ,NULL     AS T4
      ,NULL     AS AKT4
      ,NULL     AS DT4
      --Node 5
      ,NULL     AS N5
      ,NULL     AS T5
      ,NULL     AS AKT5
      ,NULL     AS DT5
      ,'N1-N2'AS ROUTE_GRAPH
      ,'PHONES_ID='||TO_CHAR(ph.PHONES_ID) AS ATTRIBUTES
      /*,DENSE_RANK() OVER(PARTITION BY 
                        ph.OBJECTS_ID       --по каджому физику последние телефоны
                        --ph.PHONE          --по каждому телефону последние дата и актуальность
                        ORDER BY ph.PHONES_AKT DESC, COALESCE(ph.MODIFICATION_DATE, SYSDATE) DESC, ph.PHONES_CREATED) DESC) rnk*/
		, ph.PHONES_RANK
    FROM ODS.PKK_PHONES ph
    INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
            FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
            WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN
              AND T1='FIO_DR' AND LEVEL_NODE=p_cur_LEVEL /*AND p_cur_LEVEL=1*/ --and rownum=1
          UNION
          SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
            FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
            WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN
              AND T3 LIKE 'FIO_DR%' /*AND OBJECTS_ID^=OBJECTS_ID_REL*/ AND LEVEL_NODE=p_cur_LEVEL) snr
      ON snr.OBJECTS_ID = ph.OBJECTS_ID AND ph.OBJECTS_TYPE=2 AND ph.PHONES_RANK<=2
    WHERE snr.SRC_OBJECTS_ID=p_cur_OBJECTS_ID
      --WHERE ph.OBJECTS_ID = p_cur_OBJECTS_ID AND ph.OBJECTS_TYPE=2
        AND NOT (NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из НБКИ:%') 
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон клиента(сайт)%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Рекомендация(ПО КЦ):%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РБО:%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПТ/АТМ банка%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из документов РБО:%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('сот. тел. заявителя(сайт)%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('ТЕЛЕФОН ИЗ НБКИ%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РНКО:%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПО КЦ%' ))
  ) --WHERE rnk<=2
UNION ALL
  SELECT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
      ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
      ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
      ,ROUTE_GRAPH, ATTRIBUTES
       FROM (
    SELECT DISTINCT 
      ph.OBJECTS_ID as OBJECTS_ID
      ,-1 AS OBJECTS_ID_REL
      ,DECODE(ph.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
      ,DECODE(-1, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
      --Node 1
      ,snr.N1    AS N1
      ,snr.T1   AS T1
      ,snr.AKT1 AS AKT1
      ,snr.DT1 AS DT1
      --Node 3
      ,NVL(ph.PHONES_COMM, 'Контакт физика '||TO_CHAR(ph.OBJECTS_ID)||' без комментария') AS N3
      ,'FIO_DOP' AS T3
      ,ph.PHONES_AKT AS AKT3
      ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT3
      --Node 2
      ,ph.PHONE       AS N2
      ,'PHONE_DOP'      AS T2
      ,ph.PHONES_AKT  AS AKT2
      ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT2
      --Node 4
      ,NULL     AS N4
      ,NULL     AS T4
      ,NULL     AS AKT4
      ,NULL     AS DT4
      --Node 5
      ,NULL     AS N5
      ,NULL     AS T5
      ,NULL     AS AKT5
      ,NULL     AS DT5
      ,'N1-N3, N3-N2' AS ROUTE_GRAPH
      ,'PHONES_ID='||TO_CHAR(ph.PHONES_ID) AS ATTRIBUTES
      /*,DENSE_RANK() OVER(PARTITION BY 
                        ph.OBJECTS_ID       --по каджому физику последние телефоны
                        --chld.PHONE          --по каждому телефону последние дата и актуальность
                        ORDER BY ph.PHONES_AKT DESC, COALESCE(ph.MODIFICATION_DATE, SYSDATE) DESC, ph.PHONES_CREATED DESC) rnk*/
		, ph.PHONES_RANK
    FROM ODS.PKK_PHONES ph
    INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
            FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
            WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN
              AND T1='FIO_DR' AND LEVEL_NODE=p_cur_LEVEL /*AND p_cur_LEVEL=1*/ --and rownum=1
          UNION
          SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
            FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
            WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN
              AND T3 LIKE 'FIO_DR%' /*AND OBJECTS_ID_REL^=-1*/ AND LEVEL_NODE=p_cur_LEVEL) snr
      ON snr.OBJECTS_ID = ph.OBJECTS_ID AND ph.OBJECTS_TYPE=200 AND ph.PHONES_RANK<=3
    WHERE snr.SRC_OBJECTS_ID=p_cur_OBJECTS_ID
      --ph.OBJECTS_ID = p_cur_OBJECTS_ID AND ph.OBJECTS_TYPE=200
        AND NOT (NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из НБКИ:%') 
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон клиента(сайт)%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Рекомендация(ПО КЦ):%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РБО:%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПТ/АТМ банка%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из документов РБО:%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('сот. тел. заявителя(сайт)%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('ТЕЛЕФОН ИЗ НБКИ%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РНКО:%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПО КЦ%' ))
  ) --WHERE rnk<=3
UNION ALL
  SELECT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
      ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
      ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
      ,ROUTE_GRAPH, ATTRIBUTES
       FROM ( SELECT DISTINCT 
        ah_src.OBJECTS_ID AS OBJECTS_ID
        ,ah_src.OBJECTS_ID AS OBJECTS_ID_REL
        ,DECODE(ah_src.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
        ,DECODE(ah_src.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
        --Node 1
        ,snr.N1    AS N1
        ,snr.T1   AS T1
        ,snr.AKT1 AS AKT1
        ,snr.DT1 AS DT1
        --Node 3
        ,DECODE(NVL(TRIM(adr.REGIONS_NAMES), '-'), '-', '', (adr.REGIONS_NAMES))
          ||DECODE(NVL(TRIM(adr.AREAS_NAMES), '-'), '-', '', (', РАЙОН '||adr.AREAS_NAMES)) 
          ||DECODE(NVL(TRIM(adr.CITIES_NAMES), '-'), '-', '', (', ' || adr.SHOTNAME_CIT ||' '|| adr.CITIES_NAMES))
          ||DECODE(NVL(TRIM(adr.STREETS_NAMES), '-'), '-', '', (', ' || adr.STREETS_NAMES)) 
          ||DECODE(NVL(TRIM(adr.HOUSE), '-'), '-', '', (', Д.'||adr.HOUSE)) 
          ||DECODE(NVL(TRIM(adr.BUILD), '-'), '-', '', (', КОРП.'||adr.BUILD)) 
          ||DECODE(NVL(TRIM(adr.FLAT),  '-'), '-', '', (', КВ.'||adr.FLAT)) AS N3
        ,'ADR_FMG'          AS T3
        ,ah_src.ADDRESS_AKT AS AKT3
        ,COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) AS DT3
        --Node 2
        ,ph.PHONE         AS N2
        ,'PHONE_FMG'      AS T2
        ,ph.PHONES_AKT    AS AKT2
        ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT2
        --Node 4
        ,NULL     AS N4
        ,NULL     AS T4
        ,NULL     AS AKT4
        ,NULL     AS DT4
        --Node 5
        ,NULL     AS N5
        ,NULL     AS T5
        ,NULL     AS AKT5
        ,NULL     AS DT5
        ,'N1-N3, N1-N2' AS ROUTE_GRAPH 
        ,'ADDRESS_ID='||TO_CHAR(ah_src.ADDRESS_ID)||'; PHONES_ID='||TO_CHAR(ph.PHONES_ID) AS ATTRIBUTES
        /*,DENSE_RANK() OVER(PARTITION BY 
                          ah_src.OBJECTS_ID     --по каждому физику последний адрес
                          --ah_src.ADDRESS_ID     --по каждому адресу последние дата и актуальность
                          ORDER BY ah_src.ADDRESS_AKT DESC, COALESCE(ah_src.MODIFICATION_DATE, SYSDATE) DESC, ah_src.ADDRESS_CREATED DESC) rnk_adr
		,ah_src.ADDRESS_RANK
        ,DENSE_RANK() OVER(PARTITION BY 
                          ah_src.ADDRESS_ID --по каждому адресу последний телефон
                          --ph.PHONE              --по каждому телефону последние дата и актуальность
                          --в зависимости от высшего PARTITION BY будет оставаться последний или все телфоны по адресу
                          ORDER BY ph.PHONES_AKT DESC, COALESCE(ph.MODIFICATION_DATE, SYSDATE) DESC, ph.PHONES_CREATED DESC) rnk_ph*/
		, ph.PHONES_RANK
      FROM ODS.PKK_ADDRESS_HISTORY ah_src
     INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
              FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
              WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN
                AND T1 LIKE 'FIO_DR%' AND LEVEL_NODE=p_cur_LEVEL --and rownum=1
            UNION
            SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
              FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
              WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN
                AND T3 LIKE 'FIO_DR%' AND LEVEL_NODE=p_cur_LEVEL) snr
        ON snr.OBJECTS_ID = ah_src.OBJECTS_ID AND ah_src.OBJECTS_TYPE=2 AND ah_src.ADDRESS_RANK<=2
      INNER JOIN ODS.PKK_ADDRESS adr
        ON ah_src.ADDRESS_ID = adr.ADDRESS_ID
      LEFT JOIN ODS.PKK_PHONES ph
        ON ph.OBJECTS_ID = ah_src.ADDRESS_ID AND ph.OBJECTS_TYPE=8 AND ph.PHONES_RANK=1 --AND ph.PHONES_AKT=1
          AND NOT PH.PHONE LIKE '9%'
          --AND ph.PHONES_CREATED <= p_DATE_RID
      WHERE --ah_src.OBJECTS_ID = p_cur_OBJECTS_ID AND ah_src.OBJECTS_TYPE=2   
        snr.SRC_OBJECTS_ID=p_cur_OBJECTS_ID         
          --2 OBJECTS_ID, 3 ORG_ID, 5 DOCUMENTS_ID, 19 ADDRESS_ID, 200 ? 
          --AND ah_src.ADDRESS_CREATED <= p_DATE_RID
    ) --WHERE rnk_adr<=2 AND rnk_ph=1
UNION ALL
  SELECT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
      ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
      ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
      ,ROUTE_GRAPH, ATTRIBUTES
       FROM ( SELECT DISTINCT 
        doc.OBJECTS_ID AS OBJECTS_ID
        ,doc.OBJECTS_ID AS OBJECTS_ID_REL
        ,DECODE(doc.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
        ,DECODE(doc.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
      --Node 1
        ,snr.N1    AS N1
        ,snr.T1   AS T1
        ,snr.AKT1 AS AKT1
        ,snr.DT1 AS DT1
      --Node 2
        ,ph.PHONE         AS N2
        ,'PHONE_PMG'      AS T2
        ,ph.PHONES_AKT    AS AKT2
        ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT2
      --Node 3
        ,DECODE(NVL(TRIM(adr.REGIONS_NAMES), '-'), '-', '', (adr.REGIONS_NAMES))
          ||DECODE(NVL(TRIM(adr.AREAS_NAMES), '-'), '-', '', (', РАЙОН '||adr.AREAS_NAMES)) 
          ||DECODE(NVL(TRIM(adr.CITIES_NAMES), '-'), '-', '', (', ' || adr.SHOTNAME_CIT ||' '|| adr.CITIES_NAMES))
          ||DECODE(NVL(TRIM(adr.STREETS_NAMES), '-'), '-', '', (', ' || adr.STREETS_NAMES)) 
          ||DECODE(NVL(TRIM(adr.HOUSE), '-'), '-', '', (', Д.'||adr.HOUSE)) 
          ||DECODE(NVL(TRIM(adr.BUILD), '-'), '-', '', (', КОРП.'||adr.BUILD)) 
          ||DECODE(NVL(TRIM(adr.FLAT),  '-'), '-', '', (', КВ.'||adr.FLAT)) AS N3
        ,'ADR_PMG' AS T3
        ,ah_src.ADDRESS_AKT AS AKT3
        ,COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) AS DT3
      --Node 4
        ,doc.DOCUMENTS_SERIAL||' '||doc.DOCUMENTS_NUMBER AS N4
        ,'PASP' AS T4
        ,doc.DOCUMENTS_AKT AS AKT4
        ,COALESCE(doc.MODIFICATION_DATE, doc.DOCUMENTS_CREATED)     AS DT4
      --Node 5
        ,NULL     AS N5
        ,NULL     AS T5
        ,NULL     AS AKT5
        ,NULL     AS DT5
        --,'N1-N3, N3-N2, N1-N4' AS ROUTE_GRAPH  
        ,'N1-N3, N1-N2, N1-N4' AS ROUTE_GRAPH  
        ,'ADDRESS_ID='||TO_CHAR(ah_src.ADDRESS_ID)||'; PHONES_ID='||TO_CHAR(ph.PHONES_ID)
            ||'; DOCUMENTS_ID='||TO_CHAR(doc.DOCUMENTS_ID) AS ATTRIBUTES
        --,LISTAGG(ph.PHONE,', ') WITHIN GROUP (ORDER BY COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED)) OVER(PARTITION BY ah_src.ADDRESS_ID ) as ph_list
      ,DENSE_RANK() OVER(PARTITION BY 
                          --doc.OBJECTS_ID                           --по каждому физику последний паспорт
                          doc.DOCUMENTS_SERIAL, doc.DOCUMENTS_NUMBER --по каждому паспорту последние дата и актуальность
                          ORDER BY doc.DOCUMENTS_AKT DESC, COALESCE(doc.MODIFICATION_DATE, SYSDATE) DESC, doc.DOCUMENTS_CREATED DESC) rnk_doc
		,doc.DOCUMENTS_RANK
        /*,DENSE_RANK() OVER(PARTITION BY 
                          doc.OBJECTS_ID        --по каждому физику последние телефоны
                          --ah_src.ADDRESS_ID   --по каждому адресу последние дата и актуальность
                          ORDER BY ah_src.ADDRESS_AKT DESC, COALESCE(ah_src.MODIFICATION_DATE, SYSDATE) DESC, ah_src.ADDRESS_CREATED DESC) rnk_adr
		,ah_src.ADDRESS_RANK
        ,DENSE_RANK() OVER(PARTITION BY 
                          ah_src.ADDRESS_ID   --по каждому адресу последний телефон
                          --ph.PHONE            --по кадому телефону последние дата и актуальность
                          ORDER BY ph.PHONES_AKT DESC, COALESCE(ph.MODIFICATION_DATE, SYSDATE) DESC, ph.PHONES_CREATED DESC) rnk_ph*/
		, ph.PHONES_RANK
      FROM ODS.PKK_DOCUMENTS_HISTORY_INFO doc
      INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
              FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
              WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN
                AND T1='FIO_DR' AND LEVEL_NODE=p_cur_LEVEL --and rownum=1
            UNION
            SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
              FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
              WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN
                AND T3 LIKE 'FIO_DR%' /*AND OBJECTS_ID^=OBJECTS_ID_REL*/ AND LEVEL_NODE=p_cur_LEVEL) snr
        ON snr.OBJECTS_ID = doc.OBJECTS_ID AND doc.DOCUMENTS_TYPE=21 --AND doc.DOCUMENTS_RANK=1
      LEFT JOIN ODS.PKK_ADDRESS_HISTORY ah_src    
        ON ah_src.OBJECTS_ID = doc.DOCUMENTS_ID 
          AND ah_src.OBJECTS_TYPE = 5 AND ah_src.ADDRESS_RANK<=1 --2 OBJECTS_ID, 3 ORG_ID, 5 DOCUMENTS_ID, 19 ADDRESS_ID, 200 ?
      LEFT JOIN ODS.PKK_ADDRESS adr
        ON ah_src.ADDRESS_ID = adr.ADDRESS_ID
      LEFT JOIN ODS.PKK_PHONES ph
        ON ph.OBJECTS_ID = ah_src.ADDRESS_ID AND ph.OBJECTS_TYPE=8 AND ph.PHONES_RANK<=1 --AND ph.PHONES_AKT=1
          AND NOT ph.PHONE LIKE '9%'
      WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID
          --doc.OBJECTS_ID = p_cur_OBJECTS_ID AND doc.DOCUMENTS_TYPE=21
      ) WHERE rnk_doc=1 --AND rnk_adr<=1 AND rnk_ph<=1
UNION ALL
  SELECT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
		,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
		,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
		,ROUTE_GRAPH, ATTRIBUTES
	FROM ( SELECT DISTINCT 
		wor.OBJECTS_ID AS OBJECTS_ID
		,wor.OBJECTS_ID AS OBJECTS_ID_REL
		,DECODE(wor.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
		,DECODE(wor.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
		--Node 1
		,snr.N1    AS N1
		,snr.T1   AS T1
		,snr.AKT1 AS AKT1
		,snr.DT1 AS DT1
        --Node 3
		,DECODE(NVL(TRIM(adr.REGIONS_NAMES), '-'), '-', '', (adr.REGIONS_NAMES))
			||DECODE(NVL(TRIM(adr.AREAS_NAMES), '-'), '-', '', (', РАЙОН '||adr.AREAS_NAMES)) 
			||DECODE(NVL(TRIM(adr.CITIES_NAMES), '-'), '-', '', (', ' || adr.SHOTNAME_CIT ||' '|| adr.CITIES_NAMES))
			||DECODE(NVL(TRIM(adr.STREETS_NAMES), '-'), '-', '', (', ' || adr.STREETS_NAMES)) 
			||DECODE(NVL(TRIM(adr.HOUSE), '-'), '-', '', (', Д.'||adr.HOUSE)) 
			||DECODE(NVL(TRIM(adr.BUILD), '-'), '-', '', (', КОРП.'||adr.BUILD)) 
			||DECODE(NVL(TRIM(adr.FLAT),  '-'), '-', '', (', КВ.'||adr.FLAT)) AS N3
		,'ADR_WORK' AS T3
		,ah_src.ADDRESS_AKT AS AKT3
		,COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) AS DT3
		,ah_src.ADDRESS_ID
		--Node 5
		,ph_wor.PHONE     AS N5
		,'PHONE_WORK'     AS T5
		,ph_wor.PHONES_AKT    AS AKT5
		,COALESCE(ph_wor.MODIFICATION_DATE, ph_wor.PHONES_CREATED) AS DT5
		--Node 2
		,ph_org.PHONE AS N2
		,'PHONE_ORG' AS T2
		,ph_org.PHONES_AKT AS AKT2
		,COALESCE(ph_org.MODIFICATION_DATE, ph_org.PHONES_CREATED)     AS DT2
		--Node 4
		,REPLACE(wor.ORG_NAME, '"', '')     AS N4
		,'ORG_NAME'     AS T4
		,wor.WORKS_AKT     AS AKT4
		,wor.WORKS_LAST
		,COALESCE(wor.MODIFICATION_DATE, wor.WORKS_CREATED)     AS DT4
		--,'N1-N4, N4-N2, N4-N3, N4-N5' AS ROUTE_GRAPH
		,'N1-N5, N1-N2, N1-N3, N2-N4, N3-N4, N5-N4' AS ROUTE_GRAPH
		,'WORKS_ID='||TO_CHAR(wor.WORKS_ID)||'; ORG_ID='||TO_CHAR(wor.ORG_ID)||'; ADDRESS_ID='||TO_CHAR(ah_src.ADDRESS_ID)
			||'; PHONES_ID='||TO_CHAR(ph_org.PHONES_ID)||'; PHONES_I_WORK='||TO_CHAR(ph_wor.PHONES_ID) AS ATTRIBUTES
		--,LISTAGG(ph.PHONE,', ') WITHIN GROUP (ORDER BY COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED)) OVER(PARTITION BY ah_src.ADDRESS_ID ) as ph_list
		,DENSE_RANK() OVER(PARTITION BY  
                          wor.OBJECTS_ID       --по каждому физику последняя работа
                          --wor.ORG_ID             --по каждой работе последние дата и актуальность
                          ORDER BY wor.WORKS_AKT DESC, COALESCE(wor.MODIFICATION_DATE, SYSDATE) DESC, wor.WORKS_CREATED DESC
                            , wor.WORKS_HISTORY_ID DESC) rnk_wor
    /*,DENSE_RANK() OVER(PARTITION BY 
                          wor.ORG_ID            --по каждомй работе последний адрес
                          --ah_src.ADDRESS_ID   --по каждому адресу последние дата и актуальность
                          ORDER BY ah_src.ADDRESS_AKT DESC, COALESCE(ah_src.MODIFICATION_DATE, SYSDATE) DESC, ah_src.ADDRESS_CREATED DESC) rnk_adr
		,ah_src.ADDRESS_RANK
        ,DENSE_RANK() OVER(PARTITION BY 
                          ph_wor.OBJECTS_ID     --по каждой работе последний телефон
                          --ph.PHONE       --по кадому телефону последние дата и актуальность
                          ORDER BY ph_wor.PHONES_AKT DESC, COALESCE(ph_wor.MODIFICATION_DATE, SYSDATE) DESC, ph_wor.PHONES_CREATED DESC) rnk_pho
		,DENSE_RANK() OVER(PARTITION BY 
                          ph_org.OBJECTS_ID       --по каждой работе последний телефон
                          --ph.PHONE       --по кадому телефону последние дата и актуальность
                          ORDER BY ph_org.PHONES_AKT DESC, COALESCE(ph_org.MODIFICATION_DATE, SYSDATE) DESC, ph_org.PHONES_CREATED DESC) rnk_phw*/
		,ph_org.PHONES_RANK as PHO_RANK
		,ph_wor.PHONES_RANK as PHW_RANK
		,wor.WORKS_RANK
      FROM ODS.PKK_WORKS_INFO wor
      INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
              FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
              WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN
                AND T1='FIO_DR' AND LEVEL_NODE=p_cur_LEVEL --and rownum=1
            UNION
            SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
              FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
              WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN
                AND T3 LIKE 'FIO_DR%' /*AND OBJECTS_ID^=OBJECTS_ID_REL*/ AND LEVEL_NODE=p_cur_LEVEL) snr
        ON snr.OBJECTS_ID = wor.OBJECTS_ID AND wor.WORKS_AKT=1 AND wor.WORKS_RANK<=3 AND wor.WORKS_CREATED > SYSDATE-365*5
      INNER JOIN ODS.PKK_ADDRESS_HISTORY ah_src    
        ON ah_src.OBJECTS_ID = wor.ORG_ID
          AND ah_src.OBJECTS_TYPE = 3 AND ah_src.ADDRESS_RANK=1 --2 OBJECTS_ID, 3 ORG_ID, 5 DOCUMENTS_ID, 19 ADDRESS_ID, 200 ? 
          AND ah_src.ADDRESS_AKT=1
      INNER JOIN ODS.PKK_ADDRESS adr
        ON ah_src.ADDRESS_ID = adr.ADDRESS_ID
      LEFT JOIN ODS.PKK_PHONES ph_wor
        ON ph_wor.OBJECTS_ID = wor.WORKS_ID AND ph_wor.OBJECTS_TYPE=12 AND ph_wor.PHONES_RANK=1 --AND ph_wor.PHONES_AKT=1
      LEFT JOIN ODS.PKK_PHONES ph_org
        ON ph_org.OBJECTS_ID = wor.ORG_ID AND ph_org.OBJECTS_TYPE=3 AND ph_org.PHONES_RANK=1  --AND ph_org.PHONES_AKT=1
      WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID
      ) WHERE rnk_wor<=3 --AND rnk_adr<=1 AND rnk_pho<=1 AND rnk_phw<=1
UNION ALL
  SELECT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
      ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
      ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
      ,ROUTE_GRAPH, ATTRIBUTES
       FROM (
    SELECT DISTINCT 
      em.OBJECTS_ID as OBJECTS_ID
      ,em.OBJECTS_ID AS OBJECTS_ID_REL
      ,DECODE(em.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
      ,DECODE(em.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
      --Node 1
      ,snr.N1    AS N1
      ,snr.T1   AS T1
      ,snr.AKT1 AS AKT1
      ,snr.DT1 AS DT1
      --Node 2
      ,NULL AS N2
      ,NULL AS T2
      ,NULL AS AKT2
      ,NULL AS DT2
      --Node 3
      ,NULL       AS N3
      ,NULL       AS T3
      ,NULL       AS AKT3
      ,NULL       AS DT3
      --Node 4
      ,em.EMAIL     AS N4
      ,'EMAIL'      AS T4
      ,em.EMAIL_AKT AS AKT4
      ,COALESCE(em.MODIFICATION_DATE, em.EMAIL_CREATED) AS DT4
      --Node 5
      ,NULL     AS N5
      ,NULL     AS T5
      ,NULL     AS AKT5
      ,NULL     AS DT5
      ,'N1-N4' AS ROUTE_GRAPH
      ,'EMAIL_ID='||TO_CHAR(em.EMAIL_ID) AS ATTRIBUTES
      ,DENSE_RANK() OVER(PARTITION BY 
                        em.OBJECTS_ID       --по каджому физику последние EMAIL
                        --ph.PHONE          --по каждому телефону последние дата и актуальность
                        ORDER BY em.EMAIL_AKT DESC, COALESCE(em.MODIFICATION_DATE, SYSDATE) DESC, em.EMAIL_CREATED DESC) rnk
    FROM ODS.PKK_EMAIL em
    INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
            FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
            WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN
              AND T1='FIO_DR' AND LEVEL_NODE=p_cur_LEVEL --and rownum=1
          UNION
          SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
            FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
            WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN
              AND T3 LIKE 'FIO_DR%' AND LEVEL_NODE=p_cur_LEVEL) snr
      ON snr.OBJECTS_ID = em.OBJECTS_ID
    WHERE snr.SRC_OBJECTS_ID=p_cur_OBJECTS_ID
        AND NVL(em.EMAIL, '-') LIKE '%@%' 
  ) WHERE rnk<=2
UNION ALL
  SELECT OBJECTS_ID as OBJECTS_ID
    ,OBJECTS_ID AS OBJECTS_ID_REL
    ,DECODE(OBJECTS_ID, OBJECTS_ID, 1, 0) AS ROOT_PERS
    ,DECODE(OBJECTS_ID, OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
    ,N1    AS N1
    ,T1   AS T1
    ,AKT1 AS AKT1
    ,DT1 AS DT1
    ,NULL AS N2, NULL AS T2, NULL AS AKT2, NULL AS DT2
    ,TO_CHAR(OBJECTS_ID||' - ')||DECODE(cnt_bnk, 0, '', 'БНК: '||TO_CHAR(cnt_bnk)||'; ')
        ||DECODE(cnt_def, 0, '', 'ДЕФОЛТ: '||TO_CHAR(cnt_def)||'; ')
        ||DECODE(cnt_frod, 0, '', 'ФРОД: '||TO_CHAR(cnt_frod)||'; ')AS N3
    ,'NEG' AS T3
    ,0 AS AKT3
    ,SYSDATE AS DT3
    ,NULL AS N4, NULL AS T4, NULL AS AKT4, NULL AS DT4
    ,NULL AS N5, NULL AS T5, NULL AS AKT5, NULL AS DT5
    ,'N1-N3' AS ROUTE_GRAPH
    ,NULL AS ATTRIBUTES
  FROM (SELECT DISTINCT snr.OBJECTS_ID, N1, T1, AKT1, DT1
      ,(SELECT COUNT(DISTINCT(BNK_CODE)) FROM SFF.RP_BNK@SNA WHERE PERSON_ID = snr.OBJECTS_ID) as cnt_bnk
      ,(SELECT COUNT(DISTINCT(REQUEST_ID)) FROM ODS.PKK_DEFOLT@SNA WHERE PERSON_ID = snr.OBJECTS_ID) as cnt_def
      ,(SELECT  COUNT(DISTINCT(TYPE_REL)) FROM SFF.FROD_RULE@SNA WHERE PERSON_ID = snr.OBJECTS_ID) as cnt_frod
    FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
    WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND snr.USER_SNA=v_LOGIN
    ) WHERE cnt_bnk + cnt_def + cnt_frod > 0
    ;

  --курсор с явно указанными связями 
  CURSOR cur_PERSON_REL(p_cur_OBJECTS_ID NUMBER) IS
     SELECT DISTINCT OBJECTS_ID
            ,OBJECTS_ID_REL
            ,ROOT_PERS
            ,ROOT_PERS_REL
            --далее пошла нужная информация родителя
            ,N1
            ,T1
            ,AKT1
            ,DT1
            --далее пошла нужная информация ребенка
            ,N3
            ,T3 --||DECODE(FAMILY_REL_STATUS, 1, '_'||FAMILY_REL_NAME||CONTACT_COMMENT, '') 
            ,AKT3
            ,DT3
            ,ROUTE_GRAPH
          FROM (
          SELECT per.OBJECTS_ID
            ,per_rel.OBJECTS_ID AS OBJECTS_ID_REL
            ,per.OBJECTS_ID AS OB_ID_REALLY
            ,DECODE(per.OBJECTS_ID, p_cur_OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(per_rel.OBJECTS_ID, p_cur_OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            ,'FIO_DR' AS T1
            --,fml.CONTACT_AKT AS AKT1
            ,per.FIO_AKT AS AKT1
            --,COALESCE(fml.CONTACT_MODIFICATION, fml.CONTACT_CREATED) AS DT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            ,per_rel.FIO4SEARCH||TO_CHAR(per_rel.BIRTH, ' dd.mm.yyyy') AS N3
            --,'REL_OB_ID_REF' AS T3
            ,'FIO_DR_REL_REF' AS T3
            ,per_rel.FIO_AKT AS AKT3
            ,COALESCE(per_rel.MODIFICATION_DATE, per_rel.FIO_CREATED) AS DT3
            --,'FIO_DR' AS T3
            ,fml.FAMILY_REL_NAME
            ,fml.FAMILY_REL_STATUS
            ,fml.CONTACT_COMMENT
            ,'N1-N3' AS ROUTE_GRAPH
            ,DENSE_RANK() OVER(PARTITION BY fml.OBJECTS_ID, fml.OB_ID
                              ORDER BY fml.CONTACT_AKT DESC, COALESCE(fml.CONTACT_MODIFICATION, fml.CONTACT_CREATED) DESC) rnk
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio
            ,DENSE_RANK() OVER(PARTITION BY per_rel.OBJECTS_ID
                              ORDER BY per_rel.FIO_AKT DESC, COALESCE(per_rel.MODIFICATION_DATE, per_rel.FIO_CREATED) DESC) rnk_fio_rel
          FROM (SELECT OBJECTS_ID, OBJECTS_ID AS OB_ID, FIO_AKT as CONTACT_AKT, '' AS CONTACT_COMMENT
                    , MODIFICATION_DATE AS CONTACT_CREATED, MODIFICATION_DATE AS CONTACT_MODIFICATION
                    , '' AS FAMILY_REL_NAME, 0 AS FAMILY_REL_STATUS
                  FROM ODS.PKK_PERSON_INFO WHERE OBJECTS_ID=p_cur_OBJECTS_ID and ROWNUM=1
                UNION
                SELECT OBJECTS_ID, OB_ID, CONTACT_AKT, '' AS CONTACT_COMMENT, CONTACT_CREATED, CONTACT_MODIFICATION
                    , FAMILY_REL_NAME, FAMILY_REL_STATUS
                  FROM ODS.PKK_FAMILY WHERE OBJECTS_ID=p_cur_OBJECTS_ID --AND CONTACT_CREATED <= p_DATE_RID
                UNION
                  SELECT OBJECTS_ID, OB_ID, CONTACT_AKT, CONTACT_COMMENT, CONTACT_CREATED, CONTACT_MODIFICATION
                    , FAMILY_REL_NAME, FAMILY_REL_STATUS
                  FROM ODS.PKK_CONTACTS WHERE OBJECTS_ID=p_cur_OBJECTS_ID --AND CONTACT_CREATED <= p_DATE_RID 
                  ) fml
          INNER JOIN ODS.PKK_PERSON_INFO per
            ON per.OBJECTS_ID = fml.OBJECTS_ID AND per.FIO_RANK=1 --AND per.FIO_CREATED <= p_DATE_RID
          INNER JOIN ODS.PKK_PERSON_INFO per_rel
            ON per_rel.OBJECTS_ID = fml.OB_ID AND per_rel.FIO_RANK=1 --AND per_rel.FIO_CREATED <= p_DATE_RID
          WHERE fml.OBJECTS_ID = p_cur_OBJECTS_ID --AND fml.CONTACT_CREATED <= p_DATE_RID
          ) WHERE rnk=1 AND rnk_fio=1 AND rnk_fio_rel=1 
      UNION ALL
        SELECT DISTINCT OBJECTS_ID
            ,OBJECTS_ID_REL
            ,ROOT_PERS
            ,ROOT_PERS_REL
            --далее пошла нужная информация родителя
            ,N1
            ,T1
            ,AKT1
            ,DT1
            --далее пошла нужная информация ребенка
            ,N3
            ,T3 --||DECODE(FAMILY_REL_STATUS, 1, '_'||FAMILY_REL_NAME||CONTACT_COMMENT, '') 
            ,AKT3
            ,DT3
            ,ROUTE_GRAPH
          FROM (
          SELECT cr.OBJECTS_ID as OBJECTS_ID
            ,cr_por.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(cr.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(cr_por.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            ,snr.N1    AS N1
            ,snr.T1   AS T1
            ,snr.AKT1 AS AKT1
            ,snr.DT1 AS DT1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N3
            ,DECODE(cr_por.TYPE_REQUEST_ID, 6, 'FIO_DR_REL_POR', 7, 'FIO_DR_REL_POR', 13, 'FIO_DR_REL_SOZ') AS T3
            ,cr_por.TYPE_REQUEST_ID
            ,per.FIO_AKT AS AKT3
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT3
            ,'N1-N3' AS ROUTE_GRAPH
            ,DENSE_RANK() OVER(PARTITION BY cr.OBJECTS_ID, cr_por.OBJECTS_ID
                              ORDER BY cr_por.OBJECTS_ID DESC, COALESCE(cr_por.MODIFICATION_DATE_REQUEST, cr_por.CREATED_DATE) DESC) rnk
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio_rel
          FROM ODS.PKK_C_REQUEST_SNA cr
          INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
                      FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                      WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
                        AND T1='FIO_DR' AND LEVEL_NODE IN(1,3) --and rownum=1
                    UNION
                    SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
                      FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                      WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
                        AND T3 LIKE 'FIO_DR%' AND LEVEL_NODE IN(1,3)) snr
            ON snr.OBJECTS_ID = cr.OBJECTS_ID AND cr.TYPE_REQUEST_ID=1
          INNER JOIN ODS.PKK_C_REQUEST_SNA cr_por
            ON cr_por.PARENT_ID = cr.PARENT_ID AND cr_por.TYPE_REQUEST_ID IN(6,7,13)
              AND cr_por.OBJECTS_ID ^= cr.OBJECTS_ID
          INNER JOIN ODS.PKK_PERSON_INFO per
            ON per.OBJECTS_ID = cr_por.OBJECTS_ID
            WHERE snr.SRC_OBJECTS_ID=p_cur_OBJECTS_ID
          ) WHERE rnk=1 AND rnk_fio_rel=1
    ;
  
  CURSOR cur_PERSON_HIDE (p_cur_OBJECTS_ID NUMBER) IS
--HIDE FIZ
  SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (
      SELECT DISTINCT dbl.OBJECTS_ID AS OBJECTS_ID
            ,dbl.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(snr.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,NULL AS N3
            ,NULL AS T3
            ,NULL AS AKT3
            ,NULL AS DT3
            --Node 2
            ,NULL		AS N2
            ,NULL		AS T2
            ,NULL		AS AKT2
            ,NULL		AS DT2
            --Node 4
            ,dbl.DOCUMENTS_SERIAL||' '||dbl.DOCUMENTS_NUMBER AS N4
            ,'PASP' AS T4
            ,dbl.DOCUMENTS_AKT AKT4
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.DOCUMENTS_CREATED) AS DT4
            --Node 5
            ,NULL		AS N5
            ,NULL		AS T5
            ,NULL		AS AKT5
            ,NULL		AS DT5
            ,'N1-N4' AS ROUTE_GRAPH  
            ,'FIO_DR_REL_HIDE_PASP; '||'DOCUMENTS_ID='||TO_CHAR(dbl.DOCUMENTS_ID) AS ATTRIBUTES
            ,DENSE_RANK() OVER(PARTITION BY 
                  --dbl.OBJECTS_ID                            --по каджому физику последние паспорта
                  dbl.DOCUMENTS_SERIAL, dbl.DOCUMENTS_NUMBER  --по каждому паспорту последние дата и актуальность
                  ORDER BY dbl.DOCUMENTS_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, SYSDATE) DESC, dbl.DOCUMENTS_CREATED DESC) rnk_pasp
            /*,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio*/
			,per.FIO_RANK
        FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
        INNER JOIN ODS.PKK_DOCUMENTS_HISTORY_INFO dbl
          ON dbl.DOCUMENTS_NUMBER = SUBSTR(snr.N4, 6, 6) AND dbl.DOCUMENTS_SERIAL = SUBSTR(snr.N4, 1, 4)  --AND doc.DOCUMENTS_RANK=1
            AND dbl.DOCUMENTS_TYPE=21
            AND snr.OBJECTS_ID ^= dbl.OBJECTS_ID --физики должны быть разными
        INNER JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = dbl.OBJECTS_ID AND per.FIO_RANK=1
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND snr.USER_SNA=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
            AND snr.T4='PASP' --AND NOT snr.N4 IS NULL
      ) WHERE rnk_pasp=1 --AND rnk_fio=1
    UNION ALL --ищем АДРЕС среди АДРЕСОВ ФМЖ
      SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (
      SELECT DISTINCT dbl.OBJECTS_ID AS OBJECTS_ID
            ,dbl.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,snr.N3 AS N3
            ,'ADR_FMG'       AS T3
            ,dbl.ADDRESS_AKT AS AKT3
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) AS DT3
            --Node 2
            ,ph.PHONE         AS N2
            ,'PHONE_FMG'      AS T2
            ,ph.PHONES_AKT    AS AKT2
            ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT2
            --Node 4
            ,NULL AS N4
            ,NULL AS T4
            ,NULL AKT4
            ,NULL AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            --,'N1-N3, N3-N2' AS ROUTE_GRAPH
            ,'N1-N3, N1-N2' AS ROUTE_GRAPH
            ,'FIO_DR_REL_HIDE_AFMG; '||'ADDRESS_ID='||TO_CHAR(dbl.ADDRESS_ID)||'; PHONES_ID='||TO_CHAR(ph.PHONES_ID) AS ATTRIBUTES
            ,DENSE_RANK() OVER(PARTITION BY 
                              --dbl.OBJECTS_ID     --по каждому физику последний адрес
                              dbl.ADDRESS_ID     --по каждому адресу последние дата и актуальность
                              ORDER BY dbl.ADDRESS_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, SYSDATE) DESC, dbl.ADDRESS_CREATED DESC
                                ,dbl.ADDRESS_HISTORY_ID DESC) rnk_adr
            ,DENSE_RANK() OVER(PARTITION BY 
                              ph.OBJECTS_ID --по каждому адресу последний телефон
                              --ph.PHONE              --по каждому телефону последние дата и актуальность
                              --в зависимости от высшего PARTITION BY будет оставаться последний или все телфоны по адресу
                              ORDER BY ph.PHONES_AKT DESC, COALESCE(ph.MODIFICATION_DATE, SYSDATE) DESC, ph.PHONES_CREATED DESC) rnk_ph
            /*,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio*/
			,per.FIO_RANK
			,ph.PHONES_RANK
			,dbl.ADDRESS_RANK
        FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
        INNER JOIN ODS.PKK_ADDRESS_HISTORY dbl
          ON dbl.ADDRESS_ID = TO_NUMBER(NVL(REGEXP_REPLACE(snr.ATTRIBUTES, 'ADDRESS_ID=([0-9]*).+', '\1'), '-1'))
            AND dbl.OBJECTS_TYPE=2 AND dbl.ADDRESS_RANK<=2
            AND snr.OBJECTS_ID ^= dbl.OBJECTS_ID --физики должны быть разными
            AND ((NVL(snr.T3, '-')='ADR_FMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) BETWEEN snr.DT3-365 AND SYSDATE)
              or (NVL(snr.T3, '-')='ADR_PMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) BETWEEN snr.DT3-365 AND SYSDATE))
            /*AND ((NVL(snr.T3, '-')='ADR_FMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) - NVL(snr.DT3, SYSDATE+365*100) > -365+(SYSDATE-snr.DT3))
              or (NVL(snr.T3, '-')='ADR_PMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) - NVL(snr.DT3, SYSDATE+365*100) > -365+(SYSDATE-snr.DT3)))*/
        LEFT JOIN ODS.PKK_PHONES ph
          ON ph.OBJECTS_ID = dbl.ADDRESS_ID AND ph.OBJECTS_TYPE=8 AND ph.PHONES_RANK=1 
            AND NOT ph.PHONE LIKE '9%'
        INNER JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = dbl.OBJECTS_ID AND per.FIO_RANK=1
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND snr.USER_SNA=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
            AND snr.T3 IN('ADR_FMG', 'ADR_PMG') AND snr.N3 LIKE '%Д.%' AND LENGTH(TRIM(snr.N3))>16 AND snr.N3 LIKE '%КВ.%' 
            AND NOT snr.N3 LIKE '%Д. 0%' AND NOT snr.N3 LIKE '%Д. -%'
      ) WHERE rnk_adr<=5 --AND rnk_ph=1 --AND rnk_fio=1
    UNION ALL --ищем АДРЕС среди адресов ПМЖ
      SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (SELECT DISTINCT doc.OBJECTS_ID AS OBJECTS_ID
            ,doc.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(doc.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(doc.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,snr.N3 AS N3
            ,'ADR_PMG'       AS T3
            ,dbl.ADDRESS_AKT AS AKT3
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) AS DT3
            --Node 2
            ,ph.PHONE         AS N2
            ,'PHONE_PMG'      AS T2
            ,ph.PHONES_AKT    AS AKT2
            ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT2
            --Node 4
            ,doc.DOCUMENTS_SERIAL||' '||doc.DOCUMENTS_NUMBER AS N4
            ,'PASP' AS T4
            ,doc.DOCUMENTS_AKT AS AKT4
            ,COALESCE(doc.MODIFICATION_DATE, doc.DOCUMENTS_CREATED)     AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            --,'N1-N3, N3-N2, N1-N4' AS ROUTE_GRAPH  
            ,'N1-N3, N1-N2, N1-N4' AS ROUTE_GRAPH  
            ,'FIO_DR_REL_HIDE_APMG; '||'ADDRESS_ID='||TO_CHAR(dbl.ADDRESS_ID)||'; PHONES_ID='||TO_CHAR(ph.PHONES_ID)
                ||'; DOCUMENTS_ID='||TO_CHAR(doc.DOCUMENTS_ID) AS ATTRIBUTES
            --,snr.ATTRIBUTES AS ATTR_SRC
            ,snr.DT3 AS DT3_SRC
            ,ROUND(COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) - NVL(snr.DT3, SYSDATE+365*100)) as D_BW
            ,DENSE_RANK() OVER(PARTITION BY 
                        --dbl.OBJECTS_ID     --по каждому физику последний адрес
                        dbl.ADDRESS_ID     --по каждому адресу последние дата и актуальность
                        ORDER BY dbl.ADDRESS_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, SYSDATE) DESC, dbl.ADDRESS_CREATED DESC
                          , dbl.ADDRESS_HISTORY_ID DESC) rnk_adr
            /*,DENSE_RANK() OVER(PARTITION BY 
                        --doc.OBJECTS_ID                           --по каждому физику последний паспорт
                        doc.DOCUMENTS_SERIAL, doc.DOCUMENTS_NUMBER --по каждому паспорту последние дата и актуальность
                        ORDER BY doc.DOCUMENTS_AKT DESC, COALESCE(doc.MODIFICATION_DATE, SYSDATE) DESC, doc.DOCUMENTS_CREATED DESC) rnk_doc
            ,DENSE_RANK() OVER(PARTITION BY 
                        ph.OBJECTS_ID --по каждому адресу последний телефон
                        --ph.PHONE              --по каждому телефону последние дата и актуальность
                        --в зависимости от высшего PARTITION BY будет оставаться последний или все телфоны по адресу
                        ORDER BY ph.PHONES_AKT DESC, COALESCE(ph.MODIFICATION_DATE, SYSDATE) DESC, ph.PHONES_CREATED DESC) rnk_ph*/
            /*,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                         ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio*/
            ,per.FIO_RANK
            ,ph.PHONES_RANK
            ,dbl.ADDRESS_RANK
        FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
        INNER JOIN ODS.PKK_ADDRESS_HISTORY dbl
          ON dbl.ADDRESS_ID = TO_NUMBER(NVL(REGEXP_REPLACE(snr.ATTRIBUTES, 'ADDRESS_ID=([0-9]*).+', '\1'), '-1'))
            AND dbl.OBJECTS_TYPE=5 AND dbl.ADDRESS_RANK<=1 AND dbl.ADDRESS_AKT=1
          AND ((NVL(snr.T3, '-') IN('ADR_FMG') AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) BETWEEN snr.DT3-365 AND SYSDATE)
             or (NVL(snr.T3, '-')='ADR_PMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) BETWEEN snr.DT3-365 AND SYSDATE)
             )
          /*AND ((NVL(snr.T3, '-')='ADR_FMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) - NVL(snr.DT3, SYSDATE+365*100) > -365+(SYSDATE-snr.DT3))
            or (NVL(snr.T3, '-')='ADR_PMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) - NVL(snr.DT3, SYSDATE+365*100) > -365+(SYSDATE-snr.DT3)))*/
        INNER JOIN ODS.PKK_DOCUMENTS_HISTORY_INFO doc
          ON doc.DOCUMENTS_ID=dbl.OBJECTS_ID AND doc.DOCUMENTS_TYPE=21
            AND snr.OBJECTS_ID ^= doc.OBJECTS_ID --физики должны быть разными
        LEFT JOIN ODS.PKK_PHONES ph
          ON ph.OBJECTS_ID = dbl.ADDRESS_ID AND ph.OBJECTS_TYPE=8 AND ph.PHONES_RANK<=1
            AND NOT ph.PHONE LIKE '9%'
        INNER JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = doc.OBJECTS_ID AND per.FIO_RANK=1
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND snr.USER_SNA=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
            AND snr.T3 IN('ADR_FMG', 'ADR_PMG') AND snr.N3 LIKE '%Д.%' AND LENGTH(TRIM(snr.N3))>16 AND snr.N3 LIKE '%КВ.%' 
            AND NOT snr.N3 LIKE '%Д. 0%' AND NOT snr.N3 LIKE '%Д. -%'
      ) WHERE rnk_adr<=5 --AND rnk_doc=1 AND rnk_ph=1 --AND rnk_fio=1
    UNION ALL --ищем EMAIL
      SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (
      SELECT DISTINCT dbl.OBJECTS_ID AS OBJECTS_ID
            ,dbl.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,NULL     AS N3
            ,NULL     AS T3
            ,NULL     AS AKT3
            ,NULL     AS DT3
            --Node 2
            ,NULL     AS N2
            ,NULL     AS T2
            ,NULL     AS AKT2
            ,NULL     AS DT2
            --Node 4
            ,snr.N4 AS N4
            ,'EMAIL'       AS T4
            ,dbl.EMAIL_AKT AS AKT4
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.EMAIL_CREATED) AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            ,'N1-N4' AS ROUTE_GRAPH
            ,'FIO_DR_REL_HIDE_EMAIL; '||'EMAIL='||TO_CHAR(dbl.EMAIL_ID) AS ATTRIBUTES
            ,DENSE_RANK() OVER(PARTITION BY 
                              --dbl.OBJECTS_ID     --по каждому физику последний EMAIL
                              dbl.EMAIL     --по каждому EMAIL последние дата и актуальность
                              ORDER BY dbl.EMAIL_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, SYSDATE) DESC, dbl.EMAIL_CREATED DESC) rnk_eml
            /*,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio*/
        ,per.FIO_RANK
        FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
        INNER JOIN ODS.PKK_EMAIL dbl
          ON dbl.EMAIL = snr.N4
            AND snr.OBJECTS_ID ^= dbl.OBJECTS_ID --физики должны быть разными
            --AND COALESCE(dbl.MODIFICATION_DATE, dbl.EMAIL_CREATED) - NVL(snr.DT4, SYSDATE+365*100) > -365*3 +(SYSDATE-snr.DT4)
            AND COALESCE(dbl.MODIFICATION_DATE, dbl.EMAIL_CREATED) BETWEEN snr.DT4-365*3 AND SYSDATE
        INNER JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = dbl.OBJECTS_ID AND per.FIO_RANK=1
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND snr.USER_SNA=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
            AND NVL(snr.T4, '-')='EMAIL'  AND snr.N4 LIKE '%@%'
      ) WHERE rnk_eml<=5 --AND rnk_fio=1
    UNION ALL --ищем телефон в закрепленном за ФМЖ
      SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (
      SELECT DISTINCT ah_src.OBJECTS_ID AS OBJECTS_ID
            ,ah_src.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(ah_src.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(ah_src.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,DECODE(NVL(TRIM(adr.REGIONS_NAMES), '-'), '-', '', (adr.REGIONS_NAMES))
              ||DECODE(NVL(TRIM(adr.AREAS_NAMES), '-'), '-', '', (', РАЙОН '||adr.AREAS_NAMES)) 
              ||DECODE(NVL(TRIM(adr.CITIES_NAMES), '-'), '-', '', (', ' || adr.SHOTNAME_CIT ||' '|| adr.CITIES_NAMES))
              ||DECODE(NVL(TRIM(adr.STREETS_NAMES), '-'), '-', '', (', ' || adr.STREETS_NAMES)) 
              ||DECODE(NVL(TRIM(adr.HOUSE), '-'), '-', '', (', Д.'||adr.HOUSE)) 
              ||DECODE(NVL(TRIM(adr.BUILD), '-'), '-', '', (', КОРП.'||adr.BUILD)) 
              ||DECODE(NVL(TRIM(adr.FLAT),  '-'), '-', '', (', КВ.'||adr.FLAT)) AS N3
            ,'ADR_FMG'          AS T3
            ,ah_src.ADDRESS_AKT AS AKT3
            ,COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) AS DT3
            --Node 2
            ,snr.N2 AS N2
            ,'PHONE_FMG' AS T2
            ,dbl.PHONES_AKT AS AKT2
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) AS DT2
            --Node 4
            ,NULL AS N4
            ,NULL AS T4
            ,NULL AS AKT4
            ,NULL AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            --,'N1-N3, N3-N2' AS ROUTE_GRAPH
            ,'N1-N3, N1-N2' AS ROUTE_GRAPH
            ,'FIO_DR_REL_HIDE_PHFMG; '||'ADDRESS_ID='||TO_CHAR(ah_src.ADDRESS_ID)||'; PHONES_ID='||TO_CHAR(dbl.PHONES_ID) AS ATTRIBUTES
            ,dbl.PHONES_AKT
            ,dbl.MODIFICATION_DATE
            ,dbl.PHONES_CREATED
            ,dbl.PHONES_ID
            ,dbl.PHONE
            ,DENSE_RANK() OVER(PARTITION BY 
                              --snr.OBJECTS_ID     --по каждому физику последний PHONES_ID
                              dbl.PHONE     --по каждому PHONES_ID последние дата и актуальность
                              ORDER BY dbl.PHONES_AKT DESC, NVL(dbl.MODIFICATION_DATE, SYSDATE) DESC, dbl.PHONES_CREATED DESC
                                ,dbl.PHONES_ID DESC) rnk_ph
            
            /*,DENSE_RANK() OVER(PARTITION BY 
                              --snr.OBJECTS_ID     --по каждому физику последний PHONES_ID
                              ah_src.ADDRESS_ID     --по каждому PHONES_ID последние дата и актуальность
                              ORDER BY ah_src.ADDRESS_AKT DESC, COALESCE(ah_src.MODIFICATION_DATE, SYSDATE) DESC, ah_src.ADDRESS_CREATED DESC
                                ,ah_src.ADDRESS_HISTORY_ID DESC) rnk_adr*/
            /*,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio*/
			,per.FIO_RANK
        FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
        INNER JOIN ODS.PKK_PHONES dbl
          ON dbl.PHONE = snr.N2 AND dbl.OBJECTS_TYPE=8 AND dbl.PHONES_RANK=1
            and dbl.PHONES_AKT=1
            --AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) - NVL(snr.DT2, SYSDATE+365*100) > -365 +(SYSDATE-snr.DT2)
            AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) BETWEEN snr.DT2-365 AND SYSDATE
        INNER JOIN ODS.PKK_ADDRESS_HISTORY ah_src
          ON ah_src.ADDRESS_ID = dbl.OBJECTS_ID AND ah_src.OBJECTS_TYPE=2 AND ah_src.ADDRESS_RANK<=1
            AND NVL(snr.OBJECTS_ID, -1) ^= ah_src.OBJECTS_ID --физики должны быть разными
        LEFT JOIN ODS.PKK_ADDRESS adr
          ON adr.ADDRESS_ID = ah_src.ADDRESS_ID
        LEFT JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = ah_src.OBJECTS_ID AND per.FIO_RANK=1
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND snr.USER_SNA=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
          AND NOT snr.N2 IS NULL AND snr.T2 IN('PHONE_MOBILE', 'PHONE_DOP', 'PHONE_FMG', 'PHONE_PMG', 'PHONE_WORK', 'PHONE_ORG')
          --AND NOT snr.N2 LIKE '%000000'
      ) WHERE rnk_ph<=5 --AND ROWNUM<=10 --AND rnk_adr=1 --AND rnk_fio=1
    UNION ALL --ищем телефон в закрепленном за ПМЖ
      SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (
      SELECT DISTINCT doc.OBJECTS_ID AS OBJECTS_ID
            ,doc.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(doc.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(doc.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            --,'FIO_DR_REL_HIDE_PHPMG' AS T1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,DECODE(NVL(TRIM(adr.REGIONS_NAMES), '-'), '-', '', (adr.REGIONS_NAMES))
              ||DECODE(NVL(TRIM(adr.AREAS_NAMES), '-'), '-', '', (', РАЙОН '||adr.AREAS_NAMES)) 
              ||DECODE(NVL(TRIM(adr.CITIES_NAMES), '-'), '-', '', (', ' || adr.SHOTNAME_CIT ||' '|| adr.CITIES_NAMES))
              ||DECODE(NVL(TRIM(adr.STREETS_NAMES), '-'), '-', '', (', ' || adr.STREETS_NAMES)) 
              ||DECODE(NVL(TRIM(adr.HOUSE), '-'), '-', '', (', Д.'||adr.HOUSE)) 
              ||DECODE(NVL(TRIM(adr.BUILD), '-'), '-', '', (', КОРП.'||adr.BUILD)) 
              ||DECODE(NVL(TRIM(adr.FLAT),  '-'), '-', '', (', КВ.'||adr.FLAT)) AS N3
            ,'ADR_PMG'          AS T3
            ,ah_src.ADDRESS_AKT AS AKT3
            ,COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) AS DT3
            --Node 2
            ,snr.N2 AS N2
            ,'PHONE_PMG' AS T2
            ,dbl.PHONES_AKT AS AKT2
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) AS DT2
            --Node 4
            ,doc.DOCUMENTS_SERIAL||' '||doc.DOCUMENTS_NUMBER AS N4
            ,'PASP' AS T4
            ,doc.DOCUMENTS_AKT AS AKT4
            ,COALESCE(doc.MODIFICATION_DATE, doc.DOCUMENTS_CREATED)     AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            --,'N1-N3, N3-N2, N1-N4' AS ROUTE_GRAPH  
            ,'N1-N3, N1-N2, N1-N4' AS ROUTE_GRAPH  
            ,'FIO_DR_REL_HIDE_PHPMG; '||'ADDRESS_ID='||TO_CHAR(ah_src.ADDRESS_ID)||'; PHONES_ID='||TO_CHAR(dbl.PHONES_ID)
                ||'; DOCUMENTS_ID='||TO_CHAR(doc.DOCUMENTS_ID) AS ATTRIBUTES
            ,DENSE_RANK() OVER(PARTITION BY 
                      --snr.OBJECTS_ID --по каждому физику последний PHONES_ID. ! Т.е мб получаем ранг самых свежих совпадений!
                      dbl.PHONE     --по каждому PHONES_ID последние дата и актуальность
                      ORDER BY dbl.PHONES_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, SYSDATE) DESC, dbl.PHONES_CREATED DESC
                        ,dbl.PHONES_ID DESC) rnk_ph
            ,ah_src.ADDRESS_AKT, ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED
            /*,DENSE_RANK() OVER(PARTITION BY 
                      --dbl.OBJECTS_ID     --по каждому физику последний PHONES_ID
                      ah_src.ADDRESS_ID     --по каждому PHONES_ID последние дата и актуальность
                      ORDER BY ah_src.ADDRESS_AKT DESC, COALESCE(ah_src.MODIFICATION_DATE, SYSDATE) DESC, ah_src.ADDRESS_CREATED DESC
                        ,ah_src.ADDRESS_HISTORY_ID DESC) rnk_pmg*/
           /* ,DENSE_RANK() OVER(PARTITION BY 
                      --doc.OBJECTS_ID                           --по каждому физику последний паспорт
                      doc.DOCUMENTS_SERIAL, doc.DOCUMENTS_NUMBER --по каждому паспорту последние дата и актуальность
                      ORDER BY doc.DOCUMENTS_AKT DESC, COALESCE(doc.MODIFICATION_DATE, SYSDATE) DESC, doc.DOCUMENTS_CREATED DESC) rnk_doc*/
            /*,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio*/
			,per.FIO_RANK
      ,ah_src.ADDRESS_RANK
        FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
        INNER JOIN ODS.PKK_PHONES dbl
          ON dbl.PHONE = snr.N2 AND dbl.OBJECTS_TYPE=8 AND dbl.PHONES_RANK<=1
            and dbl.PHONES_AKT=1
            --AND NOT snr.N2 LIKE '%000000'
            --AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) - NVL(snr.DT2, SYSDATE+365*100) > -365+(SYSDATE-snr.DT2)
            AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) BETWEEN snr.DT2-365 AND SYSDATE
        INNER JOIN ODS.PKK_ADDRESS_HISTORY ah_src
          ON ah_src.ADDRESS_ID = dbl.OBJECTS_ID AND ah_src.OBJECTS_TYPE=5 AND ah_src.ADDRESS_RANK<=1
        LEFT JOIN ODS.PKK_ADDRESS adr
          ON adr.ADDRESS_ID = ah_src.ADDRESS_ID
        INNER JOIN ODS.PKK_DOCUMENTS_HISTORY_INFO doc
          ON doc.DOCUMENTS_ID= ah_src.OBJECTS_ID AND doc.DOCUMENTS_TYPE=21 AND doc.DOCUMENTS_RANK=1
        INNER JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = doc.OBJECTS_ID AND per.FIO_RANK=1
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND snr.USER_SNA=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
          AND NOT snr.N2 IS NULL AND snr.T2 IN('PHONE_MOBILE', 'PHONE_DOP', 'PHONE_FMG', 'PHONE_PMG', 'PHONE_WORK', 'PHONE_ORG')
          AND NVL(snr.OBJECTS_ID, -1) ^= doc.OBJECTS_ID --физики должны быть разными
      ) WHERE rnk_ph<=5 --AND rnk_pmg=1 --AND rnk_doc=1 --AND rnk_fio=1
    UNION ALL --ищем телефон в ДОП. контактах
      SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (
      SELECT DISTINCT dbl.OBJECTS_ID AS OBJECTS_ID
            ,dbl.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            --,'FIO_DR_REL_HIDE_PHDOP' AS T1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,NVL(dbl.PHONES_COMM, 'Контакт физика '||TO_CHAR(dbl.OBJECTS_ID)||' без комментария') AS N3
            ,'FIO_DOP' AS T3
            ,dbl.PHONES_AKT AS AKT3
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) AS DT3
            --Node 2
            ,snr.N2 AS N2
            ,'PHONE_DOP' AS T2
            ,dbl.PHONES_AKT AS AKT2
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) AS DT2
            --Node 4
            ,NULL     AS N4
            ,NULL     AS T4
            ,NULL     AS AKT4
            ,NULL     AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            ,'N1-N3, N3-N2' AS ROUTE_GRAPH
            ,'FIO_DR_REL_HIDE_PHDOP;'||'PHONES_ID='||TO_CHAR(dbl.PHONES_ID) AS ATTRIBUTES
            ,snr.T2 as T2_src
            ,dbl.OBJECTS_TYPE
            ,DENSE_RANK() OVER(PARTITION BY 
                              --dbl.OBJECTS_ID     --по каждому физику последний PHONES_ID
                              dbl.PHONE     --по каждому PHONES_ID последние дата и актуальность
                              ORDER BY dbl.PHONES_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, SYSDATE) DESC, dbl.PHONES_CREATED DESC
                                ,dbl.PHONES_ID DESC) rnk_ph
            /*,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio*/
        ,per.FIO_RANK
        ,dbl.PHONES_RANK
        FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
        INNER JOIN ODS.PKK_PHONES dbl
          ON dbl.PHONE = snr.N2 AND dbl.OBJECTS_TYPE=200 AND dbl.PHONES_RANK<=2
            AND NVL(snr.OBJECTS_ID, -1) ^= dbl.OBJECTS_ID --физики должны быть разными
            --AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) - NVL(snr.DT2, SYSDATE+365*100) > -365+(SYSDATE-snr.DT2)
            AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) BETWEEN snr.DT2-365 AND SYSDATE
        INNER JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = dbl.OBJECTS_ID AND per.FIO_RANK=1
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND snr.USER_SNA=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
          AND NOT snr.N2 IS NULL AND snr.T2 IN('PHONE_MOBILE', 'PHONE_DOP', 'PHONE_FMG', 'PHONE_PMG', 'PHONE_WORK', 'PHONE_ORG')
          AND NOT (NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из НБКИ:%') 
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон клиента(сайт)%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Рекомендация(ПО КЦ):%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РБО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПТ/АТМ банка%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из документов РБО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('сот. тел. заявителя(сайт)%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('ТЕЛЕФОН ИЗ НБКИ%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РНКО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПО КЦ%' ))
      ) WHERE rnk_ph<=5 --AND rnk_fio=1
    UNION ALL --ищем телефон в МОБИЛЬНЫХ
      SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (
      SELECT DISTINCT dbl.OBJECTS_ID AS OBJECTS_ID
            ,dbl.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            --,'FIO_DR_REL_HIDE_PHMOB' AS T1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,NULL       AS N3
            ,NULL       AS T3
            ,NULL       AS AKT3
            ,NULL       AS DT3
            --Node 2
            ,snr.N2 AS N2
            ,'PHONE_MOBILE' AS T2
            ,dbl.PHONES_AKT AS AKT2
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) AS DT2
            ,dbl.PHONES_COMM
            --Node 4
            ,NULL     AS N4
            ,NULL     AS T4
            ,NULL     AS AKT4
            ,NULL     AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            ,'N1-N2'AS ROUTE_GRAPH
            ,'FIO_DR_REL_HIDE_PHMOB; '||'PHONES_ID='||TO_CHAR(dbl.PHONES_ID) AS ATTRIBUTES
            ,snr.T2 as T2_SRC
            ,DENSE_RANK() OVER(PARTITION BY 
                              --dbl.OBJECTS_ID     --по каждому физику последний PHONES_ID
                              dbl.PHONE     --по каждому PHONES_ID последние дата и актуальность
                              ORDER BY dbl.PHONES_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, SYSDATE) DESC, dbl.PHONES_CREATED DESC) rnk_ph
            /*,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio*/
			,per.FIO_RANK
			,dbl.PHONES_RANK
        FROM (SELECT SRC_OBJECTS_ID, OBJECTS_ID, N2, T2, DT2, AKT2 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr 
                      WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND snr.USER_SNA=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
                            AND NOT snr.N2 IS NULL
              UNION 
              SELECT SRC_OBJECTS_ID, OBJECTS_ID, N5 AS N2, T5 AS T2, DT5 AS DT2, AKT5 AS AKT2 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr 
                      WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND snr.USER_SNA=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
                            AND NOT snr.N5 IS NULL) snr
        INNER JOIN ODS.PKK_PHONES dbl
          ON dbl.PHONE = snr.N2 AND dbl.OBJECTS_TYPE=2 AND dbl.PHONES_RANK<=3
            AND NVL(snr.OBJECTS_ID, -1) ^= dbl.OBJECTS_ID --физики должны быть разными
            --AND NOT snr.N2 LIKE '%000000'
            --AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) - NVL(snr.DT2, SYSDATE+365*100) > -365+(SYSDATE-snr.DT2)
            AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) BETWEEN snr.DT2-365 AND SYSDATE
        INNER JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = dbl.OBJECTS_ID AND per.FIO_RANK=1
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
          AND NOT snr.N2 IS NULL AND snr.T2 IN('PHONE_MOBILE', 'PHONE_DOP', 'PHONE_FMG', 'PHONE_PMG', 'PHONE_WORK', 'PHONE_ORG')
          AND NOT (NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из НБКИ:%') 
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон клиента(сайт)%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Рекомендация(ПО КЦ):%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РБО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПТ/АТМ банка%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из документов РБО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('сот. тел. заявителя(сайт)%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('ТЕЛЕФОН ИЗ НБКИ%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РНКО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПО КЦ%' ))
      ) WHERE rnk_ph<=5 --AND rnk_fio=1

    /*UNION ALL --ищем телефон в РАБОЧИХ
     SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM ( SELECT DISTINCT 
            COALESCE(wor_wor.OBJECTS_ID, wor_org.OBJECTS_ID) AS OBJECTS_ID
            ,COALESCE(wor_wor.OBJECTS_ID, wor_org.OBJECTS_ID) AS OBJECTS_ID_REL
            ,DECODE(COALESCE(wor_wor.OBJECTS_ID, wor_org.OBJECTS_ID), snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(COALESCE(wor_wor.OBJECTS_ID, wor_org.OBJECTS_ID), snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            --,'FIO_DR_REL_HIDE_PHWRK' AS T1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,DECODE(NVL(TRIM(adr.REGIONS_NAMES), '-'), '-', '', (adr.REGIONS_NAMES))
              ||DECODE(NVL(TRIM(adr.AREAS_NAMES), '-'), '-', '', (', РАЙОН '||adr.AREAS_NAMES)) 
              ||DECODE(NVL(TRIM(adr.CITIES_NAMES), '-'), '-', '', (', ' || adr.SHOTNAME_CIT ||' '|| adr.CITIES_NAMES))
              ||DECODE(NVL(TRIM(adr.STREETS_NAMES), '-'), '-', '', (', ' || adr.STREETS_NAMES)) 
              ||DECODE(NVL(TRIM(adr.HOUSE), '-'), '-', '', (', Д.'||adr.HOUSE)) 
              ||DECODE(NVL(TRIM(adr.BUILD), '-'), '-', '', (', КОРП.'||adr.BUILD)) 
              ||DECODE(NVL(TRIM(adr.FLAT),  '-'), '-', '', (', КВ.'||adr.FLAT)) AS N3
            ,'ADR_WORK' AS T3
            ,ah_src.ADDRESS_AKT AS AKT3
            ,COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) AS DT3
            ,ah_src.ADDRESS_ID
            --Node 5
            ,ph.PHONE     AS N5
            ,DECODE(dbl.OBJECTS_TYPE, 3, 'PHONE_WORK', 12, 'PHONE_ORG' ) as T5
            ,ph.PHONES_AKT    AS AKT5
            ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT5
            --Node 2
            ,dbl.PHONE AS N2
            ,DECODE(dbl.OBJECTS_TYPE, 3, 'PHONE_ORG', 12, 'PHONE_WORK' ) as T2
            ,dbl.PHONES_AKT AS AKT2
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED)     AS DT2
            --Node 4
            ,COALESCE(wor_wor.ORG_NAME, wor_org.ORG_NAME)     AS N4
            ,'ORG_NAME'       AS T4
            ,COALESCE(wor_wor.WORKS_AKT, wor_org.WORKS_AKT)    AS AKT4
            ,COALESCE(wor_wor.WORKS_LAST, wor_org.WORKS_LAST) AS WORKS_LAST
            ,COALESCE(COALESCE(wor_wor.MODIFICATION_DATE, wor_wor.WORKS_CREATED)
                                , COALESCE(wor_org.MODIFICATION_DATE, wor_org.WORKS_CREATED))     AS DT4
            ,'N1-N4, N4-N2, N4-N3, N4-N5' AS ROUTE_GRAPH
            ,'FIO_DR_REL_HIDE_PHWRK; '||'WORKS_ID='||TO_CHAR(COALESCE(wor_wor.WORKS_ID, wor_org.WORKS_ID))||'; ORG_ID='||TO_CHAR(COALESCE(wor_wor.ORG_ID, wor_org.ORG_ID))||'; ADDRESS_ID='||TO_CHAR(ah_src.ADDRESS_ID)
                ||'; PHONES_ID='||TO_CHAR(dbl.PHONES_ID)||'; PHONES_ID='||TO_CHAR(ph.PHONES_ID) AS ATTRIBUTES
            ,snr.T2 as T2_SRC
            ,DENSE_RANK() OVER(PARTITION BY 
                  --wor_wor.OBJECTS_ID       --по каждому физику последняя работа
                  COALESCE(wor_wor.WORKS_ID, wor_org.ORG_ID)             --по каждой работе последние дата и актуальность
                  ORDER BY COALESCE(wor_wor.WORKS_AKT, wor_org.WORKS_AKT) DESC
                    ,COALESCE(COALESCE(wor_wor.MODIFICATION_DATE, wor_wor.WORKS_CREATED)
                                , COALESCE(wor_org.MODIFICATION_DATE, wor_org.WORKS_CREATED)) DESC) rnk_wor
            ,DENSE_RANK() OVER(PARTITION BY 
                  COALESCE(wor_wor.ORG_ID, wor_org.ORG_ID)            --по каждомй работе последний адрес
                  --ah_src.ADDRESS_ID   --по каждому адресу последние дата и актуальность
                  ORDER BY ah_src.ADDRESS_AKT DESC, COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) DESC) rnk_adr
            ,DENSE_RANK() OVER(PARTITION BY 
                  --COALESCE(wor_wor.WORKS_ID, wor_org.WORKS_ID)     --по каждой работе последний телефон
                  dbl.PHONE       --по кадому телефону последние дата и актуальность
                  ORDER BY dbl.PHONES_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) DESC) rnk_ph_dbl
            ,DENSE_RANK() OVER(PARTITION BY 
                  COALESCE(wor_wor.WORKS_ID, wor_org.ORG_ID)
                  ,DECODE(dbl.OBJECTS_TYPE, 3, wor_wor.WORKS_ID, 12, wor_org.ORG_ID ), dbl.OBJECTS_TYPE   --по каждой работе последний телефон
                  --ph.PHONE       --по кадому телефону последние дата и актуальность
                  ORDER BY ph.PHONES_AKT DESC, COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) DESC) rnk_ph
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio
        FROM (SELECT SRC_OBJECTS_ID, OBJECTS_ID, N2, T2, DT2, AKT2 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr 
                      WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
                            AND NOT snr.N2 IS NULL
              UNION 
              SELECT SRC_OBJECTS_ID, OBJECTS_ID, N5 AS N2, T5 AS T2, DT5 AS DT2, AKT5 AS AKT2 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr 
                      WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
                            AND NOT snr.N5 IS NULL) snr
        INNER JOIN ODS.PKK_PHONES 
                  --CPD.PHONES@DBLINK_PKK
                  dbl
          ON dbl.PHONE = snr.N2 AND dbl.OBJECTS_TYPE IN(3,12)
            AND NOT snr.N2 LIKE '%000000'
            --AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) - NVL(snr.DT2, SYSDATE+365*100) > -365 +(SYSDATE-snr.DT2)
            AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) BETWEEN snr.DT2-365 AND SYSDATE
        LEFT JOIN ODS.PKK_WORKS_INFO wor_org
          ON wor_org.ORG_ID = dbl.OBJECTS_ID AND dbl.OBJECTS_TYPE=3
        LEFT JOIN ODS.PKK_WORKS_INFO wor_wor
          ON wor_wor.WORKS_ID = dbl.OBJECTS_ID AND dbl.OBJECTS_TYPE=12
        LEFT JOIN ODS.PKK_PHONES 
                  --CPD.PHONES@DBLINK_PKK
                  ph
          --если исходный телефон ORG_ID, то притягиваем по WORKS_ID, иначе по ORG_ID; учитываем тип
          ON ph.OBJECTS_ID = DECODE(dbl.OBJECTS_TYPE, 3, wor_org.WORKS_ID, 12, wor_wor.ORG_ID ) 
            AND ph.OBJECTS_TYPE = DECODE(dbl.OBJECTS_TYPE, 3, 12, 12, 3)
        LEFT JOIN ODS.PKK_ADDRESS_HISTORY 
              --CPD.ADDRESS_HISTORY@DBLINK_PKK
               ah_src 
          ON --ah_src.ADDRESS_ID --!! КАК НЕТ?!
            ah_src.OBJECTS_ID = COALESCE(wor_org.ORG_ID,wor_wor.ORG_ID)
            --DECODE(dbl.OBJECTS_TYPE, 3, wor_org.ORG_ID, 12, wor_wor.ORG_ID )
            AND ah_src.OBJECTS_TYPE = 3 --2 OBJECTS_ID, 3 ORG_ID, 5 DOCUMENTS_ID, 19 ADDRESS_ID, 200 ? 
            --AND ah_src.ADDRESS_AKT=1
        LEFT JOIN ODS.PKK_ADDRESS adr
          ON ah_src.ADDRESS_ID = adr.ADDRESS_ID
        LEFT JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = COALESCE(wor_wor.OBJECTS_ID, wor_org.OBJECTS_ID, -1)
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
          AND NOT snr.N2 IS NULL AND snr.T2 IN('PHONE_MOBILE', 'PHONE_DOP', 'PHONE_ORG', 'PHONE_FMG', 'PHONE_PMG', 'PHONE_WORK')
          AND NVL(snr.OBJECTS_ID, -1) ^= COALESCE(wor_wor.OBJECTS_ID, wor_org.OBJECTS_ID, -1) --физики должны быть разными
          AND NOT COALESCE(wor_org.WORKS_ID, wor_wor.WORKS_ID) IS NULL
      ) WHERE rnk_ph_dbl<=5 AND rnk_wor<=1 AND rnk_adr=1 AND rnk_ph<=5 AND rnk_fio=1*/
      ; 
     
  CURSOR cur_PERSON_REL_CRED (p_cur_OBJECTS_ID NUMBER) IS 
      SELECT DISTINCT OBJECTS_ID
            ,OBJECTS_ID_REL
            ,ROOT_PERS
            ,ROOT_PERS_REL
            --далее пошла нужная информация родителя
            ,N1
            ,T1
            ,AKT1
            ,DT1
            --далее пошла нужная информация ребенка
            ,N3
            ,T3 --||DECODE(FAMILY_REL_STATUS, 1, '_'||FAMILY_REL_NAME||CONTACT_COMMENT, '') 
            ,AKT3
            ,DT3
            ,ROUTE_GRAPH
          FROM (
          SELECT cr.OBJECTS_ID as OBJECTS_ID
            ,cr_por.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(cr.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(cr_por.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            ,snr.N1    AS N1
            ,snr.T1   AS T1
            ,snr.AKT1 AS AKT1
            ,snr.DT1 AS DT1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N3
            ,DECODE(cr_por.TYPE_REQUEST_ID, 6, 'FIO_DR_REL_POR', 7, 'FIO_DR_REL_POR', 13, 'FIO_DR_REL_SOZ') AS T3
            ,cr_por.TYPE_REQUEST_ID
            ,per.FIO_AKT AS AKT3
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT3
            ,'N1-N3' AS ROUTE_GRAPH
            ,DENSE_RANK() OVER(PARTITION BY cr.OBJECTS_ID, cr_por.OBJECTS_ID
                              ORDER BY cr_por.OBJECTS_ID DESC, COALESCE(cr_por.MODIFICATION_DATE_REQUEST, cr_por.CREATED_DATE) DESC) rnk
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio_rel
          FROM ODS.PKK_C_REQUEST_SNA cr
          INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
                      FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                      WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN AND T1='FIO_DR' AND LEVEL_NODE IN(1,3) --and rownum=1
                    UNION
                    SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
                      FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                      WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN AND T3 LIKE 'FIO_DR%' AND LEVEL_NODE IN(1,3)) snr
            ON snr.OBJECTS_ID = cr.OBJECTS_ID AND cr.TYPE_REQUEST_ID=1
          INNER JOIN ODS.PKK_C_REQUEST_SNA cr_por
            ON cr_por.PARENT_ID = cr.PARENT_ID AND cr_por.TYPE_REQUEST_ID IN(6,7,13)
              AND cr_por.OBJECTS_ID ^= cr.OBJECTS_ID
          INNER JOIN ODS.PKK_PERSON_INFO per
            ON per.OBJECTS_ID = cr_por.OBJECTS_ID
            WHERE snr.SRC_OBJECTS_ID=p_cur_OBJECTS_ID
          ) WHERE rnk=1 AND rnk_fio_rel=1
      ;

  CURSOR cur_PERSON_BNK (p_cur_OBJECTS_ID NUMBER) IS 
      SELECT DISTINCT OBJECTS_ID
            ,OBJECTS_ID_REL
            ,ROOT_PERS
            ,ROOT_PERS_REL
            --далее пошла нужная информация родителя
            ,N1
            ,T1
            ,AKT1
            ,DT1
            --далее пошла нужная информация ребенка
            ,N3
            ,T3 --||DECODE(FAMILY_REL_STATUS, 1, '_'||FAMILY_REL_NAME||CONTACT_COMMENT, '') 
            ,AKT3
            ,DT3
            ,ROUTE_GRAPH
          FROM (
          SELECT snr.OBJECTS_ID as OBJECTS_ID
            ,snr.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(snr.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(snr.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            ,snr.N1    AS N1
            ,snr.T1   AS T1
            ,snr.AKT1 AS AKT1
            ,snr.DT1 AS DT1
            ,(SELECT TO_CHAR(snr.OBJECTS_ID)||' - '||'БНК. Всего: '||TO_CHAR(COUNT(DISTINCT R_TYPE_ID)) 
                ||'; Max: '||SUBSTR(TO_CHAR(MAX(R_TYPE_ID)), 1,2) 
              FROM SNA_REQUEST_BNK_HIST WHERE OBJECTS_ID=snr.OBJECTS_ID) AS N3
            ,'BNK_PKK' AS T3
            ,0 AS AKT3
            ,SYSDATE AS DT3
            ,'N1-N3' AS ROUTE_GRAPH
            --,DENSE_RANK() OVER(PARTITION BY cr.OBJECTS_ID, cr_por.OBJECTS_ID
              --                ORDER BY cr_por.OBJECTS_ID DESC, COALESCE(cr_por.MODIFICATION_DATE_REQUEST, cr_por.CREATED_DATE) DESC) rnk
          FROM (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
                      FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                      WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN AND T1='FIO_DR' AND LEVEL_NODE IN(1,3) --and rownum=1
                    UNION
                    SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
                      FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                      WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN AND T3 LIKE 'FIO_DR%' AND LEVEL_NODE IN(1,3)) snr
            WHERE snr.SRC_OBJECTS_ID=p_cur_OBJECTS_ID
          ) WHERE NOT N3 LIKE '%Всего: 0%'
      ;
  --cur_PERSON_NODE_1 cur_PERSON_NODE%ROWTYPE;
   
    
BEGIN
  DBMS_OUTPUT.ENABLE;
  --p_DATE_RID := SYSDATE-10;
  
  /*v_FLAG_INFO_EXISTS    :=  0;
  v_CNT_NODE            := -1;
  v_CNT_LINK            := -1;
  v_CNT_OBJECTS_ID      := -1;
  v_CNT_FROD            := -1;
  v_CNT_BNK             := -1;
  v_CNT_DEFOLT          := -1;
  v_CNT_ALERT_PERSON    := -1;*/
  /*IF v_LOGIN IS NULL THEN 
    v_LOGIN := '-';
  END IF;*/
  
  DBMS_OUTPUT.PUT_LINE('Время 0: '||TO_CHAR(SYSTIMESTAMP));
  
  DELETE FROM SNAUSER.SNA_REQUEST_NODE_ROUTES WHERE SRC_OBJECTS_ID = v_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN;

  --S_1. Ищем прямые указанные связи с реальными физиками
  FOR cur_PERSON_REL_1 IN cur_PERSON_REL(v_OBJECTS_ID) LOOP
      IF (cur_PERSON_REL_1.OBJECTS_ID=cur_PERSON_REL_1.OBJECTS_ID_REL AND cur_PERSON_REL_1.T3='FIO_DR_REL_REF') THEN
        cur_PERSON_REL_1.T3 := 'FIO_DR';
      END IF;
      
      INSERT INTO SNAUSER.SNA_REQUEST_NODE_ROUTES 
        (SRC_REQUEST_ID, SRC_OBJECTS_ID,SNA_REQUEST_DATE, USER_SNA
            ,OBJECTS_ID, OBJECTS_ID_REL
            ,ROOT_PERS, ROOT_PERS_REL
            ,N1, T1, AKT1, DT1
            ,N3, T3, AKT3, DT3
            ,ROUTE_GRAPH
            ,LEVEL_NODE
            )
          VALUES (v_REQUEST_ID, v_OBJECTS_ID, v_SNA_DATE_BEGIN, v_LOGIN
            ,cur_PERSON_REL_1.OBJECTS_ID, cur_PERSON_REL_1.OBJECTS_ID_REL
            ,cur_PERSON_REL_1.ROOT_PERS, cur_PERSON_REL_1.ROOT_PERS_REL
            ,cur_PERSON_REL_1.N1, cur_PERSON_REL_1.T1, cur_PERSON_REL_1.AKT1, cur_PERSON_REL_1.DT1
            ,cur_PERSON_REL_1.N3, cur_PERSON_REL_1.T3, cur_PERSON_REL_1.AKT3, cur_PERSON_REL_1.DT3
            ,cur_PERSON_REL_1.ROUTE_GRAPH
            ,1);
        /*DBMS_OUTPUT.PUT_LINE('Время  : '||TO_CHAR(SYSTIMESTAMP)||' - получили = '
            || cur_PERSON_REL_1.OBJECTS_ID||' '||cur_PERSON_REL_1.OBJECTS_ID_REL);*/
  END LOOP;
  --COMMIT;

  DBMS_OUTPUT.PUT_LINE('Время 1: '||TO_CHAR(SYSTIMESTAMP));
  
  --S_2. Дополняем информацией по первому кругу фзииков
  p_cnt_Level := 2;
  FOR cur_PERSON_NODE_2 IN cur_PERSON_NODE(v_OBJECTS_ID, 1) LOOP
     INSERT INTO SNAUSER.SNA_REQUEST_NODE_ROUTES 
        (SRC_REQUEST_ID, SRC_OBJECTS_ID,SNA_REQUEST_DATE, USER_SNA
            ,OBJECTS_ID, OBJECTS_ID_REL
            ,ROOT_PERS, ROOT_PERS_REL
            ,N1, T1, AKT1, DT1
            ,N2, T2, AKT2, DT2 
            ,N3, T3, AKT3, DT3
            ,N4, T4, AKT4, DT4
            ,N5, T5, AKT5, DT5
            ,ROUTE_GRAPH
            ,ATTRIBUTES
            ,LEVEL_NODE
            )
          VALUES (v_REQUEST_ID, v_OBJECTS_ID, v_SNA_DATE_BEGIN, v_LOGIN
            ,cur_PERSON_NODE_2.OBJECTS_ID, cur_PERSON_NODE_2.OBJECTS_ID_REL
            ,cur_PERSON_NODE_2.ROOT_PERS, cur_PERSON_NODE_2.ROOT_PERS_REL
            ,cur_PERSON_NODE_2.N1, cur_PERSON_NODE_2.T1, cur_PERSON_NODE_2.AKT1, cur_PERSON_NODE_2.DT1
            ,cur_PERSON_NODE_2.N2, cur_PERSON_NODE_2.T2, cur_PERSON_NODE_2.AKT2, cur_PERSON_NODE_2.DT2
            ,cur_PERSON_NODE_2.N3, cur_PERSON_NODE_2.T3, cur_PERSON_NODE_2.AKT3, cur_PERSON_NODE_2.DT3
            ,cur_PERSON_NODE_2.N4, cur_PERSON_NODE_2.T4, cur_PERSON_NODE_2.AKT4, cur_PERSON_NODE_2.DT4
            ,cur_PERSON_NODE_2.N5, cur_PERSON_NODE_2.T5, cur_PERSON_NODE_2.AKT5, cur_PERSON_NODE_2.DT5
            ,cur_PERSON_NODE_2.ROUTE_GRAPH
            ,cur_PERSON_NODE_2.ATTRIBUTES
            ,p_cnt_Level
            );
      /*IF v_FLAG_INFO_EXISTS^=1 THEN 
        v_FLAG_INFO_EXISTS := 1;
      END IF;*/
  END LOOP;
  --COMMIT;

  DBMS_OUTPUT.PUT_LINE('Время 2: '||SYSTIMESTAMP||' - после подтягивания данных '||TO_CHAR(v_OBJECTS_ID));


  --S_3. дополняем ID неявных физиков 
    FOR cur_PERSON_HIDE_1 IN cur_PERSON_HIDE(v_OBJECTS_ID) LOOP
      INSERT INTO SNAUSER.SNA_REQUEST_NODE_ROUTES 
        (SRC_REQUEST_ID, SRC_OBJECTS_ID,SNA_REQUEST_DATE, USER_SNA
            ,OBJECTS_ID, OBJECTS_ID_REL
            ,ROOT_PERS, ROOT_PERS_REL
            ,N1, T1, AKT1, DT1
            ,N2, T2, AKT2, DT2 
            ,N3, T3, AKT3, DT3
            ,N4, T4, AKT4, DT4
            ,N5, T5, AKT5, DT5
            ,ROUTE_GRAPH
            ,ATTRIBUTES
            ,LEVEL_NODE)
          VALUES (v_REQUEST_ID, v_OBJECTS_ID, v_SNA_DATE_BEGIN, v_LOGIN
            ,cur_PERSON_HIDE_1.OBJECTS_ID, cur_PERSON_HIDE_1.OBJECTS_ID_REL
            ,cur_PERSON_HIDE_1.ROOT_PERS, cur_PERSON_HIDE_1.ROOT_PERS_REL
            ,cur_PERSON_HIDE_1.N1, cur_PERSON_HIDE_1.T1, cur_PERSON_HIDE_1.AKT1, cur_PERSON_HIDE_1.DT1
            ,cur_PERSON_HIDE_1.N2, cur_PERSON_HIDE_1.T2, cur_PERSON_HIDE_1.AKT2, cur_PERSON_HIDE_1.DT2
            ,cur_PERSON_HIDE_1.N3, cur_PERSON_HIDE_1.T3, cur_PERSON_HIDE_1.AKT3, cur_PERSON_HIDE_1.DT3
            ,cur_PERSON_HIDE_1.N4, cur_PERSON_HIDE_1.T4, cur_PERSON_HIDE_1.AKT4, cur_PERSON_HIDE_1.DT4
            ,cur_PERSON_HIDE_1.N5, cur_PERSON_HIDE_1.T5, cur_PERSON_HIDE_1.AKT5, cur_PERSON_HIDE_1.DT5
            ,cur_PERSON_HIDE_1.ROUTE_GRAPH
            ,cur_PERSON_HIDE_1.ATTRIBUTES
            ,3);
  END LOOP;
  --COMMIT;
  DBMS_OUTPUT.PUT_LINE('Время 3: '||SYSTIMESTAMP||' - после подтягивания скрытых ID '||TO_CHAR(v_OBJECTS_ID));
  
  --S_5. Добавляем процентированный БНК ))
  /*FOR cur_PERSON_ID IN (SELECT DISTINCT OBJECTS_ID FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                          WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND NVL(USER_SNA, '-')=NVL(v_LOGIN, '-')
                        UNION SELECT DISTINCT OBJECTS_ID_REL AS OBJECTS_ID FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                          WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND NVL(USER_SNA, '-')=NVL(v_LOGIN, '-') ) LOOP
    ODS.FIND_R_PERSON(cur_PERSON_ID.OBJECTS_ID);
  END LOOP;
  COMMIT;
  
  FOR cur_PERSON_BNK_obj IN cur_PERSON_BNK(v_OBJECTS_ID) LOOP
      INSERT INTO SNAUSER.SNA_REQUEST_NODE_ROUTES 
        (SRC_REQUEST_ID, SRC_OBJECTS_ID,SNA_REQUEST_DATE, USER_SNA
            ,OBJECTS_ID, OBJECTS_ID_REL
            ,ROOT_PERS, ROOT_PERS_REL
            ,N1, T1, AKT1, DT1
            ,N3, T3, AKT3, DT3
            ,ROUTE_GRAPH
            ,LEVEL_NODE
            )
          VALUES (v_REQUEST_ID, v_OBJECTS_ID, v_SNA_DATE_BEGIN, v_LOGIN
            ,cur_PERSON_BNK_obj.OBJECTS_ID, cur_PERSON_BNK_obj.OBJECTS_ID_REL
            ,cur_PERSON_BNK_obj.ROOT_PERS, cur_PERSON_BNK_obj.ROOT_PERS_REL
            ,cur_PERSON_BNK_obj.N1, cur_PERSON_BNK_obj.T1, cur_PERSON_BNK_obj.AKT1, cur_PERSON_BNK_obj.DT1
            ,cur_PERSON_BNK_obj.N3, cur_PERSON_BNK_obj.T3, cur_PERSON_BNK_obj.AKT3, cur_PERSON_BNK_obj.DT3
            ,cur_PERSON_BNK_obj.ROUTE_GRAPH
            ,5);
  END LOOP;
  COMMIT;*/
  
  
  
  --S_4. дополняем информацию по неявно связанным физикам
  FOR cur_PERSON_NODE_2 IN cur_PERSON_NODE(v_OBJECTS_ID, 3) LOOP
     INSERT INTO SNAUSER.SNA_REQUEST_NODE_ROUTES 
        (SRC_REQUEST_ID, SRC_OBJECTS_ID,SNA_REQUEST_DATE, USER_SNA
            ,OBJECTS_ID, OBJECTS_ID_REL
            ,ROOT_PERS, ROOT_PERS_REL
            ,N1, T1, AKT1, DT1
            ,N2, T2, AKT2, DT2 
            ,N3, T3, AKT3, DT3
            ,N4, T4, AKT4, DT4
            ,N5, T5, AKT5, DT5
            ,ROUTE_GRAPH
            ,ATTRIBUTES
            ,LEVEL_NODE
            )
          VALUES (v_REQUEST_ID, v_OBJECTS_ID, v_SNA_DATE_BEGIN, v_LOGIN
            ,cur_PERSON_NODE_2.OBJECTS_ID, cur_PERSON_NODE_2.OBJECTS_ID_REL
            ,cur_PERSON_NODE_2.ROOT_PERS, cur_PERSON_NODE_2.ROOT_PERS_REL
            ,cur_PERSON_NODE_2.N1, cur_PERSON_NODE_2.T1, cur_PERSON_NODE_2.AKT1, cur_PERSON_NODE_2.DT1
            ,cur_PERSON_NODE_2.N2, cur_PERSON_NODE_2.T2, cur_PERSON_NODE_2.AKT2, cur_PERSON_NODE_2.DT2
            ,cur_PERSON_NODE_2.N3, cur_PERSON_NODE_2.T3, cur_PERSON_NODE_2.AKT3, cur_PERSON_NODE_2.DT3
            ,cur_PERSON_NODE_2.N4, cur_PERSON_NODE_2.T4, cur_PERSON_NODE_2.AKT4, cur_PERSON_NODE_2.DT4
            ,cur_PERSON_NODE_2.N5, cur_PERSON_NODE_2.T5, cur_PERSON_NODE_2.AKT5, cur_PERSON_NODE_2.DT5
            ,cur_PERSON_NODE_2.ROUTE_GRAPH
            ,cur_PERSON_NODE_2.ATTRIBUTES
            ,4
            );

  END LOOP;
  --COMMIT;
  
  DBMS_OUTPUT.PUT_LINE('Время 4: '||SYSTIMESTAMP||' - после подтягивания данных для скрытых ID '||TO_CHAR(v_OBJECTS_ID));
  
  
  --ПОМЕЧАЕМ НОДЫ КОТОЫРЕ БУДЕМ ПОКАЗЫВАТЬ
  --отметка N2SHOW 
  UPDATE SNAUSER.SNA_REQUEST_NODE_ROUTES SET ATTRIBUTES = ATTRIBUTES||'; N2SHOW' 
    WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND USER_SNA=v_LOGIN
    AND N2 IN (SELECT DISTINCT snr.N2 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
                  LEFT JOIN SNAUSER.SNA_REQUEST_NODE_ROUTES sn2_1
                    ON snr.SRC_OBJECTS_ID=sn2_1.SRC_OBJECTS_ID AND snr.USER_SNA=sn2_1.USER_SNA
                      AND snr.N2=sn2_1.N2 AND snr.OBJECTS_ID^=sn2_1.OBJECTS_ID AND snr.N1^=sn2_1.N1
                  LEFT JOIN SNAUSER.SNA_REQUEST_NODE_ROUTES sn2_2
                    ON snr.SRC_OBJECTS_ID=sn2_2.SRC_OBJECTS_ID AND snr.USER_SNA=sn2_2.USER_SNA
                      AND snr.N2=sn2_2.N5 AND snr.OBJECTS_ID^=sn2_2.OBJECTS_ID AND snr.N1^=sn2_2.N1
                  WHERE snr.SRC_OBJECTS_ID=v_OBJECTS_ID AND NOT COALESCE(sn2_1.SRC_OBJECTS_ID, sn2_2.SRC_OBJECTS_ID) IS NULL     
    ) ;
  --отметка N3SHOW: Адрес и Доп. контакт
  UPDATE SNAUSER.SNA_REQUEST_NODE_ROUTES SET ATTRIBUTES = ATTRIBUTES||'; N3SHOW' 
    WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND USER_SNA=v_LOGIN
    AND (N3 IN (SELECT DISTINCT snr.N3 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
                  INNER JOIN SNAUSER.SNA_REQUEST_NODE_ROUTES sn_1
                    ON snr.SRC_OBJECTS_ID=sn_1.SRC_OBJECTS_ID AND snr.USER_SNA=sn_1.USER_SNA
                      AND snr.N3=sn_1.N3 AND snr.OBJECTS_ID^=sn_1.OBJECTS_ID AND snr.N1^=sn_1.N1
                  WHERE snr.SRC_OBJECTS_ID=v_OBJECTS_ID AND (snr.T3 LIKE 'ADR%' OR snr.T3='FIO_DOP')  )
          OR (T3='FIO_DOP' AND ATTRIBUTES LIKE '%N2SHOW%' /*OR LEVEL_NODE=2*/) OR T3='NEG' OR T3='BNK_PKK'
          )
      ;
  UPDATE SNAUSER.SNA_REQUEST_NODE_ROUTES SET ATTRIBUTES = ATTRIBUTES||'; N3SHOW' 
    WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND USER_SNA=v_LOGIN AND NOT (T3 LIKE 'ADR%' OR T3='FIO_DOP')  ;
  --отметка N4SHOW: паспорта, Имя Организации, EMAIL
  UPDATE SNAUSER.SNA_REQUEST_NODE_ROUTES SET ATTRIBUTES = ATTRIBUTES||'; N4SHOW' 
    WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND USER_SNA=v_LOGIN
    AND N4 IN (SELECT DISTINCT snr.N4 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
                  INNER JOIN SNAUSER.SNA_REQUEST_NODE_ROUTES sn_1
                    ON snr.SRC_OBJECTS_ID=sn_1.SRC_OBJECTS_ID AND snr.USER_SNA=sn_1.USER_SNA
                      AND snr.N4=sn_1.N4 AND snr.OBJECTS_ID^=sn_1.OBJECTS_ID AND snr.N1^=sn_1.N1
                  WHERE snr.SRC_OBJECTS_ID=v_OBJECTS_ID  ) ;
  --отметка N5SHOW 
  UPDATE SNAUSER.SNA_REQUEST_NODE_ROUTES SET ATTRIBUTES = ATTRIBUTES||'; N5SHOW' 
    WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND USER_SNA=v_LOGIN
    AND N5 IN (SELECT DISTINCT snr.N5 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
                  LEFT JOIN SNAUSER.SNA_REQUEST_NODE_ROUTES sn5_1
                    ON snr.SRC_OBJECTS_ID=sn5_1.SRC_OBJECTS_ID AND snr.USER_SNA=sn5_1.USER_SNA
                      AND snr.N5=sn5_1.N2 AND snr.OBJECTS_ID^=sn5_1.OBJECTS_ID AND snr.N1^=sn5_1.N1
                  LEFT JOIN SNAUSER.SNA_REQUEST_NODE_ROUTES sn5_2
                    ON snr.SRC_OBJECTS_ID=sn5_2.SRC_OBJECTS_ID AND snr.USER_SNA=sn5_2.USER_SNA
                      AND snr.N5=sn5_2.N5 AND snr.OBJECTS_ID^=sn5_2.OBJECTS_ID AND snr.N1^=sn5_2.N1
                  WHERE snr.SRC_OBJECTS_ID=v_OBJECTS_ID AND NOT COALESCE(sn5_1.SRC_OBJECTS_ID, sn5_2.SRC_OBJECTS_ID) IS NULL  ) ;
  --ДЛЯ РАБОЧИХ ДАННЫХ СДЕЛАТЬ: ЕСЛИ ЕСТЬ ОТМЕТКА SHOW хотя бы на 1 АТРИБУТ, ТО заполнить остальные
  UPDATE SNAUSER.SNA_REQUEST_NODE_ROUTES SET ATTRIBUTES = ATTRIBUTES||'; N2SHOW; N3SHOW; N4SHOW; N5SHOW' 
    WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND USER_SNA=v_LOGIN AND T2='PHONE_ORG' AND ATTRIBUTES LIKE '%SHOW%';

  COMMIT;

  v_SNA_DATE_END := SYSDATE ;
  v_SNA_TIME_LOAD := ROUND((v_SNA_DATE_END-v_SNA_DATE_BEGIN)*86400, 0);
  v_RESULT_MESSAGE := 'OK';
  
  --собираем доп. информацию по собранным данным
  SELECT (CASE WHEN COUNT(SRC_OBJECTS_ID)>0 THEN 1 ELSE 0 END)
      ,COUNT(DISTINCT N1)+COUNT(DISTINCT N2)+COUNT(DISTINCT N3)+COUNT(DISTINCT N4)+COUNT(DISTINCT N5), -1
      ,COUNT(DISTINCT OBJECTS_ID)
      ,COUNT(DISTINCT (CASE WHEN T3='NEG' AND N3 LIKE '%ФРОД%' THEN 1 ELSE NULL END))
      ,COUNT(DISTINCT (CASE WHEN T3='NEG' AND N3 LIKE '%БНК%' THEN 1 ELSE NULL END))
      ,COUNT(DISTINCT (CASE WHEN T3='NEG' AND N3 LIKE '%ДЕФОЛТ%' THEN 1 ELSE NULL END))
      ,COUNT(DISTINCT (CASE WHEN T3='NEG' THEN 1 ELSE NULL END))
    INTO v_FLAG_INFO_EXISTS
      , v_CNT_NODE, v_CNT_LINK
      , v_CNT_OBJECTS_ID
      , v_CNT_FROD, v_CNT_BNK, v_CNT_DEFOLT
      , v_CNT_ALERT_PERSON
    FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
    WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND USER_SNA=v_LOGIN;

  --DELETE FROM SNAUSER.SNA_REQUEST_NODE_ROUTES_TEMP WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN;
  
  --INSERT INTO SNAUSER.SNA_REQUEST_NODE_ROUTES SELECT * FROM SNAUSER.SNA_REQUEST_NODE_ROUTES_TEMP;

  -- финализация данных в таблице логирования
   UPDATE SNAUSER.SNA_REQUEST_HISTORY SET 
          SNA_DATE_END = v_SNA_DATE_END
          ,SNA_TIME_LOAD = v_SNA_TIME_LOAD
          ,FLAG_INFO_EXISTS = v_FLAG_INFO_EXISTS
          ,OBJECTS_ID = v_OBJECTS_ID
          ,CNT_NODE = v_CNT_NODE
          ,CNT_LINK = v_CNT_LINK
          ,CNT_OBJECTS_ID = v_CNT_OBJECTS_ID 
          ,CNT_FROD = v_CNT_FROD 
          ,CNT_BNK = v_CNT_BNK 
          ,CNT_DEFOLT = v_CNT_DEFOLT 
          ,CNT_ALERT_PERSON = v_CNT_ALERT_PERSON 
    WHERE SNA_ID = v_SNA_ID; 
    
  COMMIT;

  EXCEPTION
    WHEN OTHERS
    THEN v_RESULT_MESSAGE := SQLERRM ;
        -- финализация данных в таблице логирования
         UPDATE SNAUSER.SNA_REQUEST_HISTORY SET 
          SNA_DATE_END = v_SNA_DATE_END
          ,SNA_TIME_LOAD = v_SNA_TIME_LOAD
          ,FLAG_INFO_EXISTS = v_FLAG_INFO_EXISTS
          ,OBJECTS_ID = v_OBJECTS_ID
          ,CNT_NODE = v_CNT_NODE
          ,CNT_LINK = v_CNT_LINK
          ,CNT_OBJECTS_ID = v_CNT_OBJECTS_ID 
          ,CNT_FROD = v_CNT_FROD 
          ,CNT_BNK = v_CNT_BNK 
          ,CNT_DEFOLT = v_CNT_DEFOLT 
          ,CNT_ALERT_PERSON = v_CNT_ALERT_PERSON 
    WHERE SNA_ID = v_SNA_ID; 
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_PKK_PHONES" 
-- ========================================================================
-- ==	ПРОЦЕДУРА    Обновление таблицы PKK_PHONES 
-- ==	ОПИСАНИЕ:	   Основные данные о телефонах
-- ========================================================================
-- ==	СОЗДАНИЕ:		  19.02.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	19.02.2016 (ТРАХАЧЕВ В.В.)
-- ==
/*PKK_PHONES:19.02.16 10:16:44-                 (13656). От 19.02.16 07:16:44 до 19.02.16 07:25:26
  PKK_PHONES:19.02.16 07:16:44-19.02.16 10:29:44(11982). От 19.02.16 07:27:47 до 19.02.16 07:37:55
  PKK_PHONES:19.02.16 07:29:44-19.02.16 11:02:25(13484). От 19.02.16 08:00:20 до 19.02.16 08:06:32
  PKK_PHONES:19.02.16 08:02:25-19.02.16 11:20:57(13343). От 19.02.16 08:19:01 до 19.02.16 08:24:20
  PKK_PHONES:19.02.16 08:20:57-03.03.16 15:03:02(547416). От 03.03.16 12:03:12 до 03.03.16 13:09:31
  PKK_PHONES:03.03.16 12:03:02-09.03.16 06:55:37(145835). От 09.03.16 03:55:40 до 09.03.16 04:29:34
  PKK_PHONES:29.02.16 07:33:08-09.03.16 07:38:41(847). От 09.03.16 04:38:44 до 09.03.16 04:53:57
  PKK_PHONES:23.02.16 07:38:41-09.03.16 07:55:41(445). От 09.03.16 04:55:58 до 09.03.16 05:14:24
*/
-- ========================================================================
AS    
  last_DT_SFF DATE; -- последний MODIFICATION_DATE в SFF
  last_DT_PKK DATE; -- последний MODIFICATION_DATE в ПКК
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp;
  
  SELECT MAX(PHONES_CREATED)-5/24 INTO last_DT_SFF FROM ODS.PKK_PHONES
      WHERE PHONES_ID>=(SELECT MAX(PHONES_ID)-1000 FROM ODS.PKK_PHONES);

  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_PHONES tar
	USING (SELECT * FROM ODS.VIEW_PKK_PHONES WHERE PHONES_CREATED > last_DT_SFF
          UNION 
         SELECT * FROM ODS.VIEW_PKK_PHONES WHERE MODIFICATION_DATE > last_DT_SFF
                ) src
    ON (src.PHONES_ID = tar.PHONES_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        --tar.PHONES_ID=src.PHONES_ID
        --,tar.OBJECTS_ID=src.OBJECTS_ID
        tar.OBJECTS_TYPE=src.OBJECTS_TYPE
        ,tar.PHONES_AKT=src.PHONES_AKT
        ,tar.PHONES_COMM=src.PHONES_COMM
        --,tar.PHONES_CREATED=src.PHONES_CREATED
        --,tar.CREATED_USER_ID=src.CREATED_USER_ID
        --,tar.CREATED_GROUP_ID=src.CREATED_GROUP_ID
        --,tar.GROUPS_NAME=src.GROUPS_NAME
        ,tar.MODIFICATION_DATE=src.MODIFICATION_DATE
        ,tar.PHONES_LAST=src.PHONES_LAST
        ,tar.FAMILY_REL =src.FAMILY_REL
        ,tar.PHONE=src.PHONE
        ,tar.ADDRESS_ID=src.ADDRESS_ID
        ,tar.FAMILY_REL_NAME=src.FAMILY_REL_NAME
        ,tar.FAMILY_REL_STATUS=src.FAMILY_REL_STATUS
        ,tar.FAMILY_SEQ_ID=src.FAMILY_SEQ_ID
        ,tar.DATE_UPD=SYSDATE
        ,tar.PHONES_RANK = 1
        , tar.OBJECTS_RANK = 1
      WHERE NOT ( NVL(tar.PHONES_AKT, -1) = NVL(src.PHONES_AKT, -1)
        AND NVL(tar.PHONES_COMM, '-') = NVL(src.PHONES_COMM, '-')
        --AND tar.MODIFICATION_DATE=src.MODIFICATION_DATE
        AND NVL(tar.PHONES_LAST, -1)=NVL(src.PHONES_LAST, -1)
        AND NVL(tar.FAMILY_REL, -1) = NVL(src.FAMILY_REL, -1)
        AND NVL(tar.PHONE, '-')=NVL(src.PHONE, '-') )
	WHEN NOT MATCHED THEN 
	--вставляем новое 
	INSERT (tar.PHONES_ID, tar.OBJECTS_ID
          ,tar.OBJECTS_TYPE
          ,tar.PHONES_AKT, tar.PHONES_COMM
          ,tar.PHONES_CREATED
          ,tar.CREATED_USER_ID, tar.CREATED_GROUP_ID
          ,tar.GROUPS_NAME
          ,tar.MODIFICATION_DATE
          ,tar.PHONES_LAST, tar.FAMILY_REL 
          ,tar.PHONE
          ,tar.ADDRESS_ID
          ,tar.FAMILY_REL_NAME, tar.FAMILY_REL_STATUS, tar.FAMILY_SEQ_ID
          ,tar.DATE_UPD
          ,tar.PHONES_RANK, tar.OBJECTS_RANK)
	VALUES (src.PHONES_ID
          ,src.OBJECTS_ID
          ,src.OBJECTS_TYPE
          ,src.PHONES_AKT
          ,src.PHONES_COMM
          ,src.PHONES_CREATED
          ,src.CREATED_USER_ID
          ,src.CREATED_GROUP_ID
          ,src.GROUPS_NAME
          ,src.MODIFICATION_DATE
          ,src.PHONES_LAST
          ,src.FAMILY_REL 
          ,src.PHONE
          ,src.ADDRESS_ID
          ,src.FAMILY_REL_NAME, src.FAMILY_REL_STATUS, src.FAMILY_SEQ_ID
          ,SYSDATE
          ,1, 1)
	;	
  
  cnt_MODIF := SQL%ROWCOUNT;
  SELECT MAX(PHONES_CREATED) INTO last_DT_PKK FROM ODS.PKK_PHONES
      WHERE PHONES_ID>=(SELECT MAX(PHONES_ID)-1000 FROM ODS.PKK_PHONES);
  p_ADD_INFO := 'Период обновления: '||TO_CHAR(last_DT_SFF)||' до '||TO_CHAR(last_DT_PKK);
  ODS.PR$INS_LOG ('PR$UPD_PKK_PHONES', start_time, 'ODS.PKK_PHONES', 'OK', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
  
  COMMIT;

EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_PKK_PHONES', start_time, 'ODS.PKK_PHONES', 'ERR', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_P_PKK_PHONES" (v_REQUEST_ID NUMBER DEFAULT -999, v_OBJECTS_ID IN NUMBER)
-- ========================================================================
-- ==	ПРОЦЕДУРА    Обновление таблицы PKK_PHONES 
-- ==	ОПИСАНИЕ:	   Основные данные о телефонах
-- ========================================================================
-- ==	СОЗДАНИЕ:		  19.02.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	19.02.2016 (ТРАХАЧЕВ В.В.)
-- ==
/*PKK_PHONES:19.02.16 10:16:44-                 (13656). От 19.02.16 07:16:44,663829000 до 19.02.16 07:25:26,001242000
  PKK_PHONES:19.02.16 07:16:44-19.02.16 10:29:44(11982). От 19.02.16 07:27:47,449836000 до 19.02.16 07:37:55,758731000
  PKK_PHONES:19.02.16 07:29:44-19.02.16 11:02:25(13484). От 19.02.16 08:00:20,445729000 до 19.02.16 08:06:32,499409000
*/
-- ========================================================================
AS    
  p_OBJECTS_ID NUMBER := v_OBJECTS_ID ;
  f_Exist_REQUEST_ID NUMBER;  
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;  
  
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  start_time := systimestamp; 
  
  /*IF v_REQUEST_ID^=-999 AND v_OBJECTS_ID IS NULL THEN --если параметр передали, ищем нужный OBJECTS_ID
    SELECT COUNT(*) INTO f_Exist_REQUEST_ID FROM dual 
      WHERE EXISTS(SELECT OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID);
    IF f_Exist_REQUEST_ID=1 THEN 
      SELECT NVL(OBJECTS_ID, -999) INTO p_OBJECTS_ID FROM KREDIT.C_REQUEST@DBLINK_PKK WHERE REQUEST_ID = v_REQUEST_ID;
    END IF;
  END IF;*/

  --ОБНОВЛЕНИЕ
  MERGE INTO ODS.PKK_PHONES tar
	USING (SELECT * FROM ODS.VIEW_PKK_PHONES WHERE OBJECTS_ID = p_OBJECTS_ID 
                AND COALESCE(MODIFICATION_DATE, PHONES_CREATED) > SYSDATE-7
      ) src
    ON (src.PHONES_ID = tar.PHONES_ID )
	WHEN MATCHED THEN
		--Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
		UPDATE SET
        --tar.PHONES_ID=src.PHONES_ID
        --,tar.OBJECTS_ID=src.OBJECTS_ID
        tar.OBJECTS_TYPE=src.OBJECTS_TYPE
        ,tar.PHONES_AKT=src.PHONES_AKT
        ,tar.PHONES_COMM=src.PHONES_COMM
        --,tar.PHONES_CREATED=src.PHONES_CREATED
        --,tar.CREATED_USER_ID=src.CREATED_USER_ID
        --,tar.CREATED_GROUP_ID=src.CREATED_GROUP_ID
        --,tar.GROUPS_NAME=src.GROUPS_NAME
        ,tar.MODIFICATION_DATE=src.MODIFICATION_DATE
        ,tar.PHONES_LAST=src.PHONES_LAST
        ,tar.FAMILY_REL =src.FAMILY_REL
        ,tar.PHONE=src.PHONE
        ,tar.ADDRESS_ID=src.ADDRESS_ID
        ,tar.FAMILY_REL_NAME=src.FAMILY_REL_NAME
        ,tar.FAMILY_REL_STATUS=src.FAMILY_REL_STATUS
        ,tar.FAMILY_SEQ_ID=src.FAMILY_SEQ_ID
        ,tar.DATE_UPD=SYSDATE
        ,tar.PHONES_RANK=1
        ,tar.OBJECTS_RANK=1
        WHERE NOT ( NVL(tar.PHONES_AKT, -1) = NVL(src.PHONES_AKT, -1)
        AND NVL(tar.PHONES_COMM, '-') = NVL(src.PHONES_COMM, '-')
        --AND NVL(tar.MODIFICATION_DATE, '01-01-1960')=NVL(src.MODIFICATION_DATE, '01-01-1960')
        AND NVL(tar.PHONES_LAST, -1)=NVL(src.PHONES_LAST, -1)
        AND NVL(tar.FAMILY_REL, -1) = NVL(src.FAMILY_REL, -1)
        AND NVL(tar.PHONE, '-')=NVL(src.PHONE, '-') )
	WHEN NOT MATCHED THEN 
	--вставляем новое 
	INSERT (tar.PHONES_ID, tar.OBJECTS_ID
          ,tar.OBJECTS_TYPE
          ,tar.PHONES_AKT, tar.PHONES_COMM
          ,tar.PHONES_CREATED
          ,tar.CREATED_USER_ID, tar.CREATED_GROUP_ID
          ,tar.GROUPS_NAME
          ,tar.MODIFICATION_DATE
          ,tar.PHONES_LAST, tar.FAMILY_REL 
          ,tar.PHONE
          ,tar.ADDRESS_ID
          ,tar.FAMILY_REL_NAME, tar.FAMILY_REL_STATUS, tar.FAMILY_SEQ_ID
          ,tar.DATE_UPD
          ,tar.PHONES_RANK, tar.OBJECTS_RANK)
	VALUES (src.PHONES_ID
          ,src.OBJECTS_ID
          ,src.OBJECTS_TYPE
          ,src.PHONES_AKT
          ,src.PHONES_COMM
          ,src.PHONES_CREATED
          ,src.CREATED_USER_ID
          ,src.CREATED_GROUP_ID
          ,src.GROUPS_NAME
          ,src.MODIFICATION_DATE
          ,src.PHONES_LAST
          ,src.FAMILY_REL 
          ,src.PHONE
          ,src.ADDRESS_ID
          ,src.FAMILY_REL_NAME, src.FAMILY_REL_STATUS, src.FAMILY_SEQ_ID
          ,SYSDATE
          ,1, 1)
	;	
  
  cnt_MODIF := SQL%ROWCOUNT;
  
  ODS.PR$INS_LOG ('PR$UPD_P_PKK_PHONES', start_time, 'ODS.PKK_PHONES', 'OK', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_P_PKK_PHONES', start_time, 'ODS.PKK_PHONES', 'ERR', SQLERRM, cnt_MODIF, p_OBJECTS_ID, p_ADD_INFO);
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$GET_FIO_DR" (p_OBJECTS_ID IN NUMBER, p_DATE_RID IN DATE
                            , in_out_FIODR OUT VARCHAR2, in_out_FIODR_AKT OUT NUMBER, in_out_FIODR_DT OUT DATE)
IS
  
BEGIN 
  SELECT 
      FIRST_VALUE(FIO4SEARCH||TO_CHAR(BIRTH, ' dd.mm.yyyy')) OVER(PARTITION BY OBJECTS_ID 
        ORDER BY FIO_AKT DESC, COALESCE(MODIFICATION_DATE, FIO_CREATED) DESC) 
      ,FIRST_VALUE(FIO_AKT) OVER(PARTITION BY OBJECTS_ID 
        ORDER BY FIO_AKT DESC, COALESCE(MODIFICATION_DATE, FIO_CREATED) DESC)
      ,FIRST_VALUE(COALESCE(MODIFICATION_DATE, FIO_CREATED)) OVER(PARTITION BY OBJECTS_ID 
        ORDER BY FIO_AKT DESC, COALESCE(MODIFICATION_DATE, FIO_CREATED) DESC)
      INTO in_out_FIODR, in_out_FIODR_AKT, in_out_FIODR_DT
    FROM ODS.PKK_PERSON_INFO@SNA
    WHERE OBJECTS_ID = p_OBJECTS_ID /*AND FIO_CREATED <= p_DATE_RID*/ and rownum=1;
   
END;
  CREATE OR REPLACE PROCEDURE "SNAUSER"."PR$SNA_LOAD_INFO_DEV_TEST" (v_SNA_ID IN NUMBER, v_REQUEST_ID IN NUMBER
            ,v_LOGIN IN VARCHAR2, v_ALERT_SERIES IN VARCHAR2
            ,v_SNA_DATE_BEGIN IN OUT NOCOPY DATE
            ,v_SNA_DATE_END IN OUT NOCOPY DATE
            ,v_SNA_TIME_LOAD IN OUT NOCOPY NUMBER
            ,v_FLAG_INFO_EXISTS IN OUT NOCOPY NUMBER
            ,v_OBJECTS_ID IN OUT NOCOPY NUMBER
            ,v_CNT_NODE IN OUT NOCOPY NUMBER
            ,v_CNT_LINK IN OUT NOCOPY NUMBER
            ,v_CNT_OBJECTS_ID IN OUT NOCOPY NUMBER
            ,v_CNT_FROD IN OUT NOCOPY NUMBER
            ,v_CNT_BNK IN OUT NOCOPY NUMBER
            ,v_CNT_DEFOLT IN OUT NOCOPY NUMBER
            ,v_CNT_ALERT_PERSON IN OUT NOCOPY NUMBER
            ,v_RESULT_MESSAGE IN OUT NOCOPY VARCHAR2)
----  ПРОЦЕДУРА сбора инфомрации для прорисовки сети по заявке
--==  
--==  
--==  
--=======================================================
  
IS 

  pragma autonomous_transaction; 
    
 /* p_FIODR VARCHAR2(380); -- ФИО и ДР физика для временного хранения при выборке персональных данных
  p_FIODR_AKT NUMBER(2);    -- актуальность физика для временного хранения при выборке персональных данных
  p_FIODR_DT DATE;
  p_DATE_RID DATE;        -- Дата заявки. Для отсечения будущих данных  */
  
  p_cnt_Level NUMBER := 0; --для идентификации уровня подтягивания информации
  
  --курсор с основной информацией по физику - модифицированный без таблицы PKK_PERSON_INFO    
  CURSOR cur_PERSON_NODE(p_cur_OBJECTS_ID NUMBER, p_cur_LEVEL NUMBER) IS
  SELECT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
      ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
      ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
      ,ROUTE_GRAPH, ATTRIBUTES
       FROM (
    SELECT DISTINCT 
      ph.OBJECTS_ID as OBJECTS_ID
      ,ph.OBJECTS_ID AS OBJECTS_ID_REL
      ,DECODE(ph.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
      ,DECODE(ph.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
      --Node 1
      ,snr.N1    AS N1
      ,snr.T1   AS T1
      ,snr.AKT1 AS AKT1
      ,snr.DT1 AS DT1
      --Node 2
      ,ph.PHONE AS N2
      ,'PHONE_MOBILE' AS T2
      ,ph.PHONES_AKT AS AKT2
      ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT2
      ,ph.PHONES_COMM
      --Node 3
      ,NULL       AS N3
      ,NULL       AS T3
      ,NULL       AS AKT3
      ,NULL       AS DT3
      --Node 4
      ,NULL     AS N4
      ,NULL     AS T4
      ,NULL     AS AKT4
      ,NULL     AS DT4
      --Node 5
      ,NULL     AS N5
      ,NULL     AS T5
      ,NULL     AS AKT5
      ,NULL     AS DT5
      ,'N1-N2'AS ROUTE_GRAPH
      ,'PHONES_ID='||TO_CHAR(ph.PHONES_ID) AS ATTRIBUTES
      ,DENSE_RANK() OVER(PARTITION BY 
                        ph.OBJECTS_ID       --по каджому физику последние телефоны
                        --ph.PHONE          --по каждому телефону последние дата и актуальность
                        ORDER BY ph.PHONES_AKT DESC, COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) DESC) rnk
    FROM ODS.PKK_PHONES ph
    INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
            FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
            WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
              AND T1='FIO_DR' AND LEVEL_NODE=p_cur_LEVEL /*AND p_cur_LEVEL=1*/ --and rownum=1
          UNION
          SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
            FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
            WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
              AND T3 LIKE 'FIO_DR%' /*AND OBJECTS_ID^=OBJECTS_ID_REL*/ AND LEVEL_NODE=p_cur_LEVEL) snr
      ON snr.OBJECTS_ID = ph.OBJECTS_ID AND ph.OBJECTS_TYPE=2
    WHERE snr.SRC_OBJECTS_ID=p_cur_OBJECTS_ID
      --WHERE ph.OBJECTS_ID = p_cur_OBJECTS_ID AND ph.OBJECTS_TYPE=2
        AND NOT (NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из НБКИ:%') 
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон клиента(сайт)%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Рекомендация(ПО КЦ):%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РБО:%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПТ/АТМ банка%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из документов РБО:%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('сот. тел. заявителя(сайт)%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('ТЕЛЕФОН ИЗ НБКИ%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РНКО:%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПО КЦ%' ))
  ) WHERE rnk<=3
UNION ALL
  SELECT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
      ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
      ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
      ,ROUTE_GRAPH, ATTRIBUTES
       FROM (
    SELECT DISTINCT 
      ph.OBJECTS_ID as OBJECTS_ID
      ,-1 AS OBJECTS_ID_REL
      ,DECODE(ph.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
      ,DECODE(-1, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
      --Node 1
      ,snr.N1    AS N1
      ,snr.T1   AS T1
      ,snr.AKT1 AS AKT1
      ,snr.DT1 AS DT1
      --Node 3
      ,NVL(ph.PHONES_COMM, 'Контакт физика '||TO_CHAR(ph.OBJECTS_ID)||' без комментария') AS N3
      ,'FIO_DOP' AS T3
      ,ph.PHONES_AKT AS AKT3
      ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT3
      --Node 2
      ,ph.PHONE       AS N2
      ,'PHONE_DOP'      AS T2
      ,ph.PHONES_AKT  AS AKT2
      ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT2
      --Node 4
      ,NULL     AS N4
      ,NULL     AS T4
      ,NULL     AS AKT4
      ,NULL     AS DT4
      --Node 5
      ,NULL     AS N5
      ,NULL     AS T5
      ,NULL     AS AKT5
      ,NULL     AS DT5
      ,'N1-N3, N3-N2' AS ROUTE_GRAPH
      ,'PHONES_ID='||TO_CHAR(ph.PHONES_ID) AS ATTRIBUTES
      ,DENSE_RANK() OVER(PARTITION BY 
                        ph.OBJECTS_ID       --по каджому физику последние телефоны
                        --chld.PHONE          --по каждому телефону последние дата и актуальность
                        ORDER BY ph.PHONES_AKT DESC, COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) DESC) rnk
    FROM ODS.PKK_PHONES ph
    INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
            FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
            WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
              AND T1='FIO_DR' AND LEVEL_NODE=p_cur_LEVEL /*AND p_cur_LEVEL=1*/ --and rownum=1
          UNION
          SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
            FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
            WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
              AND T3 LIKE 'FIO_DR%' /*AND OBJECTS_ID_REL^=-1*/ AND LEVEL_NODE=p_cur_LEVEL) snr
      ON snr.OBJECTS_ID = ph.OBJECTS_ID AND ph.OBJECTS_TYPE=200
    WHERE snr.SRC_OBJECTS_ID=p_cur_OBJECTS_ID
      --ph.OBJECTS_ID = p_cur_OBJECTS_ID AND ph.OBJECTS_TYPE=200
        AND NOT (NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из НБКИ:%') 
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон клиента(сайт)%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Рекомендация(ПО КЦ):%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РБО:%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПТ/АТМ банка%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из документов РБО:%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('сот. тел. заявителя(сайт)%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('ТЕЛЕФОН ИЗ НБКИ%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РНКО:%' )
              OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПО КЦ%' ))
  ) WHERE rnk<=3
UNION ALL
  SELECT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
      ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
      ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
      ,ROUTE_GRAPH, ATTRIBUTES
       FROM ( SELECT DISTINCT 
        ah_src.OBJECTS_ID AS OBJECTS_ID
        ,ah_src.OBJECTS_ID AS OBJECTS_ID_REL
        ,DECODE(ah_src.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
        ,DECODE(ah_src.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
        --Node 1
        ,snr.N1    AS N1
        ,snr.T1   AS T1
        ,snr.AKT1 AS AKT1
        ,snr.DT1 AS DT1
        --Node 3
        ,DECODE(NVL(TRIM(adr.REGIONS_NAMES), '-'), '-', '', (adr.REGIONS_NAMES))
          ||DECODE(NVL(TRIM(adr.AREAS_NAMES), '-'), '-', '', (', РАЙОН '||adr.AREAS_NAMES)) 
          ||DECODE(NVL(TRIM(adr.CITIES_NAMES), '-'), '-', '', (', ' || adr.SHOTNAME_CIT ||' '|| adr.CITIES_NAMES))
          ||DECODE(NVL(TRIM(adr.STREETS_NAMES), '-'), '-', '', (', ' || adr.STREETS_NAMES)) 
          ||DECODE(NVL(TRIM(adr.HOUSE), '-'), '-', '', (', Д.'||adr.HOUSE)) 
          ||DECODE(NVL(TRIM(adr.BUILD), '-'), '-', '', (', КОРП.'||adr.BUILD)) 
          ||DECODE(NVL(TRIM(adr.FLAT),  '-'), '-', '', (', КВ.'||adr.FLAT)) AS N3
        ,'ADR_FMG'          AS T3
        ,ah_src.ADDRESS_AKT AS AKT3
        ,COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) AS DT3
        --Node 2
        ,ph.PHONE         AS N2
        ,'PHONE_FMG'      AS T2
        ,ph.PHONES_AKT    AS AKT2
        ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT2
        --Node 4
        ,NULL     AS N4
        ,NULL     AS T4
        ,NULL     AS AKT4
        ,NULL     AS DT4
        --Node 5
        ,NULL     AS N5
        ,NULL     AS T5
        ,NULL     AS AKT5
        ,NULL     AS DT5
        ,'N1-N3, N1-N2' AS ROUTE_GRAPH 
        ,'ADDRESS_ID='||TO_CHAR(ah_src.ADDRESS_ID)||'; PHONES_ID='||TO_CHAR(ph.PHONES_ID) AS ATTRIBUTES
        ,DENSE_RANK() OVER(PARTITION BY 
                          ah_src.OBJECTS_ID     --по каждому физику последний адрес
                          --ah_src.ADDRESS_ID     --по каждому адресу последние дата и актуальность
                          ORDER BY ah_src.ADDRESS_AKT DESC, COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) DESC) rnk_adr 
        ,DENSE_RANK() OVER(PARTITION BY 
                          ah_src.ADDRESS_ID --по каждому адресу последний телефон
                          --ph.PHONE              --по каждому телефону последние дата и актуальность
                          --в зависимости от высшего PARTITION BY будет оставаться последний или все телфоны по адресу
                          ORDER BY ph.PHONES_AKT DESC, COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) DESC) rnk_ph
      FROM ODS.PKK_ADDRESS_HISTORY ah_src
     INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
              FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
              WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
                AND T1 LIKE 'FIO_DR%' AND LEVEL_NODE=p_cur_LEVEL --and rownum=1
            UNION
            SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
              FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
              WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
                AND T3 LIKE 'FIO_DR%' AND LEVEL_NODE=p_cur_LEVEL) snr
        ON snr.OBJECTS_ID = ah_src.OBJECTS_ID AND ah_src.OBJECTS_TYPE=2
      INNER JOIN ODS.PKK_ADDRESS adr
        ON ah_src.ADDRESS_ID = adr.ADDRESS_ID
      LEFT JOIN ODS.PKK_PHONES ph
        ON ph.OBJECTS_ID = ah_src.ADDRESS_ID AND ph.OBJECTS_TYPE=8 --AND ph.PHONES_AKT=1
          AND NOT PH.PHONE LIKE '9%'
          --AND ph.PHONES_CREATED <= p_DATE_RID
      WHERE --ah_src.OBJECTS_ID = p_cur_OBJECTS_ID AND ah_src.OBJECTS_TYPE=2   
        snr.SRC_OBJECTS_ID=p_cur_OBJECTS_ID         
          --2 OBJECTS_ID, 3 ORG_ID, 5 DOCUMENTS_ID, 19 ADDRESS_ID, 200 ? 
          --AND ah_src.ADDRESS_CREATED <= p_DATE_RID
    ) WHERE rnk_adr<=2 AND rnk_ph=1
UNION ALL
  SELECT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
      ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
      ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
      ,ROUTE_GRAPH, ATTRIBUTES
       FROM ( SELECT DISTINCT 
        doc.OBJECTS_ID AS OBJECTS_ID
        ,doc.OBJECTS_ID AS OBJECTS_ID_REL
        ,DECODE(doc.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
        ,DECODE(doc.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
      --Node 1
        ,snr.N1    AS N1
        ,snr.T1   AS T1
        ,snr.AKT1 AS AKT1
        ,snr.DT1 AS DT1
      --Node 2
        ,ph.PHONE         AS N2
        ,'PHONE_PMG'      AS T2
        ,ph.PHONES_AKT    AS AKT2
        ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT2
      --Node 3
        ,DECODE(NVL(TRIM(adr.REGIONS_NAMES), '-'), '-', '', (adr.REGIONS_NAMES))
          ||DECODE(NVL(TRIM(adr.AREAS_NAMES), '-'), '-', '', (', РАЙОН '||adr.AREAS_NAMES)) 
          ||DECODE(NVL(TRIM(adr.CITIES_NAMES), '-'), '-', '', (', ' || adr.SHOTNAME_CIT ||' '|| adr.CITIES_NAMES))
          ||DECODE(NVL(TRIM(adr.STREETS_NAMES), '-'), '-', '', (', ' || adr.STREETS_NAMES)) 
          ||DECODE(NVL(TRIM(adr.HOUSE), '-'), '-', '', (', Д.'||adr.HOUSE)) 
          ||DECODE(NVL(TRIM(adr.BUILD), '-'), '-', '', (', КОРП.'||adr.BUILD)) 
          ||DECODE(NVL(TRIM(adr.FLAT),  '-'), '-', '', (', КВ.'||adr.FLAT)) AS N3
        ,'ADR_PMG' AS T3
        ,ah_src.ADDRESS_AKT AS AKT3
        ,COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) AS DT3
      --Node 4
        ,doc.DOCUMENTS_SERIAL||' '||doc.DOCUMENTS_NUMBER AS N4
        ,'PASP' AS T4
        ,doc.DOCUMENTS_AKT AS AKT4
        ,COALESCE(doc.MODIFICATION_DATE, doc.DOCUMENTS_CREATED)     AS DT4
      --Node 5
        ,NULL     AS N5
        ,NULL     AS T5
        ,NULL     AS AKT5
        ,NULL     AS DT5
        --,'N1-N3, N3-N2, N1-N4' AS ROUTE_GRAPH  
        ,'N1-N3, N1-N2, N1-N4' AS ROUTE_GRAPH  
        ,'ADDRESS_ID='||TO_CHAR(ah_src.ADDRESS_ID)||'; PHONES_ID='||TO_CHAR(ph.PHONES_ID)
            ||'; DOCUMENTS_ID='||TO_CHAR(doc.DOCUMENTS_ID) AS ATTRIBUTES
        --,LISTAGG(ph.PHONE,', ') WITHIN GROUP (ORDER BY COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED)) OVER(PARTITION BY ah_src.ADDRESS_ID ) as ph_list
        ,DENSE_RANK() OVER(PARTITION BY 
                          --doc.OBJECTS_ID                           --по каждому физику последний паспорт
                          doc.DOCUMENTS_SERIAL, doc.DOCUMENTS_NUMBER --по каждому паспорту последние дата и актуальность
                          ORDER BY doc.DOCUMENTS_AKT DESC, COALESCE(doc.MODIFICATION_DATE, doc.DOCUMENTS_CREATED) DESC) rnk_doc
        ,DENSE_RANK() OVER(PARTITION BY 
                          doc.OBJECTS_ID        --по каждому физику последние телефоны
                          --ah_src.ADDRESS_ID   --по каждому адресу последние дата и актуальность
                          ORDER BY ah_src.ADDRESS_AKT DESC, COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) DESC) rnk_adr 
        ,DENSE_RANK() OVER(PARTITION BY 
                          ah_src.ADDRESS_ID   --по каждому адресу последний телефон
                          --ph.PHONE            --по кадому телефону последние дата и актуальность
                          ORDER BY ph.PHONES_AKT DESC, COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) DESC) rnk_ph
      FROM ODS.PKK_DOCUMENTS_HISTORY_INFO doc
      INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
              FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
              WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
                AND T1='FIO_DR' AND LEVEL_NODE=p_cur_LEVEL --and rownum=1
            UNION
            SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
              FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
              WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
                AND T3 LIKE 'FIO_DR%' /*AND OBJECTS_ID^=OBJECTS_ID_REL*/ AND LEVEL_NODE=p_cur_LEVEL) snr
        ON snr.OBJECTS_ID = doc.OBJECTS_ID AND doc.DOCUMENTS_TYPE=21
      LEFT JOIN ODS.PKK_ADDRESS_HISTORY ah_src    
        ON ah_src.OBJECTS_ID = doc.DOCUMENTS_ID 
          AND ah_src.OBJECTS_TYPE = 5 --2 OBJECTS_ID, 3 ORG_ID, 5 DOCUMENTS_ID, 19 ADDRESS_ID, 200 ?
      LEFT JOIN ODS.PKK_ADDRESS adr
        ON ah_src.ADDRESS_ID = adr.ADDRESS_ID
      LEFT JOIN ODS.PKK_PHONES ph
        ON ph.OBJECTS_ID = ah_src.ADDRESS_ID AND ph.OBJECTS_TYPE=8 --AND ph.PHONES_AKT=1
          AND NOT PH.PHONE LIKE '9%'
          --AND ph.PHONES_CREATED <= p_DATE_RID
      WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID
          --doc.OBJECTS_ID = p_cur_OBJECTS_ID AND doc.DOCUMENTS_TYPE=21
          --AND ah_src.ADDRESS_CREATED <= p_DATE_RID 
      ) WHERE rnk_doc=1 AND rnk_adr<=2 AND rnk_ph<=2
UNION ALL
  SELECT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
      ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
      ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
      ,ROUTE_GRAPH, ATTRIBUTES
       FROM ( SELECT DISTINCT 
        wor.OBJECTS_ID AS OBJECTS_ID
        ,wor.OBJECTS_ID AS OBJECTS_ID_REL
        ,DECODE(wor.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
        ,DECODE(wor.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
        --Node 1
        ,snr.N1    AS N1
        ,snr.T1   AS T1
        ,snr.AKT1 AS AKT1
        ,snr.DT1 AS DT1
        --Node 3
        ,DECODE(NVL(TRIM(adr.REGIONS_NAMES), '-'), '-', '', (adr.REGIONS_NAMES))
          ||DECODE(NVL(TRIM(adr.AREAS_NAMES), '-'), '-', '', (', РАЙОН '||adr.AREAS_NAMES)) 
          ||DECODE(NVL(TRIM(adr.CITIES_NAMES), '-'), '-', '', (', ' || adr.SHOTNAME_CIT ||' '|| adr.CITIES_NAMES))
          ||DECODE(NVL(TRIM(adr.STREETS_NAMES), '-'), '-', '', (', ' || adr.STREETS_NAMES)) 
          ||DECODE(NVL(TRIM(adr.HOUSE), '-'), '-', '', (', Д.'||adr.HOUSE)) 
          ||DECODE(NVL(TRIM(adr.BUILD), '-'), '-', '', (', КОРП.'||adr.BUILD)) 
          ||DECODE(NVL(TRIM(adr.FLAT),  '-'), '-', '', (', КВ.'||adr.FLAT)) AS N3
        ,'ADR_WORK' AS T3
        ,ah_src.ADDRESS_AKT AS AKT3
        ,COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) AS DT3
        ,ah_src.ADDRESS_ID
        --Node 5
        ,ph_wor.PHONE     AS N5
        ,'PHONE_WORK'     AS T5
        ,ph_wor.PHONES_AKT    AS AKT5
        ,COALESCE(ph_wor.MODIFICATION_DATE, ph_wor.PHONES_CREATED) AS DT5
        --Node 2
        ,ph_org.PHONE AS N2
        ,'PHONE_ORG' AS T2
        ,ph_org.PHONES_AKT AS AKT2
        ,COALESCE(ph_org.MODIFICATION_DATE, ph_org.PHONES_CREATED)     AS DT2
        --Node 4
        ,REPLACE(wor.ORG_NAME, '"', '')     AS N4
        ,'ORG_NAME'     AS T4
        ,wor.WORKS_AKT     AS AKT4
        ,wor.WORKS_LAST
        ,COALESCE(wor.MODIFICATION_DATE, wor.WORKS_CREATED)     AS DT4
        --,'N1-N4, N4-N2, N4-N3, N4-N5' AS ROUTE_GRAPH
        ,'N1-N5, N1-N2, N1-N3, N2-N4, N3-N4, N5-N4' AS ROUTE_GRAPH
        ,'WORKS_ID='||TO_CHAR(wor.WORKS_ID)||'; ORG_ID='||TO_CHAR(wor.ORG_ID)||'; ADDRESS_ID='||TO_CHAR(ah_src.ADDRESS_ID)
            ||'; PHONES_ID='||TO_CHAR(ph_org.PHONES_ID)||'; PHONES_I_WORK='||TO_CHAR(ph_wor.PHONES_ID) AS ATTRIBUTES
        --,LISTAGG(ph.PHONE,', ') WITHIN GROUP (ORDER BY COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED)) OVER(PARTITION BY ah_src.ADDRESS_ID ) as ph_list
        ,DENSE_RANK() OVER(PARTITION BY  
                          wor.OBJECTS_ID       --по каждому физику последняя работа
                          --wor.ORG_ID             --по каждой работе последние дата и актуальность
                          ORDER BY wor.WORKS_AKT DESC, COALESCE(wor.MODIFICATION_DATE, wor.WORKS_CREATED) DESC) rnk_wor
        ,DENSE_RANK() OVER(PARTITION BY 
                          wor.ORG_ID            --по каждомй работе последний адрес
                          --ah_src.ADDRESS_ID   --по каждому адресу последние дата и актуальность
                          ORDER BY ah_src.ADDRESS_AKT DESC, COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) DESC) rnk_adr
        ,DENSE_RANK() OVER(PARTITION BY 
                          wor.WORKS_ID     --по каждой работе последний телефон
                          --ph.PHONE       --по кадому телефону последние дата и актуальность
                          ORDER BY ph_wor.PHONES_AKT DESC, COALESCE(ph_wor.MODIFICATION_DATE, ph_wor.PHONES_CREATED) DESC) rnk_pho
        ,DENSE_RANK() OVER(PARTITION BY 
                          wor.ORG_ID       --по каждой работе последний телефон
                          --ph.PHONE       --по кадому телефону последние дата и актуальность
                          ORDER BY ph_org.PHONES_AKT DESC, COALESCE(ph_org.MODIFICATION_DATE, ph_org.PHONES_CREATED) DESC) rnk_phw
      FROM ODS.PKK_WORKS_INFO wor
      INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
              FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
              WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
                AND T1='FIO_DR' AND LEVEL_NODE=p_cur_LEVEL --and rownum=1
            UNION
            SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
              FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
              WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
                AND T3 LIKE 'FIO_DR%' /*AND OBJECTS_ID^=OBJECTS_ID_REL*/ AND LEVEL_NODE=p_cur_LEVEL) snr
        ON snr.OBJECTS_ID = wor.OBJECTS_ID AND wor.WORKS_AKT=1 AND wor.WORKS_CREATED > SYSDATE-365*5
        
      INNER JOIN ODS.PKK_ADDRESS_HISTORY ah_src    
        ON ah_src.OBJECTS_ID = wor.ORG_ID 
          AND ah_src.OBJECTS_TYPE = 3 --2 OBJECTS_ID, 3 ORG_ID, 5 DOCUMENTS_ID, 19 ADDRESS_ID, 200 ? 
          AND ah_src.ADDRESS_AKT=1
      INNER JOIN ODS.PKK_ADDRESS adr
        ON ah_src.ADDRESS_ID = adr.ADDRESS_ID
      LEFT JOIN ODS.PKK_PHONES ph_wor
        ON ph_wor.OBJECTS_ID = wor.WORKS_ID AND ph_wor.OBJECTS_TYPE=12 --AND ph_wor.PHONES_AKT=1
      LEFT JOIN ODS.PKK_PHONES ph_org
        ON ph_org.OBJECTS_ID = wor.ORG_ID AND ph_org.OBJECTS_TYPE=3  --AND ph_org.PHONES_AKT=1
          --AND ph.PHONES_CREATED <= p_DATE_RID
      WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID
          --wor.OBJECTS_ID = p_cur_OBJECTS_ID AND wor.WORKS_AKT=1 AND wor.WORKS_CREATED > SYSDATE-365*5
          --AND ah_src.ADDRESS_CREATED <= p_DATE_RID 
      ) WHERE rnk_wor<=3 AND rnk_adr=1 AND rnk_pho=1 AND rnk_phw=1
UNION ALL
  SELECT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
      ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
      ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
      ,ROUTE_GRAPH, ATTRIBUTES
       FROM (
    SELECT DISTINCT 
      em.OBJECTS_ID as OBJECTS_ID
      ,em.OBJECTS_ID AS OBJECTS_ID_REL
      ,DECODE(em.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
      ,DECODE(em.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
      --Node 1
      ,snr.N1    AS N1
      ,snr.T1   AS T1
      ,snr.AKT1 AS AKT1
      ,snr.DT1 AS DT1
      --Node 2
      ,NULL AS N2
      ,NULL AS T2
      ,NULL AS AKT2
      ,NULL AS DT2
      --Node 3
      ,NULL       AS N3
      ,NULL       AS T3
      ,NULL       AS AKT3
      ,NULL       AS DT3
      --Node 4
      ,em.EMAIL     AS N4
      ,'EMAIL'      AS T4
      ,em.EMAIL_AKT AS AKT4
      ,COALESCE(em.MODIFICATION_DATE, em.EMAIL_CREATED) AS DT4
      --Node 5
      ,NULL     AS N5
      ,NULL     AS T5
      ,NULL     AS AKT5
      ,NULL     AS DT5
      ,'N1-N4' AS ROUTE_GRAPH
      ,'EMAIL_ID='||TO_CHAR(em.EMAIL_ID) AS ATTRIBUTES
      ,DENSE_RANK() OVER(PARTITION BY 
                        em.OBJECTS_ID       --по каджому физику последние EMAIL
                        --ph.PHONE          --по каждому телефону последние дата и актуальность
                        ORDER BY em.EMAIL_AKT DESC, COALESCE(em.MODIFICATION_DATE, em.EMAIL_CREATED) DESC) rnk
    FROM ODS.PKK_EMAIL em
    INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
            FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
            WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
              AND T1='FIO_DR' AND LEVEL_NODE=p_cur_LEVEL --and rownum=1
          UNION
          SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
            FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
            WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
              AND T3 LIKE 'FIO_DR%' AND LEVEL_NODE=p_cur_LEVEL) snr
      ON snr.OBJECTS_ID = em.OBJECTS_ID
    WHERE snr.SRC_OBJECTS_ID=p_cur_OBJECTS_ID
        AND NVL(em.EMAIL, '-') LIKE '%@%' 
  ) WHERE rnk<=2
UNION ALL
  SELECT OBJECTS_ID as OBJECTS_ID
    ,OBJECTS_ID AS OBJECTS_ID_REL
    ,DECODE(OBJECTS_ID, OBJECTS_ID, 1, 0) AS ROOT_PERS
    ,DECODE(OBJECTS_ID, OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
    ,N1    AS N1
    ,T1   AS T1
    ,AKT1 AS AKT1
    ,DT1 AS DT1
    ,NULL AS N2, NULL AS T2, NULL AS AKT2, NULL AS DT2
    ,TO_CHAR(OBJECTS_ID||' - ')||DECODE(cnt_bnk, 0, '', 'БНК: '||TO_CHAR(cnt_bnk)||'; ')
        ||DECODE(cnt_def, 0, '', 'ДЕФОЛТ: '||TO_CHAR(cnt_def)||'; ')
        ||DECODE(cnt_frod, 0, '', 'ФРОД: '||TO_CHAR(cnt_frod)||'; ')AS N3
    ,'NEG' AS T3
    ,0 AS AKT3
    ,SYSDATE AS DT3
    ,NULL AS N4, NULL AS T4, NULL AS AKT4, NULL AS DT4
    ,NULL AS N5, NULL AS T5, NULL AS AKT5, NULL AS DT5
    ,'N1-N3' AS ROUTE_GRAPH
    ,NULL AS ATTRIBUTES
  FROM (SELECT DISTINCT snr.OBJECTS_ID, N1, T1, AKT1, DT1
      ,(SELECT COUNT(DISTINCT(BNK_CODE)) FROM SFF.RP_BNK@SNA WHERE PERSON_ID = snr.OBJECTS_ID) as cnt_bnk
      ,(SELECT COUNT(DISTINCT(REQUEST_ID)) FROM SFF.RP_DEFOLT@SNA WHERE PERSON_ID = snr.OBJECTS_ID) as cnt_def
      ,(SELECT  COUNT(DISTINCT(TYPE_REL)) FROM SFF.FROD_RULE@SNA WHERE PERSON_ID = snr.OBJECTS_ID) as cnt_frod
    FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
    WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID
    ) WHERE cnt_bnk+ cnt_def+ cnt_frod>0
    ;

  --курсор с явно указанными связями 
  CURSOR cur_PERSON_REL(p_cur_OBJECTS_ID NUMBER) IS
     SELECT DISTINCT OBJECTS_ID
            ,OBJECTS_ID_REL
            ,ROOT_PERS
            ,ROOT_PERS_REL
            --далее пошла нужная информация родителя
            ,N1
            ,T1
            ,AKT1
            ,DT1
            --далее пошла нужная информация ребенка
            ,N3
            ,T3 --||DECODE(FAMILY_REL_STATUS, 1, '_'||FAMILY_REL_NAME||CONTACT_COMMENT, '') 
            ,AKT3
            ,DT3
            ,ROUTE_GRAPH
          FROM (
          SELECT per.OBJECTS_ID
            ,per_rel.OBJECTS_ID AS OBJECTS_ID_REL
            ,per.OBJECTS_ID AS OB_ID_REALLY
            ,DECODE(per.OBJECTS_ID, p_cur_OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(per_rel.OBJECTS_ID, p_cur_OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            ,'FIO_DR' AS T1
            --,fml.CONTACT_AKT AS AKT1
            ,per.FIO_AKT AS AKT1
            --,COALESCE(fml.CONTACT_MODIFICATION, fml.CONTACT_CREATED) AS DT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            ,per_rel.FIO4SEARCH||TO_CHAR(per_rel.BIRTH, ' dd.mm.yyyy') AS N3
            --,'REL_OB_ID_REF' AS T3
            ,'FIO_DR_REL_REF' AS T3
            ,per_rel.FIO_AKT AS AKT3
            ,COALESCE(per_rel.MODIFICATION_DATE, per_rel.FIO_CREATED) AS DT3
            --,'FIO_DR' AS T3
            ,fml.FAMILY_REL_NAME
            ,fml.FAMILY_REL_STATUS
            ,fml.CONTACT_COMMENT
            ,'N1-N3' AS ROUTE_GRAPH
            ,DENSE_RANK() OVER(PARTITION BY fml.OBJECTS_ID, fml.OB_ID
                              ORDER BY fml.CONTACT_AKT DESC, COALESCE(fml.CONTACT_MODIFICATION, fml.CONTACT_CREATED) DESC) rnk
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio
            ,DENSE_RANK() OVER(PARTITION BY per_rel.OBJECTS_ID
                              ORDER BY per_rel.FIO_AKT DESC, COALESCE(per_rel.MODIFICATION_DATE, per_rel.FIO_CREATED) DESC) rnk_fio_rel
          FROM (SELECT OBJECTS_ID, OBJECTS_ID AS OB_ID, FIO_AKT as CONTACT_AKT, '' AS CONTACT_COMMENT
                    , MODIFICATION_DATE AS CONTACT_CREATED, MODIFICATION_DATE AS CONTACT_MODIFICATION
                    , '' AS FAMILY_REL_NAME, 0 AS FAMILY_REL_STATUS
                  FROM ODS.PKK_PERSON_INFO WHERE OBJECTS_ID=p_cur_OBJECTS_ID and ROWNUM=1
                UNION
                SELECT OBJECTS_ID, OB_ID, CONTACT_AKT, '' AS CONTACT_COMMENT, CONTACT_CREATED, CONTACT_MODIFICATION
                    , FAMILY_REL_NAME, FAMILY_REL_STATUS
                  FROM ODS.PKK_FAMILY WHERE OBJECTS_ID=p_cur_OBJECTS_ID --AND CONTACT_CREATED <= p_DATE_RID
                UNION
                  SELECT OBJECTS_ID, OB_ID, CONTACT_AKT, CONTACT_COMMENT, CONTACT_CREATED, CONTACT_MODIFICATION
                    , FAMILY_REL_NAME, FAMILY_REL_STATUS
                  FROM ODS.PKK_CONTACTS WHERE OBJECTS_ID=p_cur_OBJECTS_ID --AND CONTACT_CREATED <= p_DATE_RID 
                  ) fml
          INNER JOIN ODS.PKK_PERSON_INFO per
            ON per.OBJECTS_ID = fml.OBJECTS_ID AND per.FIO_RANK=1 --AND per.FIO_CREATED <= p_DATE_RID
          INNER JOIN ODS.PKK_PERSON_INFO per_rel
            ON per_rel.OBJECTS_ID = fml.OB_ID AND per_rel.FIO_RANK=1 --AND per_rel.FIO_CREATED <= p_DATE_RID
          WHERE fml.OBJECTS_ID = p_cur_OBJECTS_ID --AND fml.CONTACT_CREATED <= p_DATE_RID
          ) WHERE rnk=1 AND rnk_fio=1 AND rnk_fio_rel=1 
      UNION ALL
        SELECT DISTINCT OBJECTS_ID
            ,OBJECTS_ID_REL
            ,ROOT_PERS
            ,ROOT_PERS_REL
            --далее пошла нужная информация родителя
            ,N1
            ,T1
            ,AKT1
            ,DT1
            --далее пошла нужная информация ребенка
            ,N3
            ,T3 --||DECODE(FAMILY_REL_STATUS, 1, '_'||FAMILY_REL_NAME||CONTACT_COMMENT, '') 
            ,AKT3
            ,DT3
            ,ROUTE_GRAPH
          FROM (
          SELECT cr.OBJECTS_ID as OBJECTS_ID
            ,cr_por.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(cr.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(cr_por.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            ,snr.N1    AS N1
            ,snr.T1   AS T1
            ,snr.AKT1 AS AKT1
            ,snr.DT1 AS DT1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N3
            ,DECODE(cr_por.TYPE_REQUEST_ID, 6, 'FIO_DR_REL_POR', 7, 'FIO_DR_REL_POR', 13, 'FIO_DR_REL_SOZ') AS T3
            ,cr_por.TYPE_REQUEST_ID
            ,per.FIO_AKT AS AKT3
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT3
            ,'N1-N3' AS ROUTE_GRAPH
            ,DENSE_RANK() OVER(PARTITION BY cr.OBJECTS_ID, cr_por.OBJECTS_ID
                              ORDER BY cr_por.OBJECTS_ID DESC, COALESCE(cr_por.MODIFICATION_DATE_REQUEST, cr_por.CREATED_DATE) DESC) rnk
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio_rel
          FROM ODS.PKK_C_REQUEST_SNA cr
          INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
                      FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                      WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
                        AND T1='FIO_DR' AND LEVEL_NODE IN(1,3) --and rownum=1
                    UNION
                    SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
                      FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                      WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN
                        AND T3 LIKE 'FIO_DR%' AND LEVEL_NODE IN(1,3)) snr
            ON snr.OBJECTS_ID = cr.OBJECTS_ID AND cr.TYPE_REQUEST_ID=1
          INNER JOIN ODS.PKK_C_REQUEST_SNA cr_por
            ON cr_por.PARENT_ID = cr.PARENT_ID AND cr_por.TYPE_REQUEST_ID IN(6,7,13)
              AND cr_por.OBJECTS_ID ^= cr.OBJECTS_ID
          INNER JOIN ODS.PKK_PERSON_INFO per
            ON per.OBJECTS_ID = cr_por.OBJECTS_ID
            WHERE snr.SRC_OBJECTS_ID=p_cur_OBJECTS_ID
          ) WHERE rnk=1 AND rnk_fio_rel=1
    ;
  
  CURSOR cur_PERSON_HIDE (p_cur_OBJECTS_ID NUMBER) IS
    SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (
      SELECT DISTINCT dbl.OBJECTS_ID AS OBJECTS_ID
            ,dbl.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(snr.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,NULL AS N3
            ,NULL AS T3
            ,NULL AS AKT3
            ,NULL AS DT3
            --Node 2
            ,NULL         AS N2
            ,NULL      AS T2
            ,NULL    AS AKT2
            ,NULL AS DT2
            --Node 4
            ,dbl.DOCUMENTS_SERIAL||' '||dbl.DOCUMENTS_NUMBER AS N4
            ,'PASP' AS T4
            ,dbl.DOCUMENTS_AKT AKT4
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.DOCUMENTS_CREATED) AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            ,'N1-N4' AS ROUTE_GRAPH  
            ,'FIO_DR_REL_HIDE_PASP; '||'DOCUMENTS_ID='||TO_CHAR(dbl.DOCUMENTS_ID) AS ATTRIBUTES
            ,DENSE_RANK() OVER(PARTITION BY 
                  --dbl.OBJECTS_ID                            --по каджому физику последние паспорта
                  dbl.DOCUMENTS_SERIAL, dbl.DOCUMENTS_NUMBER  --по каждому паспорту последние дата и актуальность
                  ORDER BY dbl.DOCUMENTS_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, dbl.DOCUMENTS_CREATED) DESC) rnk_pasp
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio
        FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
        INNER JOIN ODS.PKK_DOCUMENTS_HISTORY_INFO dbl
          ON dbl.DOCUMENTS_NUMBER = SUBSTR(snr.N4, 6, 6) AND dbl.DOCUMENTS_SERIAL = SUBSTR(snr.N4, 1, 4)
            AND dbl.DOCUMENTS_TYPE=21 
            AND snr.OBJECTS_ID ^= dbl.OBJECTS_ID --физики должны быть разными
        INNER JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = dbl.OBJECTS_ID 
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND NVL(snr.USER_SNA, '-')=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
            AND snr.T4='PASP' --AND NOT snr.N4 IS NULL
      ) WHERE rnk_pasp=1 AND rnk_fio=1
    UNION ALL --ищем АДРЕС среди АДРЕСОВ ФМЖ
      SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (
      SELECT DISTINCT dbl.OBJECTS_ID AS OBJECTS_ID
            ,dbl.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,snr.N3 AS N3
            ,'ADR_FMG'       AS T3
            ,dbl.ADDRESS_AKT AS AKT3
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) AS DT3
            --Node 2
            ,ph.PHONE         AS N2
            ,'PHONE_FMG'      AS T2
            ,ph.PHONES_AKT    AS AKT2
            ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT2
            --Node 4
            ,NULL AS N4
            ,NULL AS T4
            ,NULL AKT4
            ,NULL AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            --,'N1-N3, N3-N2' AS ROUTE_GRAPH
            ,'N1-N3, N1-N2' AS ROUTE_GRAPH
            ,'FIO_DR_REL_HIDE_AFMG; '||'ADDRESS_ID='||TO_CHAR(dbl.ADDRESS_ID)||'; PHONES_ID='||TO_CHAR(ph.PHONES_ID) AS ATTRIBUTES
            ,DENSE_RANK() OVER(PARTITION BY 
                              --dbl.OBJECTS_ID     --по каждому физику последний адрес
                              dbl.ADDRESS_ID     --по каждому адресу последние дата и актуальность
                              ORDER BY dbl.ADDRESS_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) DESC) rnk_adr
            ,DENSE_RANK() OVER(PARTITION BY 
                              dbl.ADDRESS_ID --по каждому адресу последний телефон
                              --ph.PHONE              --по каждому телефону последние дата и актуальность
                              --в зависимости от высшего PARTITION BY будет оставаться последний или все телфоны по адресу
                              ORDER BY ph.PHONES_AKT DESC, COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) DESC) rnk_ph
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio
        FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
        INNER JOIN --ODS.PKK_ADDRESS_HISTORY 
              CPD.ADDRESS_HISTORY@DBLINK_PKK
              dbl
          ON dbl.ADDRESS_ID = TO_NUMBER(NVL(REGEXP_REPLACE(snr.ATTRIBUTES, 'ADDRESS_ID=([0-9]*).+', '\1'), '-1'))
            AND dbl.OBJECTS_TYPE=2
            AND snr.OBJECTS_ID ^= dbl.OBJECTS_ID --физики должны быть разными
            AND ((NVL(snr.T3, '-')='ADR_FMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) BETWEEN snr.DT3-365 AND SYSDATE)
              or (NVL(snr.T3, '-')='ADR_PMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) BETWEEN snr.DT3-365 AND SYSDATE))
            /*AND ((NVL(snr.T3, '-')='ADR_FMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) - NVL(snr.DT3, SYSDATE+365*100) > -365+(SYSDATE-snr.DT3))
              or (NVL(snr.T3, '-')='ADR_PMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) - NVL(snr.DT3, SYSDATE+365*100) > -365+(SYSDATE-snr.DT3)))*/
        LEFT JOIN ODS.PKK_PHONES 
                  --CPD.PHONES@DBLINK_PKK
                 ph
          ON ph.OBJECTS_ID = dbl.ADDRESS_ID AND ph.OBJECTS_TYPE=8 AND NOT ph.PHONE LIKE '9%'
        INNER JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = dbl.OBJECTS_ID 
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND NVL(snr.USER_SNA, '-')=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
            AND snr.T3 IN('ADR_FMG', 'ADR_PMG') AND snr.N3 LIKE '%Д.%' AND LENGTH(TRIM(snr.N3))>16 AND snr.N3 LIKE '%КВ.%' 
            AND NOT snr.N3 LIKE '%Д. 0%' AND NOT snr.N3 LIKE '%Д. -%'
      ) WHERE rnk_adr<=5 AND rnk_ph=1 AND rnk_fio=1
    UNION ALL --ищем АДРЕС среди адресов ПМЖ
      SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (SELECT DISTINCT doc.OBJECTS_ID AS OBJECTS_ID
            ,doc.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(doc.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(doc.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,snr.N3 AS N3
            ,'ADR_PMG'       AS T3
            ,dbl.ADDRESS_AKT AS AKT3
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) AS DT3
            --Node 2
            ,ph.PHONE         AS N2
            ,'PHONE_PMG'      AS T2
            ,ph.PHONES_AKT    AS AKT2
            ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT2
            --Node 4
            ,doc.DOCUMENTS_SERIAL||' '||doc.DOCUMENTS_NUMBER AS N4
            ,'PASP' AS T4
            ,doc.DOCUMENTS_AKT AS AKT4
            ,COALESCE(doc.MODIFICATION_DATE, doc.DOCUMENTS_CREATED)     AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            --,'N1-N3, N3-N2, N1-N4' AS ROUTE_GRAPH  
            ,'N1-N3, N1-N2, N1-N4' AS ROUTE_GRAPH  
            ,'FIO_DR_REL_HIDE_APMG; '||'ADDRESS_ID='||TO_CHAR(dbl.ADDRESS_ID)||'; PHONES_ID='||TO_CHAR(ph.PHONES_ID)
                ||'; DOCUMENTS_ID='||TO_CHAR(doc.DOCUMENTS_ID) AS ATTRIBUTES
            --,snr.ATTRIBUTES AS ATTR_SRC
            ,snr.DT3 AS DT3_SRC
            ,ROUND(COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) - NVL(snr.DT3, SYSDATE+365*100)) as D_BW
            ,DENSE_RANK() OVER(PARTITION BY 
                        --dbl.OBJECTS_ID     --по каждому физику последний адрес
                        dbl.ADDRESS_ID     --по каждому адресу последние дата и актуальность
                        ORDER BY dbl.ADDRESS_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) DESC) rnk_adr
            ,DENSE_RANK() OVER(PARTITION BY 
                        --doc.OBJECTS_ID                           --по каждому физику последний паспорт
                        doc.DOCUMENTS_SERIAL, doc.DOCUMENTS_NUMBER --по каждому паспорту последние дата и актуальность
                        ORDER BY doc.DOCUMENTS_AKT DESC, COALESCE(doc.MODIFICATION_DATE, doc.DOCUMENTS_CREATED) DESC) rnk_doc
            ,DENSE_RANK() OVER(PARTITION BY 
                        dbl.ADDRESS_ID --по каждому адресу последний телефон
                        --ph.PHONE              --по каждому телефону последние дата и актуальность
                        --в зависимости от высшего PARTITION BY будет оставаться последний или все телфоны по адресу
                        ORDER BY ph.PHONES_AKT DESC, COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) DESC) rnk_ph
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                         ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio
        FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
        INNER JOIN ODS.PKK_ADDRESS_HISTORY 
              --CPD.ADDRESS_HISTORY@DBLINK_PKK
              dbl
          ON dbl.ADDRESS_ID = TO_NUMBER(NVL(REGEXP_REPLACE(snr.ATTRIBUTES, 'ADDRESS_ID=([0-9]*).+', '\1'), '-1'))
            AND dbl.OBJECTS_TYPE=5
          AND ((NVL(snr.T3, '-')='ADR_FMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) BETWEEN snr.DT3-365 AND SYSDATE)
             or (NVL(snr.T3, '-')='ADR_PMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) BETWEEN snr.DT3-365 AND SYSDATE))
          /*AND ((NVL(snr.T3, '-')='ADR_FMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) - NVL(snr.DT3, SYSDATE+365*100) > -365+(SYSDATE-snr.DT3))
            or (NVL(snr.T3, '-')='ADR_PMG' AND COALESCE(dbl.MODIFICATION_DATE, dbl.ADDRESS_CREATED) - NVL(snr.DT3, SYSDATE+365*100) > -365+(SYSDATE-snr.DT3)))*/
        INNER JOIN ODS.PKK_DOCUMENTS_HISTORY_INFO doc
          ON doc.DOCUMENTS_ID=dbl.OBJECTS_ID AND doc.DOCUMENTS_TYPE=21
            AND snr.OBJECTS_ID ^= doc.OBJECTS_ID --физики должны быть разными
        LEFT JOIN ODS.PKK_PHONES ph
          ON ph.OBJECTS_ID = dbl.ADDRESS_ID AND ph.OBJECTS_TYPE=8 AND NOT ph.PHONE LIKE '9%'
        INNER JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = doc.OBJECTS_ID 
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND NVL(snr.USER_SNA, '-')=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
            AND snr.T3 IN('ADR_FMG', 'ADR_PMG') AND snr.N3 LIKE '%Д.%' AND LENGTH(TRIM(snr.N3))>16 AND snr.N3 LIKE '%КВ.%' 
            AND NOT snr.N3 LIKE '%Д. 0%' AND NOT snr.N3 LIKE '%Д. -%'
      ) WHERE rnk_adr<=5 AND rnk_doc=1 AND rnk_ph=1 AND rnk_fio=1
    UNION ALL --ищем EMAIL
      SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (
      SELECT DISTINCT dbl.OBJECTS_ID AS OBJECTS_ID
            ,dbl.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,NULL     AS N3
            ,NULL     AS T3
            ,NULL     AS AKT3
            ,NULL     AS DT3
            --Node 2
            ,NULL     AS N2
            ,NULL     AS T2
            ,NULL     AS AKT2
            ,NULL     AS DT2
            --Node 4
            ,snr.N4 AS N4
            ,'EMAIL'       AS T4
            ,dbl.EMAIL_AKT AS AKT4
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.EMAIL_CREATED) AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            ,'N1-N4' AS ROUTE_GRAPH
            ,'FIO_DR_REL_HIDE_EMAIL; '||'EMAIL='||TO_CHAR(dbl.EMAIL_ID) AS ATTRIBUTES
            ,DENSE_RANK() OVER(PARTITION BY 
                              --dbl.OBJECTS_ID     --по каждому физику последний EMAIL
                              dbl.EMAIL     --по каждому EMAIL последние дата и актуальность
                              ORDER BY dbl.EMAIL_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, dbl.EMAIL_CREATED) DESC) rnk_eml
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio
        FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
        INNER JOIN ODS.PKK_EMAIL dbl
          ON dbl.EMAIL = snr.N4
            AND snr.OBJECTS_ID ^= dbl.OBJECTS_ID --физики должны быть разными
            --AND COALESCE(dbl.MODIFICATION_DATE, dbl.EMAIL_CREATED) - NVL(snr.DT4, SYSDATE+365*100) > -365*3 +(SYSDATE-snr.DT4)
            AND COALESCE(dbl.MODIFICATION_DATE, dbl.EMAIL_CREATED) BETWEEN snr.DT4-365*3 AND SYSDATE
        INNER JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = dbl.OBJECTS_ID 
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND NVL(snr.USER_SNA, '-')=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
            AND NVL(snr.T4, '-')='EMAIL'  AND snr.N4 LIKE '%@%'
      ) WHERE rnk_eml<=5 AND rnk_fio=1
    UNION ALL --ищем телефон в закрепленном за ФМЖ
      SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (
      SELECT DISTINCT ah_src.OBJECTS_ID AS OBJECTS_ID
            ,ah_src.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(ah_src.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(ah_src.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,DECODE(NVL(TRIM(adr.REGIONS_NAMES), '-'), '-', '', (adr.REGIONS_NAMES))
              ||DECODE(NVL(TRIM(adr.AREAS_NAMES), '-'), '-', '', (', РАЙОН '||adr.AREAS_NAMES)) 
              ||DECODE(NVL(TRIM(adr.CITIES_NAMES), '-'), '-', '', (', ' || adr.SHOTNAME_CIT ||' '|| adr.CITIES_NAMES))
              ||DECODE(NVL(TRIM(adr.STREETS_NAMES), '-'), '-', '', (', ' || adr.STREETS_NAMES)) 
              ||DECODE(NVL(TRIM(adr.HOUSE), '-'), '-', '', (', Д.'||adr.HOUSE)) 
              ||DECODE(NVL(TRIM(adr.BUILD), '-'), '-', '', (', КОРП.'||adr.BUILD)) 
              ||DECODE(NVL(TRIM(adr.FLAT),  '-'), '-', '', (', КВ.'||adr.FLAT)) AS N3
            ,'ADR_FMG'          AS T3
            ,ah_src.ADDRESS_AKT AS AKT3
            ,COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) AS DT3
            --Node 2
            ,snr.N2 AS N2
            ,'PHONE_FMG' AS T2
            ,dbl.PHONES_AKT AS AKT2
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) AS DT2
            --Node 4
            ,NULL AS N4
            ,NULL AS T4
            ,NULL AS AKT4
            ,NULL AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            --,'N1-N3, N3-N2' AS ROUTE_GRAPH
            ,'N1-N3, N1-N2' AS ROUTE_GRAPH
            ,'FIO_DR_REL_HIDE_PHFMG; '||'ADDRESS_ID='||TO_CHAR(ah_src.ADDRESS_ID)||'; PHONES_ID='||TO_CHAR(dbl.PHONES_ID) AS ATTRIBUTES
            ,DENSE_RANK() OVER(PARTITION BY 
                              --dbl.OBJECTS_ID     --по каждому физику последний PHONES_ID
                              dbl.PHONE     --по каждому PHONES_ID последние дата и актуальность
                              ORDER BY dbl.PHONES_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) DESC) rnk_ph
            ,DENSE_RANK() OVER(PARTITION BY 
                              --dbl.OBJECTS_ID     --по каждому физику последний PHONES_ID
                              ah_src.ADDRESS_ID     --по каждому PHONES_ID последние дата и актуальность
                              ORDER BY ah_src.ADDRESS_AKT DESC, COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) DESC) rnk_adr
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio
        FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
        INNER JOIN ODS.PKK_PHONES dbl
          ON dbl.PHONE = snr.N2 AND dbl.OBJECTS_TYPE=8 --and dbl.PHONES_AKT=1
            --AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) - NVL(snr.DT2, SYSDATE+365*100) > -365 +(SYSDATE-snr.DT2)
            AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) BETWEEN snr.DT2-365 AND SYSDATE
        INNER JOIN ODS.PKK_ADDRESS_HISTORY 
              --CPD.ADDRESS_HISTORY@DBLINK_PKK
              ah_src
          ON ah_src.ADDRESS_ID = dbl.OBJECTS_ID AND ah_src.OBJECTS_TYPE=2
            AND NVL(snr.OBJECTS_ID, -1) ^= ah_src.OBJECTS_ID --физики должны быть разными
        LEFT JOIN ODS.PKK_ADDRESS adr
          ON adr.ADDRESS_ID = ah_src.ADDRESS_ID
        LEFT JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = ah_src.OBJECTS_ID 
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND NVL(snr.USER_SNA, '-')=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
          AND NOT snr.N2 IS NULL AND snr.T2 IN('PHONE_MOBILE', 'PHONE_DOP', 'PHONE_FMG', 'PHONE_PMG', 'PHONE_WORK', 'PHONE_ORG')
          --AND NOT snr.N2 LIKE '%000000'
      ) WHERE rnk_ph<=5 AND rnk_fio=1 AND rnk_adr=1
    UNION ALL --ищем телефон в закрепленном за ПМЖ
      SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (
      SELECT DISTINCT doc.OBJECTS_ID AS OBJECTS_ID
            ,doc.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(doc.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(doc.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            --,'FIO_DR_REL_HIDE_PHPMG' AS T1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,DECODE(NVL(TRIM(adr.REGIONS_NAMES), '-'), '-', '', (adr.REGIONS_NAMES))
              ||DECODE(NVL(TRIM(adr.AREAS_NAMES), '-'), '-', '', (', РАЙОН '||adr.AREAS_NAMES)) 
              ||DECODE(NVL(TRIM(adr.CITIES_NAMES), '-'), '-', '', (', ' || adr.SHOTNAME_CIT ||' '|| adr.CITIES_NAMES))
              ||DECODE(NVL(TRIM(adr.STREETS_NAMES), '-'), '-', '', (', ' || adr.STREETS_NAMES)) 
              ||DECODE(NVL(TRIM(adr.HOUSE), '-'), '-', '', (', Д.'||adr.HOUSE)) 
              ||DECODE(NVL(TRIM(adr.BUILD), '-'), '-', '', (', КОРП.'||adr.BUILD)) 
              ||DECODE(NVL(TRIM(adr.FLAT),  '-'), '-', '', (', КВ.'||adr.FLAT)) AS N3
            ,'ADR_PMG'          AS T3
            ,ah_src.ADDRESS_AKT AS AKT3
            ,COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) AS DT3
            --Node 2
            ,snr.N2 AS N2
            ,'PHONE_PMG' AS T2
            ,dbl.PHONES_AKT AS AKT2
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) AS DT2
            --Node 4
            ,doc.DOCUMENTS_SERIAL||' '||doc.DOCUMENTS_NUMBER AS N4
            ,'PASP' AS T4
            ,doc.DOCUMENTS_AKT AS AKT4
            ,COALESCE(doc.MODIFICATION_DATE, doc.DOCUMENTS_CREATED)     AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            --,'N1-N3, N3-N2, N1-N4' AS ROUTE_GRAPH  
            ,'N1-N3, N1-N2, N1-N4' AS ROUTE_GRAPH  
            ,'FIO_DR_REL_HIDE_PHPMG; '||'ADDRESS_ID='||TO_CHAR(ah_src.ADDRESS_ID)||'; PHONES_ID='||TO_CHAR(dbl.PHONES_ID)
                ||'; DOCUMENTS_ID='||TO_CHAR(doc.DOCUMENTS_ID) AS ATTRIBUTES
            ,DENSE_RANK() OVER(PARTITION BY 
                      --dbl.OBJECTS_ID     --по каждому физику последний PHONES_ID
                      dbl.PHONE     --по каждому PHONES_ID последние дата и актуальность
                      ORDER BY dbl.PHONES_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) DESC) rnk_ph
            ,DENSE_RANK() OVER(PARTITION BY 
                      --dbl.OBJECTS_ID     --по каждому физику последний PHONES_ID
                      ah_src.ADDRESS_ID     --по каждому PHONES_ID последние дата и актуальность
                      ORDER BY ah_src.ADDRESS_AKT DESC, COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) DESC) rnk_pmg
            ,DENSE_RANK() OVER(PARTITION BY 
                      --doc.OBJECTS_ID                           --по каждому физику последний паспорт
                      doc.DOCUMENTS_SERIAL, doc.DOCUMENTS_NUMBER --по каждому паспорту последние дата и актуальность
                      ORDER BY doc.DOCUMENTS_AKT DESC, COALESCE(doc.MODIFICATION_DATE, doc.DOCUMENTS_CREATED) DESC) rnk_doc
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio
        FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
        INNER JOIN ODS.PKK_PHONES 
                  --CPD.PHONES@DBLINK_PKK
                  dbl
          ON dbl.PHONE = snr.N2 AND dbl.OBJECTS_TYPE=8 --and dbl.PHONES_AKT=1
            --AND NOT snr.N2 LIKE '%000000'
            --AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) - NVL(snr.DT2, SYSDATE+365*100) > -365+(SYSDATE-snr.DT2)
            AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) BETWEEN snr.DT2-365 AND SYSDATE
        INNER JOIN ODS.PKK_ADDRESS_HISTORY 
              --CPD.ADDRESS_HISTORY@DBLINK_PKK
              ah_src
          ON ah_src.ADDRESS_ID = dbl.OBJECTS_ID AND ah_src.OBJECTS_TYPE=5
        LEFT JOIN ODS.PKK_ADDRESS adr
          ON adr.ADDRESS_ID = ah_src.ADDRESS_ID
        INNER JOIN ODS.PKK_DOCUMENTS_HISTORY_INFO doc
          ON doc.DOCUMENTS_ID= ah_src.OBJECTS_ID AND doc.DOCUMENTS_TYPE=21
        INNER JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = doc.OBJECTS_ID 
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND NVL(snr.USER_SNA, '-')=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
          AND NOT snr.N2 IS NULL AND snr.T2 IN('PHONE_MOBILE', 'PHONE_DOP', 'PHONE_FMG', 'PHONE_PMG', 'PHONE_WORK', 'PHONE_ORG')
          AND NVL(snr.OBJECTS_ID, -1) ^= doc.OBJECTS_ID --физики должны быть разными
      ) WHERE rnk_ph<=5 AND rnk_pmg=1 AND rnk_doc=1 AND rnk_fio=1
    UNION ALL --ищем телефон в ДОП. контактах
      SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (
      SELECT DISTINCT dbl.OBJECTS_ID AS OBJECTS_ID
            ,dbl.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            --,'FIO_DR_REL_HIDE_PHDOP' AS T1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,NVL(dbl.PHONES_COMM, 'Контакт физика '||TO_CHAR(dbl.OBJECTS_ID)||' без комментария') AS N3
            ,'FIO_DOP' AS T3
            ,dbl.PHONES_AKT AS AKT3
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) AS DT3
            --Node 2
            ,snr.N2 AS N2
            ,'PHONE_DOP' AS T2
            ,dbl.PHONES_AKT AS AKT2
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) AS DT2
            --Node 4
            ,NULL     AS N4
            ,NULL     AS T4
            ,NULL     AS AKT4
            ,NULL     AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            ,'N1-N3, N3-N2' AS ROUTE_GRAPH
            ,'FIO_DR_REL_HIDE_PHDOP;'||'PHONES_ID='||TO_CHAR(dbl.PHONES_ID) AS ATTRIBUTES
            ,snr.T2 as T2_src
            ,dbl.OBJECTS_TYPE
            ,DENSE_RANK() OVER(PARTITION BY 
                              --dbl.OBJECTS_ID     --по каждому физику последний PHONES_ID
                              dbl.PHONE     --по каждому PHONES_ID последние дата и актуальность
                              ORDER BY dbl.PHONES_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) DESC) rnk_ph
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio
        FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
        INNER JOIN ODS.PKK_PHONES dbl
          ON dbl.PHONE = snr.N2 AND dbl.OBJECTS_TYPE=200
            AND NVL(snr.OBJECTS_ID, -1) ^= dbl.OBJECTS_ID --физики должны быть разными
            --AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) - NVL(snr.DT2, SYSDATE+365*100) > -365+(SYSDATE-snr.DT2)
            AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) BETWEEN snr.DT2-365 AND SYSDATE
        INNER JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = dbl.OBJECTS_ID 
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND NVL(snr.USER_SNA, '-')=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
          AND NOT snr.N2 IS NULL AND snr.T2 IN('PHONE_MOBILE', 'PHONE_DOP', 'PHONE_FMG', 'PHONE_PMG', 'PHONE_WORK', 'PHONE_ORG')
          AND NOT (NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из НБКИ:%') 
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон клиента(сайт)%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Рекомендация(ПО КЦ):%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РБО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПТ/АТМ банка%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из документов РБО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('сот. тел. заявителя(сайт)%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('ТЕЛЕФОН ИЗ НБКИ%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РНКО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПО КЦ%' ))
      ) WHERE rnk_ph<=7 AND rnk_fio=1
    UNION ALL --ищем телефон в МОБИЛЬНЫХ
      SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM (
      SELECT DISTINCT dbl.OBJECTS_ID AS OBJECTS_ID
            ,dbl.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(dbl.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            --,'FIO_DR_REL_HIDE_PHMOB' AS T1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,NULL       AS N3
            ,NULL       AS T3
            ,NULL       AS AKT3
            ,NULL       AS DT3
            --Node 2
            ,snr.N2 AS N2
            ,'PHONE_MOBILE' AS T2
            ,dbl.PHONES_AKT AS AKT2
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) AS DT2
            ,dbl.PHONES_COMM
            --Node 4
            ,NULL     AS N4
            ,NULL     AS T4
            ,NULL     AS AKT4
            ,NULL     AS DT4
            --Node 5
            ,NULL     AS N5
            ,NULL     AS T5
            ,NULL     AS AKT5
            ,NULL     AS DT5
            ,'N1-N2'AS ROUTE_GRAPH
            ,'FIO_DR_REL_HIDE_PHMOB; '||'PHONES_ID='||TO_CHAR(dbl.PHONES_ID) AS ATTRIBUTES
            ,snr.T2 as T2_SRC
            ,DENSE_RANK() OVER(PARTITION BY 
                              --dbl.OBJECTS_ID     --по каждому физику последний PHONES_ID
                              dbl.PHONE     --по каждому PHONES_ID последние дата и актуальность
                              ORDER BY dbl.PHONES_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) DESC) rnk_ph
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio
        FROM (SELECT SRC_OBJECTS_ID, OBJECTS_ID, N2, T2, DT2, AKT2 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr 
                      WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
                            AND NOT snr.N2 IS NULL
              UNION 
              SELECT SRC_OBJECTS_ID, OBJECTS_ID, N5 AS N2, T5 AS T2, DT5 AS DT2, AKT5 AS AKT2 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr 
                      WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
                            AND NOT snr.N5 IS NULL) snr
        INNER JOIN ODS.PKK_PHONES 
                  --CPD.PHONES@DBLINK_PKK
                dbl
          ON dbl.PHONE = snr.N2 AND dbl.OBJECTS_TYPE=2
            AND NVL(snr.OBJECTS_ID, -1) ^= dbl.OBJECTS_ID --физики должны быть разными
            --AND NOT snr.N2 LIKE '%000000'
            --AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) - NVL(snr.DT2, SYSDATE+365*100) > -365+(SYSDATE-snr.DT2)
            AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) BETWEEN snr.DT2-365 AND SYSDATE
        INNER JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = dbl.OBJECTS_ID 
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
          AND NOT snr.N2 IS NULL AND snr.T2 IN('PHONE_MOBILE', 'PHONE_DOP', 'PHONE_FMG', 'PHONE_PMG', 'PHONE_WORK', 'PHONE_ORG')
          AND NOT (NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из НБКИ:%') 
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон клиента(сайт)%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Рекомендация(ПО КЦ):%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РБО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПТ/АТМ банка%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из документов РБО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('сот. тел. заявителя(сайт)%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('ТЕЛЕФОН ИЗ НБКИ%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РНКО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПО КЦ%' ))
      ) WHERE rnk_ph<=5 AND rnk_fio=1
    /*UNION ALL --ищем телефон в РАБОЧИХ
     SELECT DISTINCT OBJECTS_ID, OBJECTS_ID_REL, ROOT_PERS, ROOT_PERS_REL
          ,N1, T1, AKT1, DT1, N2, T2, AKT2, DT2, N3, T3, AKT3, DT3
          ,N4, T4, AKT4, DT4, N5, T5, AKT5, DT5
          ,ROUTE_GRAPH, ATTRIBUTES
           FROM ( SELECT DISTINCT 
            COALESCE(wor_wor.OBJECTS_ID, wor_org.OBJECTS_ID) AS OBJECTS_ID
            ,COALESCE(wor_wor.OBJECTS_ID, wor_org.OBJECTS_ID) AS OBJECTS_ID_REL
            ,DECODE(COALESCE(wor_wor.OBJECTS_ID, wor_org.OBJECTS_ID), snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(COALESCE(wor_wor.OBJECTS_ID, wor_org.OBJECTS_ID), snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            --Node 1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N1
            --,'FIO_DR_REL_HIDE_PHWRK' AS T1
            ,'FIO_DR' AS T1
            ,per.FIO_AKT AS AKT1
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT1
            --Node 3
            ,DECODE(NVL(TRIM(adr.REGIONS_NAMES), '-'), '-', '', (adr.REGIONS_NAMES))
              ||DECODE(NVL(TRIM(adr.AREAS_NAMES), '-'), '-', '', (', РАЙОН '||adr.AREAS_NAMES)) 
              ||DECODE(NVL(TRIM(adr.CITIES_NAMES), '-'), '-', '', (', ' || adr.SHOTNAME_CIT ||' '|| adr.CITIES_NAMES))
              ||DECODE(NVL(TRIM(adr.STREETS_NAMES), '-'), '-', '', (', ' || adr.STREETS_NAMES)) 
              ||DECODE(NVL(TRIM(adr.HOUSE), '-'), '-', '', (', Д.'||adr.HOUSE)) 
              ||DECODE(NVL(TRIM(adr.BUILD), '-'), '-', '', (', КОРП.'||adr.BUILD)) 
              ||DECODE(NVL(TRIM(adr.FLAT),  '-'), '-', '', (', КВ.'||adr.FLAT)) AS N3
            ,'ADR_WORK' AS T3
            ,ah_src.ADDRESS_AKT AS AKT3
            ,COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) AS DT3
            ,ah_src.ADDRESS_ID
            --Node 5
            ,ph.PHONE     AS N5
            ,DECODE(dbl.OBJECTS_TYPE, 3, 'PHONE_WORK', 12, 'PHONE_ORG' ) as T5
            ,ph.PHONES_AKT    AS AKT5
            ,COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) AS DT5
            --Node 2
            ,dbl.PHONE AS N2
            ,DECODE(dbl.OBJECTS_TYPE, 3, 'PHONE_ORG', 12, 'PHONE_WORK' ) as T2
            ,dbl.PHONES_AKT AS AKT2
            ,COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED)     AS DT2
            --Node 4
            ,COALESCE(wor_wor.ORG_NAME, wor_org.ORG_NAME)     AS N4
            ,'ORG_NAME'       AS T4
            ,COALESCE(wor_wor.WORKS_AKT, wor_org.WORKS_AKT)    AS AKT4
            ,COALESCE(wor_wor.WORKS_LAST, wor_org.WORKS_LAST) AS WORKS_LAST
            ,COALESCE(COALESCE(wor_wor.MODIFICATION_DATE, wor_wor.WORKS_CREATED)
                                , COALESCE(wor_org.MODIFICATION_DATE, wor_org.WORKS_CREATED))     AS DT4
            ,'N1-N4, N4-N2, N4-N3, N4-N5' AS ROUTE_GRAPH
            ,'FIO_DR_REL_HIDE_PHWRK; '||'WORKS_ID='||TO_CHAR(COALESCE(wor_wor.WORKS_ID, wor_org.WORKS_ID))||'; ORG_ID='||TO_CHAR(COALESCE(wor_wor.ORG_ID, wor_org.ORG_ID))||'; ADDRESS_ID='||TO_CHAR(ah_src.ADDRESS_ID)
                ||'; PHONES_ID='||TO_CHAR(dbl.PHONES_ID)||'; PHONES_ID='||TO_CHAR(ph.PHONES_ID) AS ATTRIBUTES
            ,snr.T2 as T2_SRC
            ,DENSE_RANK() OVER(PARTITION BY 
                  --wor_wor.OBJECTS_ID       --по каждому физику последняя работа
                  COALESCE(wor_wor.WORKS_ID, wor_org.ORG_ID)             --по каждой работе последние дата и актуальность
                  ORDER BY COALESCE(wor_wor.WORKS_AKT, wor_org.WORKS_AKT) DESC
                    ,COALESCE(COALESCE(wor_wor.MODIFICATION_DATE, wor_wor.WORKS_CREATED)
                                , COALESCE(wor_org.MODIFICATION_DATE, wor_org.WORKS_CREATED)) DESC) rnk_wor
            ,DENSE_RANK() OVER(PARTITION BY 
                  COALESCE(wor_wor.ORG_ID, wor_org.ORG_ID)            --по каждомй работе последний адрес
                  --ah_src.ADDRESS_ID   --по каждому адресу последние дата и актуальность
                  ORDER BY ah_src.ADDRESS_AKT DESC, COALESCE(ah_src.MODIFICATION_DATE, ah_src.ADDRESS_CREATED) DESC) rnk_adr
            ,DENSE_RANK() OVER(PARTITION BY 
                  --COALESCE(wor_wor.WORKS_ID, wor_org.WORKS_ID)     --по каждой работе последний телефон
                  dbl.PHONE       --по кадому телефону последние дата и актуальность
                  ORDER BY dbl.PHONES_AKT DESC, COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) DESC) rnk_ph_dbl
            ,DENSE_RANK() OVER(PARTITION BY 
                  COALESCE(wor_wor.WORKS_ID, wor_org.ORG_ID)
                  ,DECODE(dbl.OBJECTS_TYPE, 3, wor_wor.WORKS_ID, 12, wor_org.ORG_ID ), dbl.OBJECTS_TYPE   --по каждой работе последний телефон
                  --ph.PHONE       --по кадому телефону последние дата и актуальность
                  ORDER BY ph.PHONES_AKT DESC, COALESCE(ph.MODIFICATION_DATE, ph.PHONES_CREATED) DESC) rnk_ph
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio
        FROM (SELECT SRC_OBJECTS_ID, OBJECTS_ID, N2, T2, DT2, AKT2 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr 
                      WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
                            AND NOT snr.N2 IS NULL
              UNION 
              SELECT SRC_OBJECTS_ID, OBJECTS_ID, N5 AS N2, T5 AS T2, DT5 AS DT2, AKT5 AS AKT2 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr 
                      WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND USER_SNA=v_LOGIN AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
                            AND NOT snr.N5 IS NULL) snr
        INNER JOIN ODS.PKK_PHONES 
                  --CPD.PHONES@DBLINK_PKK
                  dbl
          ON dbl.PHONE = snr.N2 AND dbl.OBJECTS_TYPE IN(3,12)
            AND NOT snr.N2 LIKE '%000000'
            --AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) - NVL(snr.DT2, SYSDATE+365*100) > -365 +(SYSDATE-snr.DT2)
            AND COALESCE(dbl.MODIFICATION_DATE, dbl.PHONES_CREATED) BETWEEN snr.DT2-365 AND SYSDATE
        LEFT JOIN ODS.PKK_WORKS_INFO wor_org
          ON wor_org.ORG_ID = dbl.OBJECTS_ID AND dbl.OBJECTS_TYPE=3
        LEFT JOIN ODS.PKK_WORKS_INFO wor_wor
          ON wor_wor.WORKS_ID = dbl.OBJECTS_ID AND dbl.OBJECTS_TYPE=12
        LEFT JOIN ODS.PKK_PHONES 
                  --CPD.PHONES@DBLINK_PKK
                  ph
          --если исходный телефон ORG_ID, то притягиваем по WORKS_ID, иначе по ORG_ID; учитываем тип
          ON ph.OBJECTS_ID = DECODE(dbl.OBJECTS_TYPE, 3, wor_org.WORKS_ID, 12, wor_wor.ORG_ID ) 
            AND ph.OBJECTS_TYPE = DECODE(dbl.OBJECTS_TYPE, 3, 12, 12, 3)
        LEFT JOIN ODS.PKK_ADDRESS_HISTORY 
              --CPD.ADDRESS_HISTORY@DBLINK_PKK
               ah_src 
          ON --ah_src.ADDRESS_ID --!! КАК НЕТ?!
            ah_src.OBJECTS_ID = COALESCE(wor_org.ORG_ID,wor_wor.ORG_ID)
            --DECODE(dbl.OBJECTS_TYPE, 3, wor_org.ORG_ID, 12, wor_wor.ORG_ID )
            AND ah_src.OBJECTS_TYPE = 3 --2 OBJECTS_ID, 3 ORG_ID, 5 DOCUMENTS_ID, 19 ADDRESS_ID, 200 ? 
            --AND ah_src.ADDRESS_AKT=1
        LEFT JOIN ODS.PKK_ADDRESS adr
          ON ah_src.ADDRESS_ID = adr.ADDRESS_ID
        LEFT JOIN ODS.PKK_PERSON_INFO per
          ON per.OBJECTS_ID = COALESCE(wor_wor.OBJECTS_ID, wor_org.OBJECTS_ID, -1)
        WHERE snr.SRC_OBJECTS_ID = p_cur_OBJECTS_ID AND snr.OBJECTS_ID = p_cur_OBJECTS_ID
          AND NOT snr.N2 IS NULL AND snr.T2 IN('PHONE_MOBILE', 'PHONE_DOP', 'PHONE_ORG', 'PHONE_FMG', 'PHONE_PMG', 'PHONE_WORK')
          AND NVL(snr.OBJECTS_ID, -1) ^= COALESCE(wor_wor.OBJECTS_ID, wor_org.OBJECTS_ID, -1) --физики должны быть разными
          AND NOT COALESCE(wor_org.WORKS_ID, wor_wor.WORKS_ID) IS NULL
      ) WHERE rnk_ph_dbl<=5 AND rnk_wor<=1 AND rnk_adr=1 AND rnk_ph<=5 AND rnk_fio=1*/
      ; 
     
  CURSOR cur_PERSON_REL_CRED (p_cur_OBJECTS_ID NUMBER) IS 
      SELECT DISTINCT OBJECTS_ID
            ,OBJECTS_ID_REL
            ,ROOT_PERS
            ,ROOT_PERS_REL
            --далее пошла нужная информация родителя
            ,N1
            ,T1
            ,AKT1
            ,DT1
            --далее пошла нужная информация ребенка
            ,N3
            ,T3 --||DECODE(FAMILY_REL_STATUS, 1, '_'||FAMILY_REL_NAME||CONTACT_COMMENT, '') 
            ,AKT3
            ,DT3
            ,ROUTE_GRAPH
          FROM (
          SELECT cr.OBJECTS_ID as OBJECTS_ID
            ,cr_por.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(cr.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(cr_por.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            ,snr.N1    AS N1
            ,snr.T1   AS T1
            ,snr.AKT1 AS AKT1
            ,snr.DT1 AS DT1
            ,per.FIO4SEARCH||TO_CHAR(per.BIRTH, ' dd.mm.yyyy') AS N3
            ,DECODE(cr_por.TYPE_REQUEST_ID, 6, 'FIO_DR_REL_POR', 7, 'FIO_DR_REL_POR', 13, 'FIO_DR_REL_SOZ') AS T3
            ,cr_por.TYPE_REQUEST_ID
            ,per.FIO_AKT AS AKT3
            ,COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) AS DT3
            ,'N1-N3' AS ROUTE_GRAPH
            ,DENSE_RANK() OVER(PARTITION BY cr.OBJECTS_ID, cr_por.OBJECTS_ID
                              ORDER BY cr_por.OBJECTS_ID DESC, COALESCE(cr_por.MODIFICATION_DATE_REQUEST, cr_por.CREATED_DATE) DESC) rnk
            ,DENSE_RANK() OVER(PARTITION BY per.OBJECTS_ID
                               ORDER BY per.FIO_AKT DESC, COALESCE(per.MODIFICATION_DATE, per.FIO_CREATED) DESC) rnk_fio_rel
          FROM ODS.PKK_C_REQUEST_SNA cr
          INNER JOIN (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
                      FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                      WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN AND T1='FIO_DR' AND LEVEL_NODE IN(1,3) --and rownum=1
                    UNION
                    SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
                      FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                      WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN AND T3 LIKE 'FIO_DR%' AND LEVEL_NODE IN(1,3)) snr
            ON snr.OBJECTS_ID = cr.OBJECTS_ID AND cr.TYPE_REQUEST_ID=1
          INNER JOIN ODS.PKK_C_REQUEST_SNA cr_por
            ON cr_por.PARENT_ID = cr.PARENT_ID AND cr_por.TYPE_REQUEST_ID IN(6,7,13)
              AND cr_por.OBJECTS_ID ^= cr.OBJECTS_ID
          INNER JOIN ODS.PKK_PERSON_INFO per
            ON per.OBJECTS_ID = cr_por.OBJECTS_ID
            WHERE snr.SRC_OBJECTS_ID=p_cur_OBJECTS_ID
          ) WHERE rnk=1 AND rnk_fio_rel=1
      ;

  CURSOR cur_PERSON_BNK (p_cur_OBJECTS_ID NUMBER) IS 
      SELECT DISTINCT OBJECTS_ID
            ,OBJECTS_ID_REL
            ,ROOT_PERS
            ,ROOT_PERS_REL
            --далее пошла нужная информация родителя
            ,N1
            ,T1
            ,AKT1
            ,DT1
            --далее пошла нужная информация ребенка
            ,N3
            ,T3 --||DECODE(FAMILY_REL_STATUS, 1, '_'||FAMILY_REL_NAME||CONTACT_COMMENT, '') 
            ,AKT3
            ,DT3
            ,ROUTE_GRAPH
          FROM (
          SELECT snr.OBJECTS_ID as OBJECTS_ID
            ,snr.OBJECTS_ID AS OBJECTS_ID_REL
            ,DECODE(snr.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS
            ,DECODE(snr.OBJECTS_ID, snr.OBJECTS_ID, 1, 0) AS ROOT_PERS_REL
            ,snr.N1    AS N1
            ,snr.T1   AS T1
            ,snr.AKT1 AS AKT1
            ,snr.DT1 AS DT1
            ,(SELECT TO_CHAR(snr.OBJECTS_ID)||' - '||'БНК. Всего: '||TO_CHAR(COUNT(DISTINCT R_TYPE_ID)) 
                ||'; Max: '||SUBSTR(TO_CHAR(MAX(R_TYPE_ID)), 1,2) 
              FROM SNA_REQUEST_BNK_HIST WHERE OBJECTS_ID=snr.OBJECTS_ID) AS N3
            ,'BNK_PKK' AS T3
            ,0 AS AKT3
            ,SYSDATE AS DT3
            ,'N1-N3' AS ROUTE_GRAPH
            --,DENSE_RANK() OVER(PARTITION BY cr.OBJECTS_ID, cr_por.OBJECTS_ID
              --                ORDER BY cr_por.OBJECTS_ID DESC, COALESCE(cr_por.MODIFICATION_DATE_REQUEST, cr_por.CREATED_DATE) DESC) rnk
          FROM (SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID, N1, T1, AKT1, DT1 
                      FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                      WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN AND T1='FIO_DR' AND LEVEL_NODE IN(1,3) --and rownum=1
                    UNION
                    SELECT DISTINCT SRC_OBJECTS_ID, OBJECTS_ID_REL AS OBJECTS_ID, N3 AS N1, T1 AS T1, AKT3 AS AKT1, DT3 AS DT1
                      FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                      WHERE SRC_OBJECTS_ID=p_cur_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN AND T3 LIKE 'FIO_DR%' AND LEVEL_NODE IN(1,3)) snr
            WHERE snr.SRC_OBJECTS_ID=p_cur_OBJECTS_ID
          ) WHERE NOT N3 LIKE '%Всего: 0%'
      ;
  --cur_PERSON_NODE_1 cur_PERSON_NODE%ROWTYPE;
   
    
BEGIN
  DBMS_OUTPUT.ENABLE;
  --p_DATE_RID := SYSDATE-10;
  
  /*v_FLAG_INFO_EXISTS    :=  0;
  v_CNT_NODE            := -1;
  v_CNT_LINK            := -1;
  v_CNT_OBJECTS_ID      := -1;
  v_CNT_FROD            := -1;
  v_CNT_BNK             := -1;
  v_CNT_DEFOLT          := -1;
  v_CNT_ALERT_PERSON    := -1;*/
  /*IF v_LOGIN IS NULL THEN 
    v_LOGIN := '-';
  END IF;*/
  
  DBMS_OUTPUT.PUT_LINE('Время 0: '||TO_CHAR(SYSTIMESTAMP));
  
  DELETE FROM SNAUSER.SNA_REQUEST_NODE_ROUTES WHERE SRC_OBJECTS_ID = v_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN;

  --S_1. Ищем прямые указанные связи с реальными физиками
  FOR cur_PERSON_REL_1 IN cur_PERSON_REL(v_OBJECTS_ID) LOOP
      IF (cur_PERSON_REL_1.OBJECTS_ID=cur_PERSON_REL_1.OBJECTS_ID_REL AND cur_PERSON_REL_1.T3='FIO_DR_REL_REF') THEN
        cur_PERSON_REL_1.T3 := 'FIO_DR';
      END IF;
      
      INSERT INTO SNAUSER.SNA_REQUEST_NODE_ROUTES 
        (SRC_REQUEST_ID, SRC_OBJECTS_ID,SNA_REQUEST_DATE, USER_SNA
            ,OBJECTS_ID, OBJECTS_ID_REL
            ,ROOT_PERS, ROOT_PERS_REL
            ,N1, T1, AKT1, DT1
            ,N3, T3, AKT3, DT3
            ,ROUTE_GRAPH
            ,LEVEL_NODE
            )
          VALUES (v_REQUEST_ID, v_OBJECTS_ID, v_SNA_DATE_BEGIN, v_LOGIN
            ,cur_PERSON_REL_1.OBJECTS_ID, cur_PERSON_REL_1.OBJECTS_ID_REL
            ,cur_PERSON_REL_1.ROOT_PERS, cur_PERSON_REL_1.ROOT_PERS_REL
            ,cur_PERSON_REL_1.N1, cur_PERSON_REL_1.T1, cur_PERSON_REL_1.AKT1, cur_PERSON_REL_1.DT1
            ,cur_PERSON_REL_1.N3, cur_PERSON_REL_1.T3, cur_PERSON_REL_1.AKT3, cur_PERSON_REL_1.DT3
            ,cur_PERSON_REL_1.ROUTE_GRAPH
            ,1);
        /*DBMS_OUTPUT.PUT_LINE('Время  : '||TO_CHAR(SYSTIMESTAMP)||' - получили = '
            || cur_PERSON_REL_1.OBJECTS_ID||' '||cur_PERSON_REL_1.OBJECTS_ID_REL);*/
  END LOOP;
  --COMMIT;

  DBMS_OUTPUT.PUT_LINE('Время 1: '||TO_CHAR(SYSTIMESTAMP));
  
  --S_2. Дополняем информацией по первому кругу фзииков
  p_cnt_Level := 2;
  FOR cur_PERSON_NODE_2 IN cur_PERSON_NODE(v_OBJECTS_ID, 1) LOOP
     INSERT INTO SNAUSER.SNA_REQUEST_NODE_ROUTES 
        (SRC_REQUEST_ID, SRC_OBJECTS_ID,SNA_REQUEST_DATE, USER_SNA
            ,OBJECTS_ID, OBJECTS_ID_REL
            ,ROOT_PERS, ROOT_PERS_REL
            ,N1, T1, AKT1, DT1
            ,N2, T2, AKT2, DT2 
            ,N3, T3, AKT3, DT3
            ,N4, T4, AKT4, DT4
            ,N5, T5, AKT5, DT5
            ,ROUTE_GRAPH
            ,ATTRIBUTES
            ,LEVEL_NODE
            )
          VALUES (v_REQUEST_ID, v_OBJECTS_ID, v_SNA_DATE_BEGIN, v_LOGIN
            ,cur_PERSON_NODE_2.OBJECTS_ID, cur_PERSON_NODE_2.OBJECTS_ID_REL
            ,cur_PERSON_NODE_2.ROOT_PERS, cur_PERSON_NODE_2.ROOT_PERS_REL
            ,cur_PERSON_NODE_2.N1, cur_PERSON_NODE_2.T1, cur_PERSON_NODE_2.AKT1, cur_PERSON_NODE_2.DT1
            ,cur_PERSON_NODE_2.N2, cur_PERSON_NODE_2.T2, cur_PERSON_NODE_2.AKT2, cur_PERSON_NODE_2.DT2
            ,cur_PERSON_NODE_2.N3, cur_PERSON_NODE_2.T3, cur_PERSON_NODE_2.AKT3, cur_PERSON_NODE_2.DT3
            ,cur_PERSON_NODE_2.N4, cur_PERSON_NODE_2.T4, cur_PERSON_NODE_2.AKT4, cur_PERSON_NODE_2.DT4
            ,cur_PERSON_NODE_2.N5, cur_PERSON_NODE_2.T5, cur_PERSON_NODE_2.AKT5, cur_PERSON_NODE_2.DT5
            ,cur_PERSON_NODE_2.ROUTE_GRAPH
            ,cur_PERSON_NODE_2.ATTRIBUTES
            ,p_cnt_Level
            );
      /*IF v_FLAG_INFO_EXISTS^=1 THEN 
        v_FLAG_INFO_EXISTS := 1;
      END IF;*/
  END LOOP;
  --COMMIT;

  DBMS_OUTPUT.PUT_LINE('Время 2: '||SYSTIMESTAMP||' - после подтягивания данных '||TO_CHAR(v_OBJECTS_ID));


  --S_3. дополняем ID неявных физиков 
    FOR cur_PERSON_HIDE_1 IN cur_PERSON_HIDE(v_OBJECTS_ID) LOOP
      INSERT INTO SNAUSER.SNA_REQUEST_NODE_ROUTES 
        (SRC_REQUEST_ID, SRC_OBJECTS_ID,SNA_REQUEST_DATE, USER_SNA
            ,OBJECTS_ID, OBJECTS_ID_REL
            ,ROOT_PERS, ROOT_PERS_REL
            ,N1, T1, AKT1, DT1
            ,N2, T2, AKT2, DT2 
            ,N3, T3, AKT3, DT3
            ,N4, T4, AKT4, DT4
            ,N5, T5, AKT5, DT5
            ,ROUTE_GRAPH
            ,ATTRIBUTES
            ,LEVEL_NODE)
          VALUES (v_REQUEST_ID, v_OBJECTS_ID, v_SNA_DATE_BEGIN, v_LOGIN
            ,cur_PERSON_HIDE_1.OBJECTS_ID, cur_PERSON_HIDE_1.OBJECTS_ID_REL
            ,cur_PERSON_HIDE_1.ROOT_PERS, cur_PERSON_HIDE_1.ROOT_PERS_REL
            ,cur_PERSON_HIDE_1.N1, cur_PERSON_HIDE_1.T1, cur_PERSON_HIDE_1.AKT1, cur_PERSON_HIDE_1.DT1
            ,cur_PERSON_HIDE_1.N2, cur_PERSON_HIDE_1.T2, cur_PERSON_HIDE_1.AKT2, cur_PERSON_HIDE_1.DT2
            ,cur_PERSON_HIDE_1.N3, cur_PERSON_HIDE_1.T3, cur_PERSON_HIDE_1.AKT3, cur_PERSON_HIDE_1.DT3
            ,cur_PERSON_HIDE_1.N4, cur_PERSON_HIDE_1.T4, cur_PERSON_HIDE_1.AKT4, cur_PERSON_HIDE_1.DT4
            ,cur_PERSON_HIDE_1.N5, cur_PERSON_HIDE_1.T5, cur_PERSON_HIDE_1.AKT5, cur_PERSON_HIDE_1.DT5
            ,cur_PERSON_HIDE_1.ROUTE_GRAPH
            ,cur_PERSON_HIDE_1.ATTRIBUTES
            ,3);
  END LOOP;
  --COMMIT;
  DBMS_OUTPUT.PUT_LINE('Время 3: '||SYSTIMESTAMP||' - после подтягивания скрытых ID '||TO_CHAR(v_OBJECTS_ID));
  
  --S_5. Добавляем процентированный БНК ))
  /*FOR cur_PERSON_ID IN (SELECT DISTINCT OBJECTS_ID FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                          WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND NVL(USER_SNA, '-')=NVL(v_LOGIN, '-')
                        UNION SELECT DISTINCT OBJECTS_ID_REL AS OBJECTS_ID FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
                          WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND NVL(USER_SNA, '-')=NVL(v_LOGIN, '-') ) LOOP
    ODS.FIND_R_PERSON(cur_PERSON_ID.OBJECTS_ID);
  END LOOP;
  COMMIT;
  
  FOR cur_PERSON_BNK_obj IN cur_PERSON_BNK(v_OBJECTS_ID) LOOP
      INSERT INTO SNAUSER.SNA_REQUEST_NODE_ROUTES 
        (SRC_REQUEST_ID, SRC_OBJECTS_ID,SNA_REQUEST_DATE, USER_SNA
            ,OBJECTS_ID, OBJECTS_ID_REL
            ,ROOT_PERS, ROOT_PERS_REL
            ,N1, T1, AKT1, DT1
            ,N3, T3, AKT3, DT3
            ,ROUTE_GRAPH
            ,LEVEL_NODE
            )
          VALUES (v_REQUEST_ID, v_OBJECTS_ID, v_SNA_DATE_BEGIN, v_LOGIN
            ,cur_PERSON_BNK_obj.OBJECTS_ID, cur_PERSON_BNK_obj.OBJECTS_ID_REL
            ,cur_PERSON_BNK_obj.ROOT_PERS, cur_PERSON_BNK_obj.ROOT_PERS_REL
            ,cur_PERSON_BNK_obj.N1, cur_PERSON_BNK_obj.T1, cur_PERSON_BNK_obj.AKT1, cur_PERSON_BNK_obj.DT1
            ,cur_PERSON_BNK_obj.N3, cur_PERSON_BNK_obj.T3, cur_PERSON_BNK_obj.AKT3, cur_PERSON_BNK_obj.DT3
            ,cur_PERSON_BNK_obj.ROUTE_GRAPH
            ,5);
  END LOOP;
  COMMIT;*/
  
  
  
  --S_4. дополняем информацию по неявно связанным физикам
  FOR cur_PERSON_NODE_2 IN cur_PERSON_NODE(v_OBJECTS_ID, 3) LOOP
     INSERT INTO SNAUSER.SNA_REQUEST_NODE_ROUTES 
        (SRC_REQUEST_ID, SRC_OBJECTS_ID,SNA_REQUEST_DATE, USER_SNA
            ,OBJECTS_ID, OBJECTS_ID_REL
            ,ROOT_PERS, ROOT_PERS_REL
            ,N1, T1, AKT1, DT1
            ,N2, T2, AKT2, DT2 
            ,N3, T3, AKT3, DT3
            ,N4, T4, AKT4, DT4
            ,N5, T5, AKT5, DT5
            ,ROUTE_GRAPH
            ,ATTRIBUTES
            ,LEVEL_NODE
            )
          VALUES (v_REQUEST_ID, v_OBJECTS_ID, v_SNA_DATE_BEGIN, v_LOGIN
            ,cur_PERSON_NODE_2.OBJECTS_ID, cur_PERSON_NODE_2.OBJECTS_ID_REL
            ,cur_PERSON_NODE_2.ROOT_PERS, cur_PERSON_NODE_2.ROOT_PERS_REL
            ,cur_PERSON_NODE_2.N1, cur_PERSON_NODE_2.T1, cur_PERSON_NODE_2.AKT1, cur_PERSON_NODE_2.DT1
            ,cur_PERSON_NODE_2.N2, cur_PERSON_NODE_2.T2, cur_PERSON_NODE_2.AKT2, cur_PERSON_NODE_2.DT2
            ,cur_PERSON_NODE_2.N3, cur_PERSON_NODE_2.T3, cur_PERSON_NODE_2.AKT3, cur_PERSON_NODE_2.DT3
            ,cur_PERSON_NODE_2.N4, cur_PERSON_NODE_2.T4, cur_PERSON_NODE_2.AKT4, cur_PERSON_NODE_2.DT4
            ,cur_PERSON_NODE_2.N5, cur_PERSON_NODE_2.T5, cur_PERSON_NODE_2.AKT5, cur_PERSON_NODE_2.DT5
            ,cur_PERSON_NODE_2.ROUTE_GRAPH
            ,cur_PERSON_NODE_2.ATTRIBUTES
            ,4
            );

  END LOOP;
  --COMMIT;
  
  DBMS_OUTPUT.PUT_LINE('Время 4: '||SYSTIMESTAMP||' - после подтягивания данных для скрытых ID '||TO_CHAR(v_OBJECTS_ID));
  
  
  --ПОМЕЧАЕМ НОДЫ КОТОЫРЕ БУДЕМ ПОКАЗЫВАТЬ
  --отметка N2SHOW 
  UPDATE SNAUSER.SNA_REQUEST_NODE_ROUTES SET ATTRIBUTES = ATTRIBUTES||'; N2SHOW' 
    WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND USER_SNA=v_LOGIN
    AND N2 IN (SELECT DISTINCT snr.N2 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
                  LEFT JOIN SNAUSER.SNA_REQUEST_NODE_ROUTES sn2_1
                    ON snr.SRC_OBJECTS_ID=sn2_1.SRC_OBJECTS_ID AND snr.USER_SNA=sn2_1.USER_SNA
                      AND snr.N2=sn2_1.N2 AND snr.OBJECTS_ID^=sn2_1.OBJECTS_ID AND snr.N1^=sn2_1.N1
                  LEFT JOIN SNAUSER.SNA_REQUEST_NODE_ROUTES sn2_2
                    ON snr.SRC_OBJECTS_ID=sn2_2.SRC_OBJECTS_ID AND snr.USER_SNA=sn2_2.USER_SNA
                      AND snr.N2=sn2_2.N5 AND snr.OBJECTS_ID^=sn2_2.OBJECTS_ID AND snr.N1^=sn2_2.N1
                  WHERE snr.SRC_OBJECTS_ID=v_OBJECTS_ID AND NOT COALESCE(sn2_1.SRC_OBJECTS_ID, sn2_2.SRC_OBJECTS_ID) IS NULL     
    ) ;
  --отметка N3SHOW: Адрес и Доп. контакт
  UPDATE SNAUSER.SNA_REQUEST_NODE_ROUTES SET ATTRIBUTES = ATTRIBUTES||'; N3SHOW' 
    WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND USER_SNA=v_LOGIN
    AND (N3 IN (SELECT DISTINCT snr.N3 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
                  INNER JOIN SNAUSER.SNA_REQUEST_NODE_ROUTES sn_1
                    ON snr.SRC_OBJECTS_ID=sn_1.SRC_OBJECTS_ID AND snr.USER_SNA=sn_1.USER_SNA
                      AND snr.N3=sn_1.N3 AND snr.OBJECTS_ID^=sn_1.OBJECTS_ID AND snr.N1^=sn_1.N1
                  WHERE snr.SRC_OBJECTS_ID=v_OBJECTS_ID AND (snr.T3 LIKE 'ADR%' OR snr.T3='FIO_DOP')  )
          OR (T3='FIO_DOP' AND ATTRIBUTES LIKE '%N2SHOW%') OR T3='NEG' OR T3='BNK_PKK'
          )
      ;
  UPDATE SNAUSER.SNA_REQUEST_NODE_ROUTES SET ATTRIBUTES = ATTRIBUTES||'; N3SHOW' 
    WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND USER_SNA=v_LOGIN AND NOT (T3 LIKE 'ADR%' OR T3='FIO_DOP')  ;
  --отметка N4SHOW: паспорта, Имя Организации, EMAIL
  UPDATE SNAUSER.SNA_REQUEST_NODE_ROUTES SET ATTRIBUTES = ATTRIBUTES||'; N4SHOW' 
    WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND USER_SNA=v_LOGIN
    AND N4 IN (SELECT DISTINCT snr.N4 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
                  INNER JOIN SNAUSER.SNA_REQUEST_NODE_ROUTES sn_1
                    ON snr.SRC_OBJECTS_ID=sn_1.SRC_OBJECTS_ID AND snr.USER_SNA=sn_1.USER_SNA
                      AND snr.N4=sn_1.N4 AND snr.OBJECTS_ID^=sn_1.OBJECTS_ID AND snr.N1^=sn_1.N1
                  WHERE snr.SRC_OBJECTS_ID=v_OBJECTS_ID  ) ;
  --отметка N5SHOW 
  UPDATE SNAUSER.SNA_REQUEST_NODE_ROUTES SET ATTRIBUTES = ATTRIBUTES||'; N5SHOW' 
    WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND USER_SNA=v_LOGIN
    AND N5 IN (SELECT DISTINCT snr.N5 FROM SNAUSER.SNA_REQUEST_NODE_ROUTES snr
                  LEFT JOIN SNAUSER.SNA_REQUEST_NODE_ROUTES sn5_1
                    ON snr.SRC_OBJECTS_ID=sn5_1.SRC_OBJECTS_ID AND snr.USER_SNA=sn5_1.USER_SNA
                      AND snr.N5=sn5_1.N2 AND snr.OBJECTS_ID^=sn5_1.OBJECTS_ID AND snr.N1^=sn5_1.N1
                  LEFT JOIN SNAUSER.SNA_REQUEST_NODE_ROUTES sn5_2
                    ON snr.SRC_OBJECTS_ID=sn5_2.SRC_OBJECTS_ID AND snr.USER_SNA=sn5_2.USER_SNA
                      AND snr.N5=sn5_2.N5 AND snr.OBJECTS_ID^=sn5_2.OBJECTS_ID AND snr.N1^=sn5_2.N1
                  WHERE snr.SRC_OBJECTS_ID=v_OBJECTS_ID AND NOT COALESCE(sn5_1.SRC_OBJECTS_ID, sn5_2.SRC_OBJECTS_ID) IS NULL  ) ;
  --ДЛЯ РАБОЧИХ ДАННЫХ СДЕЛАТЬ: ЕСЛИ ЕСТЬ ОТМЕТКА SHOW хотя бы на 1 АТРИБУТ, ТО заполнить остальные
  UPDATE SNAUSER.SNA_REQUEST_NODE_ROUTES SET ATTRIBUTES = ATTRIBUTES||'; N2SHOW; N3SHOW; N4SHOW; N5SHOW' 
    WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND USER_SNA=v_LOGIN AND T2='PHONE_ORG' AND ATTRIBUTES LIKE '%SHOW%';

  COMMIT;

  v_SNA_DATE_END := SYSDATE ;
  v_SNA_TIME_LOAD := ROUND((v_SNA_DATE_END-v_SNA_DATE_BEGIN)*86400, 0);
  v_RESULT_MESSAGE := 'OK';
  
  --собираем доп. информацию по собранным данным
  SELECT (CASE WHEN COUNT(SRC_OBJECTS_ID)>0 THEN 1 ELSE 0 END)
      ,COUNT(DISTINCT N1)+COUNT(DISTINCT N2)+COUNT(DISTINCT N3)+COUNT(DISTINCT N4)+COUNT(DISTINCT N5), -1
      ,COUNT(DISTINCT OBJECTS_ID)
      ,COUNT(DISTINCT (CASE WHEN T3='NEG' AND N3 LIKE '%ФРОД%' THEN 1 ELSE NULL END))
      ,COUNT(DISTINCT (CASE WHEN T3='NEG' AND N3 LIKE '%БНК%' THEN 1 ELSE NULL END))
      ,COUNT(DISTINCT (CASE WHEN T3='NEG' AND N3 LIKE '%ДЕФОЛТ%' THEN 1 ELSE NULL END))
      ,COUNT(DISTINCT (CASE WHEN T3='NEG' THEN 1 ELSE NULL END))
    INTO v_FLAG_INFO_EXISTS
      , v_CNT_NODE, v_CNT_LINK
      , v_CNT_OBJECTS_ID
      , v_CNT_FROD, v_CNT_BNK, v_CNT_DEFOLT
      , v_CNT_ALERT_PERSON
    FROM SNAUSER.SNA_REQUEST_NODE_ROUTES
    WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN;

  --DELETE FROM SNAUSER.SNA_REQUEST_NODE_ROUTES_TEMP WHERE SRC_OBJECTS_ID=v_OBJECTS_ID AND NVL(USER_SNA, '-')=v_LOGIN;
  
  --INSERT INTO SNAUSER.SNA_REQUEST_NODE_ROUTES SELECT * FROM SNAUSER.SNA_REQUEST_NODE_ROUTES_TEMP;

  -- финализация данных в таблице логирования
   UPDATE SNAUSER.SNA_REQUEST_HISTORY SET 
          SNA_DATE_END = v_SNA_DATE_END
          ,SNA_TIME_LOAD = v_SNA_TIME_LOAD
          ,FLAG_INFO_EXISTS = v_FLAG_INFO_EXISTS
          ,OBJECTS_ID = v_OBJECTS_ID
          ,CNT_NODE = v_CNT_NODE
          ,CNT_LINK = v_CNT_LINK
          ,CNT_OBJECTS_ID = v_CNT_OBJECTS_ID 
          ,CNT_FROD = v_CNT_FROD 
          ,CNT_BNK = v_CNT_BNK 
          ,CNT_DEFOLT = v_CNT_DEFOLT 
          ,CNT_ALERT_PERSON = v_CNT_ALERT_PERSON 
    WHERE SNA_ID = v_SNA_ID; 
    
  COMMIT;

  EXCEPTION
    WHEN OTHERS
    THEN v_RESULT_MESSAGE := SQLERRM ;
        -- финализация данных в таблице логирования
         UPDATE SNAUSER.SNA_REQUEST_HISTORY SET 
          SNA_DATE_END = v_SNA_DATE_END
          ,SNA_TIME_LOAD = v_SNA_TIME_LOAD
          ,FLAG_INFO_EXISTS = v_FLAG_INFO_EXISTS
          ,OBJECTS_ID = v_OBJECTS_ID
          ,CNT_NODE = v_CNT_NODE
          ,CNT_LINK = v_CNT_LINK
          ,CNT_OBJECTS_ID = v_CNT_OBJECTS_ID 
          ,CNT_FROD = v_CNT_FROD 
          ,CNT_BNK = v_CNT_BNK 
          ,CNT_DEFOLT = v_CNT_DEFOLT 
          ,CNT_ALERT_PERSON = v_CNT_ALERT_PERSON 
    WHERE SNA_ID = v_SNA_ID; 
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_PHONES_RANK_BETWEEN" (v_st_ID IN NUMBER, v_en_ID IN NUMBER)

IS
  p_st_ID NUMBER := v_st_ID;
  p_en_ID NUMBER := v_en_ID;
  -- 2000-7000000: 2419104 штук за 18041 сек
  p_CNT_UPD NUMBER := 0;
  p_CNT NUMBER := 0; 
  p_START_TIME TIMESTAMP := SYSTIMESTAMP;
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;
  FOR cur IN (SELECT OBJECTS_ID FROM ODS.Q_PKK_PHONES_RNK_MIS 
                                WHERE OBJECTS_ID BETWEEN p_st_ID AND p_en_ID ORDER BY OBJECTS_ID) LOOP
    --UPDATE PKK_PHONES SET PHONES_RANK=NULL WHERE OBJECTS_ID = cur.OBJECTS_ID;          
    
    MERGE INTO ODS.PKK_PHONES tar
        USING (SELECT PHONES_ID
                ,DENSE_RANK() OVER(PARTITION BY 
                      OBJECTS_ID, OBJECTS_TYPE       --по каджому физику последние телефоны
                      --chld.PHONE          --по каждому телефону последние дата и актуальность
                      ORDER BY PHONES_AKT DESC, COALESCE(MODIFICATION_DATE, SYSDATE), PHONES_CREATED DESC
                        ,PHONES_ID DESC) PHONES_RANK
                FROM ODS.PKK_PHONES WHERE OBJECTS_ID=cur.OBJECTS_ID
              AND ((OBJECTS_TYPE IN(2, 200) AND 
                NOT (NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из НБКИ:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон клиента(сайт)%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Рекомендация(ПО КЦ):%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РБО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПТ/АТМ банка%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из документов РБО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('сот. тел. заявителя(сайт)%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('ТЕЛЕФОН ИЗ НБКИ%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РНКО:%') 
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПО КЦ%')) )
              OR (OBJECTS_TYPE=8 AND NOT NVL(PHONE, '-') LIKE '9%' )
              OR (NOT OBJECTS_TYPE IN(2, 8, 200)  ) 
              
              )
              ) src
          ON (tar.PHONES_ID=src.PHONES_ID )
        WHEN MATCHED THEN
          UPDATE SET tar.PHONES_RANK = src.PHONES_RANK 
          WHERE NOT NVL(tar.PHONES_RANK, -1) = NVL(src.PHONES_RANK, -2)
          ;
     -- IF SQL%ROWCOUNT>0 THEN 
        p_CNT := p_CNT+1;
      --END IF;
      IF MOD(p_CNT, 100)=0 THEN 
        /*DBMS_OUTPUT.PUT_LINE('Обновлено записей ранжирования телефонов: '||TO_CHAR(p_CNT_UPD)||'; Прогнано: '||TO_CHAR(p_CNT)
            ||'; ID='||TO_CHAR(cur.OBJECTS_ID)||' - '||TO_CHAR(SYSTIMESTAMP));*/
        COMMIT;
      END IF;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
      
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей ранжирования телефонов: '||TO_CHAR(p_CNT_UPD)||'; Прогнано: '||TO_CHAR(p_CNT));
  
  ODS.PR$INS_LOG ('PR$UPD_PHONES_RANK_BETWEEN', p_START_TIME, 'ODS.PKK_PHONES', 'OK', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование телефонов. '||TO_CHAR(v_st_ID)||'-'||TO_CHAR(v_en_ID)||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_PHONES_RANK_BETWEEN', p_START_TIME, 'ODS.PKK_PHONES', 'ERR', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование телефонов. '||TO_CHAR(v_st_ID)||'-'||TO_CHAR(v_en_ID)||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."FIND_R_PERSON" 
(
  nPersonId IN NUMBER
)
--RETURN type_t_reject_descr PIPELINED 
IS
nTmp             NUMBER;
cStrOu           VARCHAR2(4000);
cStrAdrOu        VARCHAR2(4000);

nFlFindFIO       NUMBER;
nFlFindBirthD    NUMBER;
nFlFindBirthM    NUMBER;
nFlFindBirthY    NUMBER;
nFlFindRegion    NUMBER;
nFlFindArea      NUMBER;
nFlFindCity      NUMBER;
nFlFindStreet    NUMBER;
nFlFindBuild     NUMBER;
nFlFindHouse     NUMBER;
nFlFindFlat      NUMBER;
nFlFindDType     NUMBER;
nFlFindRDoc      NUMBER;
nFlFindDoc       NUMBER;

nFlFindAdr       NUMBER;
nFlagAdrSovp     NUMBER;
nFlagAdrSovpGlob NUMBER;

nFlCount         NUMBER;
nPrzF            NUMBER;

-- Грищенко М.В. 2010.10.20 начало
  CURSOR curRDoc(pType NUMBER, pSerial VARCHAR2, pNumber VARCHAR2) IS
    SELECT *
    FROM KREDIT.view_r_documents@DBLINK_PKK
    WHERE decode(documents_type, 1001, 31, documents_type) = decode(pType, 1001, 31, pType)
    AND   documents_serial = pSerial
    AND   documents_number = pNumber;
  CURSOR curPDoc(pId NUMBER, pAkt NUMBER) IS
    SELECT *
    FROM KREDIT.view_documents_history@DBLINK_PKK
    WHERE objects_id = pId
    AND documents_akt = pAkt;
-- Грищенко М.В. 2010.10.20 завершение
BEGIN
    DBMS_OUTPUT.ENABLE;
    --DBMS_OUTPUT.PUT_LINE('Старт: '||TO_CHAR(systimestamp));
    
    DELETE FROM SNAUSER.SNA_REQUEST_BNK_HIST@SNA WHERE OBJECTS_ID=nPersonId;
                
    FOR curPerson IN (SELECT * FROM KREDIT.view_persons@DBLINK_PKK WHERE person_id=nPersonId) LOOP
        FOR curRPerson IN (SELECT * FROM KREDIT.view_r_person@DBLINK_PKK WHERE fio4search=curPerson.fio4search and deleted=0) LOOP
            nFlFindFIO       :=0;
            nFlFindBirthD    :=0;
            nFlFindBirthM    :=0;
            nFlFindBirthY    :=0;
            nFlFindRegion    :=0;
            nFlFindArea      :=0;
            nFlFindCity      :=0;
            nFlFindStreet    :=0;
            nFlFindBuild     :=0;
            nFlFindHouse     :=0;
            nFlFindFlat      :=0;
            nFlFindDType     :=0;
            nFlFindDoc       :=0;

            cStrAdrOu        :='';
            cStrOu           :='';

            nFlCount         :=0;
            nFlagAdrSovpGlob :=0;

             --========== начинаем разбирать физика по запчастям
             -- 1.  Совпадение ФИО
            nFlFindFIO    :=1;
            nFlCount := nFlCount +1;
             -- 2.  Совпадение дня рождения
            IF TO_NUMBER(SUBSTR (curPerson.bdate,9,2)) = curRPerson.birth_day THEN
                nFlFindBirthD :=1;
                nFlCount := nFlCount +1;
            END IF;
             -- 3.  Совпадение месяца рождения
            IF TO_NUMBER(SUBSTR (curPerson.bdate,6,2)) = curRPerson.birth_month THEN
                nFlFindBirthM :=1;
                nFlCount := nFlCount +1;
            END IF;
             -- 4.  Совпадение года рождения
            IF TO_NUMBER(SUBSTR (curPerson.bdate,1,4)) = curRPerson.birth_year THEN
                nFlFindBirthY :=1;
                nFlCount := nFlCount +1;
            END IF;
             -- 5.  Совпадение адреса регистрации по истории адресов запрашиваемого физика
FOR curAPerson IN
             (
              SELECT OBJECTS_TYPE, ADDRESS_AKT
                , SUBSTR(REGIONS_NAMES, 1 , 100) AS REGIONS_NAMES
                , SUBSTR(AREAS_NAMES, 1 , 100) AS AREAS_NAMES
                , SUBSTR(CITIES_NAMES, 1 , 100) AS CITIES_NAMES
                , SUBSTR(STREETS_NAMES, 1 , 100) AS STREETS_NAMES
                , build, house, NVL(flat, '-') as flat
              FROM KREDIT.view_address_history@DBLINK_PKK
              WHERE objects_id=curPerson.person_id
              AND (OBJECTS_TYPE =2 OR OBJECTS_TYPE=5)
             )
            LOOP
                 -- 5.1 Регион
                IF curAPerson.REGIONS_NAMES !='-' AND curAPerson.REGIONS_NAMES = curRPerson.REGIONS_NAMES THEN
                    nFlFindRegion :=1;
                    nFlCount := nFlCount +1;
                END IF;
                 -- 5.1 Район
                IF curAPerson.AREAS_NAMES !='-' AND curAPerson.AREAS_NAMES = curRPerson.AREAS_NAMES THEN
                    nFlFindArea :=1;
                    nFlCount := nFlCount +1;
                END IF;
                 -- 5.1 Город
                IF curAPerson.CITIES_NAMES !='-' and curAPerson.CITIES_NAMES = curRPerson.CITIES_NAMES THEN
                    nFlFindCity :=1;
                    nFlCount := nFlCount +1;
                END IF;
                 -- 5.1 Улица
                IF curAPerson.STREETS_NAMES !='-' AND curAPerson.STREETS_NAMES = curRPerson.STREETS_NAMES THEN
                    nFlFindStreet :=1;
                    nFlCount := nFlCount +1;
                END IF;
                 -- 5.1 Строение
                IF curAPerson.build = curRPerson.build THEN
                    nFlFindBuild :=1;
                    nFlCount := nFlCount +1;
                END IF;
                 -- 5.1 Дом
                IF curAPerson.house = curRPerson.house THEN
                  nFlFindHouse :=1;
                    nFlCount := nFlCount +1;
                END IF;
                 -- 5.1 Кврартира
                IF curAPerson.flat = curRPerson.flat THEN
                    nFlFindFlat :=1;
                    nFlCount := nFlCount +1;
                END IF;
                 --===== анализируем совпадение адреса:
                 -- -- сам адрес
                nFlagAdrSovp :=0;
                IF nFlagAdrSovp =0 AND nFlFindRegion =1 AND nFlFindCity =1 AND nFlFindArea =1 AND nFlFindStreet =1 AND nFlFindHouse =1 AND nFlFindBuild =1 AND nFlFindFlat =1 THEN
                    --cStrAdrOu := cStrAdrOu || 'полное совпадение адреса ';
                    nFlagAdrSovp :=5;
                END IF;
                IF nFlagAdrSovp =0 AND nFlFindRegion =1 AND nFlFindCity =1 AND nFlFindStreet =1 AND nFlFindHouse =1 AND nFlFindFlat =1 THEN
                    --cStrAdrOu := cStrAdrOu || 'адрес - регион, город, улица, дом, квартира ';
                    nFlagAdrSovp :=4;
                END IF;
                IF nFlagAdrSovp =0 AND nFlFindCity=1 AND nFlFindStreet =1 AND nFlFindHouse =1 AND nFlFindFlat =1 THEN
                    --cStrAdrOu := cStrAdrOu || 'адрес - город, улица, дом, квартира ';
                    nFlagAdrSovp :=3;
                END IF;
                IF nFlagAdrSovp =0 AND nFlFindRegion =1 AND nFlFindCity=1 AND nFlFindStreet =1 AND nFlFindHouse =1 THEN
                    --cStrAdrOu := cStrAdrOu || 'адрес - регион, город, улица, дом ';
                    nFlagAdrSovp :=2;
                END IF;
                IF nFlagAdrSovp =0 AND nFlFindCity=1 AND nFlFindStreet =1 AND nFlFindHouse =1 THEN
                    --cStrAdrOu := cStrAdrOu || 'совпадение адреса: город, улица, дом ';
                    nFlagAdrSovp :=1;
                END IF;
                IF nFlagAdrSovp =0 AND nFlFindRegion=1 AND nFlFindStreet =1 AND nFlFindHouse =1 THEN
                    --cStrAdrOu := cStrAdrOu || 'совпадение адреса: регион, улица, дом ';
                    nFlagAdrSovp :=1;
                END IF;
                IF nFlagAdrSovp =0 AND nFlFindRegion=1 THEN
                    --cStrAdrOu := cStrAdrOu || 'совпадение адреса: регион ';
                    nFlagAdrSovp :=0.99;
                END IF;
                 -- -- тип адреса
                IF curAPerson.address_akt =0 AND nFlagAdrSovp >0 THEN
                    --cStrAdrOu := cStrAdrOu || 'из истории ';
                    NULL;
                END IF;
                IF curAPerson.OBJECTS_TYPE =2 AND nFlagAdrSovp >0 THEN
                    ----cStrAdrOu := cStrAdrOu || 'проживания';
                    NULL;
                END IF;
                IF curAPerson.OBJECTS_TYPE =5 AND nFlagAdrSovp >0 THEN
                    --cStrAdrOu := cStrAdrOu || 'регистрации';
                    NULL;
                END IF;
                IF nFlagAdrSovp >0 THEN
                    --cStrAdrOu := cStrAdrOu || '; ';
                    NULL;
                END IF;

                IF nFlagAdrSovp >0 THEN
                    IF nFlagAdrSovp > nFlagAdrSovpGlob THEN
                        nFlagAdrSovpGlob := nFlagAdrSovp;  --устанавливаем глобальный флаг
                    END IF;
                    nFlagAdrSovp :=0;                      --сброс локального флага
                END IF;

                 -- сброс переменных для след.проверки
                nFlFindRegion :=0;
                nFlFindArea :=0;
                nFlFindCity :=0;
                nFlFindStreet :=0;
                nFlFindBuild :=0;
                nFLFindHouse  :=0;
                nFlFindFlat :=0;
                NULL;
            END LOOP;

             -- ========== проверяем документ
            FOR curPDoc IN (SELECT * FROM KREDIT.view_documents_history@DBLINK_PKK WHERE objects_id =nPersonId) LOOP
                IF curRPerson.DOCUMENTS_TYPE   = curPDoc.DOCUMENTS_TYPE   AND
                   curRPerson.documents_serial = curPDoc.documents_serial AND
                   curRPerson.documents_number = curPDoc.documents_number THEN
                    nFlFindDoc :=1;
                END IF;
            END LOOP;
             --========== подводим итог
            IF nFlFindFIO =1 THEN
                cStrOu := cStrOu || 'ФИО, ';
            END IF;
            IF nFlFindBirthD =1 AND nFlFindBirthM =1 AND nFlFindBirthY =1 THEN
                cStrOu := cStrOu || 'дата рождения, ';
            ELSE
                IF nFlFindBirthD =1 THEN
                    cStrOu := cStrOu || 'день даты р., ';
                END IF;
                IF nFlFindBirthM =1 THEN
                    cStrOu := cStrOu || 'месяц даты р., ';
                END IF;
                IF nFlFindBirthY =1 THEN
                    cStrOu := cStrOu || 'год даты р., ';
                END IF;
            END IF;

            IF nFlFindRDoc =1 THEN
                cStrOu := cStrOu || 'актуальный документ - в утерянных, ';
            END IF;

            cStrOu := cStrOu || cStrAdrOu;

              -- ========== расчет вероятности совпадения
            nPrzF :=0; -- опускаем признак совпадения

            IF nPrzF =0 AND nFlFindRDoc =1 THEN -- утерянный основной документ
                nPrzF :=100;
            END IF;
            IF nPrzF =0 AND nFlFindDoc =1 THEN -- совпадение по истории документов
                nPrzF :=100;
            END IF;
            IF nPrzF =0 AND nFlFindBirthD =1 AND nFlFindBirthM =1 AND nFlFindBirthY =1 AND nFlagAdrSovpGlob >=3 THEN
                nPrzF :=100;
            END IF;
            IF nPrzF =0 AND nFlFindBirthY =1 AND nFlFindBirthM =1 AND nFlagAdrSovpGlob >=3 THEN
                nPrzF :=98;
            END IF;
            IF nPrzF =0 AND nFlFindBirthY =1 AND nFlagAdrSovpGlob >=3 THEN
                nPrzF :=95;
            END IF;
            IF nPrzF =0 AND nFlFindBirthY =1 AND nFlagAdrSovpGlob >=1 THEN
                nPrzF :=75;
            END IF;
            IF nPrzF =0 AND nFlagAdrSovpGlob >=3 THEN
                nPrzF :=60;
            END IF;
            IF nPrzF =0 AND nFlagAdrSovpGlob >=1 AND nFlagAdrSovpGlob <3 THEN
                nPrzF :=50;
            END IF;
            IF nPrzF =0 AND nFlFindBirthY =1 AND nFlFindBirthM =1 AND nFlFindBirthD =1 THEN
                nPrzF :=50;
            END IF;
            IF nPrzF =0 AND nFlFindBirthY =1 AND nFlFindBirthM =1 and nFlagAdrSovpGlob =0.99 THEN
                nPrzF :=30;
            END IF;
            IF nPrzF =0 AND nFlFindBirthY =1 AND nFlFindBirthM =1 THEN
                nPrzF :=25;
            END IF;
            IF (nPrzF =0 AND nFlFindBirthY =1 and nFlagAdrSovpGlob =0.99) THEN
                nPrzF :=15;
            END IF;
            IF nPrzF =0 AND nFlFindBirthY =1 THEN
                nPrzF :=10;
            END IF;
            IF nPrzF =0 AND nFlagAdrSovpGlob =0.99 THEN -- совпадает только регион
                nPrzF :=5;
            END IF;
            
            IF nPrzF>0 THEN
              cStrOu := SUBSTR(cStrOu, 1, 2000);
              INSERT INTO SNAUSER.SNA_REQUEST_BNK_HIST@SNA (OBJECTS_ID, R_TYPE_ID, DATE_INS, DESCRIPT
                    , PERCENT_MATCH, STR_DESC, DATE_MATCH)
                VALUES
                (nPersonId, curRPerson.r_type_id, curRPerson.DATE_INS, curRPerson.DESCRIPT, nPrzF, cStrOu, SYSTIMESTAMP);
              --DBMS_OUTPUT.PUT_LINE(TO_CHAR(nPersonId)||' '||TO_CHAR(curRPerson.r_type_id)||' '||TO_CHAR(nPrzF)||': '||cStrOu);
            END IF;
            --pipe ROW(type_o_reject_descr(curRPerson.r_person_id, 'person', nPrzF, cStrOu));
        END LOOP;
    END LOOP;
         -- пробиваем по утерянным документам самого физика
        FOR recPDoc IN curPDoc(nPersonId, 1) LOOP
            FOR recRDoc IN curRDoc(recPDoc.documents_type, recPDoc.documents_serial, recPDoc.documents_number) LOOP
              --IF nPrzF>0 THEN
                cStrOu := SUBSTR(cStrOu, 1, 2000);
                INSERT INTO SNAUSER.SNA_REQUEST_BNK_HIST@SNA (OBJECTS_ID, R_TYPE_ID, DATE_INS, DESCRIPT
                  , PERCENT_MATCH, STR_DESC, DATE_MATCH)
                VALUES
                (nPersonId, 80, recRDoc.DATE_INS, 'основной документ - в утерянных', 100, cStrOu, SYSTIMESTAMP);
                
                --DBMS_OUTPUT.PUT_LINE(TO_CHAR(nPersonId)||' '||TO_CHAR(recRDoc.documents_id)||' '
                 --   ||TO_CHAR(100)||': '||'основной документ - в утерянных');
                --pipe ROW(type_o_reject_descr(recRDoc.documents_id, 'documents', 100, 'основной документ - в утерянных'));
              --END IF;
            END LOOP;
        END LOOP;
   /*      -- ========== пробиваем по "заряженным" телефонам
         -- ----- контактные телефоны заявителя
        FOR curPCont IN 
         (
          select rp.phone_full,rp.r_type,rp.descr,p.phones_akt from CPD.phones@DBLINK_PKK p, KREDIT.r_phones@DBLINK_PKK rp 
          where objects_id = nPersonId and objects_type =2 
          and p.phones_code || p.phones = rp.phone_full and rp.deleted =0
         )
        LOOP
            cStrOu :='';
            if curPCont.phones_akt =1 then  
                cStrOu := cStrOu || 'контактный телефон, актуальный (' || curPCont.phone_full || '):' || curPCont.descr;
            end if;
            if (curPCont.phones_akt =0) then  
                cStrOu := cStrOu || 'контактный телефон, не актуальный ('|| curPCont.phone_full || '):' || curPCont.descr;
            end if;
            
            cStrOu := SUBSTR(cStrOu, 1, 2000);
            INSERT INTO SNAUSER.SNA_REQUEST_BNK_HIST@SNA (OBJECTS_ID, R_TYPE_ID, PERCENT_MATCH, STR_DESC, DATE_MATCH)
                VALUES
                (nPersonId, -1, 100, cStrOu, SYSTIMESTAMP);
            --DBMS_OUTPUT.PUT_LINE(TO_CHAR(nPersonId)||' '||TO_CHAR(-1)||' '||TO_CHAR(100)||': '||cStrOu);
            --pipe ROW(type_o_reject_descr(-1, 'phone', 100, cStrOu));
        END LOOP;  
         -- ----- телефон адреса проживания, адреса берем только актуальные
        FOR curPCont IN 
         (
          select ah.address_id,rp.phone_full,rp.r_type,rp.descr,p.phones_akt 
          from CPD.address_history@DBLINK_PKK ah, CPD.phones@DBLINK_PKK p, KREDIT.r_phones@DBLINK_PKK rp where 
          ah.objects_id=nPersonId and ah.objects_type = 2 and ah.address_akt =1
          and ah.address_id = p.objects_id and p.objects_type =8 and p.phones_akt =1
          and p.phones_code || p.phones = rp.phone_full and rp.deleted =0
         )
        LOOP
            cStrOu :='';
            cStrOu := cStrOu || 'телефон адреса проживания, актуальный (' || curPCont.phone_full || '):' || curPCont.descr;
            cStrOu := SUBSTR(cStrOu, 1, 2000);
            INSERT INTO SNAUSER.SNA_REQUEST_BNK_HIST@SNA (OBJECTS_ID, R_TYPE_ID, PERCENT_MATCH, STR_DESC, DATE_MATCH)
                VALUES
                (nPersonId, -1, 100, cStrOu, SYSTIMESTAMP);
                
            --DBMS_OUTPUT.PUT_LINE(TO_CHAR(nPersonId)||' '||TO_CHAR(-1)||' '||TO_CHAR(100)||': '||cStrOu);
            --pipe ROW(type_o_reject_descr(-1, 'phone', 100, cStrOu));
        END LOOP;  
         -- ----- телефон адреса регистрации, адреса берем только актуальные
        FOR curPCont IN 
         (
          select dh.documents_id, ah.address_id,rp.phone_full,rp.r_type,rp.descr,p.phones_akt 
          from CPD.documents_history@DBLINK_PKK dh, CPD.address_history@DBLINK_PKK ah, CPD.phones@DBLINK_PKK p, KREDIT.r_phones@DBLINK_PKK rp 
          where dh.objects_id=nPersonId and dh.documents_akt =1 
          and dh.documents_id = ah.objects_id and ah.objects_type =5 and ah.address_akt =1
          and ah.address_id = p.objects_id and p.objects_type =8 and p.phones_akt =1
          and p.phones_code || p.phones = rp.phone_full and rp.deleted =0
         )
        LOOP
            cStrOu :='';
            cStrOu := cStrOu || 'телефон адреса регистрации, актуальный (' || curPCont.phone_full || '):' || curPCont.descr;
            cStrOu := SUBSTR(cStrOu, 1, 2000);
            INSERT INTO SNAUSER.SNA_REQUEST_BNK_HIST@SNA (OBJECTS_ID, R_TYPE_ID, PERCENT_MATCH, STR_DESC, DATE_MATCH)
                VALUES
                (nPersonId, -1, 100, cStrOu, SYSTIMESTAMP);
            --DBMS_OUTPUT.PUT_LINE(TO_CHAR(nPersonId)||' '||TO_CHAR(-1)||' '||TO_CHAR(100)||': '||cStrOu);
            --pipe ROW(type_o_reject_descr(-1, 'phone', 100, cStrOu));
        END LOOP;  
         -- ----- рабочий телефон, только актуальный
        FOR curPCont IN 
         (
          select wh.works_id,rp.phone_full,rp.r_type,rp.descr,p.phones_akt
          from CPD.works_history@DBLINK_PKK wh, CPD.phones@DBLINK_PKK p, KREDIT.r_phones@DBLINK_PKK rp
          where wh.objects_id=nPersonId and wh.objects_type=2 and wh.works_akt=1 and wh.works_last =1
          and wh.works_id = p.objects_id and p.objects_type =12 and p.phones_akt =1
          and p.phones_code || p.phones = rp.phone_full and rp.deleted =0
         )
        LOOP
            cStrOu :='';
            cStrOu := cStrOu || 'рабочий телефон, актуальный (' || curPCont.phone_full || '):' || curPCont.descr;
            
            cStrOu := SUBSTR(cStrOu, 1, 2000);
            INSERT INTO SNAUSER.SNA_REQUEST_BNK_HIST@SNA (OBJECTS_ID, R_TYPE_ID, PERCENT_MATCH, STR_DESC, DATE_MATCH)
                VALUES
                (nPersonId, -1, 100, cStrOu, SYSTIMESTAMP);
            --DBMS_OUTPUT.PUT_LINE(TO_CHAR(nPersonId)||' '||TO_CHAR(-1)||' '||TO_CHAR(100)||': '||cStrOu);
            -- pipe ROW(type_o_reject_descr(-1, 'phone', 100, cStrOu));
        END LOOP;  
         -- ----- телефон организации работодателя, все доступные
        FOR curPCont IN 
         (
          select wh.works_id, w.org_id, rp.phone_full,rp.r_type,rp.descr,p.phones_akt, wh.works_last
          from CPD.works_history@DBLINK_PKK wh, CPD.works@DBLINK_PKK w, CPD.phones@DBLINK_PKK p, KREDIT.r_phones@DBLINK_PKK rp
          where wh.objects_id=nPersonId and wh.objects_type=2 --and wh.works_akt=1 and wh.works_last =1
          and wh.works_id = w.works_id and w.org_id = p.objects_id and p.objects_type =3 and p.phones_akt =1
          and p.phones_code || p.phones = rp.phone_full and rp.deleted =0
         )
        LOOP
            cStrOu :='';
            if (curPCont.Works_Last =1) then
                cStrOu := cStrOu || 'телефон организации-работодателя, последний актуальный (' || curPCont.phone_full || '):' || curPCont.descr;
            end if;
            if (curPCont.Works_Last =0) then
                cStrOu := cStrOu || 'телефон организации-работодателя, из истории работы (' || curPCont.phone_full || '):' || curPCont.descr;
            end if;
            --pipe ROW(type_o_reject_descr(-1, 'phone', 100, cStrOu));
        END LOOP;*/
    --RETURN;
    --DBMS_OUTPUT.PUT_LINE('End: '||TO_CHAR(systimestamp));
    
  EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE('Ошибка: OBJECTS_ID='||TO_CHAR(nPersonId)||' - '||SQLERRM);
  /*EXCEPTION
    WHEN OTHERS
    THEN v_RESULT_MESSAGE := SQLERRM ;*/
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_PKK_PERSON_COLLAPSED" 
/*
PKK_PERSON_COLLAPSED: 22.04.16 05:44:41-22.04.16 05:44:41(0). От 22.04.16 04:16:37 до 22.04.16 04:16:59
*/
IS
  p_STAMP_LAST DATE;
  p_cnt_MODIF NUMBER;
  
  p_STAMP_LAST_AFTER TIMESTAMP;
  p_START_TIME TIMESTAMP;
  
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
BEGIN 
  DBMS_OUTPUT.ENABLE; 
  p_START_TIME := systimestamp;
  
  SELECT MAX(STAMP) INTO p_STAMP_LAST FROM ODS.PKK_PERSON_COLLAPSED;
  -- 1-2 минуты
  MERGE INTO ODS.PKK_PERSON_COLLAPSED tar
  USING (SELECT TARGET_PERSON_ID, CLONE_PERSON_ID, STAMP
          FROM (SELECT DISTINCT TARGET_PERSON_ID
            ,CLONE_PERSON_ID
            ,STAMP
            ,DENSE_RANK() OVER (PARTITION BY TARGET_PERSON_ID, CLONE_PERSON_ID ORDER BY STAMP desc, rownum) as rank
          FROM CPD.PERSON_COLLAPSED@DBLINK_PKK WHERE STAMP >= p_STAMP_LAST ) WHERE rank=1 ) src
    ON (src.TARGET_PERSON_ID=tar.TARGET_PERSON_ID AND src.CLONE_PERSON_ID=tar.CLONE_PERSON_ID)
  --WHEN MATCHED THEN UPDATE SET tar.OBJECTS_TYPE=src.OBJECTS_TYPE, tar.CREATED=src.CREATED, tar.DELETED=src.DELETED
  WHEN NOT MATCHED THEN 
    INSERT (tar.TARGET_PERSON_ID, tar.CLONE_PERSON_ID, tar.STAMP) 
    VALUES (src.TARGET_PERSON_ID, src.CLONE_PERSON_ID, src.STAMP)   ;
    
  p_cnt_MODIF := SQL%ROWCOUNT;
  SELECT MAX(STAMP) INTO p_STAMP_LAST_AFTER FROM ODS.PKK_PERSON_COLLAPSED;
  p_ADD_INFO := 'Период обновления: '||TO_CHAR(p_STAMP_LAST)||' до '||TO_CHAR(p_STAMP_LAST_AFTER);
  ODS.PR$INS_LOG ('PR$UPD_PKK_PERSON_COLLAPSED', p_START_TIME, 'ODS.PKK_PERSON_COLLAPSED', 'OK', SQLERRM, p_cnt_MODIF, -1, p_ADD_INFO);
  COMMIT;
  
  EXCEPTION
      WHEN OTHERS
      THEN ODS.PR$INS_LOG ('PR$UPD_PKK_PERSON_COLLAPSED', p_START_TIME, 'ODS.PKK_PERSON_COLLAPSED', 'ERR', SQLERRM, p_cnt_MODIF, -1, p_ADD_INFO);
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_RANK_BETWEEN_PERSON" (v_st_ID IN NUMBER, v_en_ID IN NUMBER)

IS
  p_st_ID NUMBER := v_st_ID;
  p_en_ID NUMBER := v_en_ID;
  -- 2000-7000000: 2419104 штук за 18041 сек
  p_CNT_UPD NUMBER := 0;
  p_CNT NUMBER := 0;
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;
 /* FOR cur IN (SELECT OBJECTS_ID FROM ODS.QUERY_FOR_PKK_PERSON_INFO
                                WHERE OBJECTS_ID BETWEEN p_st_ID AND p_en_ID ORDER BY OBJECTS_ID) LOOP
    MERGE INTO ODS.PKK_PERSON_INFO tar
        USING (SELECT FIO_HISTORY_PK
                ,DENSE_RANK() OVER(PARTITION BY 
                      OBJECTS_ID       --по каджому физику последние телефоны
                      --chld.PHONE          --по каждому телефону последние дата и актуальность
                      ORDER BY FIO_AKT DESC, COALESCE(MODIFICATION_DATE, FIO_CREATED) DESC) FIO_RANK
                FROM ODS.PKK_PERSON_INFO WHERE OBJECTS_ID=cur.OBJECTS_ID) src
          ON (tar.FIO_HISTORY_PK=src.FIO_HISTORY_PK )
        WHEN MATCHED THEN
          UPDATE SET tar.FIO_RANK = src.FIO_RANK 
          WHERE NOT NVL(tar.FIO_RANK, -1) = NVL(src.FIO_RANK, -1)
          ; 
     -- IF SQL%ROWCOUNT>0 THEN 
        p_CNT := p_CNT+1;
      --END IF;
      IF MOD(p_CNT, 1000)=0 THEN  
        COMMIT;
      END IF;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
  END LOOP;*/
  DBMS_OUTPUT.PUT_LINE('Обновлено записей ранжирования телефонов: '||TO_CHAR(p_CNT_UPD)||'; Прогнано: '||TO_CHAR(p_CNT));
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_OBJECTS_ID_IN_PKK" 

IS 
  p_STAMP_LAST DATE;
  CURSOR cur_PERSON_DOUBLE(p_cur_OBJECTS_ID NUMBER) IS
    SELECT DISTINCT CLONE_PERSON_ID, TARGET_PERSON_ID, STAMP
      FROM ODS.PKK_PERSON_COLLAPSED
      --WHERE STAMP<SYSDATE
      ;
  p_CNT_UPD NUMBER := 0;
  p_CNT_REC NUMBER := 0;
  p_EXIST_OB NUMBER := 0;
  p_START_DATE DATE := TO_DATE('10-04-2016', 'dd-mm-yyyy');
BEGIN 
  DBMS_OUTPUT.ENABLE;
  --p_START_DATE := SYSDATE-90;

  FOR p_cur_DBL IN(SELECT DISTINCT CLONE_PERSON_ID, TARGET_PERSON_ID, STAMP FROM ODS.PKK_PERSON_COLLAPSED
        WHERE STAMP>p_START_DATE) LOOP
    --01 ОБНОВЛЯЕМ ODS.PKK_PERSON_INFO
    SELECT COUNT(*) INTO p_EXIST_OB FROM DUAL 
      WHERE EXISTS(SELECT * FROM ODS.PKK_PERSON_INFO WHERE OBJECTS_ID = p_cur_DBL.CLONE_PERSON_ID);
      
    IF p_EXIST_OB=1 THEN 
      --UPDATE ODS.PKK_PERSON_INFO SET OBJECTS_ID = p_cur_DBL.TARGET_PERSON_ID WHERE OBJECTS_ID=p_cur_DBL.CLONE_PERSON_ID;
      p_CNT_REC := p_CNT_REC + 1;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
    END IF;
    
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей в ODS.PKK_PERSON_INFO: '||TO_CHAR(p_CNT_UPD)||'; Обнаружено: '||TO_CHAR(p_CNT_REC));

  p_CNT_REC := 0;
  p_CNT_UPD := 0;
  FOR p_cur_DBL IN(SELECT DISTINCT CLONE_PERSON_ID, TARGET_PERSON_ID, STAMP FROM ODS.PKK_PERSON_COLLAPSED
        WHERE STAMP>p_START_DATE) LOOP
    --02 ОБНОВЛЯЕМ ODS.PKK_PHONES
    SELECT COUNT(*) INTO p_EXIST_OB FROM DUAL 
      WHERE EXISTS(SELECT * FROM ODS.PKK_PHONES WHERE OBJECTS_ID = p_cur_DBL.CLONE_PERSON_ID);
      
    IF p_EXIST_OB=1 THEN 
      --UPDATE ODS.PKK_PHONES SET OBJECTS_ID = p_cur_DBL.TARGET_PERSON_ID WHERE OBJECTS_ID=p_cur_DBL.CLONE_PERSON_ID;
      p_CNT_REC := p_CNT_REC + 1;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
    END IF;
    
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей в ODS.PKK_PHONES: '||TO_CHAR(p_CNT_UPD)||'; Обнаружено: '||TO_CHAR(p_CNT_REC));
  
  p_CNT_REC := 0;
  p_CNT_UPD := 0;
  FOR p_cur_DBL IN(SELECT DISTINCT CLONE_PERSON_ID, TARGET_PERSON_ID, STAMP FROM ODS.PKK_PERSON_COLLAPSED
        WHERE STAMP>p_START_DATE) LOOP
    --03 ОБНОВЛЯЕМ ODS.PKK_ADDRESS_HISTORY
    SELECT COUNT(*) INTO p_EXIST_OB FROM DUAL 
      WHERE EXISTS(SELECT * FROM ODS.PKK_ADDRESS_HISTORY WHERE OBJECTS_ID = p_cur_DBL.CLONE_PERSON_ID);
      
    IF p_EXIST_OB=1 THEN 
      --UPDATE ODS.PKK_ADDRESS_HISTORY SET OBJECTS_ID = p_cur_DBL.TARGET_PERSON_ID WHERE OBJECTS_ID=p_cur_DBL.CLONE_PERSON_ID;
      p_CNT_REC := p_CNT_REC + 1;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
    END IF;
    
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей в ODS.PKK_ADDRESS_HISTORY: '||TO_CHAR(p_CNT_UPD)||'; Обнаружено: '||TO_CHAR(p_CNT_REC));
  
  p_CNT_REC := 0;
  p_CNT_UPD := 0;
  FOR p_cur_DBL IN(SELECT DISTINCT CLONE_PERSON_ID, TARGET_PERSON_ID, STAMP FROM ODS.PKK_PERSON_COLLAPSED
        WHERE STAMP>p_START_DATE) LOOP
    --04 ОБНОВЛЯЕМ ODS.PKK_C_CREDIT_INFO_SNA
    SELECT COUNT(*) INTO p_EXIST_OB FROM DUAL 
      WHERE EXISTS(SELECT * FROM ODS.PKK_C_CREDIT_INFO_SNA WHERE PERSON_ID = p_cur_DBL.CLONE_PERSON_ID);
      
    IF p_EXIST_OB=1 THEN 
      --UPDATE ODS.PKK_C_CREDIT_INFO_SNA SET PERSON_ID = p_cur_DBL.TARGET_PERSON_ID WHERE PERSON_ID=p_cur_DBL.CLONE_PERSON_ID;
      p_CNT_REC := p_CNT_REC + 1;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
    END IF;
    
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей в ODS.PKK_C_CREDIT_INFO_SNA: '||TO_CHAR(p_CNT_UPD)||'; Обнаружено: '||TO_CHAR(p_CNT_REC));
  
  p_CNT_REC := 0;
  p_CNT_UPD := 0;
  FOR p_cur_DBL IN(SELECT DISTINCT CLONE_PERSON_ID, TARGET_PERSON_ID, STAMP FROM ODS.PKK_PERSON_COLLAPSED
        WHERE STAMP>p_START_DATE) LOOP
    --05 ОБНОВЛЯЕМ ODS.PKK_C_REQUEST_SNA
    SELECT COUNT(*) INTO p_EXIST_OB FROM DUAL 
      WHERE EXISTS(SELECT * FROM ODS.PKK_C_REQUEST_SNA WHERE OBJECTS_ID = p_cur_DBL.CLONE_PERSON_ID);
      
    IF p_EXIST_OB=1 THEN 
      --UPDATE ODS.PKK_C_REQUEST_SNA SET OBJECTS_ID = p_cur_DBL.TARGET_PERSON_ID WHERE OBJECTS_ID=p_cur_DBL.CLONE_PERSON_ID;
      p_CNT_REC := p_CNT_REC + 1;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
    END IF;
    
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей в ODS.PKK_C_REQUEST_SNA: '||TO_CHAR(p_CNT_UPD)||'; Обнаружено: '||TO_CHAR(p_CNT_REC));
  
  p_CNT_REC := 0;
  p_CNT_UPD := 0;
  FOR p_cur_DBL IN(SELECT DISTINCT CLONE_PERSON_ID, TARGET_PERSON_ID, STAMP FROM ODS.PKK_PERSON_COLLAPSED
        WHERE STAMP>p_START_DATE) LOOP
    --06 ОБНОВЛЯЕМ ODS.PKK_CONTACTS
    SELECT COUNT(*) INTO p_EXIST_OB FROM DUAL 
      WHERE EXISTS(SELECT * FROM ODS.PKK_CONTACTS WHERE OBJECTS_ID = p_cur_DBL.CLONE_PERSON_ID);
      
    IF p_EXIST_OB=1 THEN 
      --UPDATE ODS.PKK_CONTACTS SET PERSON_ID = p_cur_DBL.TARGET_PERSON_ID WHERE OBJECTS_ID=p_cur_DBL.CLONE_PERSON_ID;
      p_CNT_REC := p_CNT_REC + 1;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
    END IF;
    
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей в ODS.PKK_CONTACTS: '||TO_CHAR(p_CNT_UPD)||'; Обнаружено: '||TO_CHAR(p_CNT_REC));
  
  p_CNT_REC := 0;
  p_CNT_UPD := 0;
  FOR p_cur_DBL IN(SELECT DISTINCT CLONE_PERSON_ID, TARGET_PERSON_ID, STAMP FROM ODS.PKK_PERSON_COLLAPSED
        WHERE STAMP>p_START_DATE) LOOP
    --07 ОБНОВЛЯЕМ ODS.PKK_DOCUMENTS_HISTORY_INFO
    SELECT COUNT(*) INTO p_EXIST_OB FROM DUAL 
      WHERE EXISTS(SELECT * FROM ODS.PKK_DOCUMENTS_HISTORY_INFO WHERE OBJECTS_ID = p_cur_DBL.CLONE_PERSON_ID);
      
    IF p_EXIST_OB=1 THEN 
      --UPDATE ODS.PKK_DOCUMENTS_HISTORY_INFO SET OBJECTS_ID = p_cur_DBL.TARGET_PERSON_ID WHERE OBJECTS_ID=p_cur_DBL.CLONE_PERSON_ID;
      p_CNT_REC := p_CNT_REC + 1;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
    END IF;
    
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей в ODS.PKK_DOCUMENTS_HISTORY_INFO: '||TO_CHAR(p_CNT_UPD)||'; Обнаружено: '||TO_CHAR(p_CNT_REC));
  
  p_CNT_REC := 0;
  p_CNT_UPD := 0;
  FOR p_cur_DBL IN(SELECT DISTINCT CLONE_PERSON_ID, TARGET_PERSON_ID, STAMP FROM ODS.PKK_PERSON_COLLAPSED
        WHERE STAMP>p_START_DATE) LOOP
    --08 ОБНОВЛЯЕМ ODS.PKK_EMAIL
    SELECT COUNT(*) INTO p_EXIST_OB FROM DUAL 
      WHERE EXISTS(SELECT * FROM ODS.PKK_EMAIL WHERE OBJECTS_ID = p_cur_DBL.CLONE_PERSON_ID);
      
    IF p_EXIST_OB=1 THEN 
      --UPDATE ODS.PKK_EMAIL SET OBJECTS_ID = p_cur_DBL.TARGET_PERSON_ID WHERE OBJECTS_ID=p_cur_DBL.CLONE_PERSON_ID;
      p_CNT_REC := p_CNT_REC + 1;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
    END IF;
    
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей в ODS.PKK_EMAIL: '||TO_CHAR(p_CNT_UPD)||'; Обнаружено: '||TO_CHAR(p_CNT_REC));
  
  p_CNT_REC := 0;
  p_CNT_UPD := 0;
  FOR p_cur_DBL IN(SELECT DISTINCT CLONE_PERSON_ID, TARGET_PERSON_ID, STAMP FROM ODS.PKK_PERSON_COLLAPSED
        WHERE STAMP>p_START_DATE) LOOP
    --09 ОБНОВЛЯЕМ ODS.PKK_FAMILY
    SELECT COUNT(*) INTO p_EXIST_OB FROM DUAL 
      WHERE EXISTS(SELECT * FROM ODS.PKK_FAMILY WHERE OBJECTS_ID = p_cur_DBL.CLONE_PERSON_ID);
      
    IF p_EXIST_OB=1 THEN 
      --UPDATE ODS.PKK_FAMILY SET OBJECTS_ID = p_cur_DBL.TARGET_PERSON_ID WHERE OBJECTS_ID=p_cur_DBL.CLONE_PERSON_ID;
      p_CNT_REC := p_CNT_REC + 1;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
    END IF;
    
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей в ODS.PKK_FAMILY: '||TO_CHAR(p_CNT_UPD)||'; Обнаружено: '||TO_CHAR(p_CNT_REC));
  
  p_CNT_REC := 0;
  p_CNT_UPD := 0;
  FOR p_cur_DBL IN(SELECT DISTINCT CLONE_PERSON_ID, TARGET_PERSON_ID, STAMP FROM ODS.PKK_PERSON_COLLAPSED
        WHERE STAMP>p_START_DATE) LOOP
    --09 ОБНОВЛЯЕМ ODS.PKK_WORKS_INFO
    SELECT COUNT(*) INTO p_EXIST_OB FROM DUAL 
      WHERE EXISTS(SELECT * FROM ODS.PKK_WORKS_INFO WHERE OBJECTS_ID = p_cur_DBL.CLONE_PERSON_ID);
      
    IF p_EXIST_OB=1 THEN 
      --UPDATE ODS.PKK_WORKS_INFO SET OBJECTS_ID = p_cur_DBL.TARGET_PERSON_ID WHERE OBJECTS_ID=p_cur_DBL.CLONE_PERSON_ID;
      p_CNT_REC := p_CNT_REC + 1;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
    END IF;
    
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей в ODS.PKK_WORKS_INFO: '||TO_CHAR(p_CNT_UPD)||'; Обнаружено: '||TO_CHAR(p_CNT_REC));
  
  p_CNT_REC := 0;
  p_CNT_UPD := 0;
  FOR p_cur_DBL IN(SELECT DISTINCT CLONE_PERSON_ID, TARGET_PERSON_ID, STAMP FROM ODS.PKK_PERSON_COLLAPSED
        WHERE STAMP>p_START_DATE) LOOP
    --доп. 10 ОБНОВЛЯЕМ ODS.PKK_CONTACTS связи
    SELECT COUNT(*) INTO p_EXIST_OB FROM DUAL 
      WHERE EXISTS(SELECT * FROM ODS.PKK_CONTACTS WHERE OB_ID = p_cur_DBL.CLONE_PERSON_ID);
      
    IF p_EXIST_OB=1 THEN 
      --UPDATE ODS.PKK_CONTACTS SET OB_ID = p_cur_DBL.TARGET_PERSON_ID WHERE OB_ID=p_cur_DBL.CLONE_PERSON_ID;
      p_CNT_REC := p_CNT_REC + 1;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
    END IF;
    
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей в ODS.PKK_CONTACTS rel: '||TO_CHAR(p_CNT_UPD)||'; Обнаружено: '||TO_CHAR(p_CNT_REC));
  
  p_CNT_REC := 0;
  p_CNT_UPD := 0;
  FOR p_cur_DBL IN(SELECT DISTINCT CLONE_PERSON_ID, TARGET_PERSON_ID, STAMP FROM ODS.PKK_PERSON_COLLAPSED
        WHERE STAMP>p_START_DATE) LOOP
    --доп. 11 ОБНОВЛЯЕМ ODS.PKK_FAMILY связи
    SELECT COUNT(*) INTO p_EXIST_OB FROM DUAL 
      WHERE EXISTS(SELECT * FROM ODS.PKK_FAMILY WHERE OB_ID = p_cur_DBL.CLONE_PERSON_ID);
      
    IF p_EXIST_OB=1 THEN 
      --UPDATE ODS.PKK_FAMILY SET OB_ID = p_cur_DBL.TARGET_PERSON_ID WHERE OB_ID=p_cur_DBL.CLONE_PERSON_ID;
      p_CNT_REC := p_CNT_REC + 1;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
    END IF;
    
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей в ODS.PKK_FAMILY rel: '||TO_CHAR(p_CNT_UPD)||'; Обнаружено: '||TO_CHAR(p_CNT_REC));
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_RANK_BETWEEN_DOCUMENTS" (v_st_ID IN NUMBER, v_en_ID IN NUMBER)

IS
  p_st_ID NUMBER := v_st_ID;
  p_en_ID NUMBER := v_en_ID;
  -- 2000-7000000: 2419104 штук за 18041 сек
  p_CNT_UPD NUMBER := 0;
  p_CNT NUMBER := 0;
BEGIN
  DBMS_OUTPUT.ENABLE;
  /*FOR cur IN (SELECT DISTINCT OBJECTS_ID FROM ODS.QUERY_FOR_PKK_DOC_HISTORY_DIST
                                WHERE OBJECTS_ID BETWEEN p_st_ID AND p_en_ID ORDER BY OBJECTS_ID) LOOP
    MERGE INTO ODS.PKK_DOCUMENTS_HISTORY_INFO tar
        USING (SELECT DOCUMENTS_HISTORY_ID
                ,DENSE_RANK() OVER(PARTITION BY 
                      OBJECTS_ID, DOCUMENTS_TYPE       --по каджому физику последние телефоны
                      ORDER BY DOCUMENTS_AKT DESC, COALESCE(MODIFICATION_DATE, SYSDATE), DOCUMENTS_CREATED DESC
                        ,DOCUMENTS_HISTORY_ID) DOCUMENTS_RANK
                FROM ODS.PKK_DOCUMENTS_HISTORY_INFO WHERE OBJECTS_ID=cur.OBJECTS_ID) src
          ON (tar.DOCUMENTS_HISTORY_ID=src.DOCUMENTS_HISTORY_ID )
        WHEN MATCHED THEN
          UPDATE SET tar.DOCUMENTS_RANK = src.DOCUMENTS_RANK 
          WHERE NOT NVL(tar.DOCUMENTS_RANK, -1) = NVL(src.DOCUMENTS_RANK, -1)
          ;
     -- IF SQL%ROWCOUNT>0 THEN  
        p_CNT := p_CNT+1;
      --END IF;
      IF MOD(p_CNT, 500)=0 THEN 
        COMMIT;
      END IF;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
  END LOOP;*/
  DBMS_OUTPUT.PUT_LINE('Обновлено записей ранжирования телефонов: '||TO_CHAR(p_CNT_UPD)||'; Прогнано: '||TO_CHAR(p_CNT));
END;
  CREATE OR REPLACE PROCEDURE "SNAUSER"."PR$AQ_PKK_SNA_CALLBACK" (
      context  RAW,
      reginfo  SYS.AQ$_REG_INFO,
      descr    SYS.AQ$_DESCRIPTOR,
      payload  RAW,
      payloadl NUMBER
) AS
-- ========================================================================
-- ==	ПРОЦЕДУРА     "Считывание очереди, для регистрации в событии планировщика (REGISTER)"
-- ==	ОПИСАНИЕ:	    Считывание очередных сообщений в очереди.
-- == Внимание!     После запуска процесс может останавливаться с осложнениями.
-- ==               Тогда надо пытаться остановить очередь, убить процесс и т.п. Пока он не перестанет восстанавливаться.              
-- ========================================================================
-- ==	СОЗДАНИЕ:		  05.05.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	05.05.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================

  r_dequeue_options     DBMS_AQ.DEQUEUE_OPTIONS_T;
  r_message_properties  DBMS_AQ.MESSAGE_PROPERTIES_T;
  v_message_handle      RAW(16);
  p_MSG                 AQ_PKK_SNA_TYPE;
  /*o_payload            demo_queue_payload_type;*/
  pragma autonomous_transaction; 
BEGIN
    r_dequeue_options.msgid := descr.msg_id;
    r_dequeue_options.consumer_name := descr.consumer_name;
    --r_dequeue_options.consumer_name := 'STATUS';
    
    DBMS_AQ.DEQUEUE(queue_name         => descr.queue_name,
                    dequeue_options    => r_dequeue_options,
                    message_properties => r_message_properties,
                    payload            => p_MSG,
                    msgid              => v_message_handle );
  
    --если заявка пошла через ручное рассмотрение, то записать ее в таблицу SNA_dequeue_msg.
    --IF SFF.FN_CHECK_HAND_RID(p_MSG.request_id)=1 THEN   
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
                ,descr.consumer_name) ;
    --END IF;
        IF NOT p_MSG.created_group_id=11455 THEN 
          ODS.PR$UPD__P_PKK(p_MSG.request_id, p_MSG.objects_id );        
        END IF;

    COMMIT; 
     
EXCEPTION WHEN OTHERS THEN
        INSERT INTO SNAUSER.AQ_PKK_SNA_LOG_MESSAGE (/*msg_Id
                ,msg_date
                ,request_id
                ,objects_id 
                ,score_tree_route_id 
                ,created_group_id 
                ,type_request_id
                ,old_status_id
                ,new_status_id
                ,*/DATE_INSERT)
          VALUES (/*p_MSG.msg_Id
                /*,p_MSG.msg_date
                ,p_MSG.request_id
                ,p_MSG.objects_id 
                ,p_MSG.score_tree_route_id 
                ,p_MSG.created_group_id 
                ,p_MSG.type_request_id
                ,p_MSG.old_status_id
                ,p_MSG.new_status_id
                ,*/systimestamp) ;
          COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD__PKK" 
IS 
  p_START_TIME TIMESTAMP;
  p_ADD_INFO VARCHAR2(1000);
  
  pragma autonomous_transaction;
BEGIN
    p_START_TIME := SYSTIMESTAMP; 
     
    DBMS_OUTPUT.ENABLE;
    PR$UPD_PKK_ADDRESS;
    PR$UPD_PKK_ADDRESS_HISTORY;
    PR$UPD_PKK_CONTACTS;
    PR$UPD_PKK_DOCUMENTS_HISTORY;
    PR$UPD_PKK_EMAIL;
    PR$UPD_PKK_FAMILY; 
    PR$UPD_PKK_PERSON_INFO;
    PR$UPD_PKK_WORKS_INFO;
    PR$UPD_PKK_PHONES;
    PR$UPD_PKK_C_REQUEST_SNA;
    PR$UPD_PKK_C_CREDIT_INFO;
    PR$UPD_PKK_SNA_DEFOLT;
    PR$UPD_PKK_PERSON_COLLAPSED; 

    ODS.PR$INS_LOG ('PR$UPD__PKK', p_START_TIME, 'ODS.PKK', 'OK', SQLERRM, NULL, -1, 'СКРИПТ ПАКЕТНОЙ ЗАГРУЗКИ PKK');
  
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD__PKK', p_START_TIME, 'ODS.PKK', 'ERR', SQLERRM, NULL, -1, p_ADD_INFO);
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$INS_LOG" (v_PROC_NAME IN VARCHAR2, v_PROC_DATE_START IN TIMESTAMP
              ,v_TARGET_TABLE IN VARCHAR2
              ,v_PROC_RESULT IN VARCHAR2, v_MSG_ERROR IN VARCHAR2
              ,v_COUNT_REC IN NUMBER, v_OBJECTS_ID IN NUMBER, v_ADD_INFO IN VARCHAR2)
IS
/*
  Процедура логирования выполнения других процедур.  
*/
  p_PROC_DATE_END TIMESTAMP := SYSTIMESTAMP;
  p_TIME INTERVAL DAY(9) TO SECOND(6) := p_PROC_DATE_END - v_PROC_DATE_START;
  pragma autonomous_transaction;
BEGIN
  INSERT INTO ODS.LOG_CALL_PROC  
    (PROC_NAME
      , PROC_DATE_START
      , PROC_DATE_END
      , PROC_DURATION_TIME
      , TARGET_TABLE
      , MSG_ERROR
      , PROC_RESULT
      ,COUNT_REC
      ,OBJECTS_ID
      ,ADD_INFO)
    SELECT v_PROC_NAME AS PROC_NAME
        ,v_PROC_DATE_START AS PROC_DATE_START
        ,p_PROC_DATE_END AS PROC_DATE_END
        ,p_TIME AS PROC_DURATION_TIME
        ,v_TARGET_TABLE AS TARGET_TABLE
        ,v_MSG_ERROR AS MSG_ERROR
        ,v_PROC_RESULT AS PROC_RESULT 
        ,v_COUNT_REC AS COUNT_REC
        ,v_OBJECTS_ID AS OBJECTS_ID
        ,v_ADD_INFO AS ADD_INFO
    FROM DUAL
   ;
   COMMIT;
  
END;
  CREATE OR REPLACE TRIGGER "ODS"."LOG_CALL_PROC_ID_TRG" BEFORE INSERT ON LOG_CALL_PROC 
FOR EACH ROW 
BEGIN
  <<COLUMN_SEQUENCES>>
  BEGIN
    IF :NEW.ID IS NULL THEN
      SELECT LOG_CALL_PROC_ID_SEQ.NEXTVAL INTO :NEW.ID FROM DUAL;
    END IF;
  END COLUMN_SEQUENCES;
END;
ALTER TRIGGER "ODS"."LOG_CALL_PROC_ID_TRG" ENABLE"
"
  CREATE OR REPLACE PROCEDURE "SNAUSER"."PR$AQ_PKK_SNA_DEQUEUE" 
AS
  r_dequeue_options     DBMS_AQ.DEQUEUE_OPTIONS_T;
  r_message_properties  DBMS_AQ.MESSAGE_PROPERTIES_T;
  v_message_handle      RAW(16);
  p_MSG                 AQ_PKK_SNA_TYPE;
  
  /*l_msg_id raw(16);
  l_deq_opt dbms_aq.dequeue_options_t;
  l_msg_prop dbms_aq.message_properties_t;
  l_payload AQ_PKK_SNA_TYPE;*/
  --pragma autonomous_transaction;
BEGIN
  dbms_output.ENABLE;
  r_dequeue_options.consumer_name := 'STATUS';
   --r_dequeue_options.wait := DBMS_AQ.NO_WAIT; -- =DBMS_AQ.NO_WAIT - ставим отсутсвие ожидания 
  FOR i IN 1..3 loop
    dbms_aq.dequeue(
      queue_name         => 'SNAUSER.AQ_PKK_SNA',
      --queue_name         => 'QUEUE_PKK',
      dequeue_options    => r_dequeue_options,
      message_properties => r_message_properties,
      payload            => p_MSG,
      msgid              => v_message_handle
    );

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
      
      IF NOT p_MSG.created_group_id=11455 THEN 
        ODS.PR$UPD__P_PKK(p_MSG.request_id, p_MSG.objects_id );        
      END IF;
      COMMIT;
  END LOOP;
  
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD__P_PKK" (v_REQUEST_ID IN NUMBER, v_OBJECTS_ID IN NUMBER)
IS 
  p_START_TIME TIMESTAMP;
  p_ADD_INFO VARCHAR2(1000);
  
  pragma autonomous_transaction;
BEGIN 
    p_START_TIME := SYSTIMESTAMP; 
    
    DBMS_OUTPUT.ENABLE;
    PR$UPD_P_PKK_ADDRESS(v_REQUEST_ID);
    PR$UPD_P_PKK_ADDRESS_HISTORY(v_REQUEST_ID, v_OBJECTS_ID); 
    PR$UPD_P_PKK_C_CREDIT_INFO(v_REQUEST_ID, v_OBJECTS_ID);
    PR$UPD_P_PKK_C_REQUEST_SNA(v_REQUEST_ID);
    PR$UPD_P_PKK_CONTACTS(v_REQUEST_ID, v_OBJECTS_ID);
    PR$UPD_P_PKK_DOCUMENTS_HISTORY(v_REQUEST_ID, v_OBJECTS_ID);
    PR$UPD_P_PKK_EMAIL(v_REQUEST_ID, v_OBJECTS_ID);
    PR$UPD_P_PKK_FAMILY(v_REQUEST_ID, v_OBJECTS_ID); 
    PR$UPD_P_PKK_PERSON_INFO(v_REQUEST_ID, v_OBJECTS_ID);
    PR$UPD_P_PKK_WORKS_INFO(v_REQUEST_ID, v_OBJECTS_ID); 
    PR$UPD_P_PKK_PHONES(v_REQUEST_ID, v_OBJECTS_ID);
    
  ODS.PR$INS_LOG ('PR$UPD__P_PKK', p_START_TIME, 'ODS.PKK', 'OK', SQLERRM, NULL, v_OBJECTS_ID, 'СКРИПТ ТОЧЕЧНОЙ ЗАГРУЗКИ ИЗ PKK');
  
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD__PKK', p_START_TIME, 'ODS.PKK', 'ERR', SQLERRM, NULL, v_OBJECTS_ID, 'СКРИПТ ТОЧЕЧНОЙ ЗАГРУЗКИ ИЗ PKK');
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_RANK_PHONE_ARR" (v_st_DT IN DATE DEFAULT SYSDATE-21)

IS
  p_st_DT DATE := v_st_DT;
  p_CNT_UPD NUMBER := 0;
  p_CNT NUMBER := 0;
  p_START_TIME TIMESTAMP := SYSTIMESTAMP;
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;
  FOR cur IN (SELECT DISTINCT OBJECTS_ID FROM ODS.PKK_PHONES 
                                WHERE DATE_UPD>p_st_DT AND PHONES_RANK=1 --AND ROWNUM<50
              ) LOOP
    UPDATE PKK_PHONES SET PHONES_RANK=NULL WHERE OBJECTS_ID = cur.OBJECTS_ID;          
    
    MERGE INTO ODS.PKK_PHONES tar
        USING (SELECT PHONES_ID
                ,DENSE_RANK() OVER(PARTITION BY 
                      OBJECTS_ID, OBJECTS_TYPE       --по каджому физику последние телефоны
                      --chld.PHONE                   --по каждому телефону последние дата и актуальность
                      ORDER BY PHONES_AKT DESC, COALESCE(MODIFICATION_DATE, SYSDATE) DESC, PHONES_CREATED DESC
                        ,PHONES_ID DESC) PHONES_RANK
                FROM ODS.PKK_PHONES WHERE OBJECTS_ID=cur.OBJECTS_ID
              AND ((OBJECTS_TYPE IN(2, 200)
                AND NOT (NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из НБКИ:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон клиента(сайт)%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Рекомендация(ПО КЦ):%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РБО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПТ/АТМ банка%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из документов РБО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('сот. тел. заявителя(сайт)%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('ТЕЛЕФОН ИЗ НБКИ%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из РНКО:%' )
                OR NVL(PHONES_COMM, '-') LIKE UPPER('Телефон из ПО КЦ%')) )
              OR (OBJECTS_TYPE=8 AND NOT NVL(PHONE, '-') LIKE '9%' )
              OR (NOT OBJECTS_TYPE IN(2, 8, 200)  )
              ) 
              ) src
          ON (tar.PHONES_ID=src.PHONES_ID )
        WHEN MATCHED THEN
          UPDATE SET tar.PHONES_RANK = src.PHONES_RANK 
          WHERE NOT NVL(tar.PHONES_RANK, -1) = NVL(src.PHONES_RANK, -2)
          ;
          
      p_CNT := p_CNT+1;
      IF MOD(p_CNT, 100)=0 THEN 
        COMMIT;
      END IF;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
  END LOOP;
  
  ODS.PR$INS_LOG ('PR$UPD_RANK_PHONE_ARR', p_START_TIME, 'ODS.PKK_PHONES', 'OK', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование телефонов. От '||TO_CHAR(p_st_DT, 'dd.mm.yyyy')||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_RANK_PHONE_ARR', p_START_TIME, 'ODS.PKK_PHONES', 'ERR', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование телефонов. От '||TO_CHAR(p_st_DT, 'dd.mm.yyyy')||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_RANK_OB_ID_ADR_ARR" (v_st_DT IN DATE DEFAULT SYSDATE-21)

IS
  p_st_DT DATE := SYSDATE-21;
  p_CNT_UPD NUMBER := 0;
  p_CNT NUMBER := 0;
  p_START_TIME TIMESTAMP := SYSTIMESTAMP;
BEGIN
  DBMS_OUTPUT.ENABLE;
  FOR cur IN (SELECT DISTINCT ADDRESS_ID FROM ODS.PKK_ADDRESS_HISTORY
                                WHERE DATE_UPD > p_st_DT AND OBJECTS_RANK=1 --AND ROWNUM<100 --ORDER BY OBJECTS_ID
              ) LOOP
    MERGE INTO ODS.PKK_ADDRESS_HISTORY tar
        USING (SELECT ADDRESS_HISTORY_ID
                ,DENSE_RANK() OVER(PARTITION BY 
                      ADDRESS_ID, OBJECTS_TYPE       --по каджому физику последние паспорта
                      ORDER BY ADDRESS_AKT DESC, COALESCE(MODIFICATION_DATE, SYSDATE) DESC, ADDRESS_CREATED DESC) OBJECTS_RANK
                FROM ODS.PKK_ADDRESS_HISTORY WHERE ADDRESS_ID=cur.ADDRESS_ID) src
          ON (tar.ADDRESS_HISTORY_ID=src.ADDRESS_HISTORY_ID )
        WHEN MATCHED THEN
          UPDATE SET tar.OBJECTS_RANK = src.OBJECTS_RANK 
          WHERE NOT NVL(tar.OBJECTS_RANK, -1) = NVL(src.OBJECTS_RANK, -2);

      p_CNT := p_CNT+1;
      IF MOD(p_CNT, 100)=0 THEN
        COMMIT;
      END IF;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
  END LOOP;
  
  ODS.PR$INS_LOG ('PR$UPD_RANK_OB_ID_ADR_ARR', p_START_TIME, 'ODS.PKK_ADDRESS_HISTORY', 'OK', SQLERRM, p_CNT_UPD, -1
      , 'Ранжирование физиков по адресам. От '||TO_CHAR(p_st_DT, 'dd.mm.yyyy')||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
  COMMIT;

EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_RANK_OB_ID_ADR_ARR', p_START_TIME, 'ODS.PKK_ADDRESS_HISTORY', 'ERR', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование физиков по адресам. От '||TO_CHAR(p_st_DT, 'dd.mm.yyyy')||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_P_OBJECTS_ID_IN_PKK" (v_OLD_OBJECTS_ID IN NUMBER, v_NEW_OBJECTS_ID IN NUMBER)
IS 
  p_START_TIME TIMESTAMP := systimestamp;
  p_CNT_UPD NUMBER := 0;
  pragma autonomous_transaction;
BEGIN 
  DBMS_OUTPUT.ENABLE;
   
  UPDATE ODS.PKK_PERSON_INFO SET OBJECTS_ID = v_NEW_OBJECTS_ID WHERE OBJECTS_ID=v_OLD_OBJECTS_ID;    
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  UPDATE ODS.PKK_PHONES SET OBJECTS_ID = v_NEW_OBJECTS_ID WHERE OBJECTS_ID=v_OLD_OBJECTS_ID;
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  UPDATE ODS.PKK_ADDRESS_HISTORY SET OBJECTS_ID = v_NEW_OBJECTS_ID WHERE OBJECTS_ID=v_OLD_OBJECTS_ID;
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  UPDATE ODS.PKK_C_CREDIT_INFO_SNA SET PERSON_ID = v_NEW_OBJECTS_ID WHERE PERSON_ID=v_OLD_OBJECTS_ID;
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  UPDATE ODS.PKK_C_REQUEST_SNA SET OBJECTS_ID = v_NEW_OBJECTS_ID WHERE OBJECTS_ID=v_OLD_OBJECTS_ID;
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  UPDATE ODS.PKK_CONTACTS SET OBJECTS_ID = v_NEW_OBJECTS_ID WHERE OBJECTS_ID=v_OLD_OBJECTS_ID;
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  UPDATE ODS.PKK_DOCUMENTS_HISTORY_INFO SET OBJECTS_ID = v_NEW_OBJECTS_ID WHERE OBJECTS_ID=v_OLD_OBJECTS_ID;
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  UPDATE ODS.PKK_EMAIL SET OBJECTS_ID = v_NEW_OBJECTS_ID WHERE OBJECTS_ID=v_OLD_OBJECTS_ID; 
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  UPDATE ODS.PKK_FAMILY SET OBJECTS_ID = v_NEW_OBJECTS_ID WHERE OBJECTS_ID=v_OLD_OBJECTS_ID;
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  UPDATE ODS.PKK_WORKS_INFO SET OBJECTS_ID = v_NEW_OBJECTS_ID WHERE OBJECTS_ID=v_OLD_OBJECTS_ID;
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  UPDATE ODS.PKK_CONTACTS SET OB_ID = v_NEW_OBJECTS_ID WHERE OB_ID=v_OLD_OBJECTS_ID;
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  UPDATE ODS.PKK_FAMILY SET OB_ID = v_NEW_OBJECTS_ID WHERE OB_ID=v_OLD_OBJECTS_ID;
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  
  --обновления доп. таблиц
  UPDATE ODS.PKK_DEFOLT SET PERSON_ID = v_NEW_OBJECTS_ID WHERE PERSON_ID=v_OLD_OBJECTS_ID;
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  UPDATE SFF.RP_BNK@SNA SET PERSON_ID = v_NEW_OBJECTS_ID WHERE PERSON_ID=v_OLD_OBJECTS_ID;
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  UPDATE SFF.FROD_RULE_DEMO@SNA SET PERSON_ID = v_NEW_OBJECTS_ID WHERE PERSON_ID=v_OLD_OBJECTS_ID;
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  UPDATE SFF.FROD_RULE_DEMO@SNA SET PERSON_ID_REL = v_NEW_OBJECTS_ID WHERE PERSON_ID_REL=v_OLD_OBJECTS_ID;
    p_CNT_UPD := p_CNT_UPD+SQL%ROWCOUNT;
  
  ODS.PR$INS_LOG ('PR$UPD_P_OBJECTS_ID_IN_PKK', p_START_TIME, 'PKK_...', 'OK', SQLERRM, p_CNT_UPD, v_NEW_OBJECTS_ID
    , 'Замена OBJECTS_ID '||TO_CHAR(v_OLD_OBJECTS_ID)||' на '||TO_CHAR(v_NEW_OBJECTS_ID));
  COMMIT;
  
  EXCEPTION WHEN OTHERS THEN 
        ODS.PR$INS_LOG ('PR$UPD_P_OBJECTS_ID_IN_PKK', p_START_TIME, 'PKK_...', 'ERR', SQLERRM, p_CNT_UPD, v_NEW_OBJECTS_ID
            , 'Замена OBJECTS_ID '||TO_CHAR(v_OLD_OBJECTS_ID)||' на '||TO_CHAR(v_NEW_OBJECTS_ID));
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_RANK_DOC_ARR" (v_st_DT IN DATE DEFAULT SYSDATE-21)

IS
  p_st_DT DATE := v_st_DT;
  p_CNT_UPD NUMBER := 0;
  p_CNT NUMBER := 0;
  p_START_TIME TIMESTAMP := SYSTIMESTAMP;
BEGIN
  DBMS_OUTPUT.ENABLE;
  FOR cur IN (SELECT DISTINCT OBJECTS_ID FROM ODS.PKK_DOCUMENTS_HISTORY_INFO
                                WHERE DATE_UPD > p_st_DT AND DOCUMENTS_RANK=1 --AND ROWNUM<100 --ORDER BY OBJECTS_ID
              ) LOOP
    MERGE INTO ODS.PKK_DOCUMENTS_HISTORY_INFO tar
        USING (SELECT DOCUMENTS_HISTORY_ID
                ,DENSE_RANK() OVER(PARTITION BY 
                      OBJECTS_ID, DOCUMENTS_TYPE       --по каджому физику последние паспорта
                      ORDER BY DOCUMENTS_AKT DESC, COALESCE(MODIFICATION_DATE, SYSDATE) DESC, DOCUMENTS_CREATED DESC
                        ,DOCUMENTS_HISTORY_ID) DOCUMENTS_RANK
                FROM ODS.PKK_DOCUMENTS_HISTORY_INFO WHERE OBJECTS_ID=cur.OBJECTS_ID) src
          ON (tar.DOCUMENTS_HISTORY_ID=src.DOCUMENTS_HISTORY_ID )
        WHEN MATCHED THEN
          UPDATE SET tar.DOCUMENTS_RANK = src.DOCUMENTS_RANK 
          WHERE NOT NVL(tar.DOCUMENTS_RANK, -1) = NVL(src.DOCUMENTS_RANK, -2);
          
      p_CNT := p_CNT+1;
      IF MOD(p_CNT, 100)=0 THEN 
        COMMIT;
      END IF; 
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
  END LOOP;
  
  ODS.PR$INS_LOG ('PR$UPD_RANK_DOC_ARR', p_START_TIME, 'ODS.PKK_DOCUMENTS_HISTORY_INFO', 'OK', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование паспортов. От '||TO_CHAR(p_st_DT, 'dd.mm.yyyy')||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_RANK_DOC_ARR', p_START_TIME, 'ODS.PKK_DOCUMENTS_HISTORY_INFO', 'ERR', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование паспортов. От '||TO_CHAR(p_st_DT, 'dd.mm.yyyy')||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_RANK_PERSON_ARR" (v_st_DT IN DATE DEFAULT SYSDATE-21)

IS
  p_st_DT DATE := v_st_DT;
  p_CNT_UPD NUMBER := 0;
  p_CNT NUMBER := 0;
  p_START_TIME TIMESTAMP := SYSTIMESTAMP;
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;
  FOR cur IN (SELECT DISTINCT OBJECTS_ID FROM ODS.PKK_PERSON_INFO 
                                WHERE DATE_UPD>p_st_DT AND FIO_RANK=1 --AND ROWNUM<50
              ) LOOP
    MERGE INTO ODS.PKK_PERSON_INFO tar
        USING (SELECT FIO_HISTORY_PK
                ,DENSE_RANK() OVER(PARTITION BY 
                      OBJECTS_ID       --по каджому физику последние телефоны
                      ORDER BY FIO_AKT DESC, COALESCE(MODIFICATION_DATE, SYSDATE) DESC, FIO_CREATED DESC) FIO_RANK
                FROM ODS.PKK_PERSON_INFO WHERE OBJECTS_ID=cur.OBJECTS_ID) src
          ON (tar.FIO_HISTORY_PK=src.FIO_HISTORY_PK )
        WHEN MATCHED THEN
          UPDATE SET tar.FIO_RANK = src.FIO_RANK 
          WHERE NOT NVL(tar.FIO_RANK, -1) = NVL(src.FIO_RANK, -2)
          ;
          
      p_CNT := p_CNT+1;
      IF MOD(p_CNT, 100)=0 THEN 
        COMMIT;
      END IF;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
  END LOOP;
  
  ODS.PR$INS_LOG ('PR$UPD_RANK_PERSON_ARR', p_START_TIME, 'ODS.PKK_PERSON_INFO', 'OK', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование физиков. От '||TO_CHAR(p_st_DT, 'dd.mm.yyyy')||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
  COMMIT;
  
EXCEPTION WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_RANK_PERSON_ARR', p_START_TIME, 'ODS.PKK_PERSON_INFO', 'ERR', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование физиков. От '||TO_CHAR(p_st_DT, 'dd.mm.yyyy')||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_PKK_SNA_DEFOLT" (v_DAY_AGO IN NUMBER DEFAULT 10)
-- ========================================================================
-- ==	ПРОЦЕДУРА "Обновление криминальных дефолтников"
-- ==	ОПИСАНИЕ:		обновляет таблицу с заявками для кредитов по которым был криминальный дефолт
-- ========================================================================
-- ==	СОЗДАНИЕ:		25.08.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	25.08.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
IS
  p_st_DT DATE := SYSDATE-v_DAY_AGO;
  p_CNT_UPD NUMBER := 0;
  p_START_TIME TIMESTAMP := SYSTIMESTAMP;
  pragma autonomous_transaction;
BEGIN 
  
 MERGE INTO ODS.PKK_DEFOLT tar
        USING (SELECT cci.REQUEST_ID
            ,crid.OBJECTS_ID AS PERSON_ID
            ,crid.CREATED_DATE
            ,cci.MODIFICATION_DATE 
            ,SUBSTR(cci.MOP_DELAY,1,5) as MOP_lst5
            ,NVL(SUBSTR(cci.MOP_DELAY,-5,5), cci.MOP_DELAY) as MOP_fst5
            ,NVL(SUBSTR(RTRIM(cci.MOP_DELAY,'0-'),-3,3), RTRIM(cci.MOP_DELAY,'0-')) AS MOP_LST_TRIM
            ,1 as DEFOLT
            ,crid.STATUS_ID
            ,cci.MOP_DELAY
          FROM KREDIT.C_CREDIT_INFO@DBLINK_PKK cci
          LEFT OUTER JOIN KREDIT.C_REQUEST@DBLINK_PKK crid
            ON cci.REQUEST_ID=crid.REQUEST_ID
          WHERE cci.MODIFICATION_DATE > p_st_DT
              AND regexp_substr(NVL(SUBSTR(RTRIM(cci.MOP_DELAY,'0-'),-3,3), RTRIM(cci.MOP_DELAY,'0-')), '[2345789]', 1)>0
              --AND NOT EXISTS(SELECT REQUEST_ID FROM ODS.PKK_DEFOLT WHERE REQUEST_ID=cci.REQUEST_ID AND MOP_DELAY=cci.MOP_DELAY)
        ) src
          ON (tar.REQUEST_ID=src.REQUEST_ID )
        WHEN MATCHED THEN
          UPDATE SET tar.PERSON_ID = src.PERSON_ID 
              ,tar.CREATED_DATE = src.CREATED_DATE 
              ,tar.MODIFICATION_DATE = src.MODIFICATION_DATE 
              ,tar.MOP_lst5 = src.MOP_lst5 
              ,tar.MOP_fst5 = src.MOP_fst5 
              ,tar.MOP_LST_TRIM = src.MOP_LST_TRIM 
              ,tar.DEFOLT = src.DEFOLT 
              ,tar.STATUS_ID = src.STATUS_ID 
              ,tar.MOP_DELAY = src.MOP_DELAY 
              ,tar.RP_DEFOLT_DATE = SYSDATE
          WHERE NOT ( NVL(tar.MOP_DELAY, '-')=NVL(src.MOP_DELAY, '-') )
        WHEN NOT MATCHED THEN
          INSERT (tar.REQUEST_ID, tar.PERSON_ID, tar.CREATED_DATE, tar.MODIFICATION_DATE
                , tar.MOP_lst5, tar.MOP_fst5, tar.MOP_LST_TRIM, tar.DEFOLT, tar.STATUS_ID, tar.MOP_DELAY
                , tar.RP_DEFOLT_DATE)
            VALUES
            (src.REQUEST_ID, src.PERSON_ID, src.CREATED_DATE, src.MODIFICATION_DATE
                , src.MOP_lst5, src.MOP_fst5, src.MOP_LST_TRIM, src.DEFOLT, src.STATUS_ID, src.MOP_DELAY
                , SYSDATE)
          ;
    p_CNT_UPD := SQL%ROWCOUNT;
    COMMIT;

  ODS.PR$INS_LOG ('PR$UPD_PKK_SNA_DEFOLT', p_START_TIME, 'ODS.PKK_DEFOLT', 'OK', SQLERRM, p_CNT_UPD, -1
            , 'Обновление PKK_DEFOLT. От '||TO_CHAR(p_st_DT, 'dd.mm.yyyy'));
  COMMIT;
  
EXCEPTION WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_PKK_SNA_DEFOLT', p_START_TIME, 'ODS.PKK_DEFOLT', 'ERR', SQLERRM, p_CNT_UPD, -1
            , 'Обновление PKK_DEFOLT. От '||TO_CHAR(p_st_DT, 'dd.mm.yyyy'));
        COMMIT;
END;
  CREATE OR REPLACE TRIGGER "ODS"."TRG_PKK_PERSON_COLLAPSED" BEFORE INSERT ON ODS.PKK_PERSON_COLLAPSED 
FOR EACH ROW 
BEGIN
  PR$UPD_P_OBJECTS_ID_IN_PKK(:NEW.CLONE_PERSON_ID, :NEW.TARGET_PERSON_ID);
END;
ALTER TRIGGER "ODS"."TRG_PKK_PERSON_COLLAPSED" ENABLE"
"
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_RANK_ADR_ARR" (v_st_DT IN DATE DEFAULT SYSDATE-21)

IS
  p_st_DT DATE := v_st_DT;
  p_CNT_UPD NUMBER := 0;
  p_CNT NUMBER := 0;
  p_START_TIME TIMESTAMP := SYSTIMESTAMP;
BEGIN
  DBMS_OUTPUT.ENABLE;
  FOR cur IN (SELECT DISTINCT OBJECTS_ID FROM ODS.PKK_ADDRESS_HISTORY
                                WHERE DATE_UPD > p_st_DT AND ADDRESS_RANK=1 --AND ROWNUM<100 --ORDER BY OBJECTS_ID
              ) LOOP
    MERGE INTO ODS.PKK_ADDRESS_HISTORY tar 
        USING (SELECT ADDRESS_HISTORY_ID
                ,DENSE_RANK() OVER(PARTITION BY 
                  OBJECTS_ID, OBJECTS_TYPE       --по каджому физику последние паспорта
                  ORDER BY ADDRESS_AKT DESC, COALESCE(MODIFICATION_DATE, SYSDATE) DESC, ADDRESS_CREATED DESC
                    ,ADDRESS_HISTORY_ID DESC) ADDRESS_RANK
                FROM ODS.PKK_ADDRESS_HISTORY WHERE OBJECTS_ID=cur.OBJECTS_ID) src
          ON (tar.ADDRESS_HISTORY_ID=src.ADDRESS_HISTORY_ID )
        WHEN MATCHED THEN
          UPDATE SET tar.ADDRESS_RANK = src.ADDRESS_RANK 
          WHERE NOT NVL(tar.ADDRESS_RANK, -1) = NVL(src.ADDRESS_RANK, -2);
          
      p_CNT := p_CNT+1;
      IF MOD(p_CNT, 100)=0 THEN 
        COMMIT;
      END IF;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
  END LOOP;
  
  ODS.PR$INS_LOG ('PR$UPD_RANK_ADR_ARR', p_START_TIME, 'ODS.PKK_ADDRESS_HISTORY', 'OK', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование адресов. От '||TO_CHAR(p_st_DT, 'dd.mm.yyyy')||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_RANK_ADR_ARR', p_START_TIME, 'ODS.PKK_ADDRESS_HISTORY', 'ERR', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование адресов. От '||TO_CHAR(p_st_DT, 'dd.mm.yyyy')||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_RANK_BETWEEN_ADDRESS" (v_st_ID IN NUMBER, v_en_ID IN NUMBER)

IS
  p_st_ID NUMBER := v_st_ID;
  p_en_ID NUMBER := v_en_ID;
  -- 2000-7000000: 2419104 штук за 18041 сек
  p_CNT_UPD NUMBER := 0;
  p_CNT NUMBER := 0;
    p_START_TIME TIMESTAMP := SYSTIMESTAMP;
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;
  FOR cur IN (SELECT OBJECTS_ID FROM ODS.QUERY_FOR_PKK_ADR 
                                WHERE OBJECTS_ID BETWEEN p_st_ID AND p_en_ID ORDER BY OBJECTS_ID) LOOP
    MERGE INTO ODS.PKK_ADDRESS_HISTORY tar
        USING (SELECT ADDRESS_HISTORY_ID 
                ,DENSE_RANK() OVER(PARTITION BY 
                      OBJECTS_ID, OBJECTS_TYPE       --по каджому физику последние паспорта
                      ORDER BY ADDRESS_AKT DESC, COALESCE(MODIFICATION_DATE, SYSDATE), ADDRESS_CREATED DESC
                          ,ADDRESS_HISTORY_ID DESC) ADDRESS_RANK
                FROM ODS.PKK_ADDRESS_HISTORY WHERE OBJECTS_ID=cur.OBJECTS_ID) src
          ON (tar.ADDRESS_HISTORY_ID=src.ADDRESS_HISTORY_ID )
        WHEN MATCHED THEN
          UPDATE SET tar.ADDRESS_RANK = src.ADDRESS_RANK 
          WHERE NOT NVL(tar.ADDRESS_RANK, -1) = NVL(src.ADDRESS_RANK, -1);
     -- IF SQL%ROWCOUNT>0 THEN 
        p_CNT := p_CNT+1;
      --END IF;
      IF MOD(p_CNT, 100)=0 THEN 
        /*DBMS_OUTPUT.PUT_LINE('Обновлено записей ранжирования физиков: '||TO_CHAR(p_CNT_UPD)||'; Прогнано: '||TO_CHAR(p_CNT)
            ||'; ID='||TO_CHAR(cur.OBJECTS_ID)||' - '||TO_CHAR(SYSTIMESTAMP));*/
        COMMIT;
      END IF;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей ранжирования адресов: '||TO_CHAR(p_CNT_UPD)||'; Прогнано: '||TO_CHAR(p_CNT));
  
  ODS.PR$INS_LOG ('PR$UPD_RANK_BETWEEN_ADDRESS', p_START_TIME, 'ODS.PKK_ADDRESS_HISTORY', 'OK', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование адресов. От '||TO_CHAR(v_st_ID)||' до '||TO_CHAR(v_en_ID)||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_RANK_BETWEEN_ADDRESS', p_START_TIME, 'ODS.PKK_ADDRESS_HISTORY', 'ERR', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование адресов. От '||TO_CHAR(v_st_ID)||' до '||TO_CHAR(v_en_ID)||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_RANK_BETWEEN_WORKS" (v_st_ID IN NUMBER, v_en_ID IN NUMBER)

IS
  p_st_ID NUMBER := v_st_ID;
  p_en_ID NUMBER := v_en_ID;
  -- 2000-7000000: 2419104 штук за 18041 сек
  p_CNT_UPD NUMBER := 0;
  p_CNT NUMBER := 0;
    p_START_TIME TIMESTAMP := SYSTIMESTAMP;
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;
  FOR cur IN (SELECT OBJECTS_ID FROM ODS.Q_FOR_PKK_WORKS_RNK
                                WHERE OBJECTS_ID BETWEEN p_st_ID AND p_en_ID ORDER BY OBJECTS_ID) LOOP
    --UPDATE ODS.PKK_WORKS_INFO SET WORKS_RANK = 1 WHERE OBJECTS_ID=cur.OBJECTS_ID AND WORKS_RANK^=1 ;
    MERGE INTO ODS.PKK_WORKS_INFO tar
        USING (SELECT WORKS_HISTORY_ID
                ,DENSE_RANK() OVER(PARTITION BY 
                      OBJECTS_ID       --по каджому физику последние паспорта
                      ORDER BY WORKS_AKT DESC, COALESCE(MODIFICATION_DATE, SYSDATE) DESC, WORKS_CREATED DESC
                        ,WORKS_HISTORY_ID DESC) WORKS_RANK
                FROM ODS.PKK_WORKS_INFO WHERE OBJECTS_ID=cur.OBJECTS_ID) src
          ON (tar.WORKS_HISTORY_ID=src.WORKS_HISTORY_ID )
        WHEN MATCHED THEN
          UPDATE SET tar.WORKS_RANK = src.WORKS_RANK
          WHERE NOT NVL(tar.WORKS_RANK, -1) = NVL(src.WORKS_RANK, -2) ;
     -- IF SQL%ROWCOUNT>0 THEN 
        p_CNT := p_CNT+1;
      --END IF; 
      IF MOD(p_CNT, 100)=0 THEN 
        /*DBMS_OUTPUT.PUT_LINE('Обновлено записей ранжирования физиков: '||TO_CHAR(p_CNT_UPD)||'; Прогнано: '||TO_CHAR(p_CNT)
            ||'; ID='||TO_CHAR(cur.OBJECTS_ID)||' - '||TO_CHAR(SYSTIMESTAMP));*/
        COMMIT;
      END IF;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Обновлено записей ранжирования адресов: '||TO_CHAR(p_CNT_UPD)||'; Прогнано: '||TO_CHAR(p_CNT));
  
  ODS.PR$INS_LOG ('PR$UPD_RANK_BETWEEN_WORKS', p_START_TIME, 'ODS.PKK_WORKS_INFO', 'OK', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование работы. От '||TO_CHAR(v_st_ID)||' до '||TO_CHAR(v_en_ID)||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_RANK_BETWEEN_WORKS', p_START_TIME, 'ODS.PKK_WORKS_INFO', 'ERR', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование работы. От '||TO_CHAR(v_st_ID)||' до '||TO_CHAR(v_en_ID)||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_RANK_WORKS_ARR" (v_st_DT IN DATE DEFAULT SYSDATE-21)

IS
  p_st_DT DATE := v_st_DT;
  p_CNT_UPD NUMBER := 0;
  p_CNT NUMBER := 0;
  p_START_TIME TIMESTAMP := SYSTIMESTAMP;
BEGIN
  DBMS_OUTPUT.ENABLE;
  FOR cur IN (SELECT DISTINCT OBJECTS_ID FROM ODS.PKK_WORKS_INFO
                                WHERE DATE_UPD > p_st_DT AND WORKS_RANK=1 --AND ROWNUM<100 --ORDER BY OBJECTS_ID
              ) LOOP
    MERGE INTO ODS.PKK_WORKS_INFO tar
        USING (SELECT WORKS_HISTORY_ID
                ,DENSE_RANK() OVER(PARTITION BY 
                      OBJECTS_ID       --по каджому физику последние паспорта
                      ORDER BY WORKS_AKT DESC, COALESCE(MODIFICATION_DATE, SYSDATE) DESC, WORKS_CREATED DESC
                        ,WORKS_HISTORY_ID DESC) WORKS_RANK
                FROM ODS.PKK_WORKS_INFO WHERE OBJECTS_ID=cur.OBJECTS_ID) src
          ON (tar.WORKS_HISTORY_ID=src.WORKS_HISTORY_ID )
        WHEN MATCHED THEN
          UPDATE SET tar.WORKS_RANK = src.WORKS_RANK 
          WHERE NOT NVL(tar.WORKS_RANK, -1) = NVL(src.WORKS_RANK, -2);
          
      p_CNT := p_CNT+1;
      IF MOD(p_CNT, 100)=0 THEN 
        COMMIT;
      END IF;
      p_CNT_UPD := p_CNT_UPD + SQL%ROWCOUNT;
  END LOOP; 
  
  ODS.PR$INS_LOG ('PR$UPD_RANK_WORKS_ARR', p_START_TIME, 'ODS.PKK_WORKS_INFO', 'OK', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование работы. От '||TO_CHAR(p_st_DT, 'dd.mm.yyyy')||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_RANK_WORKS_ARR', p_START_TIME, 'ODS.PKK_WORKS_INFO', 'ERR', SQLERRM, p_CNT_UPD, -1
            , 'Ранжирование работы. От '||TO_CHAR(p_st_DT, 'dd.mm.yyyy')||' прогнали: '||TO_CHAR(p_CNT)||' OBJECTS_ID');
        COMMIT;
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_C_PKK_ADDRESS_HISTORY" 
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицы с адресной историей
-- ==	ОПИСАНИЕ:	   
-- ========================================================================
-- ==	СОЗДАНИЕ:		  24.12.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	12.02.2016 (ТРАХАЧЕВ В.В.)
/*PKK_ADDRESS_HISTORY:11.01.16 07:03:56-10.02.16 07:03:53(645058). От 02.03.16 06:30:33 до 02.03.16 10:11:09

*/
-- ========================================================================
AS    
  last_DT_SFF DATE; -- последний MODIFICATION_DATE в SFF
  last_DT_PKK DATE; -- последний MODIFICATION_DATE в ПКК
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER := 0;
  p_ADD_INFO VARCHAR2(1000);
  p_CNT_PERSON_ID NUMBER :=0;
  p_CNTALL_PERSON_ID NUMBER := 0;
  pragma autonomous_transaction;
BEGIN
  DBMS_OUTPUT.ENABLE;  
  
  start_time := systimestamp;
  
  SELECT MAX(ADDRESS_CREATED)-24/24 INTO last_DT_SFF FROM ODS.PKK_ADDRESS_HISTORY
       WHERE ADDRESS_HISTORY_ID>=(SELECT MAX(ADDRESS_HISTORY_ID)-1000 FROM ODS.PKK_ADDRESS_HISTORY);

  FOR ci IN(SELECT ADDRESS_ID
          ,OBJECTS_ID
          ,OBJECTS_TYPE
          ,ADDRESS_AKT
          ,ADDRESS_CREATED
          ,CREATED_SOURCE
          ,CREATED_USER_ID
          ,CREATED_GROUP_ID
          ,CREATED_IPADR
          ,MODIFICATION_DATE
          ,MODIFICATION_SOURCE
          ,MODIFICATION_USER_ID
          ,MODIFICATION_GROUP_ID
          ,MODIFICATION_IPADR
          ,ADDRESS_HISTORY_ID
          ,ADDRESS_COMM FROM ODS.VIEW_PKK_ADDRESS_HISTORY WHERE ADDRESS_CREATED > last_DT_SFF
            --AND rownum<10000
          UNION 
         SELECT ADDRESS_ID
          ,OBJECTS_ID
          ,OBJECTS_TYPE
          ,ADDRESS_AKT
          ,ADDRESS_CREATED
          ,CREATED_SOURCE
          ,CREATED_USER_ID
          ,CREATED_GROUP_ID
          ,CREATED_IPADR
          ,MODIFICATION_DATE
          ,MODIFICATION_SOURCE
          ,MODIFICATION_USER_ID
          ,MODIFICATION_GROUP_ID
          ,MODIFICATION_IPADR
          ,ADDRESS_HISTORY_ID
          ,ADDRESS_COMM FROM ODS.VIEW_PKK_ADDRESS_HISTORY WHERE MODIFICATION_DATE > last_DT_SFF
            --AND rownum<10000
            )
  LOOP
      --ОБНОВЛЕНИЕ
      MERGE INTO ODS.PKK_ADDRESS_HISTORY tar
        USING (SELECT ci.ADDRESS_ID AS ADDRESS_ID
                ,ci.OBJECTS_ID as OBJECTS_ID
                ,ci.OBJECTS_TYPE as OBJECTS_TYPE
                ,ci.ADDRESS_AKT as ADDRESS_AKT
                ,ci.ADDRESS_CREATED as ADDRESS_CREATED
                ,ci.CREATED_SOURCE as CREATED_SOURCE
                ,ci.CREATED_USER_ID as CREATED_USER_ID
                ,ci.CREATED_GROUP_ID as CREATED_GROUP_ID
                ,ci.CREATED_IPADR as CREATED_IPADR
                ,ci.MODIFICATION_DATE as MODIFICATION_DATE
                ,ci.MODIFICATION_SOURCE as MODIFICATION_SOURCE
                ,ci.MODIFICATION_USER_ID as MODIFICATION_USER_ID
                ,ci.MODIFICATION_GROUP_ID as MODIFICATION_GROUP_ID
                ,ci.MODIFICATION_IPADR as MODIFICATION_IPADR
                ,ci.ADDRESS_HISTORY_ID as ADDRESS_HISTORY_ID
                ,ci.ADDRESS_COMM  as ADDRESS_COMM
                FROM DUAL          
          ) src
          ON (src.ADDRESS_HISTORY_ID = tar.ADDRESS_HISTORY_ID )
        WHEN MATCHED THEN
          --Обновляем существующее (клюевые и неизменяемые поля не нужно обновлять )
          UPDATE SET
              tar.ADDRESS_ID=src.ADDRESS_ID
              ,tar.OBJECTS_ID=src.OBJECTS_ID
              --,tar.OBJECTS_TYPE=src.OBJECTS_TYPE
              ,tar.ADDRESS_AKT=src.ADDRESS_AKT
              --,tar.ADDRESS_CREATED=src.ADDRESS_CREATED
              --,tar.CREATED_SOURCE=src.CREATED_SOURCE
              --,tar.CREATED_USER_ID=src.CREATED_USER_ID
              --,tar.CREATED_GROUP_ID=src.CREATED_GROUP_ID
              --,tar.CREATED_IPADR=src.CREATED_IPADR
              ,tar.MODIFICATION_DATE=src.MODIFICATION_DATE
              ,tar.MODIFICATION_SOURCE=src.MODIFICATION_SOURCE
              ,tar.MODIFICATION_USER_ID=src.MODIFICATION_USER_ID
              ,tar.MODIFICATION_GROUP_ID=src.MODIFICATION_GROUP_ID
              ,tar.MODIFICATION_IPADR=src.MODIFICATION_IPADR
              --,tar.ADDRESS_HISTORY_ID=src.ADDRESS_HISTORY_ID
              --,tar.ADDRESS_COMM=src.ADDRESS_COMM
              ,tar.DATE_UPD=SYSDATE
              ,tar.ADDRESS_RANK=1
              ,tar.OBJECTS_RANK=1
            WHERE NOT ( NVL(tar.ADDRESS_ID, -1) = NVL(src.ADDRESS_ID, -1)
              AND NVL(tar.ADDRESS_AKT, -1) = NVL(src.ADDRESS_AKT, -1)
              AND NVL(tar.MODIFICATION_DATE, TO_DATE('01-01-1900', 'dd-mm-yyyy')) = NVL(src.MODIFICATION_DATE, TO_DATE('01-01-1900', 'dd-mm-yyyy'))
              AND NVL(tar.MODIFICATION_USER_ID, -1) = NVL(src.MODIFICATION_USER_ID, -1)
              AND NVL(tar.MODIFICATION_GROUP_ID, -1) = NVL(src.MODIFICATION_GROUP_ID, -1)
              AND NVL(tar.MODIFICATION_IPADR, -1) = NVL(src.MODIFICATION_IPADR, -1)
              )
        WHEN NOT MATCHED THEN 
        --вставляем новое
        INSERT (	tar.ADDRESS_ID
                  ,tar.OBJECTS_ID, tar.OBJECTS_TYPE
                  ,tar.ADDRESS_AKT, tar.ADDRESS_CREATED
                  ,tar.CREATED_SOURCE, tar.CREATED_USER_ID,tar.CREATED_GROUP_ID,tar.CREATED_IPADR
                  ,tar.MODIFICATION_DATE, tar.MODIFICATION_SOURCE, tar.MODIFICATION_USER_ID, tar.MODIFICATION_GROUP_ID
                  ,tar.MODIFICATION_IPADR, tar.ADDRESS_HISTORY_ID, tar.ADDRESS_COMM
                  ,tar.DATE_UPD
                  ,tar.ADDRESS_RANK, tar.OBJECTS_RANK)
        VALUES (src.ADDRESS_ID
                  ,src.OBJECTS_ID, src.OBJECTS_TYPE
                  ,src.ADDRESS_AKT, src.ADDRESS_CREATED
                  ,src.CREATED_SOURCE, src.CREATED_USER_ID, src.CREATED_GROUP_ID, src.CREATED_IPADR
                  ,src.MODIFICATION_DATE, src.MODIFICATION_SOURCE, src.MODIFICATION_USER_ID, src.MODIFICATION_GROUP_ID
                  ,src.MODIFICATION_IPADR, src.ADDRESS_HISTORY_ID, src.ADDRESS_COMM
                  ,SYSDATE
                  ,1, 1)
        ;	
  
      p_CNTALL_PERSON_ID := p_CNTALL_PERSON_ID + 1;
      IF SQL%ROWCOUNT>0 THEN 
       cnt_MODIF := cnt_MODIF + SQL%ROWCOUNT;
       p_CNT_PERSON_ID := p_CNT_PERSON_ID + 1;
       COMMIT;
      END IF;
      --COMMIT;
  END LOOP;
  
  SELECT MAX(ADDRESS_CREATED) INTO last_DT_PKK FROM ODS.PKK_ADDRESS_HISTORY
       WHERE ADDRESS_HISTORY_ID>=(SELECT MAX(ADDRESS_HISTORY_ID)-1000 FROM ODS.PKK_ADDRESS_HISTORY);
       
  p_ADD_INFO := 'От: '||TO_CHAR(last_DT_SFF)||' кол.OBJECTS_ID='||TO_CHAR(p_CNT_PERSON_ID)||'/'||TO_CHAR(p_CNT_PERSON_ID);
  ODS.PR$INS_LOG ('PR$UPD_C_PKK_ADDRESS_HISTORY', start_time, 'ODS.PKK_ADDRESS_HISTORY', 'OK', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
  
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_C_PKK_ADDRESS_HISTORY', start_time, 'ODS.PKK_ADDRESS_HISTORY', 'ERR', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
END;
  CREATE OR REPLACE PROCEDURE "ODS"."PR$UPD_C_PKK_ADDRESS" 
-- ========================================================================
-- ==	ПРОЦЕДУРА     Обновление таблицу с данными из C_REQUEST
-- ==	ОПИСАНИЕ:	    Обновляем статусов и прочее для заявок
-- ========================================================================
-- ==	СОЗДАНИЕ:		  09.11.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	25.01.2016 (ТРАХАЧЕВ В.В.)
-- ==
/*PKK_ADDRESS:45147121 - 45180956. От :25.01.16 06:01:19,117174000 UTC до: 25.01.16 06:11:52,391406000 UTC
*/
-- ========================================================================
AS    
  last_ID_SFF NUMBER; -- последний существующий REQUEST_ID в SFF
  last_ID_PKK NUMBER; -- последний существующий REQUEST_ID в ПКК
  
  start_time TIMESTAMP;
  cnt_MODIF NUMBER;
  
  p_ADD_INFO VARCHAR2(1000);
  pragma autonomous_transaction;
  p_CNT NUMBER := 0;
BEGIN
  start_time := systimestamp;
  
  SELECT MAX(ADDRESS_ID)-10 INTO last_ID_SFF FROM ODS.PKK_ADDRESS;
  SELECT MAX(ADDRESS_ID) INTO last_ID_PKK FROM CPD.ADDRESS@DBLINK_PKK;

  FOR ic IN (SELECT
              adr.ADDRESS_ID
              ,cntr.COUNTRIES_ISO2 as COUNTRY		--Страна
              ,adr.REGIONS_UID
              ,reg.REGIONS_NAMES					--Регион
              ,adr.AREAS_UID    
              ,are.AREAS_NAMES					--Район 
              ,adr.CITIES_UID						--Id нас. пункта
              ,cit.CITIES_NAMES					--НП
              ,cit.CITIES_TYPE					--Тип НП ID
              ,KLADR_SIT.SHOTNAME as SHOTNAME_CIT	--Тип НП 
              ,adr.STREETS_UID					--ID типа улицы
              ,str.STREETS_NAMES					--Улица 
              ,str.STREETS_TYPE					--Тип улицы ID
              ,KLADR_STR.SHOTNAME as SHOTNAME_STR	--Тип улицы 
              ,adr.HOUSE							--Дом 
              ,adr.BUILD							--Корпус 
              ,adr.FLAT							--Квартира
              ,adr.POSTOFFICE						--Индекс
              ,adr.GEO_ID							--ID геоданных
              ,geo.QUALITY_CODE					--Код качества при преобразовании исходных адресных данных для получения геокоординат
              ,geo.GEO_LAT						--Широта
              ,geo.GEO_LNG						--Долгота
              ,geo.GEO_QC							--Точность определения преобразования адреса для получения координат
              ,geo.ADDRESS_STR AS GEO_ADR			--Преобразованный адрес, по которому цеплялись координаты
              ,geo.CREATED_DATE AS GEO_CREATED	--Дата создания кординат
            FROM CPD.ADDRESS@dblink_pkk adr 
            LEFT JOIN CPD.REGIONS_NAMES@dblink_pkk reg ON adr.REGIONS_UID = reg.REGIONS_UID
            LEFT JOIN CPD.AREAS_NAMES@dblink_pkk are ON adr.AREAS_UID = are.AREAS_UID
            LEFT JOIN CPD.CITIES_NAMES@dblink_pkk cit ON adr.CITIES_UID = cit.CITIES_UID
            LEFT JOIN CPD.STREETS_NAMES@dblink_pkk str ON adr.STREETS_UID = str.STREETS_UID
            LEFT JOIN CPD.COUNTRIES@dblink_pkk cntr ON adr.COUNTRIES_ID = cntr.COUNTRIES_ID
            LEFT JOIN CPD.KLADR_SOCR@dblink_pkk KLADR_SIT ON cit.CITIES_TYPE = KLADR_SIT.SOCR_ID
            LEFT JOIN CPD.KLADR_SOCR@dblink_pkk KLADR_STR ON str.STREETS_TYPE = KLADR_STR.SOCR_ID
            LEFT JOIN CPD.GEOCOORDINATES@dblink_pkk geo ON adr.GEO_ID = geo.ID
            WHERE 
              adr.ADDRESS_ID BETWEEN last_ID_SFF AND last_ID_PKK
              /*AND ROWNUM<5*/)
  LOOP
    BEGIN
    --ОБНОВЛЕНИЕ
    MERGE INTO ODS.PKK_ADDRESS tar
    USING (SELECT ic.ADDRESS_ID as ADDRESS_ID
        ,ic.COUNTRY as COUNTRY
        ,ic.REGIONS_UID as REGIONS_UID, ic.REGIONS_NAMES as REGIONS_NAMES, ic.AREAS_UID as AREAS_UID, ic.AREAS_NAMES as AREAS_NAMES
        ,ic.CITIES_UID as CITIES_UID, ic.CITIES_NAMES as CITIES_NAMES, ic.CITIES_TYPE as CITIES_TYPE, ic.SHOTNAME_CIT as SHOTNAME_CIT
        ,ic.STREETS_UID as STREETS_UID, ic.STREETS_NAMES as STREETS_NAMES, ic.STREETS_TYPE as STREETS_TYPE, ic.SHOTNAME_STR as SHOTNAME_STR
        ,ic.HOUSE as HOUSE, ic.BUILD as BUILD, ic.FLAT as FLAT
        ,ic.POSTOFFICE as POSTOFFICE
        ,ic.GEO_ID as GEO_ID, ic.QUALITY_CODE as QUALITY_CODE, ic.GEO_LAT as GEO_LAT, ic.GEO_LNG as GEO_LNG
        ,ic.GEO_QC as GEO_QC, ic.GEO_ADR as GEO_ADR, ic.GEO_CREATED as GEO_CREATED
        FROM DUAL
      ) src
      ON (tar.ADDRESS_ID=src.ADDRESS_ID )
    WHEN MATCHED THEN
      --клюевые и неизменяемые поля не нужно обновлять. 
      UPDATE SET
        --tar.ADDRESS_ID=src.ADDRESS_ID
        tar.COUNTRY=src.COUNTRY
        ,tar.REGIONS_UID=src.REGIONS_UID
        ,tar.REGIONS_NAMES=src.REGIONS_NAMES
        ,tar.AREAS_UID=src.AREAS_UID
        ,tar.AREAS_NAMES=src.AREAS_NAMES
        ,tar.CITIES_UID=src.CITIES_UID
        ,tar.CITIES_NAMES=src.CITIES_NAMES
        ,tar.CITIES_TYPE=src.CITIES_TYPE
        ,tar.SHOTNAME_CIT=src.SHOTNAME_CIT
        ,tar.STREETS_UID=src.STREETS_UID
        ,tar.STREETS_NAMES=src.STREETS_NAMES
        ,tar.STREETS_TYPE=src.STREETS_TYPE
        ,tar.SHOTNAME_STR=src.SHOTNAME_STR
        ,tar.HOUSE=src.HOUSE
        ,tar.BUILD=src.BUILD
        ,tar.FLAT=src.FLAT
        ,tar.POSTOFFICE=src.POSTOFFICE
        ,tar.GEO_ID=src.GEO_ID
        ,tar.QUALITY_CODE=src.QUALITY_CODE
        ,tar.GEO_LAT=src.GEO_LAT
        ,tar.GEO_LNG=src.GEO_LNG
        ,tar.GEO_QC=src.GEO_QC
        ,tar.GEO_ADR=src.GEO_ADR
        ,tar.GEO_CREATED=src.GEO_CREATED
        ,tar.DATE_UPD=SYSDATE
    WHEN NOT MATCHED THEN 
    --вставляем новое
    INSERT (tar.ADDRESS_ID
        ,tar.COUNTRY
        ,tar.REGIONS_UID, tar.REGIONS_NAMES, tar.AREAS_UID, tar.AREAS_NAMES
        ,tar.CITIES_UID, tar.CITIES_NAMES, tar.CITIES_TYPE, tar.SHOTNAME_CIT
        ,tar.STREETS_UID, tar.STREETS_NAMES, tar.STREETS_TYPE, tar.SHOTNAME_STR
        ,tar.HOUSE, tar.BUILD, tar.FLAT
        ,tar.POSTOFFICE
        ,tar.GEO_ID, tar.QUALITY_CODE, tar.GEO_LAT, tar.GEO_LNG
        ,tar.GEO_QC, tar.GEO_ADR, tar.GEO_CREATED
        ,tar.DATE_UPD)
    VALUES (src.ADDRESS_ID
        ,src.COUNTRY
        ,src.REGIONS_UID, src.REGIONS_NAMES, src.AREAS_UID, src.AREAS_NAMES
        ,src.CITIES_UID, src.CITIES_NAMES, src.CITIES_TYPE, src.SHOTNAME_CIT
        ,src.STREETS_UID, src.STREETS_NAMES, src.STREETS_TYPE, src.SHOTNAME_STR
        ,src.HOUSE, src.BUILD, src.FLAT
        ,src.POSTOFFICE
        ,src.GEO_ID, src.QUALITY_CODE, src.GEO_LAT, src.GEO_LNG
        ,src.GEO_QC, src.GEO_ADR, src.GEO_CREATED
        ,SYSDATE)
    ;
    p_CNT := p_CNT+1;
    --информация для вывода при ручном обновлении
    cnt_MODIF := SQL%ROWCOUNT;
  
    IF MOD(p_CNT, 50)=0 OR cnt_MODIF>0 THEN 
      COMMIT;
    END IF;
    IF SQL%ROWCOUNT>0 THEN 
     ODS.PR$INS_LOG ('PR$UPD_C_PKK_ADDRESS', start_time, 'ODS.PKK_ADDRESS', 'OK', SQLERRM, cnt_MODIF, ic.ADDRESS_ID
            , 'Обновление ADDRESS_ID='||TO_CHAR(ic.ADDRESS_ID)||'; i='||TO_CHAR(p_CNT));
      COMMIT;
    END IF;
    
    
    EXCEPTION
        WHEN OTHERS
        THEN ODS.PR$INS_LOG ('PR$UPD_C_PKK_ADDRESS', start_time, 'ODS.PKK_ADDRESS', 'ERR', SQLERRM, cnt_MODIF, ic.ADDRESS_ID
          , 'Обновление ADDRESS_ID='||TO_CHAR(ic.ADDRESS_ID)||'; i='||TO_CHAR(p_CNT));
    END;
  END LOOP;

  --p_ADD_INFO := 'Период обновления: '||TO_CHAR(last_ID_SFF)||' до '||TO_CHAR(last_ID_PKK);
  
  ODS.PR$INS_LOG ('PR$UPD_C_PKK_ADDRESS', start_time, 'ODS.PKK_ADDRESS', 'OK', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
  COMMIT;
  
EXCEPTION
    WHEN OTHERS
    THEN ODS.PR$INS_LOG ('PR$UPD_C_PKK_ADDRESS', start_time, 'ODS.PKK_ADDRESS', 'ERR', SQLERRM, cnt_MODIF, -1, p_ADD_INFO);
END;
