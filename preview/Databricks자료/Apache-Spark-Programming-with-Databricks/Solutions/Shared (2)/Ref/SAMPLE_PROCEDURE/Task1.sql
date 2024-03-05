-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC IF EXISTS 수정, CATALOG, Schema 수정 <br>
-- MAGIC 변수 명 -> var. , SET 추가 <br>
-- MAGIC ISNULL -> IFNULL<br>
-- MAGIC current_timestamp<br>
-- MAGIC timestamp sub 변경 -> timestampdiff<br>
-- MAGIC rowid 사용 체크<br>
-- MAGIC rowcount 구문 python으로 변경

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import datetime
-- MAGIC now = str(datetime.datetime.now())
-- MAGIC print(now)
-- MAGIC spark.conf.set('var.vts_start_dttm_py', now)

-- COMMAND ----------


SET var.vts_start_dttm        = TIMESTAMP('${var.vts_start_dttm_py}'); -- 작업 시작일시
SET var.vnv_procedure_name    = 'MART.DWUSER.UP_BI_D_MAIN_DEAL_INFO_UPD_01'  ;
SET var.vnv_job_name          = "${vnv_in_job_name}";
SET var.vnv_job_status        = 'SUCCESS';
SET var.vnv_job_arguments     = '${vnc_in_std_ymd}' ||' | ' || '${vnc_in_end_ymd}' ;
SET var.vnv_job_description   = '티몬 일별 매출 실적 적재_정상';
SET var.vnv_source_table_name = 'MART.DWUSER.BI_D_DEAL, MART.DWUSER.BI_D_DEAL_INFO, MART.DWUSER.BI_D_TN_PROMOTION, MART.DWUSER.VW_BI_D_AD_DEAL_INFO, MART.DWUSER.BI_D_MAIN_DEAL_MASTER_INFO';
SET var.vnv_target_table_name = 'MART.DWUSER.BI_D_MAIN_DEAL_INFO';
SET var.vii_error_code          = 0    ;
SET var.vnv_error_msg           = NULL ;

    -- input 변수 변환
SET var.vdt_st_dt = DATE('${vnc_in_std_ymd}');
SET var.vdt_ed_dt = DATE('${vnc_in_end_ymd}');

-- COMMAND ----------


SET var.vsi_process_step = 1; --작업 Step 1
--raise notice 'step 1';
SELECT raise_error('step 1');

-- 참조되는 테이블의 Load_dt를 참고하여 갱신된 데이터 list 생성
-- vdt_ed_dt 조건절에 추가하는 경우 누락되는 현상 발생으로 제외
DROP TABLE IF EXISTS tmon_netezza.mart.TMP_ALTER_LIST;
CREATE TABLE tmon_netezza.mart.TMP_ALTER_LIST AS
SELECT DISTINCT
MAIN_DEAL_SRL
FROM (
    SELECT DISTINCT
    MAIN_DEAL_SRL
    FROM tmon_netezza.mart.BI_D_DEAL
    WHERE LOAD_DT >= ${var.vdt_st_dt}
    UNION
    SELECT DISTINCT
    D.MAIN_DEAL_SRL
    FROM tmon_netezza.ODS.ODS_DEAL_OPTION_EXTRA A
    JOIN tmon_netezza.mart.BI_D_DEAL D ON A.DEAL_SRL = D.DEAL_SRL
    WHERE A.LOAD_DT >= ${var.vdt_st_dt}
    UNION
    SELECT DISTINCT
    D.MAIN_DEAL_SRL
    FROM tmon_netezza.ODS.ODS_DEAL_MAX M
    JOIN tmon_netezza.mart.BI_D_DEAL D ON M.DEAL_SRL = D.DEAL_SRL AND M.DEAL_MAX_SRL = D.DEAL_MAX_SRL
    WHERE M.LOAD_DT >= ${var.vdt_st_dt}
) TMP
;
-- ##01.데이터 비우기
TRUNCATE TABLE tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_01;
TRUNCATE TABLE tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_02;

-- COMMAND ----------

SET var.vsi_process_step = 2; --작업 Step 2
SELECT raise_error('step 2');
    
    -- ##02.데이터 적재 (main_deal_srl 기준 정보들)
    INSERT INTO tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_01 (
      MAIN_DEAL_SRL
    , DEAL_KIND
    , MAIN_DEAL_NM
    , START_DATE
    , END_DATE
    , TICKET_START_DATE
    , TICKET_END_DATE
    , STATUS_TYPE
    , PARTNER_SRL
    , CATEGORY_SRL
    , DEAL_TYPE
    , DELIVERY_SPOT
    , TAG_SRL
    , DEAL_AMOUNT
    , DEAL_DISAMOUNT
    , DEAL_DISRATE
    , ENCOREDEAL_FLAG 
    , IS_EXTERNAL_DEAL
    , DELIVERY_TYPE
    , IS_FDIRECT_DELIVERY
    , ALWAYS_SALE
    , DEAL_FROM
    , ADULT_TYPE
    , USE_DIRECT_PIN
    , ONE_MAN_MAX_COUNT	
	, STOCK_COUNT
    , ACTION_FROM
    , CONTENTS_MAKING
    , CD_STATUS_TYPE
    , DEAL_DELIVERY_POLICY
    , DEAL_DELIVERY_AMOUNT
    , DEAL_DELIVERY_IF_AMOUNT
    , USER_VIEW_YN
    , LOAD_DT
    )
    SELECT 
        D.MAIN_DEAL_SRL,
        'T' AS DEAL_KIND, -- T:티켓상품, N;나우상품
        D.TITLE AS MAIN_DEAL_NM,
        CAST(D.START_DATE AS DATE) AS START_DATE, -- 딜전시시작일
        CAST(D.END_DATE AS DATE) AS END_DATE, -- 딜전시종료일
        CAST(D.TICKET_START_DATE AS DATE) AS TICKET_START_DATE, -- 티켓딜사용시작일
        CAST(D.TICKET_END_DATE AS DATE) AS TICKET_END_DATE, -- 티켓딜사용종료일
        D.STATUS_TYPE, -- 딜상태    
        D.PARTNER_SRL, -- 파트너번호
        D.CATEGORY_SRL, -- 카테고리
        D.DEAL_TYPE, -- 딜타입
        IFNULL(D.DELIVERY_SPOT,'^') AS DELIVERY_SPOT, -- 딜배송주체 (DIRECT : 판매자 직접배송, LIKE 'TMON%' : 티몬, ^ : 기타)
        IFNULL(D.TAG_SRL,9999) AS TAG_SRL, -- 딜컨셉카테고리코드
        D.DEAL_AMOUNT, -- 딜판매가
        D.DEAL_DISAMOUNT, -- 딜할인판매가
        D.DEAL_DISRATE,  -- 딜할인율
        IFNULL(DI.FLAG_ENCORE,'N') AS ENCOREDEAL_FLAG,  -- 앵콜딜여부
        --2019.09.25 딜정보 추가 JHJANG
        D.IS_EXTERNAL_DEAL,  --외부연동 딜
        D.DELIVERY_TYPE,  --배송유형
        D.IS_FDIRECT_DELIVERY,  --해외직배송딜여부
        D.ALWAYS_SALE,  --상시판매
        D.DEAL_FROM,  --딜 최초 등록한 곳
        D.ADULT_TYPE,  --성인 인증 유형
        D.USE_DIRECT_PIN,  --사용다이렉트핀
        D.ONE_MAN_MAX_COUNT, -- 인당 구매제한 개수 (대표딜 기준, main_deal_srl = deal_srl)
        (COALESCE(M.MAX_COUNT,0) - COALESCE(M.BUY_COUNT,0) + COALESCE(M.CANCEL_COUNT,0) + COALESCE(M.WAIT_MAX_COUNT,0)) AS STOCK_COUNT, -- 재고수량
        NULL AS ACTION_FROM,
        NULL AS CONTENTS_MAKING,
        NULL AS CD_STATUS_TYPE,
        D.DELIVERY_POLICY AS DEAL_DELIVERY_POLICY,
        D.DELIVERY_AMOUNT AS DEAL_DELIVERY_AMOUNT,
        D.DELIVERY_IF_AMOUNT AS DEAL_DELIVERY_IF_AMOUNT,
        CASE WHEN
            D.STATUS_TYPE = 'AV' -- 상태
            AND D.IS_HIDE_DEAL = 'N' -- 폐쇄딜 여부 (deal_info.is_pause 는 동기화 이슈있음)
            AND D.IS_CONTRACT = 'Y' -- 전자계약 여부 (deal_info.is_pause 는 동기화 이슈있음)
            AND D.IS_PAUSE = 'N' -- 딜 일시중지 여부 (deal_info.is_pause 는 동기화 이슈있음)
            AND DI.FLAG_VIEW = 'Y' -- 노출여부
        THEN 'Y' ELSE 'N' END AS USER_VIEW_YN,
        NOW() AS LOAD_DT
    FROM tmon_netezza.mart.TMP_ALTER_LIST TMP
    JOIN tmon_netezza.mart.BI_D_DEAL D on tmp.main_deal_srl = d.main_deal_srl
    LEFT JOIN tmon_netezza.ods.ODS_DEAL_MAX M ON D.DEAL_MAX_SRL = M.DEAL_MAX_SRL
    LEFT JOIN tmon_netezza.mart.BI_D_DEAL_INFO DI ON D.MAIN_DEAL_SRL = DI.MAIN_DEAL_SRL
    WHERE D.DEAL_SRL = D.MAIN_DEAL_SRL
    ;

-- COMMAND ----------

SET var.vsi_process_step = 3; --작업 Step 3
SELECT raise_error('step 3');

    -- ##03.데이터 적재 (deal_srl 참조가 필요한 정보들)
    INSERT INTO tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_02 (
      MAIN_DEAL_SRL
    , EQUAL_PRICE_YN
    , EQUAL_RATE_YN
    , LOW_RATE_YN
    , ONE_MAN_MAX_COUNT_OPTION
    , MAX_COUNT
    , LOAD_DT
    )
    SELECT
        O.MAIN_DEAL_SRL
        ,CASE WHEN O.EQUAL_PRICE_YN = 1 THEN 'Y' ELSE 'N' END AS EQUAL_PRICE_YN
        ,CASE WHEN O.EQUAL_RATE_CNT = 1 THEN 'Y' ELSE 'N' END AS EQUAL_RATE_YN
        ,CASE WHEN O.RATE_NULL > 0 THEN 'X'
              WHEN O.RATE_NULL = 0 AND O.RATE_UNDER_3 > 0 THEN 'Y'
         ELSE 'N' END AS LOW_RATE_YN
        ,NVL(O.ONE_MAN_MAX_COUNT_AV, O.ONE_MAN_MAX_COUNT) AS ONE_MAN_MAX_COUNT_OPTION
        ,NVL(O.MAX_COUNT_AV, O.MAX_COUNT) AS MAX_COUNT
        ,NOW() AS LOAD_DT
    FROM (
        SELECT
            D.MAIN_DEAL_SRL
            ,COUNT(DISTINCT D.DEAL_DISAMOUNT) AS EQUAL_PRICE_YN
            ,COUNT(DISTINCT CASE WHEN D.MAIN_DEAL_SRL <> D.DEAL_SRL AND D.STATUS_TYPE = 'AV' AND D.IS_OPT_DISPLAY = 'Y' THEN NVL(D.TMON_RATE,0) END) AS EQUAL_RATE_CNT
            ,COUNT(DISTINCT CASE WHEN D.MAIN_DEAL_SRL <> D.DEAL_SRL AND D.STATUS_TYPE = 'AV' AND D.IS_OPT_DISPLAY = 'Y' AND NVL(D.TMON_RATE,0) < 3.3 THEN 1 END) AS RATE_UNDER_3
            ,COUNT(DISTINCT CASE WHEN D.MAIN_DEAL_SRL <> D.DEAL_SRL AND D.STATUS_TYPE = 'AV' AND D.IS_OPT_DISPLAY = 'Y' AND NVL(D.TMON_RATE,99999) = 99999 THEN 1 END) AS RATE_NULL
            ,MAX(CASE WHEN DE.IS_MAIN_OPTION = 1 AND D.STATUS_TYPE = 'AV' THEN D.ONE_MAN_MAX_COUNT END) AS ONE_MAN_MAX_COUNT_AV
            ,MAX(CASE WHEN DE.IS_MAIN_OPTION = 1 THEN D.ONE_MAN_MAX_COUNT END) AS ONE_MAN_MAX_COUNT
            ,MAX(CASE WHEN DE.IS_MAIN_OPTION = 1 AND D.STATUS_TYPE = 'AV' THEN DM.MAX_COUNT END) AS MAX_COUNT_AV
            ,MAX(CASE WHEN DE.IS_MAIN_OPTION = 1 THEN DM.MAX_COUNT END) AS MAX_COUNT
        FROM tmon_netezza.mart.TMP_ALTER_LIST TMP
        JOIN tmon_netezza.mart.BI_D_DEAL D on tmp.main_deal_srl = d.main_deal_srl
        LEFT JOIN tmon_netezza.ods.ODS_DEAL_OPTION_EXTRA DE ON DE.DEAL_SRL = D.DEAL_SRL
        LEFT JOIN tmon_netezza.ods.ODS_DEAL_MAX DM ON D.DEAL_SRL = DM.DEAL_SRL AND D.DEAL_MAX_SRL = DM.DEAL_MAX_SRL
        GROUP BY D.MAIN_DEAL_SRL
    ) O
    ;

-- COMMAND ----------

SET var.vsi_process_step = 4; --작업 Step 4

SELECT raise_error('step 4');
    -- ##06.데이터 삭제
DELETE FROM tmon_netezza.mart.BI_D_MAIN_DEAL_INFO
WHERE MAIN_DEAL_SRL IN (
SELECT A.MAIN_DEAL_SRL 
FROM tmon_netezza.mart.BI_D_MAIN_DEAL_INFO AS A
JOIN tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_01 AS B
ON A.MAIN_DEAL_SRL = B.MAIN_DEAL_SRL
AND A.DEAL_KIND = B.DEAL_KIND
);
	
	

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set('var.vsi_process_step','5')#작업 Step 5
-- MAGIC
-- MAGIC df = spark.sql("""
-- MAGIC -- ##07.데이터 입력
-- MAGIC INSERT INTO tmon_netezza.mart.BI_D_MAIN_DEAL_INFO (
-- MAGIC 	MAIN_DEAL_SRL
-- MAGIC , DEAL_KIND
-- MAGIC , MAIN_DEAL_NM
-- MAGIC , START_DATE
-- MAGIC , END_DATE
-- MAGIC , TICKET_START_DATE
-- MAGIC , TICKET_END_DATE
-- MAGIC , STATUS_TYPE
-- MAGIC , PARTNER_SRL
-- MAGIC , CATEGORY_SRL
-- MAGIC , DEAL_TYPE
-- MAGIC , DELIVERY_SPOT
-- MAGIC , TAG_SRL
-- MAGIC , DEAL_AMOUNT
-- MAGIC , DEAL_DISAMOUNT
-- MAGIC , DEAL_DISRATE
-- MAGIC , ENCOREDEAL_FLAG --앵콜딜 여부
-- MAGIC , IS_EXTERNAL_DEAL
-- MAGIC , DELIVERY_TYPE
-- MAGIC , IS_FDIRECT_DELIVERY
-- MAGIC , ALWAYS_SALE
-- MAGIC , DEAL_FROM
-- MAGIC , ADULT_TYPE
-- MAGIC , USE_DIRECT_PIN
-- MAGIC , ONE_MAN_MAX_COUNT	
-- MAGIC , STOCK_COUNT
-- MAGIC , ACTION_FROM
-- MAGIC , CONTENTS_MAKING
-- MAGIC , CD_STATUS_TYPE
-- MAGIC , EQUAL_PRICE_YN
-- MAGIC , EQUAL_RATE_YN
-- MAGIC , LOW_RATE_YN
-- MAGIC , ONE_MAN_MAX_COUNT_OPTION
-- MAGIC , MAX_COUNT
-- MAGIC , DEAL_DELIVERY_POLICY
-- MAGIC , DEAL_DELIVERY_AMOUNT
-- MAGIC , DEAL_DELIVERY_IF_AMOUNT
-- MAGIC , USER_VIEW_YN
-- MAGIC , LOAD_DT
-- MAGIC ) 
-- MAGIC SELECT 
-- MAGIC 	TA.MAIN_DEAL_SRL
-- MAGIC , TA.DEAL_KIND
-- MAGIC , TA.MAIN_DEAL_NM
-- MAGIC , TA.START_DATE
-- MAGIC , TA.END_DATE
-- MAGIC , TA.TICKET_START_DATE
-- MAGIC , TA.TICKET_END_DATE
-- MAGIC , TA.STATUS_TYPE
-- MAGIC , TA.PARTNER_SRL
-- MAGIC , TA.CATEGORY_SRL
-- MAGIC , TA.DEAL_TYPE
-- MAGIC , TA.DELIVERY_SPOT
-- MAGIC , TA.TAG_SRL
-- MAGIC , TA.DEAL_AMOUNT
-- MAGIC , TA.DEAL_DISAMOUNT
-- MAGIC , TA.DEAL_DISRATE
-- MAGIC , TA.ENCOREDEAL_FLAG --앵콜딜 여부
-- MAGIC , TA.IS_EXTERNAL_DEAL
-- MAGIC , TA.DELIVERY_TYPE
-- MAGIC , TA.IS_FDIRECT_DELIVERY
-- MAGIC , TA.ALWAYS_SALE
-- MAGIC , TA.DEAL_FROM
-- MAGIC , TA.ADULT_TYPE
-- MAGIC , TA.USE_DIRECT_PIN
-- MAGIC , TA.ONE_MAN_MAX_COUNT
-- MAGIC , TA.STOCK_COUNT
-- MAGIC , TA.ACTION_FROM
-- MAGIC , TA.CONTENTS_MAKING
-- MAGIC , TA.CD_STATUS_TYPE
-- MAGIC , TB.EQUAL_PRICE_YN
-- MAGIC , TB.EQUAL_RATE_YN
-- MAGIC , TB.LOW_RATE_YN
-- MAGIC , TB.ONE_MAN_MAX_COUNT_OPTION
-- MAGIC , TB.MAX_COUNT
-- MAGIC , TA.DEAL_DELIVERY_POLICY
-- MAGIC , TA.DEAL_DELIVERY_AMOUNT
-- MAGIC , TA.DEAL_DELIVERY_IF_AMOUNT
-- MAGIC , TA.USER_VIEW_YN
-- MAGIC , TA.LOAD_DT
-- MAGIC FROM tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_01 TA
-- MAGIC LEFT JOIN tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_02 TB ON TA.MAIN_DEAL_SRL = TB.MAIN_DEAL_SRL AND TA.DEAL_KIND = 'T'
-- MAGIC ;""")
-- MAGIC spark.conf.set('var.row_count',df.first()['num_inserted_rows'])#작업 Step 5
-- MAGIC

-- COMMAND ----------


SET var.vsi_process_step = 6; --작업 Step 6
SELECT raise_error('step 6');


-- ##08.작업테이블 데이터 비우기
TRUNCATE TABLE tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_01;
TRUNCATE TABLE tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_02;

    -- SAVE THE COUNT INSERTED
SET var.vii_insert_cnt = ${var.row_count};
SET var.vii_select_cnt = ${var.vii_insert_cnt};

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import datetime
-- MAGIC now = str(datetime.datetime.now())
-- MAGIC print(now)
-- MAGIC spark.conf.set('var.vts_finish_dttm_py', now)

-- COMMAND ----------


/*======================================================================================
=============================== Job Log 정보 Setting Parts =============================
======================================================================================*/
SET var.vts_finish_dttm = TIMESTAMP('${var.vts_finish_dttm_py}');

SET var.vnv_elapsed_time  = timestampdiff(second, ${var.vts_finish_dttm}, ${var.vts_start_dttm}); -- 작업 소요 시간

SET var.vii_source_cnt = ${var.vii_select_cnt} ;
SET var.vii_target_cnt = ${var.vii_insert_cnt} ;

SET var.vnv_result =  ${var.vnv_job_name} || '.' || ${var.vnv_procedure_name} || ': [ELAPSED_TIME:' || ${var.vnv_elapsed_time} || ', COUNT:' || ${var.vii_target_cnt} || ']' ;
    /* DB Log로 작업 결과 출력 */
SELECT ${var.vnv_result}

-- COMMAND ----------

INSERT into tmon_netezza.log.job_runlog values(${var.vts_start_dttm}
                          ,${var.vnv_job_name}
                          ,${var.vnv_procedure_name}
                          ,${var.vsi_process_step}
                          ,${var.vnv_job_status}
                          ,${var.vts_finish_dttm}
                          ,${var.vnv_elapsed_time}
                          ,${var.vnv_source_table_name}
                          ,${var.vii_source_cnt}
                          ,${var.vnv_target_table_name}
                          ,${var.vii_target_cnt}
                          ,${var.vnv_error_msg}
                          ,${var.vnv_job_arguments}
                          ,${var.vnv_job_description}
                          ,now());

-- COMMAND ----------

EXCEPTION WHEN OTHERS THEN
vts_finish_dttm     := now()  ; -- 작업 종료 일시
vnv_elapsed_time  := vts_finish_dttm - vts_start_dttm ; -- 작업 소요 시간
vnv_job_status := 'ABORT' ;
vii_error_code := 888;
vnv_error_msg := 'Error Code [' || CAST(vii_error_code as nvarchar(10)) || '] : ' ||translate(translate(nvl(SQLERRM,' '),'''','"'), chr(10), '');
raise notice 'ERROR_MSG => %' ,  vnv_error_msg ;

/* Job Log 저장 */
CALL DWUSER.UP_ETL_JOB_RUNLOG(vts_start_dttm
                      ,vnv_job_name
                      ,vnv_procedure_name
                      ,vsi_process_step
                      ,vnv_job_status
                      ,vts_finish_dttm
                      ,vnv_elapsed_time
                      ,vnv_source_table_name
                      ,vii_source_cnt
                      ,vnv_target_table_name
                      ,vii_target_cnt
                      ,vnv_error_msg
                      ,vnv_job_arguments
                      ,vnv_job_description
                      ,now());
raise exception 'ERROR_MSG => %' ,  vnv_error_msg ;
