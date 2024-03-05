# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC IF EXISTS 수정, CATALOG, Schema 수정 <br>
# MAGIC 변수 명 -> var. , SET 추가 <br>
# MAGIC ISNULL -> IFNULL<br>
# MAGIC current_timestamp<br>
# MAGIC timestamp sub 변경 -> timestampdiff<br>
# MAGIC rowid 사용 체크<br>
# MAGIC rowcount 구문 python으로 변경

# COMMAND ----------

try:
    import datetime
    now = str(datetime.datetime.now())
    print(now)
    spark.conf.set('var.vts_start_dttm_py', now)
    spark.conf.set('var.vts_start_dttm', spark.conf.get('var.vts_start_dttm_py'))
    spark.conf.set('var.vnv_procedure_name', 'MART.DWUSER.UP_BI_D_MAIN_DEAL_INFO_UPD_01')
    spark.conf.set('var.vnv_job_name', dbutils.widgets.get('vnv_in_job_name'))
    spark.conf.set('var.vnv_job_status', 'SUCCESS')
    spark.conf.set('var.vnv_job_arguments', dbutils.widgets.get('vnc_in_std_ymd') +' | ' + dbutils.widgets.get('vnc_in_end_ymd'))
    spark.conf.set('var.vnv_job_description', '티몬 일별 매출 실적 적재_정상')
    spark.conf.set('var.vnv_source_table_name', 'MART.DWUSER.BI_D_DEAL, MART.DWUSER.BI_D_DEAL_INFO, MART.DWUSER.BI_D_TN_PROMOTION, MART.DWUSER.VW_BI_D_AD_DEAL_INFO, MART.DWUSER.BI_D_MAIN_DEAL_MASTER_INFO')
    spark.conf.set('var.vnv_target_table_name', 'MART.DWUSER.BI_D_MAIN_DEAL_INFO')
    spark.conf.set('var.vii_error_code', 0)
    spark.conf.set('var.vnv_error_msg', '')

    vdt_st_dt = datetime.datetime.strptime(dbutils.widgets.get('vnc_in_std_ymd'), '%Y-%m-%dT%H:%M:%S.%f%z')
    vdt_ed_dt = datetime.datetime.strptime(dbutils.widgets.get('vnc_in_end_ymd'), '%Y-%m-%dT%H:%M:%S.%f%z')
    vdt_st_dt = vdt_st_dt.strftime('%Y-%m-%d')
    vdt_ed_dt = vdt_ed_dt.strftime('%Y-%m-%d')
    spark.conf.set('var.vdt_st_dt', vdt_st_dt)
    spark.conf.set('var.vdt_ed_dt', vdt_ed_dt)


    spark.conf.set('var.vsi_process_step', 1)
    print('step 1')
    spark.sql("""
    --SET var.vsi_process_step = 1; --작업 Step 1
    --raise notice 'step 1';
    --SELECT raise_error('step 1');

    -- 참조되는 테이블의 Load_dt를 참고하여 갱신된 데이터 list 생성
    -- vdt_ed_dt 조건절에 추가하는 경우 누락되는 현상 발생으로 제외
    DROP TABLE IF EXISTS tmon_netezza.mart.TMP_ALTER_LIST;""")
    spark.sql("""
    CREATE TABLE tmon_netezza.mart.TMP_ALTER_LIST AS
    SELECT DISTINCT
    MAIN_DEAL_SRL
    FROM (
        SELECT DISTINCT
        MAIN_DEAL_SRL
        FROM tmon_netezza.mart.BI_D_DEAL
        WHERE LOAD_DT >= '${var.vdt_st_dt}'
        UNION
        SELECT DISTINCT
        D.MAIN_DEAL_SRL
        FROM tmon_netezza.ODS.ODS_DEAL_OPTION_EXTRA A
        JOIN tmon_netezza.mart.BI_D_DEAL D ON A.DEAL_SRL = D.DEAL_SRL
        WHERE A.LOAD_DT >= '${var.vdt_st_dt}'
        UNION
        SELECT DISTINCT
        D.MAIN_DEAL_SRL
        FROM tmon_netezza.ODS.ODS_DEAL_MAX M
        JOIN tmon_netezza.mart.BI_D_DEAL D ON M.DEAL_SRL = D.DEAL_SRL AND M.DEAL_MAX_SRL = D.DEAL_MAX_SRL
        WHERE M.LOAD_DT >= '${var.vdt_st_dt}'
    ) TMP
    ;""")
    spark.sql("""
    -- ##01.데이터 비우기
    TRUNCATE TABLE tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_01;
    """)
    spark.sql("""
    TRUNCATE TABLE tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_02;
    """)



    spark.conf.set('var.vsi_process_step', 2)
    print('step 2')
    spark.sql("""    
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
        """)




    spark.conf.set('var.vsi_process_step', 3)#작업 Step 3
    print('step 3')
    spark.sql("""
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
    """)


    spark.conf.set('var.vsi_process_step', 4)#작업 Step 4
    print('step 4')
    spark.sql(
    """    -- ##06.데이터 삭제
    DELETE FROM tmon_netezza.mart.BI_D_MAIN_DEAL_INFO
    WHERE MAIN_DEAL_SRL IN (
    SELECT A.MAIN_DEAL_SRL 
    FROM tmon_netezza.mart.BI_D_MAIN_DEAL_INFO AS A
    JOIN tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_01 AS B
    ON A.MAIN_DEAL_SRL = B.MAIN_DEAL_SRL
    AND A.DEAL_KIND = B.DEAL_KIND
    );""")




    spark.conf.set('var.vsi_process_step','5')#작업 Step 5
    print('step 5')

    df = spark.sql("""
    -- ##07.데이터 입력
    INSERT INTO tmon_netezza.mart.BI_D_MAIN_DEAL_INFO (
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
    , ENCOREDEAL_FLAG --앵콜딜 여부
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
    , EQUAL_PRICE_YN
    , EQUAL_RATE_YN
    , LOW_RATE_YN
    , ONE_MAN_MAX_COUNT_OPTION
    , MAX_COUNT
    , DEAL_DELIVERY_POLICY
    , DEAL_DELIVERY_AMOUNT
    , DEAL_DELIVERY_IF_AMOUNT
    , USER_VIEW_YN
    , LOAD_DT
    ) 
    SELECT 
        TA.MAIN_DEAL_SRL
    , TA.DEAL_KIND
    , TA.MAIN_DEAL_NM
    , TA.START_DATE
    , TA.END_DATE
    , TA.TICKET_START_DATE
    , TA.TICKET_END_DATE
    , TA.STATUS_TYPE
    , TA.PARTNER_SRL
    , TA.CATEGORY_SRL
    , TA.DEAL_TYPE
    , TA.DELIVERY_SPOT
    , TA.TAG_SRL
    , TA.DEAL_AMOUNT
    , TA.DEAL_DISAMOUNT
    , TA.DEAL_DISRATE
    , TA.ENCOREDEAL_FLAG --앵콜딜 여부
    , TA.IS_EXTERNAL_DEAL
    , TA.DELIVERY_TYPE
    , TA.IS_FDIRECT_DELIVERY
    , TA.ALWAYS_SALE
    , TA.DEAL_FROM
    , TA.ADULT_TYPE
    , TA.USE_DIRECT_PIN
    , TA.ONE_MAN_MAX_COUNT
    , TA.STOCK_COUNT
    , TA.ACTION_FROM
    , TA.CONTENTS_MAKING
    , TA.CD_STATUS_TYPE
    , TB.EQUAL_PRICE_YN
    , TB.EQUAL_RATE_YN
    , TB.LOW_RATE_YN
    , TB.ONE_MAN_MAX_COUNT_OPTION
    , TB.MAX_COUNT
    , TA.DEAL_DELIVERY_POLICY
    , TA.DEAL_DELIVERY_AMOUNT
    , TA.DEAL_DELIVERY_IF_AMOUNT
    , TA.USER_VIEW_YN
    , TA.LOAD_DT
    FROM tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_01 TA
    LEFT JOIN tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_02 TB ON TA.MAIN_DEAL_SRL = TB.MAIN_DEAL_SRL AND TA.DEAL_KIND = 'T'
    ;""")
    spark.conf.set('var.row_count',df.first()['num_inserted_rows'])#작업 Step 5



    spark.conf.set('var.vsi_process_step','6')#작업 Step 6
    print('step 6')

    spark.sql("""
    -- ##08.작업테이블 데이터 비우기
    TRUNCATE TABLE tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_01;""")
    spark.sql("""
    TRUNCATE TABLE tmon_netezza.mart.BI_D_MAIN_DEAL_INFO_WORK_02;""")
    # SAVE THE COUNT INSERTED
    spark.conf.set('var.vii_insert_cnt', spark.conf.get('var.row_count'))
    spark.conf.set('var.vii_select_cnt', spark.conf.get('var.vii_insert_cnt'))
    
    now = str(datetime.datetime.now())
    print(now)
    spark.conf.set('var.vts_finish_dttm_py', now)

    #========================================================================================
    #=============================== Job Log 정보 Setting Parts =============================
    #========================================================================================
    spark.conf.set('var.vts_finish_dttm', spark.conf.get('var.vts_finish_dttm_py'))
    d1 = datetime.datetime.strptime(spark.conf.get('var.vts_finish_dttm'), '%Y-%m-%d %H:%M:%S.%f')
    d2 = datetime.datetime.strptime(spark.conf.get('var.vts_start_dttm'), '%Y-%m-%d %H:%M:%S.%f')
    vnv_elapsed_time = (d1-d2).total_seconds()
    spark.conf.set('var.vnv_elapsed_time', int(vnv_elapsed_time))
    spark.conf.set('var.vii_source_cnt', spark.conf.get('var.vii_select_cnt'))
    spark.conf.set('var.vii_target_cnt', spark.conf.get('var.vii_insert_cnt'))
    spark.conf.set('var.vnv_result', spark.conf.get('var.vnv_job_name') + '.' + spark.conf.get('var.vnv_procedure_name') + ': [ELAPSED_TIME:' + spark.conf.get('var.vnv_elapsed_time')+', COUNT:'+spark.conf.get('var.vii_target_cnt')+']')
    print(spark.conf.get('var.vnv_result'))
    spark.sql("""
    INSERT into tmon_netezza.log.job_runlog values('${var.vts_start_dttm}'
                            ,'${var.vnv_job_name}'
                            ,'${var.vnv_procedure_name}'
                            ,${var.vsi_process_step}
                            ,'${var.vnv_job_status}'
                            ,'${var.vts_finish_dttm}'
                            ,${var.vnv_elapsed_time}
                            ,'${var.vnv_source_table_name}'
                            ,${var.vii_source_cnt}
                            ,'${var.vnv_target_table_name}'
                            ,${var.vii_target_cnt}
                            ,'${var.vnv_error_msg}'
                            ,'${var.vnv_job_arguments}'
                            ,'${var.vnv_job_description}'
                            ,now());""")
except Exception as e:
    spark.conf.set('vts_finish_dttm', str(datetime.datetime.now()))# -- 작업 종료 일시
    d1 = datetime.datetime.strptime(spark.conf.get('var.vts_finish_dttm'), '%Y-%m-%d %H:%M:%S.%f')
    d2 = datetime.datetime.strptime(spark.conf.get('var.vts_start_dttm'), '%Y-%m-%d %H:%M:%S.%f')
    spark.conf.set('var.vnv_elapsed_time',int((d1-d2).total_seconds()))# -- 작업 소요 시간
    spark.conf.set('var.vnv_job_status', 'ABORT')
    spark.conf.set('var.vii_error_code', 888)
    #print(e)
    # rint(str(spark.conf.get('var.vii_error_code')))
    # print('Error Code [' + str(spark.conf.get('var.vii_error_code')) + '] : ' + str(e))
    spark.conf.set('var.vnv_error_msg', 'Error Code [' + str(spark.conf.get('var.vii_error_code')) + '] : ' + str(e)[:50]);
    print('ERROR_MSG => %' + spark.conf.get('var.vnv_error_msg'))
    spark.sql("""
INSERT into tmon_netezza.log.job_runlog values('${var.vts_start_dttm}'
                          ,'${var.vnv_job_name}'
                          ,'${var.vnv_procedure_name}'
                          ,${var.vsi_process_step}
                          ,'${var.vnv_job_status}'
                          ,'${var.vts_finish_dttm}'
                          ,${var.vnv_elapsed_time}
                          ,'${var.vnv_source_table_name}'
                          ,${var.vii_source_cnt}
                          ,'${var.vnv_target_table_name}'
                          ,${var.vii_target_cnt}
                          ,'${var.vnv_error_msg}'
                          ,'${var.vnv_job_arguments}'
                          ,'${var.vnv_job_description}'
                          ,now());""")

# COMMAND ----------

try:
    spark.sql('select * from daslfkajselesf')
except Exception as e:
    print(''+str(e))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmon_netezza.log.job_runlog
