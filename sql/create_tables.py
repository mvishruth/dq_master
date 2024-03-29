sql_create_final_tables = """ CREATE TABLE IF NOT EXISTS {table_name} (
                                tbl_id integer PRIMARY KEY,
                                schema_tableName text NOT NULL, 
                                process_id integer, 
                                status text, 
                                last_update_date_column text, 
                                primary_identifier_Column varchar,
                                total_downstreams integer, 
                                final_table_description text , 
                                modified_time datetime, 
                                modified_by text
                            ); """
# ID,Process_Title,Process_Platform,Refresh_Schedule,Process_Status,
# Comments,L0_Process,MSA_Owned,Compliance,FinalTable_DB,Data_Hierarchy_ID,
# Process_Description,Github_Link,UpdatedDate_Compliance,Modified,Modified By

sql_create_process_info = """
CREATE TABLE IF NOT EXISTS {table_name} (
    process_id INTEGER PRIMARY KEY NOT NULL,
    process_title TEXT,
    process_platform TEXT,
    refresh_schedule TEXT,
    process_status TEXT,
    comments TEXT,
    l0_process BLOB,
    msa_owned BLOB,
    compliance TEXT,
    finaltable_DB TEXT,
    data_hierarchy_id INTEGER,
    process_description TEXT,
    github_link URL,
    updatedDate_Compliance TEXT,
    modified_time TIMESTAMP,
    modified_by TEXT
);
"""

sql_dq_master_tbl = """
    DROP TABLE IF EXISTS dq_master;
    CREATE TABLE dq_master as 
    SELECT
        TBL_ID, SCHEMA_TABLENAME
        --,PROCESS_ID
        ,STATUS,LAST_UPDATE_DATE_COLUMN,
        PRIMARY_IDENTIFIER_COLUMN,TOTAL_DOWNSTREAMS,
        FINAL_TABLE_DESCRIPTION,tft.MODIFIED_TIME as TBL_MODIFIED_TIME, tft.MODIFIED_BY as TBL_MODIFIED_BY,

        tpi.PROCESS_ID,PROCESS_TITLE,PROCESS_PLATFORM,REFRESH_SCHEDULE,PROCESS_STATUS,
        COMMENTS,L0_PROCESS,MSA_OWNED,COMPLIANCE,FINALTABLE_DB,
        DATA_HIERARCHY_ID,PROCESS_DESCRIPTION,GITHUB_LINK,
        UPDATEDDATE_COMPLIANCE,tpi.MODIFIED_TIME AS PROCESS_MODIFIED_TIME, tpi.MODIFIED_BY as PROCESS_MODIFIED_BY,
        
        CAST(NULL AS DATETIME) AS scd_from,
        CAST(NULL AS DATETIME) AS scd_to ,
        NULL as DQ_FLAG,
        NULL as BIG_TABLE_FLG,
        NULL as LAST_DQ_RUN_DATE,
        NULL as DQ_START_TS,
        NULL as DQ_END_TS
    FROM
        tmp_final_tables as tft
    LEFT JOIN tmp_process_info as tpi on
        tft.process_id = tpi.process_id ;

"""