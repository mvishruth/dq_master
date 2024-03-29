sql_create_final_tables = """ CREATE TABLE IF NOT EXISTS final_tables (
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

sql_create_process_info = """ CREATE TABLE IF NOT EXISTS process_info (
                                process_id integer PRIMARY KEY NOT NULL,
                                process_title text,
                                process_platform text,
                                refresh_schedule text,
                                process_status text ,
                                comments text ,
                                l0_process binary,
                                msa_owned binary,
                                compliance text,
                                finaltable_DB text,
                                data_hierarchy_id integer,
                                process_description text,
                                github_link url,
                                updated datetime,
                                Date_Compliance text,
                                modified_time timestamp,
                                modified_by text
                            ); """
