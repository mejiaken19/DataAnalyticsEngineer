WITH combined_accounts_table AS (
    SELECT t1.*, a.gl_account_no, a.source 
    FROM src_xform.account_reg_table a
    LEFT JOIN (
        SELECT 
            No, 
            MAX(CAST(SUBSTRING(SystemModifiedAt, 1, 27) as DATETIME2(6))) as max_lmdt, 
            MAX(CAST(adf_pipeline_run_time as datetime2)) AS max_adfprt
        FROM src.bc_accounts GROUP BY No
    ) t2 ON a.gl_account_no = t2.No
    LEFT JOIN src.bc_accounts t1
    ON t1.No = t2.No AND t1.SystemModifiedAt = t2.max_lmdt AND t1.adf_pipeline_run_time = t2.max_adfprt
    WHERE t1.No IS NOT NULL
    UNION
    SELECT t1.*, a.gl_account_no, a.source 
    FROM src_xform.account_reg_table a
    LEFT JOIN (
        SELECT 
            No, 
            MAX(CAST(SUBSTRING(SystemModifiedAt, 1, 27) as DATETIME2(6))) as max_lmdt, 
            MAX(CAST(adf_pipeline_run_time as datetime2)) AS max_adfprt
        FROM src.bc_csa_accounts GROUP BY No
    ) t2 ON a.gl_account_no = t2.No
    LEFT JOIN src.company2_accounts t1
    ON t1.No = t2.No AND t1.SystemModifiedAt = t2.max_lmdt AND t1.adf_pipeline_run_time = t2.max_adfprt
    WHERE a.source = 'company2_accounts'
    AND t1.No NOT IN (SELECT gl_account_no FROM src_xform.account_reg_table WHERE a.source = 'company1_accounts')
    UNION
    SELECT t1.*, a.gl_account_no, a.source 
    FROM src_xform.account_reg_table a
    LEFT JOIN (
        SELECT 
            No, 
            MAX(CAST(SUBSTRING(SystemModifiedAt, 1, 27) as DATETIME2(6))) as max_lmdt, 
            MAX(CAST(adf_pipeline_run_time as datetime2)) AS max_adfprt
        FROM src.company3_accounts GROUP BY No
    ) t2 ON a.gl_account_no = t2.No
    LEFT JOIN src.company3_accounts t1
    ON t1.No = t2.No AND t1.SystemModifiedAt = t2.max_lmdt AND t1.adf_pipeline_run_time = t2.max_adfprt
    WHERE a.source = 'company3_accounts'
    AND t1.No NOT IN (SELECT gl_account_no FROM src_xform.account_reg_table WHERE a.source IN ('company1_accounts', 'company2_accounts'))
    UNION
    SELECT t1.*, a.gl_account_no, a.source 
    FROM src_xform.account_reg_table a
    LEFT JOIN (
        SELECT 
            No, 
            MAX(CAST(SUBSTRING(SystemModifiedAt, 1, 27) as DATETIME2(6))) as max_lmdt, 
            MAX(CAST(adf_pipeline_run_time as datetime2)) AS max_adfprt
        FROM src.company4_accounts GROUP BY No
    ) t2 ON a.gl_account_no = t2.No
    LEFT JOIN src.company4_accounts t1
    ON t1.No = t2.No AND t1.SystemModifiedAt = t2.max_lmdt AND t1.adf_pipeline_run_time = t2.max_adfprt
    WHERE a.source = 'company4_accounts'
    AND t1.No NOT IN (SELECT gl_account_no FROM src_xform.account_reg_table WHERE a.source IN ('company1_accounts', 'company2_accounts','company3_accounts'))
)

INSERT INTO itg.account
SELECT 
    gl_account_no,
    COALESCE(Search_Name, 'Not Available') as display_name, 
    COALESCE(Income_Balance, 'Not Available') as income_balance,
    COALESCE(NULLIF(Account_Category, ''), 'Not Available') as account_category, 
    COALESCE(NULLIF(Account_Subcategory_Descript, ''), 'Not Available') as account_subcategory, 
    COALESCE(Account_Type, 'Not Available') as account_type, 
    COALESCE(CAST(SUBSTRING(SystemModifiedAt, 1, 27) as DATETIME2(6)), '9999-01-01') as last_modified_datetime,
    COALESCE(SUBSTRING(SystemModifiedAt, 28, 34), 'Not Available') as last_modified_datetime_utc_offset,
    GETDATE() as dw_insert_timestamp
FROM combined_accounts_table
