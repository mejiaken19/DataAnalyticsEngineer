WITH transformed_job_cards AS (
    SELECT 
        jc.task_guid AS job_identifier,
        'Company1' AS source_origin,
        jc.job_no AS source_code,
        t1.Description AS job_description,
        c1.customer_no AS bill_to_customer_number,
        c2.customer_no AS sell_to_customer_number,
        d1.code AS company_code,
        d2.code AS division_code,
        t1.Status AS job_status,
        t1.Creation_Date AS created_at,
        t1.Ending_Date AS ended_at,
        t1.SystemModifiedAt AS last_modified_at,
        t1.SystemModifiedAt AS last_modified_utc_offset
    FROM src_xform.job_card_reg_table jc
    LEFT JOIN (
        SELECT 
            No, 
            MAX(CAST(SUBSTRING(SystemModifiedAt, 1, 27) as DATETIME2(6))) AS max_last_modified_at, 
            MAX(CAST(adf_pipeline_run_time AS datetime2)) AS max_adf_pipeline_run_time
        FROM src.bc_job_card GROUP BY No
    ) t2 ON jc.job_no = t2.No
    LEFT JOIN src.bc_job_card t1
    ON t1.No = t2.No AND CAST(SUBSTRING(t1.SystemModifiedAt, 1, 27) as DATETIME2(6)) = t2.max_last_modified_at 
    AND CAST(t1.adf_pipeline_run_time AS datetime2) = t2.max_adf_pipeline_run_time
    LEFT JOIN src_xform.customer_reg_table c1 
    ON c1.customer_no = t1.Bill_to_Customer_No AND c1.source IN ('bc_gl_entries', 'bc_customer', 'bc_job_card')
    LEFT JOIN src_xform.customer_reg_table c2
    ON c2.customer_no = t1.Sell_to_Customer_No AND c2.source IN ('bc_gl_entries', 'bc_customer', 'bc_job_card')
    LEFT JOIN src_xform.bc_dimension_reg_table d1
    ON d1.dimension_code = 'ENTITY' AND d1.code = t1.Global_Dimension_1_Code
    LEFT JOIN src_xform.bc_dimension_reg_table d2
    ON d2.dimension_code = 'DIVISION' AND d2.code = t1.Global_Dimension_2_Code 
    WHERE jc.source IN ('bc_gl_entries', 'bc_job_card')
    UNION
    SELECT 
        jc.task_guid AS job_identifier,
        'Company2' AS source_origin,
        jc.job_no AS source_code,
        t1.Description AS job_description,
        c1.customer_no AS bill_to_customer_number,
        c2.customer_no AS sell_to_customer_number,
        d1.code AS company_code,
        d2.code AS division_code,
        t1.Status AS job_status,
        t1.Creation_Date AS created_at,
        t1.Ending_Date AS ended_at,
        t1.SystemModifiedAt AS last_modified_at,
        t1.SystemModifiedAt AS last_modified_utc_offset
    FROM src_xform.job_card_reg_table jc
    LEFT JOIN (
        SELECT 
            No, 
            MAX(CAST(SUBSTRING(SystemModifiedAt, 1, 27) as DATETIME2(6))) AS max_last_modified_at, 
            MAX(CAST(adf_pipeline_run_time AS datetime2)) AS max_adf_pipeline_run_time
        FROM src.Company2_job_card GROUP BY No
    ) t2 ON jc.job_no = t2.No
    LEFT JOIN src.Company2_job_card t1
    ON t1.No = t2.No AND CAST(SUBSTRING(t1.SystemModifiedAt, 1, 27) as DATETIME2(6)) = t2.max_last_modified_at 
    AND CAST(t1.adf_pipeline_run_time AS datetime2) = t2.max_adf_pipeline_run_time
    LEFT JOIN src_xform.customer_reg_table c1 
    ON c1.customer_no = t1.Bill_to_Customer_No AND c1.source IN ('Company2_gl_entries', 'Company2_customer', 'Company2_job_card')
    LEFT JOIN src_xform.customer_reg_table c2
    ON c2.customer_no = t1.Sell_to_Customer_No AND c2.source IN ('Company2_gl_entries', 'Company2_customer', 'Company2_job_card')
    LEFT JOIN src_xform.bc_dimension_reg_table d1
    ON d1.dimension_code = 'ENTITY' AND d1.code = t1.Global_Dimension_1_Code
    LEFT JOIN src_xform.bc_dimension_reg_table d2
    ON d2.dimension_code = 'DIVISION' AND d2.code = t1.Global_Dimension_2_Code 
    WHERE jc.source IN ('Company2_gl_entries', 'Company2_job_card')
)

INSERT INTO itg.job_card_data
SELECT 
    job_identifier,
    source_origin,
    source_code,
    NULLIF(job_description, '') AS job_description,
    COALESCE(bill_to_customer_number, 'Not Available') AS bill_to_customer_number,
    COALESCE(sell_to_customer_number, 'Not Available') AS sell_to_customer_number,
    COALESCE(company_code, 'Not Available') AS company_code,
    COALESCE(division_code, 'Not Available') AS division_code,
    NULLIF(job_status, '') AS job_status,
    NULLIF(created_at, '') AS created_at,
    NULLIF(ended_at, '') AS ended_at,
    COALESCE(CAST(SUBSTRING(last_modified_at, 1, 27) as DATETIME2(6)), '9999-01-01') AS last_modified_at,
    COALESCE(SUBSTRING(last_modified_utc_offset, 28, 34), 'Not Available') AS last_modified_utc_offset,
    GETDATE() AS dw_insert_timestamp
FROM transformed_job_cards
