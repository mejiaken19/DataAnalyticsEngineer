-- GRANTING READ & WRITE ACCESS TO THE PROD SQL POOL
CREATE USER [contoso-dwh-prod-rw] FROM EXTERNAL PROVIDER
EXEC sp_addrolemember 'db_datawriter', 'contoso-dwh-prod-rw';
EXEC sp_addrolemember 'db_datareader', 'contoso-dwh-prod-rw';

-- GRANTING READ ACCESS TO THE PROD SQL POOL
CREATE USER [contoso-dwh-prod-r] FROM EXTERNAL PROVIDER
EXEC sp_addrolemember 'db_datareader', 'contoso-dwh-prod-r';

-- GRANTING OWNER ACCESS TO THE PROD SQL POOL
CREATE USER [contoso-dwh-admin] FROM EXTERNAL PROVIDER
EXEC sp_addrolemember 'db_owner', 'contoso-dwh-admin';

-- GRANTING READ/WRITE ACCESS TO [ADF SERVICE PRINCIPAL]
CREATE USER [contoso-adf-to-synapse-user] FROM EXTERNAL PROVIDER
EXEC sp_addrolemember 'db_owner', 'contoso-adf-to-synapse-user';
