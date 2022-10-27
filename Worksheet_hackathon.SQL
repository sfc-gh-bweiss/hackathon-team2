-- setup permissions and context
use database CLINICAL_SUBMISSIONS;
GRANT ALL PRIVILEGES on database CLINICAL_SUBMISSIONS to role ACCOUNTADMIN;
create schema RAW;
show schemas;
use schema RAW;

-- Create Stages for files
create stage Uploaded_Datafiles;
create stage Uploaded_ImageFiles;

-- Table creation and review
show tables;
select * from JUNK1;

-- Tagging
show columns in table JUNK1;
create tag mapping;
alter table JUNK1 modify column EMAIL set tag MAPPING = 'EMAIL';
show tags in schema RAW;
