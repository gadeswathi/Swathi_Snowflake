create table patient_test (
patient_id number(1),
name varchar,
address varchar,
pharmacy_code number(2),
disease varchar

);

Create stream patientupdates on table patient_test;
select * from patientupdates where metadata$action = 'insert';

--Update patient_test set name='veena' where address='DL'

--delete from patient_test where name='geetha'

insert into patient_test values(8,'renu','Hyd',20,'cold');
insert into patient_test values(9,'anu','DL',30,'fever');
insert into patient_test values(3,'geetha','UP',30,'cancer');
insert into patient_test values(4,'seetha','HP',20,'pain');
insert into patient_test values(5,'vani','AP',20,'fever');

Create table Patient_agg
(DISEASE varchar , patient_count number);

select * from patientupdates;
Tssks:
By default task state is suspended

Create or replace task Report_test
warehouse='COMPUTE_WH'
SCHEDULE='2 MINUTE'
AS INSERT INTO patient_test
SELECT DISEASE , count(*) as patient_count
from patient_diagnosis1
group by disease;

Create or replace task Report_test2
warehouse='COMPUTE_WH'
SCHEDULE='2 MINUTE'
when system$stream_has_data('patientupdates')
AS INSERT INTO Patient_agg
SELECT DISEASE , count(*) as patient_count
from patientupdates where metadata$action='INSERT'
group by disease;


alter task Report_test2 resume;
execute task Report_test2;
select * from PATIENT_AGG;