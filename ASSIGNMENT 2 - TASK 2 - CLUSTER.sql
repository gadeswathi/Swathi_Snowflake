select * from patient_test

explain
select disease , count(*) from patient_test
where name='ram'
group by
disease

alter table patient_test cluster by (patient_id);
 select * from patient_test where patient_id=4
