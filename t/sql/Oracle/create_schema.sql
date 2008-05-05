CREATE SEQUENCE emp_seq;


CREATE TABLE dept (
 deptno NUMBER(2) CONSTRAINT dept_pk PRIMARY KEY,
 dname  VARCHAR2(20),
 loc    VARCHAR2(20)
); 

CREATE TABLE emp(
 empno      NUMBER NOT NULL,
 ename      VARCHAR2(10),
 job        VARCHAR2(20),
 mgr        NUMBER(4),
 hiredate   DATE,
 sal        NUMBER(7,2),
 comm       NUMBER(7,2),
 deptno     NUMBER(2),
 CONSTRAINT emp_pk PRIMARY KEY(empno),
 FOREIGN KEY (deptno) REFERENCES dept (deptno) 
);

CREATE OR REPLACE TRIGGER emp_autogen
BEFORE INSERT ON emp FOR EACH ROW
BEGIN
    IF :new.empno is null then
        SELECT emp_seq.nextval INTO :new.empno FROM dual;
    END IF;
END;

CREATE TABLE bonus(
ename VARCHAR2(10),
 JOB  VARCHAR2(20),
 SAL  NUMBER,
 COMM NUMBER
);

CREATE TABLE project (
 projno NUMBER, 
 name  VARCHAR2(100),
 CONSTRAINT proj_pk PRIMARY KEY(projno)
);

CREATE TABLE emp_project(
  empno NUMBER,
  projno NUMBER,
  leader VARCHAR2(1),
  CONSTRAINT emp_proj_pk PRIMARY KEY(empno, projno),
  FOREIGN KEY (empno) REFERENCES emp(empno),
  FOREIGN KEY (projno) REFERENCES project(projno)
);


CREATE TABLE seq_generator
(
  pk_column VARCHAR2(30),
  value_column NUMBER
);
