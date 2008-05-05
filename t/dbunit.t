use strict;
use warnings;

use Test::More tests => 37;

my $class;

BEGIN {
    $class = 'DBUnit';
    use_ok($class, ':all');
}


my $dbunit = $class->new(connection_name => 'test');
isa_ok($dbunit, $class);

my $dataset = [
    table1 => [],
    table5 => [],
    table1 => [col1 => 1, col2 => 2],
    table2 => [col1 => 1, col2 => 2],
    table3 => [col1 => 1, col2 => 2],
    table4 => [col1 => 1, col2 => 2],
    table5 => [col1 => 1, col2 => 2],
];



{
    my $sql = "CREATE TABLE table1 (   id integer,   col1 varchar(128) );
CREATE TABLE table2
(
  id integer,
  col1 varchar(128)
);

BEGIN
do dome stuff;
END;

CREATE SEQUENCE seq1;
";

    my %objects= $dbunit->objects_to_create($sql);
    is_deeply(\%objects,  {
        'TABLE table1' => 'CREATE TABLE table1 (   id integer,   col1 varchar(128) )',
        'TABLE table2' => 'CREATE TABLE table2
(
  id integer,
  col1 varchar(128)
)',
    0 => 'BEGIN
do dome stuff;
END;',
    'SEQUENCE seq1' => 'CREATE SEQUENCE seq1'
}, 'should have list of table to create');
}

{
    my @tables = $dbunit->empty_tables_to_delete($dataset);
    is_deeply(\@tables, ['table1', 'table5'], 'should have empty_tables_to_delete');
}

{
    my @tables = $dbunit->tables_to_delete($dataset);
    is_deeply(\@tables, ['table1', 'table5', 'table4' ,'table3', 'table2'], 'should have tables_to_delete');

}

{
    my $sql = "
INSERT INTO dept(deptno, dname, loc)
VALUES(10, 'HR', 'Warsaw');

INSERT INTO dept(deptno, dname, loc)
VALUES(20, 'IT', 'Katowice;'); ";
    my @rows = $dbunit->rows_to_insert($sql);
    is_deeply(\@rows, ['
INSERT INTO dept(deptno, dname, loc)
VALUES(10, \'HR\', \'Warsaw\')',
          '

INSERT INTO dept(deptno, dname, loc)
VALUES(20, \'IT\', \'Katowice;\')',
    ], 'should have rows to insert');
    
}

{
    my $result = DBUnit::compare_datasets({key1 => 1, key2 => 3}, {key1 => 1}, 'table1', 'key1', 'key2');
    is($result, "found difference in table1 key2:
  [ key1 => '1' key2 => '' ]
  [ key1 => '1' key2 => '3' ]", 'should find difference');
}

{
    my $result = DBUnit::compare_datasets({key1 => 1, key2 => 3}, {key1 => 1, key2 => 3.0}, 'table1','key1', 'key2');
    ok(! $result, 'should not find differences');
}


    {
        my $result = $dbunit->load_xml('t/dbunit.dataset.xml');
        
        is_deeply($result->{properties}, {load_strategy => 'INSERT_LOAD_STRATEGY', reset_sequences => undef}, 'should have properties');
        is_deeply($result->{dataset}, [
            emp => [deptno => '10', empno => 1, ename => 'scott', job => 'project manager'],
            emp => [deptno => '10', empno => 2, ename => 'john',  job => 'engineer'],
            emp => [deptno => '10', empno => 3 ,ename => 'mark',  job => 'sales assistant'],
            bonus => [ename => 'scott', job => 'project manager', sal => '20']
        ], 'should have dataset');
    }

SKIP: {
    
    skip('missing env varaibles DB_TEST_CONNECTION, DB_TEST_USERNAME DB_TEST_PASSWORD', 13)
      unless $ENV{DB_TEST_CONNECTION};
    use DBIx::Connection;
    my $connection = DBIx::Connection->new(
        name     => 'test',
        dsn      => $ENV{DB_TEST_CONNECTION},
        username => $ENV{DB_TEST_USERNAME},
        password => $ENV{DB_TEST_PASSWORD},
    );

    {
        my $script = "t/sql/". $connection->dbms_name . "/create_schema.sql";
        $dbunit->reset_schema($script);
        ok(@{$connection->table_info('dept')}, "should have dept table");
        ok(@{$connection->table_info('emp')}, "should have emp table");
    }
    
    {
        my $script = "t/sql/". $connection->dbms_name . "/populate_schema.sql";
        $dbunit->populate_schema($script);
        my $result = $connection->record("SELECT * FROM dept WHERE deptno = ?", 10);
        is_deeply($result, {deptno => 10, dname =>'HR', loc => 'Warsaw'}, 'should have populated data');
    }

    {
        print "## insert load strategy tests\n";
        #insert load strategy
	#adds some random data
        $connection->do("INSERT INTO emp (empno, ename) VALUES(1, 'test')");
	$connection->do("INSERT INTO bonus (ename, sal) VALUES('test', 10.4)");

        is($dbunit->load_strategy, INSERT_LOAD_STRATEGY, 'should have insert load strategy');
	my %emp_1 = (empno => 1, ename => 'scott', deptno => 10, job => 'consultant');
	my %emp_2 = (empno => 2, ename => 'john',  deptno => 10, job => 'consultant');
	my %bonus = (ename => 'scott', job => 'consultant', sal => 30);
        $dbunit->dataset(
            emp   => [%emp_1],
            emp   => [%emp_2],
            bonus => [%bonus],
        );

	{
	    my $record = $connection->record("SELECT empno, ename, deptno, job FROM emp WHERE empno = ?", 1);
            is_deeply($record, \%emp_1, 'should have emp1 row');
	}

	{
	    my $record = $connection->record("SELECT empno, ename, deptno, job FROM emp WHERE empno = ?", 2);
            is_deeply($record, \%emp_2, 'should have emp2 row');
	}

	{
	    my $record = $connection->record("SELECT ename, job, sal FROM bonus WHERE ename = ?", 'scott');
            is_deeply($record, \%bonus, 'should have bonus row');
	}

	{
	    my $record = $connection->record("SELECT COUNT(*) AS cnt FROM bonus");
            is($record->{cnt}, 1, 'should have one bonus row');
	}

	{
	    my $record = $connection->record("SELECT COUNT(*) AS cnt FROM emp");
            is($record->{cnt}, 2, 'should have two emp rows');
	}

        #expected resultset
        $connection->do("INSERT INTO emp (empno, ename) VALUES(20, 'test')");
        $connection->do("INSERT INTO bonus (ename, sal) VALUES('scott', 10.4)");
        $connection->execute_statement("UPDATE emp SET ename = ? WHERE empno = ?", 'John', 2);

        my %emp_20 = (empno => 20, ename => 'test');
        ok(! $dbunit->expected_dataset(
            emp   => [%emp_1],
            emp   => [%emp_2, ename => 'John'],
            emp   => [%emp_20],
            bonus => [%bonus],
            bonus => [ename => 'scott', sal => 10.4],
        ), 'should have expected data');

        
        $connection->do("INSERT INTO bonus (ename, sal) VALUES('scott', 10.4)");
        my $result = $dbunit->expected_dataset(
            emp   => [%emp_1],
            emp   => [%emp_2, ename => 'John'],
            emp   => [%emp_20],
            bonus => [%bonus],
            bonus => [ename => 'scott', sal => '10.4'],
        );
        is($result, "found difference in number of the bonus rows - has 3 rows, should have 2", 'should find difference in number of rows');

        {
            my $result = $dbunit->expected_dataset(emp => [ename => 'Test', empno => 30]);
            like($result, qr{missing entry}, 'shuld find difference - missing entry');
        }
    
        {
            my $result = $dbunit->expected_dataset(emp => [ename => 'Test', empno => 1  ]);
            like($result, qr{found difference}, 'should find difference');
        }

    }

    {
        print "## refresh load strategy tests\n";
        #refresh load strategy
        $dbunit->set_load_strategy(REFRESH_LOAD_STRATEGY);
        is($dbunit->load_strategy, REFRESH_LOAD_STRATEGY, 'should have insert load strategy');
        
        $connection->do("INSERT INTO emp (empno, ename, deptno, job) VALUES(3, 'john3', 10, 'engineer')");
	my %emp_1 = (empno => 1, ename => 'scott', deptno => 10, job => 'project manager');
	my %emp_2 = (empno => 2, ename => 'john',  deptno => 10, job => 'engineer');
        my %emp_3 = (empno => 3, ename => 'john3',  deptno => 10, job => 'engineer');
	my %bonus = (ename => 'scott', job => 'project manager', sal => 20);
        $dbunit->dataset(
            emp   => [%emp_1],
            emp   => [%emp_2],
            bonus => [%bonus],
        );

	{
	    my $record = $connection->record("SELECT empno, ename, deptno, job FROM emp WHERE empno = ?", 1);
            is_deeply($record, \%emp_1, 'should have emp1 row');
	}
        
	{
	    my $record = $connection->record("SELECT empno, ename, deptno, job FROM emp WHERE empno = ?", 2);
            is_deeply($record, \%emp_2, 'should have emp2 row');
	}

	{
	    my $record = $connection->record("SELECT empno, ename, deptno, job FROM emp WHERE empno = ?", 3);
            is_deeply($record, \%emp_3, 'should have emp3 row');
	}
    
	{
	    my $record = $connection->record("SELECT ename, job, sal FROM bonus WHERE ename = ? AND sal = ?", 'scott', 20);
            is_deeply($record, \%bonus, 'should have bonus row');
	}
    
    
        $connection->execute_statement("UPDATE emp SET ename = ? WHERE empno = ?", 'John', 2);
    
        ok(! $dbunit->expected_dataset(
            emp   => [%emp_1],
            emp   => [%emp_2, ename => 'John'],
            bonus => [%bonus],
        ), 'have expected data');
        
        {
            my $result = $dbunit->expected_dataset(
                emp   => [%emp_1],
                emp   => [%emp_2],
                bonus => [%bonus],
            );
            like($result, qr{found difference}, 'should find difference');
        }

        {
            my $result = $dbunit->expected_dataset(
                emp   => [%emp_1],
                emp   => [%emp_2, ename => 'John'],
                bonus => [%bonus, => sal => 32],
            );
            like($result, qr{missing entry}, 'should find difference - missing entry');
        }
    }


    print "## xml dataset tests\n";
    #xml dataset tests  
    {
        $dbunit->xml_dataset('t/dbunit.dataset.xml');
        
	{
	    my $record = $connection->record("SELECT empno, ename, deptno, job FROM emp WHERE empno = ?", 1);
            is_deeply($record, {deptno => '10', empno => 1, ename => 'scott', job => 'project manager'}, 'should have emp1 row');
	}
        
        {
	    my $record = $connection->record("SELECT empno, ename, deptno, job FROM emp WHERE empno = ?", 2);
            is_deeply($record, {deptno => '10', empno => 2, ename => 'john',  job => 'engineer'}, 'should have emp2 row');
	}

        {
	    my $record = $connection->record("SELECT empno, ename, deptno, job FROM emp WHERE empno = ?", 3);
            is_deeply($record, {deptno => '10', empno => 3 ,ename => 'mark',  job => 'sales assistant'}, 'should have emp3 row');
	}

        $connection->execute_statement("UPDATE emp SET ename = ? WHERE empno = ?", 'Scott', 1);
        $connection->execute_statement("UPDATE emp SET ename = ? WHERE empno = ?", 'John', 2);
        $connection->execute_statement("UPDATE emp SET ename = ? WHERE empno = ?", 'Mark', 3);

        ok(! $dbunit->xml_expected_dataset('t/dbunit.resultset.xml'), 'should have all expected data');
    }
    
    ok(!$dbunit->dataset(emp => []), 'should sunc database to dataset');
    my $record = $connection->record("SELECT count(*) as rows_number FROM emp");
    is($record->{rows_number}, 0, "should delete all rows from emp");
    
}
