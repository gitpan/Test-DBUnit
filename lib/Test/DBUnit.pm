package Test::DBUnit;
use strict;
use warnings;

use vars qw($VERSION @EXPORT);
use base qw(Exporter);

use DBUnit ':all';
use DBIx::Connection;
use Carp 'confess';
use Sub::Uplevel qw(uplevel);
use Test::Builder;

$VERSION = '0.12';

@EXPORT = qw(expected_dataset_ok dataset_ok expected_xml_dataset_ok xml_dataset_ok reset_schema_ok populate_schema_ok reset_sequence_ok set_refresh_load_strategy set_insert_load_strategy test_connection set_test_connection add_test_connection test_dbh);

=head1 NAME

Test::DBUnit - Database test framework.

=head1 SYNOPSIS

    use DBIx::Connection;

    use Test::DBUnit connection_name => 'test';
    use Test::More tests => $tests;

    DBIx::Connection->new(
        name     => 'test',
        dsn      => $ENV{DB_TEST_CONNECTION},
        username => $ENV{DB_TEST_USERNAME},
        password => $ENV{DB_TEST_PASSWORD},
    );

    #or

    use Test::More;
    use Test::DBUnit dsn => 'dbi:Oracle:localhost:1521/ORACLE_INSTANCE', username => 'user', password => 'password';
    plan tests => $tests;

    my $connection = test_connection();
    my $dbh = test_dbh();

    reset_schema_ok('t/sql/create_schema.sql');

    populate_schema_ok('t/sql/create_schema.sql');

    xml_dataset_ok('test1');

    #you database operations here
    $connection->execute_statement("UPDATE ....");

    expected_xml_dataset_ok('test1');

    #or

    reset_sequence_ok('table1_seq1');

    dataset_ok(
        table1 => [column1 => 'x', column2 => 'y'],
        table1 => [column1 => 'x1_X', column2 => 'y1_X'],
        ...
        table2 => [column1 => 'x2, column2 => 'y2'],
        table2 => [column1 => 'x1_N', column2 => 'y1_N'],
    );

    #you database operations here
    $connection->execute_statement("UPDATE ....");

    expected_dataset_ok(
        table1 => [column1 => 'z', column2 => 'y'],
    )


=head1 DESCRIPTION

Database test framework to verify that your database data match expected set of values.

=head2 Managing test data

Database tests should giving you complete and fine grained control over the test data that is used.

    use Test::DBUnit dsn => $dsn, username => $username, password => $password;
    reset_schema_ok('t/sql/create_schema.sql');
    populate_schema_ok('t/sql/create_schema.sql');
    reset_sequence_ok('emp_seq');

=head2 Loading test data sets

Before you want to test your business logic it is essential to have repeatable snapshot of your
tables you want to test, so this module allows you populate/synchronize your database with
the passed in data structure or with the xml content.

    dataset_ok(
        emp => [ename => "john", deptno => "10", job => "project manager"],
        emp => [ename => "scott", deptno => "10", job => "project manager"],
        bonus => [ename => "scott", job => "project manager", sal => "20"],
    );
    or
    xml_dataset_ok('test1');
    t/test_unit.test1.xml #given that you testing module is t/test_unit.t
    <?xml version='1.0' encoding='UTF-8'?>
    <dataset load_strategy="INSERT_LOAD_STRATEGY">
        <emp empno="1" ename="scott" deptno="10" job="project manager" />
        <emp empno="2" ename="john"  deptno="10" job="engineer" />
        <bonus ename="scott" job="project manager" sal="20" />
    </dataset>

=head2 Getting connection to test database

    my $connection = test_connection();
    #business logic that change tested data comes here
    ....

=head2 Verifying test results

It can be useful to use data sets for checking the contents of a database after is has been modified by a test.
You may want to check the result of a update/insert/delete method or a stored procedure.

    expected_dataset_ok(
        emp   => [empno => "1", ename => "Scott", deptno => "10", job => "project manager"],
        emp   => [empno => "2", ename => "John",  deptno => "10", job => "engineer"],
        emp   => [empno => "3", ename => "Mark",  deptno => "10", job => "sales assistant"],
        bonus => [ename => "scott", job => "project manager", sal => "20"],
    );
    or

    expected_xml_dataset_ok('test1');
    t/test_unit.test1-result.xml #given that you testing module is t/test_unit.t

    <?xml version='1.0' encoding='UTF-8'?>
    <dataset>
        <emp empno="1" ename="Scott" deptno="10" job="project manager" />
        <emp empno="2" ename="John"  deptno="10" job="engineer" />
        <emp empno="3" ename="Mark"  deptno="10" job="sales assistant" />
        <bonus ename="scott" job="project manager" sal="20" />
    </dataset>


=head3 dynamic_tests

You may want to check not just a particular value but range of values or perform complex condition checking against
database column's value, so then you can use callback. It takes database column's value as parameter and should return
true to pass the test, false otherwise.

    expected_dataset_ok(
        emp   => [empno => "1", ename => "Scott", deptno => "10", job => "project manager"],
        emp   => [empno => "2", ename => "John",  deptno => "10", job => "engineer"],
        emp   => [empno => "3", ename => "Mark",  deptno => "10",
            job => sub {
                my $value = shift;
                !! ($value =~ /sales assistant/i);
            }
        ],
        bonus => [ename => "scott", job => "project manager", sal => "20"],
    );


=head2 Configuring the dataset load strategy

By default, datasets are loaded into the database using an insert load strategy.
This means that all data in the tables that are present in the dataset is deleted,
after which the test data records are inserted. Order in with all data is deleted
depends on reverse table occurrence in the dataset, however you may force order of
data by specifying empty table:

        table1 => [],  #this fore delete operation in occurrence order
        table1 => [col1 => 1, col2 => 'some data'],    
        or in xml file
        <table1 />
        <table1 col1="1" col2="some data"/>

In this strategy number of rows will be validated against datasets in (xml_)expexted_dataset_ok method.
Load strategy behavior is configurable,
it can be modified by calling:

    set_insert_load_strategy();
    or in XML
    <?xml version='1.0' encoding='UTF-8'?>
    <dataset load_strategy="INSERT_LOAD_STRATEGY">
        <emp empno="1" ename="Scott" deptno="10" job="project manager" />
        ....
    </dataset>

    set_refresh_load_strategy();
    or in XML
    <?xml version='1.0' encoding='UTF-8'?>
    <dataset load_strategy="REFRESH_LOAD_STRATEGY">
        <emp empno="1" ename="Scott" deptno="10" job="project manager" />
        ....
    </dataset>

The alternative to the insert load strategy is refresh load strategy.
In this case update on existing rows will take place or insert occurs if rows are missing.

=head3 Tests with multiple database instances.

You may need to test data from more then one database instance,
so that you have to specify connection againt which tests will be performed
either by adding prefix to test methods, or by seting explicit test connection context.


    use Test::DBUnit connection_names => ['my_connection_1', 'my_connection_2'];
    my $dbh = DBI->connect($dsn_1, $username, $password);
    
    add_test_connection('my_connection_1', dbh => $dbh);
    # or
     my $connection = DBIx::Connection->new(
        name     => 'my_connection_2',
        dsn      => $dsn_2,
        username => $username,
        password => $password,
    );
    add_test_connection($connection);


    #set connection context by prefix
    my_connection_1_reset_schema_ok('t/sql/create_schema_1.sql');
    my_connection_1_populate_schema_ok('t/sql/create_schema_1.sql');

    my_connection_2_xml_dataset_ok('test1');
    ...
    my_connection_2_expected_xml_dataset_ok('test1');


    #set connection context explicitly.
    set_test_connection('my_connection_2');
    reset_schema_ok('t/sql/create_schema_2.sql');
    populate_schema_ok('t/sql/create_schema_2.sql');
    xml_dataset_ok('test1');

    expected_xml_dataset_ok('test1');


=head2 Working with sequences

You may use sequences or auto generated features, so this module allows you handle that.

    reset_sequence_ok('emp_seq');
    or for MySQL
    reset_sequence_ok('test_table_name')

The ALTER TABLE test_table_name AUTO_INCREMENT = 1 will be issued
Note that for MySQL reset sequence the test_table_name must be empty.

    or in XML
    <?xml version='1.0' encoding='UTF-8'?>
    <dataset reset_sequences="emp_seq, dept_seq">
        <emp empno="1" ename="Scott" deptno="10" job="project manager" />
        ....
    </dataset>

=head3 Sequence tests with Oracle

    t/sql/create_schema.sql
    CREATE SEQUENCE emp_seq;
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

    #unit test
    reset_sequence_ok('emp_seq');

    dataset_ok(
        emp => [ename => "John", deptno => "10", job => "project manager"],
        emp => [ename => "Scott", deptno => "10", job => "project manager"]
    );

    .... 

    expected_dataset_ok(
        emp => [empno => 1, ename => "John", deptno => "10", job => "project manager"],
        emp => [empno => 2, ename => "Scott", deptno => "10", job => "project manager"]
    )

=head3 Sequence tests with PostgreSQL

    t/sql/create_schema.sql
    CREATE SEQUENCE emp_seq;
    CREATE TABLE emp(
    empno      INT4 DEFAULT nextval('emp_seq') NOT NULL,
    ename      VARCHAR(10),
    job        VARCHAR(20),
    mgr        NUMERIC(4),
    hiredate   DATE,
    sal        NUMERIC(7,2),
    comm       NUMERIC(7,2),
    deptno     NUMERIC(2),
    CONSTRAINT emp_pk PRIMARY KEY(empno),
    FOREIGN KEY (deptno) REFERENCES dept (deptno) 
   );

    #unit test
    reset_sequence_ok('emp_seq');
    ....

=head3 Auto generated filed values tests with MySQL

    t/sql/create_schema.sql
    CREATE TABLE emp(
    empno     MEDIUMINT AUTO_INCREMENT, 
    ename      VARCHAR(10),
    job        VARCHAR(20),
    mgr        NUMERIC(4),
    hiredate   DATE,
    sal        NUMERIC(7,2),
    comm       NUMERIC(7,2),
    deptno     NUMERIC(2),
    CONSTRAINT emp_pk PRIMARY KEY(empno),
    FOREIGN KEY (deptno) REFERENCES dept (empno) 
   );

    #unit test
    reset_sequence_ok('emp');

    dataset_ok(
        emp => [ename => "John", deptno => "10", job => "project manager"],
        emp => [ename => "Scott", deptno => "10", job => "project manager"]
    );

    .... 

    expected_dataset_ok(
        emp => [empno => 1, ename => "John", deptno => "10", job => "project manager"],
        emp => [empno => 2, ename => "Scott", deptno => "10", job => "project manager"]
    )

=head2 Working with LOBs

For handling very large datasets, the DB vendors provide the LOB (large object) data types.
You may use this features, and this module allows you test it.

=head3 LOBs tests with Oracle

Oracle BLOB data type that contains binary data with a maximum size of 4 gigabytes. 
It is advisable to store blob size in separate column to optimize fetch process.(doc_size)

    CREATE TABLE image(id NUMBER, name VARCHAR2(100), doc_size NUMBER, blob_content BLOB);

    dataset_ok(
        emp   => [empno => 1, ename => 'scott', deptno => 10],
        image  => [id => 1, name => 'Moon'
            blob_content => {file => 'data/chart1.jpg', size_column => 'doc_size'}
        ]
    );

    .....

    expected_dataset_ok(
        emp   => [empno => 1, ename => 'scott', deptno => 10],
        image  => [id => 1, name => 'Moon'
            blob_content => {file => 'data/chart2.jpg', size_column => 'doc_size'}
        ]
    );


=head3 LOBs tests with PostgreSQL

PostgreSQL has a large object facility, but in this case the tested table doesn't contain LOBs type
but keeps reference to lob_id, created by lo_creat PostgreSQL function.
It is required to store blob size in separate column to be able fetch blob.(doc_size)

    CREATE TABLE image(id NUMERIC, name VARCHAR(100), doc_size NUMERIC, blob_content oid)

    dataset_ok(
        emp   => [empno => 1, ename => 'scott', deptno => 10],
        image  => [id => 1, name => 'Moon'
            blob_content => {file => 'data/chart1.jpg', size_column => 'doc_size'}
        ]
    );


=head3 LOBs test with MySQL

In MySQL, binary LOBs are just fields in the table, so storing blob size is optional.

    CREATE TABLE lob_test(id NUMERIC, name VARCHAR(100), doc_size NUMERIC, blob_content LONGBLOB)

    dataset_ok(
        emp   => [empno => 1, ename => 'scott', deptno => 10],
        image  => [id => 1, name => 'Moon'
            blob_content => {file => 'data/chart1.jpg', size_column => 'doc_size'}
        ]
    );


=head2 EXPORT

expected_data_set_ok
dataset_ok
expected_xml_dataset_ok
xml_dataset_ok
reset_schema_ok
populate_schema_ok
reset_sequence_ok
set_refresh_load_strategy
set_insert_load_strategy
add_test_connection
set_test_connection
test_connection
test_dbh
<connection_name>_(expected_data_set_ok | dataset_ok | expected_xml_dataset_ok | xml_dataset_ok | reset_schema_ok | populate_schema_ok | reset_sequence_ok | set_refresh_load_strategy | set_insert_load_strategy)
by default.

=head2 METHODS

=over

=item connection_name

=cut

{
    
my $Tester = Test::Builder->new;
my $dbunit;
my $multiple_tests;
    sub import {
        my ($self, %args) = @_;
        if($args{connection_names}) {
            generate_connection_test_stubs($args{connection_names});
            $multiple_tests = 1;
            
        } elsif($args{connection_name}) {
            $dbunit = DBUnit->new(%args);
            
        } elsif(scalar(%args)) {
            eval {
                $dbunit = DBUnit->new(connection_name => 'test');
                _initialise_connection(%args);
            };
            if ($@) {
                my ($msg) = ($@ =~ /([^\n]+)/);
                $Tester->plan( skip_all => $msg);
            }
        } 
       $dbunit ||= DBUnit->new(connection_name => 'test');
       $self->export_to_level( 1, $self, $_ ) foreach @EXPORT;
    }


=item generate_connection_test_stubs

Generated test stubs on fly for passed in connection names.

=cut

sub generate_connection_test_stubs {
    my ($connections) = @_;
    for my $connection (@$connections) {
        for my $exp (@EXPORT[0 ..9]) {
            my $method_name = "${connection}_$exp";
            Abstract::Meta::Class::add_method(__PACKAGE__,
                $method_name, sub {
                    my $ory_connection_name = $dbunit->connection_name;
                    set_test_connection($connection);
                    my $method = __PACKAGE__->can($exp);
                    $method->(@_);
                    set_test_connection($ory_connection_name);
                }
            );
            push @EXPORT, $method_name;
        }
    }
    
}

=item reset_schema_ok

Tests database schema reset using sql file. Takes file name as parameter.

    use Test::More tests => $tests; 
    use Test::DBUnit dsn => $dsn, username => $username, password => $password;

    ...

    reset_schema_ok('t/sql/create_schema.sql');

=cut

    sub reset_schema_ok {
        my ($file_name) = @_;
        my $description = "should reset schema" . test_connection_context() . " (${file_name})";
        my $ok;
        eval {
            $dbunit->reset_schema($file_name);
            $ok = 1;
        };
        my $explanation = "";
        $explanation .= "\n" . $@ if $@;
        $Tester->ok($ok, $description );
        $Tester->diag($explanation) unless $ok;
        $ok;
    }


=item populate_schema_ok

Tests database schema population using sql file. Takes file name as parameter.

    use Test::More tests => $tests; 
    use Test::DBUnit dsn => $dsn, username => $username, password => $password;

    ...

    populate_schema_ok('t/sql/populate_schema.sql');

=cut


    sub populate_schema_ok {
        my ($file_name) = @_;
        my $description = "should populate schema". test_connection_context() ." (${file_name})";
        my $ok;
        eval {
            $dbunit->populate_schema($file_name);
            $ok = 1;
        };
        my $explanation = "";
        $explanation .= "\n" . $@ if $@;
        $Tester->ok( $ok, $description );
        $Tester->diag($explanation) unless $ok;
        $ok;
    }


=item reset_sequence_ok

Resets database sequence. Takes sequence name as parameter.

    use Test::More tests => $tests; 
    use Test::DBUnit dsn => $dsn, username => $username, password => $password;


    reset_sequnce('table_seq1');

=cut

    sub reset_sequence_ok {
        my ($sequence_name) = @_;
        my $description = "should reset sequence" . test_connection_context() . " ${sequence_name}";
        my $ok;
        eval {
            $dbunit->reset_sequence($sequence_name);
            $ok = 1;
        };
        my $explanation = "";
        $explanation .= "\n" . $@ if $@;
        $Tester->ok( $ok, $description );
        $Tester->diag($explanation) unless $ok;
        $ok;
    }


=item xml_dataset_ok

Tests database schema population/synch  to the content of the xml file.
Takes test unit name, that is used to resolve xml file name.
Xml file name that will be loaded is build as follow
<test_file>.<unit_name>.xml
for instance
the following invocation xml_dataset_ok('test1') from t/sub_dir/001_test.t file will
expect t/sub_dir/001_test.test1.xml file.

    <dataset load_strategy="INSERT_LOAD_STRATEGY" reset_sequences="emp_seq">
        <emp ename="scott" deptno="10" job="project manager" />
        <emp ename="john"  deptno="10" job="engineer" />
        <emp ename="mark"  deptno="10" job="sales assistant" />
        <bonus ename="scott" job="project manager" sal="20" />
    </dataset>


=cut

    sub xml_dataset_ok {
        my ($unit_name) = @_;
        my $xm_file = ($unit_name =~ /.xml$/i)
            ? $unit_name
            : _xml_test_file($unit_name) . ".xml";
        my $description = "should load dataset" . test_connection_context() . " (${xm_file})";
        my $ok;
        eval {
            $dbunit->xml_dataset($xm_file);
            $ok = 1;
        };
        my $explanation = "";
        $explanation .= "\n" . $@ if $@;
        $Tester->ok( $ok, $description );
        $Tester->diag($explanation) unless $ok;
        $ok;
    }


=item expected_xml_dataset_ok

Validates expected database loaded from xml file against database schema.
Takes test unit name, that is used to resolve xml file name.
Xml file name that will be loaded is build as follow
<test_file>.<unit_name>.xml unless you pass full xml file name.
for instance
the following invocation xml_dataset_ok('test1') from t/sub_dir/001_test.t file will
expect t/sub_dir/001_test.test1.xml file.

    <dataset load_strategy="INSERT_LOAD_STRATEGY" reset_sequences="emp_seq,dept_seq">
        <emp ename="Scott" deptno="10" job="project manager" />
        <emp ename="John"  deptno="10" job="engineer" />
        <emp ename="Mark"  deptno="10" job="sales assistant" />
        <bonus ename="Scott" job="project manager" sal="20" />
    </dataset>

=cut

    sub expected_xml_dataset_ok {
        my ($unit_name) = @_;
        my $xm_file = ($unit_name =~ /.xml$/i)
            ? $unit_name
            : _xml_test_file($unit_name) . "-result.xml";
        my $description = "should validate expected dataset" . test_connection_context() . "(${xm_file})";
        my $validation;
        my $ok;
        eval {
            $validation = $dbunit->expected_xml_dataset($xm_file);
            $ok = 1 unless $validation;
        };
        my $explanation = "";
        $explanation .= "\n" . $validation if $validation;
        $explanation .= "\n" . $@ if $@;
        $Tester->ok( $ok, $description );
        $Tester->diag($explanation) unless $ok;
        $ok;
    }


=item dataset_ok

Tests database schema population/synch to the passed in dataset.

    dataset_ok(
        table1 => [], #this deletes all data from table1 (DELETE FROM table1)
        table2 => [], #this deletes all data from table2 (DELETE FROM table2)
        table1 => [col1 => 'va1', col2 => 'val2'], #this insert or update depend on strategy
        table1 => [col1 => 'xval1', col2 => 'xval2'],
    )

=cut

    sub dataset_ok {
        my (@dataset) = @_;
        my $description = "should load dataset" . test_connection_context();
        my $ok;
        eval {
            $dbunit->dataset(@dataset);
            $ok = 1;
        };
        my $explanation = "";
        $explanation .= "\n" . $@ if $@;
        $Tester->ok( $ok, $description );
        $Tester->diag($explanation) unless $ok;
        $ok;
    }


=item expected_dataset_ok

Validates database schema against passed in dataset.

    expected_dataset_ok(
        table1 => [col1 => 'va1', col2 => 'val2'], 
    )

=cut

    sub expected_dataset_ok {
        my (@dataset) = @_;
        my $description = "should validate expected dataset" . test_connection_context();
        my $validation;
        my $ok;
        eval {
            $validation = $dbunit->expected_dataset(@dataset);
            $ok = 1 unless $validation;
        };
        my $explanation = "";
        $explanation .= "\n" . $validation if $validation;
        $explanation .= "\n" . $@ if $@;
        $Tester->ok( $ok, $description );
        $Tester->diag($explanation) unless $ok;
        $ok;
    }


=item _initialise_connection

Initialises default test connection

=cut

    my $connection;
    sub _initialise_connection {
        add_test_connection('test', @_);
    }


=item test_connection_context

Returns tested connection name,

=cut

sub test_connection_context {
    return '' unless $multiple_tests;
    "[" .$dbunit->connection_name . "]";
}

=item test_connection

Returns test connection object.

=cut

    sub test_connection {
        $connection = DBIx::Connection->connection($dbunit->connection_name);
    }
    

=item add_test_connection

Adds tests connection


    use Test::DBUnit;

    # or

    use Test::DBUnit connection_names => ['my_connection_name', 'my_connection_name1'];

    my $connection = DBIx::Connection->new(...);
    add_test_connection($connection);

    #or

    add_test_connection('my_connection_name', dsn =>  $dsn, username => $username, password => 'password');

    #or

    add_test_connection('my_connection_name', dbh => $dbh);


=cut

    sub add_test_connection {
        my ($connection_, @args) = @_;
        if(ref($connection_)) {
            $connection = $connection_;
            $connection_ = $connection->name;
        }
        set_test_connection($connection_);
        if(@args) {
            $connection = DBIx::Connection->new(name => $connection_, @args);
        }
        
    }

=item set_test_connection

Sets test connection that will be tested.

=cut

    sub set_test_connection {
        my ($connection_name) = @_;
        $dbunit->set_connection_name($connection_name);
    }


=item test_dbh

Returns test database handler.

=cut

    sub test_dbh {
        test_connection()->dbh;
    }
    

=item set_insert_load_strategy

Sets insert as the load strategy

=cut

    sub set_insert_load_strategy {
        $dbunit->set_load_strategy(INSERT_LOAD_STRATEGY);
    }


=item set_refresh_load_strategy

Sets refresh as the load strategy

=cut

    sub set_refresh_load_strategy {
        $dbunit->set_load_strategy(REFRESH_LOAD_STRATEGY);
    }

}


=item _xml_test_file

Returns xml file prefix  to test

=cut

sub _xml_test_file {
    my ($unit_name) = @_;
    my $test_file = $0;
    $test_file =~ s/\.t/.$unit_name/;
    $test_file;
}



1;

__END__

=back

=head1 COPYRIGHT AND LICENSE

The Test::DBUnit module is free software. You may distribute under the terms of
either the GNU General Public License or the Artistic License, as specified in
the Perl README file.

=head1 SEE ALSO

L<DBUnit>
L<DBIx::Connection>

=head1 AUTHOR

Adrian Witas, adrian@webapp.strefa.pl

=cut
