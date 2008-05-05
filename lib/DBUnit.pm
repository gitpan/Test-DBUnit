package DBUnit;

use strict;
use warnings;
use vars qw(@EXPORT_OK %EXPORT_TAGS $VERSION);

$VERSION = '0.01';
use Abstract::Meta::Class ':all';
use base 'Exporter';
use Carp 'confess';
use DBIx::Connection;
use Simple::SAX::Serializer;

@EXPORT_OK = qw(INSERT_LOAD_STRATEGY REFRESH_LOAD_STRATEGY reset_schema populate_schema expected_dataset dataset xml_expected_dataset xml_dataset);
%EXPORT_TAGS = (all => \@EXPORT_OK);

use constant INSERT_LOAD_STRATEGY => 0;
use constant REFRESH_LOAD_STRATEGY => 1; 

=head1 NAME

Database test framework.

=head1 SYNOPSIS

    use DBUnit ':all';


=head1 DESCRIPTION

Database test framework to verify that your database data match expected set of values.
It has ability to populate dataset and expected set from xml files.

=head2 EXPORT

None by default.

reset_schema
populate_schema
expected_dataset
xml_expected_dataset
dataset
xml_dataset by tag 'all'

=head2 ATTRIBUTES

=over

=item connection_name

=cut

has '$.connection_name' => (required => 1);


=item load_strategy

INSERT_LOAD_STRATEGY(default)
Deletes all data from tables that are present in test dataset in reverse order
unless empty table without attribute is stated, that force deletion in occurrence order
In this strategy expected dataset is also tested against number of rows for all used tables.

REFRESH_LOAD_STRATEGY
Merges (update/insert) data to the given dataset snapshot.
In this scenario only rows in expected dataset are tested.

=cut

has '$.load_strategy' => (default => INSERT_LOAD_STRATEGY());


=item primary_key_values_stash

This option is stored as hash_ref:
the key is the table name with the schema prefix
and value is stored as array ref of primary key's values.

=cut

has '%.primary_key_values_stash';


=item primary_key_definition_cache

This option is stored as hash_ref:
the key is the table name with the schema prefix
and value is stored as array ref of primary key column names.


=cut

has '%.primary_key_definition_cache';


=back

=head2 METHODS

=over

=item reset_schema

Resets schema

=cut


sub reset_schema {
    my ($self, $file_name) = @_;
    my @tables_list = $self->objects_to_create(_load_file_content($file_name));
    my @to_drop;
    my @to_create;
    for (my $i = 0; $i <= $#tables_list; $i += 2) {
        push @to_drop, $tables_list[$i];
        push @to_create, $tables_list[$i + 1];
    }
    $self->drop_objects(reverse @to_drop);
    $self->create_tables(@to_create);
}



=item drop_objects

Removes existing schema

=cut

sub drop_objects{
    my ($self, @objects) = @_;
    my $connection = DBIx::Connection->connection($self->connection_name);
    for my $object (@objects) {
        next if ($object =~ /^\d+$/);
        if($object =~ m/table\s+(\w+)/i) {
            my $table = $1;
            $connection->do("DROP $object") 
                if $connection->has_table($table);
                
        } elsif($object =~ m/sequence\s+(\w+)/i) {
            my $sequence = $1;
            $connection->do("DROP $object")
                if $connection->has_sequence($sequence);
        }
        
    }
    $connection->close();
}


=item create_tables

=cut

sub create_tables {
    my ($self, @tables) = @_;
    my $connection = DBIx::Connection->connection($self->connection_name);
    for my $sql (@tables) {
        $connection->do($sql);
    }
    $connection->close();
}



=item objects_to_create

Returns list of pairs values('object_type object_name', create_sql, ..., 'object_typeN object_nameN', create_sqlN)

TODO - extend detection for complex plsql blocks
=cut

sub objects_to_create {
    my ($self, $sql) = @_;
    my @result;
    my @create_sql = split /;/, $sql;
    my $i = 0;
    my $plsql_block = "";
    my $inside_plsql_block;
    for my $sql_statement (@create_sql) {
        next unless ($sql_statement =~ /\w+/);
        my ($object) = ($sql_statement =~ m/create\s+(\w+\s+\w+)/i);
        if ($sql_statement =~ /begin/i) {
            $inside_plsql_block = 1 ;
            $plsql_block .= $sql_statement .";";
            next;
        } elsif ($sql_statement =~ /end$/i) {
            $sql_statement = $plsql_block . $sql_statement .";";
            $inside_plsql_block = 0;
            $plsql_block = "";
        } elsif ($inside_plsql_block) {
            $plsql_block .= $sql_statement . ";";
            next;
        }

        $object = $i++ unless $object;
        $sql_statement =~ s/^[\n\r\s]+// if ($sql_statement =~ m/^[\n\r\s]+/);
        push @result, $object, $sql_statement;
    }
    @result;
}


=item populate_schema

Populates schema

=cut

sub populate_schema {
    my ($self, $file_name) = @_;
    my @rows = $self->rows_to_insert(_load_file_content($file_name));
    my $connection = DBIx::Connection->connection($self->connection_name);
    for my $sql (@rows) {
        $connection->do($sql);
    }
    $connection->close();
}


=item rows_to_insert

=cut

sub rows_to_insert {
    my ($self, $sql) = @_;
    map  {($_ =~ /\w+/ ?  $_ .')' : ())} split qr{\)\W*;}, $sql;
   
}


=item dataset

Synchronizes/populates database to the passed in dataset.

dataset(
    table1 => [], #this deletes all data from table1 (DELETE FROM table1)
    table2 => [], #this deletes all data from table2 (DELETE FROM table2)
    table1 => [col1 => 'va1', col2 => 'val2'], #this insert or update depend on strategy
    table1 => [col1 => 'xval1', col2 => 'xval2'],
)

=cut

sub dataset {
    my ($self, @dataset) = @_;
    my $connection = DBIx::Connection->connection($self->connection_name);
    $self->delete_data(\@dataset, $connection);
    my $operation = ($self->load_strategy eq INSERT_LOAD_STRATEGY()) ? 'insert' : 'merge';
    for  (my $i = 0; $i < $#dataset; $i += 2) {
        my $table = $dataset[$i];
        my $data = $dataset[$i + 1];
        next unless @$data;
        $self->$operation($table, {@$data}, $connection);
    }
    $connection->close();
}


=item insert

Inserts data

=cut

sub insert {
    my ($self, $table, $field_values, $connection) = @_;
    my @fields = keys %$field_values;
    my $sql = sprintf "INSERT INTO %s (%s) VALUES (%s)",
        $table, join(",", @fields), join(",", ("?")x @fields);
    $connection->execute_statement($sql, map {$field_values->{$_}} @fields);
}


=item merge

Merges passed in data

=cut

sub merge {
    my ($self, $table, $field_values, $connection) = @_;
    my $operation  = ($self->has_primary_key_values($table, $field_values, $connection)) ? 'update' : 'insert'; 
    $self->$operation($table, $field_values, $connection);
}


=item update

Updates table values.

=cut

sub update {
    my ($self, $table, $field_values, $connection) = @_;
    my %pk_values = $self->primary_key_values($table, $field_values, $connection);
    my @fields = keys %$field_values;
    my @pk_fields = (sort keys %pk_values);
    my $where_clause = join(" AND ", map { $_ ." = ? " } @pk_fields);
    my $sql = sprintf "UPDATE %s SET %s WHERE %s",
        $table,
        join (", ", map { $_ . ' = ?' } @fields),
        $where_clause;
    $connection->execute_statement($sql, (map {$field_values->{$_}} @fields), (map { $pk_values{$_} } @pk_fields));
}


=item has_primary_key_values

Returns true if passed in dataset have primary key values

=cut

sub has_primary_key_values {
    my ($self, $table_name, $dataset, $connection) = @_;
    !! $self->primary_key_values($table_name, $dataset, $connection);
}


=item primary_key_values

=cut

sub primary_key_values {
    my ($self, $table_name, $dataset, $connection) = @_;
    my $pk_columns = $self->primary_key_definition_cache->{$table_name} ||= [$connection->primary_key_columns($table_name)];
    my @result;
    for my $column (@$pk_columns) {
        my $value = $dataset->{$column};
        return ()  unless defined $value;
        push @result, $column, $value;
    }
    @result;
}


=item delete_data

Deletes data from passed in tables.

=cut

sub delete_data {
    my ($self, $dataset, $connection) = @_;
    my @tables = $self->tables_to_delete($dataset);
    for my $table (@tables) {
        $connection->do("DELETE FROM $table");
    }
}


=item tables_to_delete

=cut

sub tables_to_delete {
    my ($self, $dataset) = @_;
    my @result = $self->empty_tables_to_delete($dataset);
    return @result if ($self->load_strategy ne INSERT_LOAD_STRATEGY());
    my %has_table = (map { $_ => 1 } @result);
    for  (my $i = $#{$dataset} - 1; $i >= 0; $i -= 2) {
        my $table = $dataset->[$i];
        next if $has_table{$table};
        $has_table{$table} = 1;
        push @result, $table;
    }
    @result;
}


=item tables_to_delete_explicitly

Returns list of table that are part of dataset table and are represented by table without attributes

  table1 => [],

  or in xml file

  <table1 />

=cut

sub empty_tables_to_delete {
     my ($self, $dataset) = @_;
     my @result;
     for  (my $i = 0; $i < $#{$dataset}; $i += 2) {
        next if @{$dataset->[$i + 1]};
        push @result, $dataset->[$i]
    }
    @result;
}


=item expected_dataset

Validates database schema against passed in dataset.

=cut

sub expected_dataset {
    my ($self, @dataset) = @_;
    my $operation = ($self->load_strategy eq INSERT_LOAD_STRATEGY())
        ? 'expected_dataset_for_insert_load_strategy'
        : 'expected_dataset_for_refresh_load_strategy';
    my $connection = DBIx::Connection->connection($self->connection_name);
    my $result = $self->$operation(\@dataset, $connection);
    $connection->close();
    $result;
}


=item expected_dataset_for_insert_load_strategy

Validates expected dataset for the insert load strategy.

=cut

sub expected_dataset_for_insert_load_strategy {
    my ($self, $exp_dataset, $connection) = @_;
    my %tables = (@$exp_dataset);
    my %tables_rows = (map { $_ => 0}keys %tables);
    my $tables_rows = $self->retrive_tables_data($connection, keys %tables);
    for (my $i = 0; $i < $#{$exp_dataset}; $i += 2) {
        my $table_name = $exp_dataset->[$i];
        my %values = @{$exp_dataset->[$i + 1]};
        next unless %values;
        $tables_rows{$table_name}++;
        my $pk_columns = $self->primary_key_definition_cache->{$table_name} ||= [$connection->primary_key_columns($table_name)];
        my $result = $self->validate_datasets($tables_rows->{$table_name}, \%values, $pk_columns, $table_name);
        return $result if $result;
    }
    $self->validate_number_of_rows(\%tables_rows, $connection);
}


=item validate_number_of_rows

Validates number of rows.

=cut

sub validate_number_of_rows {
    my ($self, $expected_result, $connection) = @_;
    foreach my $table_name (keys %$expected_result) {
        my $result = $connection->record("SELECT COUNT(*) AS cnt FROM ${table_name}");
        return "found difference in number of the ${table_name} rows - has "  . $result->{cnt} . " rows, should have " . $expected_result->{$table_name}
            if (! defined $result->{cnt} ||  $expected_result->{$table_name} ne $result->{cnt});
    }
}


=item validate_datasets

Validates passed exp dataset against fetched rows.
Return undef if there are not difference otherwise returns validation error.

=cut

sub validate_datasets {
    my ($self, $rows, $exp_dataset, $pk_columns, $table_name) = @_;
    my $hash_key = primary_key_hash_value($pk_columns, $exp_dataset);
    my @columns = keys %$exp_dataset;
    if ($hash_key) {
        my $result = compare_datasets($rows->{$hash_key}, $exp_dataset, $table_name, @columns);
        if ($rows->{$hash_key}) {
            return $result if $result;
            delete $rows->{$hash_key};
            return;
        }
    } else {#validation without primary key values
        my $exp_hash = join("-", map { $_ || '' } values %$exp_dataset);
        foreach my $k (keys %$rows) {
            my $dataset = $rows->{$k};
            my $rowhash = join("-", map {($dataset->{$_} || '')} @columns);
            if ($rowhash eq $exp_hash) {
                delete $rows->{$k};
                return;
            }
        }
    }
    "found difference in $table_name - missing entry: "
    . "\n  ". format_values($exp_dataset, @columns);
}


=item expected_dataset_for_refresh_load_strategy

Validates expected dataset for the refresh load strategy.

=cut

sub expected_dataset_for_refresh_load_strategy {
    my ($self, $exp_dataset, $connection) = @_;
    for (my $i = 0; $i < $#{$exp_dataset}; $i += 2) {
        my $table_name = $exp_dataset->[$i];
        my %values = @{$exp_dataset->[$i + 1]};
        my $pk_columns = $self->primary_key_definition_cache->{$table_name} ||= [$connection->primary_key_columns($table_name)];
        my $result = $self->validate_expexted_dataset(\%values, $pk_columns, $table_name, $connection);
        return $result if $result;
    }
}


=item validate_expexted_dataset

Validates passed exp dataset against database schema
Return undef if there are not difference otherwise returns validation error.

=cut

sub validate_expexted_dataset {
    my ($self, $exp_dataset, $pk_columns, $table_name, $connection) = @_;
    my @condition_columns = (@$pk_columns ? @$pk_columns : keys %$exp_dataset);
    my $where_clause = join(" AND ", map { $_ ." = ? " } @condition_columns);
    my $record = $connection->record("SELECT * FROM ${table_name} WHERE ". $where_clause, map    { $exp_dataset->{$_} } @condition_columns);
    if(grep { defined $_ } values %$record) {
        return compare_datasets($record, $exp_dataset, $table_name, keys %$exp_dataset);
    }
    "found difference in $table_name - missing entry: "
    . "\n  ". format_values($exp_dataset, keys %$exp_dataset);
}


=item compare_datasets

Compares two dataset hashes using passed in keys
Returns undef if there are not difference, otherwise difference details.

=cut

sub compare_datasets {
    my ($dataset, $exp_dataset, $table_name, @keys) = @_;
    for my $k (@keys) {
        return "found difference in $table_name $k:"
        . "\n  " . format_values($exp_dataset, @keys)
        . "\n  " . format_values($dataset, @keys)
        if (($dataset->{$k} || '') ne ($exp_dataset->{$k} || ''));
    }
}


=item format_values

=cut

sub format_values {
    my ($dataset, @keys) = @_;
    "[ " . join(" ",  map { $_ . " => '" . ($dataset->{$_} || '')  . "'" } @keys) ." ]";
}


=item retrive_tables_data

Returns retrieved data for passed in tables

=cut

sub retrive_tables_data {
    my ($self, $connection, @tables) = @_;
    my $result = {};
    for my $table_name (@tables) {
        $result->{$table_name} = $self->retrive_table_data($connection, $table_name);
    }
    $result;
}


=item retrive_table_data

Returns retrieved data for passed in table.

=cut

sub retrive_table_data {
    my ($self, $connection, $table_name) = @_;
    my $counter = 0;
    my $pk_columns = $self->primary_key_definition_cache->{$table_name} ||= [$connection->primary_key_columns($table_name)];
    my $cursor = $connection->query_cursor(sql => "SELECT * FROM ${table_name}");
    my $result_set = $cursor->execute();
    my $has_pk = !! @$pk_columns;
    my $result = {};
    while ($cursor->fetch()) {
        my $key = $has_pk ? primary_key_hash_value($pk_columns, $result_set) : "__" . ($counter++);
        $result->{$key} = {%$result_set};
    }
    $result;
}


=item primary_key_hash_value

Returns primary key values hash.

=cut

sub primary_key_hash_value {
    my ($primary_key_columns, $field_values) = @_;
    my $result = "";
    for (@$primary_key_columns) {
        return undef unless defined($field_values->{$_});
        $result .= $field_values->{$_} . "#";
    }
    $result;
}


=item reset_sequence

Resets passed in sequence

=cut

sub reset_sequence {
    my ($self, $sequence_name) = @_;
    my $connection = DBIx::Connection->connection($self->connection_name);
    $connection->reset_sequence($sequence_name);
    $connection->close();
}


=item xml_dataset

Loads xml file to dataset and populate/synchronize it to the database schema.

<dataset load_strategy="INSERT_LOAD_STRATEGY" reset_sequences="emp_seq">
    <emp ename="scott" deptno="10" job="project manager" />
    <emp ename="john"  deptno="10" job="engineer" />
    <emp ename="mark"  deptno="10" job="sales assistant" />
    <bonus ename="scott" job="project manager" sal="20" />
</dataset>

=cut

sub xml_dataset {
    my ($self, $file) = @_;
    my $xml = $self->load_xml($file);
    $self->apply_properties($xml->{properties});
    $self->dataset(@{$xml->{dataset}});
}


=item xml_expected_dataset dataset

Loads xml file to expected dataset and validated it against the database schema.

=cut

sub xml_expected_dataset {
    my ($self, $file) = @_;
    my $xml = $self->load_xml($file);
    $self->apply_properties($xml->{properties});
    $self->expected_dataset(@{$xml->{dataset}});
}


=item apply_properties

=cut

sub apply_properties {
    my ($self, $properties) = @_;
    my $strategy = $properties->{load_strategy};
    if ($strategy) {
        $self->set_load_strategy(INSERT_LOAD_STRATEGY)
            if ($strategy eq "INSERT_LOAD_STRATEGY");
                
        $self->set_load_strategy(REFRESH_LOAD_STRATEGY)
            if ($strategy eq "REFRESH_LOAD_STRATEGY");
    }
    
    my $reset_sequences = $properties->{reset_sequences};
    if ($reset_sequences) {
        my @seqs = split /,/, $reset_sequences;
        for my $sequence_name (@seqs) {
            $self->reset_sequence($sequence_name);
        }
    }
    

}


=item xml_dataset_handler

=cut

{   my $xml;

    sub xml_dataset_handler {
        unless($xml) {
            $xml = Simple::SAX::Serializer->new;
            $xml->handler('dataset', sub {
                    my ($self, $element, $parent) = @_;
                    $element->validate_attributes([],
                        {load_strategy => "INSERT_LOAD_STRATEGY", reset_sequences => undef}
                    );
                    my $attributes = $element->attributes;
                    my $children_result = $element->children_result;
                    {properties => $attributes, dataset => $children_result}
                }
            );
            $xml->handler('*', sub {
                my ($self, $element, $parent) = @_;
                my $attributes = $element->attributes;
                my $children_result = $parent->children_array_result;
                my $result = $parent->children_result;
                push @$children_result, $element->name => [map { $_ => $attributes->{$_}} sort keys %$attributes];
            });
        }
        $xml;
    }
}

=item load_xml

Loads xml

=cut

sub load_xml {
    my ($self, $file) = @_;    
    my $xml = $self->xml_dataset_handler;
    $xml->parse_file($file);
}


=item _load_file_content

=cut

sub _load_file_content {
    my $file_name = shift;
    open my $fh, '<', $file_name or confess "cant open file ${file_name}";
    local $/ = undef;
    my $content = <$fh>;
    close $fh;
    $content ;
}

1;

__END__

=back

=head1 COPYRIGHT

The DBUnit module is free software. You may distribute under the terms of
either the GNU General Public License or the Artistic License, as specified in
the Perl README file.

=head1 SEE ALSO

L<DBIx::Connection>

=head1 AUTHOR

Adrian Witas, E<lt>adrian@webapp.strefa.pl</gt>

See also 

=cut