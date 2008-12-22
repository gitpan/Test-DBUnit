package Test::DBUnit::Generator;
use strict;
use warnings;
use Data::Dumper;
use Abstract::Meta::Class ':all';
use DBIx::Connection;
use Carp 'confess';
use XML::Writer;
use IO::File;

use vars qw($VERSION);

$VERSION = '0.21';


=head1 NAME

Test::DBUnit::Generator - dbunit dataset generator

=head1 SYNOPSIS

    use Test::DBUnit::Generator;

    my $connection = DBIx::Connection->new(
        name     => 'test',
        dsn      => $ENV{DB_TEST_CONNECTION},
        username => $ENV{DB_TEST_USERNAME},
        password => $ENV{DB_TEST_PASSWORD},
    );

    my $generator = Test::DBUnit::Generator->new(
        connection      => $connection,
        datasets => {
            emp => 'SELECT * FROM emp',
            dept => 'SELECT * FROM demp',
        },
    );
    
    print $generator->xml;
    print $generator->perl;
    

=head1 DESCRIPTION

This class generates xml or perl test datasets based on passed in sql 

=head2 ATTRIBUTES

=over

=item connection

=cut

has '$.connection';


=item datasets_order

Specifies order of the dataset in the generation result.

    my $generator = Test::DBUnit::Generator->new(
        connection      => $connection,
        datasets_order   => ['emp', 'dept'],
        datasets => {
            emp => 'SELECT * FROM emp',
            dept => 'SELECT * FROM demp',
        },
    );


=cut

has '@.datasets_order';


=item datasets

=cut

has '%.datasets' => (item_accessor => 'dataset');


=back

=head2 METHODS

=over

=item xml

Returns xml content that contains dataset 

=cut

sub xml {
    my ($self) = @_;
    my $output;
    my $file = IO::File->new;
    $file->open(\$output, '>');
    my $writer = new XML::Writer(OUTPUT => $file, NEWLINES => 1);
    $writer->xmlDecl("UTF-8");
    $writer->startTag("dataset", );
    my $datasets = $self->datasets;
    my @datasets_order = $self->datasets_order;
    @datasets_order = keys %$datasets unless @datasets_order;
    foreach my $k (@datasets_order) {
    my $data = $self->select_dataset($k);
        for my $row (@$data) {
            $writer->emptyTag($k, %$row);
        }
    }
    $writer->endTag("dataset");
    $writer->end();
    $output =~ s/[\n\r](\s*\/>)/$1\n/g;
    $output =~ s/[\n\r](\s*>)/$1\n/g;
    $output;
}


=item perl

=cut

sub perl {
    my ($self) = @_;
    my $datasets = $self->datasets;
    local $Data::Dumper::Indent = 0;
    my $result = '';
    my @datasets_order = $self->datasets_order;
    @datasets_order = keys %$datasets unless(@datasets_order);
    foreach my $k (@datasets_order) {
        my $data = $self->select_dataset($k);
        for my $row (@$data) {
            my $var = Dumper([%$row]);
            $var =~ s/\$VAR1/    $k/;
            $var =~ s/;$/,/;
            $var =~ s/=/=>/;
            $result .= ($result ? "\n" : ''). $var;
        }
    }
    q{dataset_ok(
} . $result . q{
);}
}


=item select_dataset

Returns dataset structure

=cut

sub select_dataset {
    my ($self, $name) = @_;
    my $sql = $self->dataset($name);
    my $cursor = $self->connection->query_cursor(sql => $sql);
    my $resultset = $cursor->execute();
    my $result = [];
    while($cursor->fetch()) {
        push @$result, {%$resultset};
    }
    $result;
}

=back

=cut

1;