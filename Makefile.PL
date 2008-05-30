
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME         => 'Test::DBUnit',
    VERSION_FROM => 'lib/Test/DBUnit.pm',
    DISTNAME     => 'Test-DBUnit',
    PREREQ_PM    => { 
        'Abstract::Meta::Class'   => 0,
	'DBIx::Connection'        => 0.05,
	'Simple::SAX::Serializer' => 0,
        'Test::Pod'               => 0,
        'Test::Pod::Coverage'     => 0,
    },
    dist         => { COMPRESS => 'gzip', SUFFIX => 'gz' },
    clean        => { FILES => 'MANIFEST.bak' },
);
