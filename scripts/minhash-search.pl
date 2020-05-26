#
# Search a minhash database for the given genome.
#
# Input is either a contigs file or a genome object. If a genome object is
# used the output will be a genome object modified with the closest genomes added
# to the close_genomes list.
#

use strict;
use GenomeTypeObject;
use Getopt::Long::Descriptive;
use Data::Dumper;

my($opt, $usage) = describe_options("%c %o reference-database [reference-database ... ] input-file",
				    ["max-pvalue|v=f", "Maximum p-value to report (0-1)", { default => 0.2 }],
				    ["max-distance|d=f", "Maximium distance to report (0-1)", { default => 1 }],
				    ["parallelism|p=i", "Parallelism", { default => 4 }],
				    ["help|h", "Show this help message"]);
print($usage->text), exit 0 if $opt->help;
die($usage->text) if @ARGV < 2;

my $input = pop;
my @refs = @ARGV;

my @err;
for my $ref (@refs)
{
    if (!-s $ref)
    {
	push(@err, "Reference database $ref not found\n");
    }
}
die join("", @err) if @err;

my $type;

open(I, "<", $input) or die "Cannot open input file $input: $!";

if ($input =~ /\.msh$/)
{
    $type = 'sketch';
}
elsif ($input =~ /\.fq\.gz$/ || $input =~ /\.fastq\.gz$/)
{
	$type = 'fastq';
}
elsif ($input =~ /\.gz$/)
{
	$type = 'fasta';
}
else
{
    my $l1 = <I>;
    
    if ($l1 =~ /^\{/)
    {
	$type = 'gto';
    }
    elsif ($l1 =~ /^>/)
    {
	$type = 'fasta';
    }
    elsif ($l1 =~ /^@/)
    {
	$type = 'fastq';
    }
    else
    {
	die "Input file $input doesn't look like either a genome object or a fasta file\n";
    }
    seek(I, 0, 0);
}

my $gto;
my $fasta;
if ($type eq 'gto')
{
    #
    # Need to read and parse the GTO, then extract contigs to a file.
    #
    $gto = GenomeTypeObject->new({ file => $input });
    $gto or die "Could not create genome object from $input\n";
    $fasta = $gto->extract_contig_sequences_to_temp_file();
}
else
{
    $fasta = $input;
}

#
# Run mash with appropriate parameters.
#

my @opts;

if ($type eq 'fastq')
{
    push(@opts, "-r");
}

my @vals;

for my $ref (@refs)
{
    my @cmd = ("mash", "dist",
	       "-p", $opt->parallelism,
	       "-v", $opt->max_pvalue,
	       "-d", $opt->max_distance,
	       @opts,
	       $ref,
	       $fasta);
    
    open(P, "-|", @cmd) or die "Cannot run @cmd: $!";
    while (<P>)
    {
	chomp;
	my($ref_file, $me, $dist, $pv, $counts) = split(/\t/);
	my($g, $tax, $idx) = $ref_file =~ m,genomes/((\d+)\.(\d+)),;
	push(@vals, [$g, $tax, $idx, $dist, $pv, $counts]);
    }
}

for my $ent (sort { $a->[3] <=> $b->[3] or $a->[1] <=> $b->[1] or $a->[2] <=> $b->[2] } @vals)
{
    my($g, undef, undef,  $dist, $pv, $counts) = @$ent;
    print join("\t", $g, $dist, $pv, $counts), "\n";
}

