#
# Search a minhash database for the given genome.
#
# Input is either a contigs file or a genome object. If a genome object is
# used the output will be a genome object modified with the closest genomes added
# to the close_genomes list.
#

use strict;
use P3DataAPI;
use GenomeTypeObject;
use Getopt::Long::Descriptive;
use Data::Dumper;

my($opt, $usage) = describe_options("%c %o reference-database input-file",
				    ["reference", "Include reference genomes"],
				    ["representative", "Include representative genomes"],
				    ["max-pvalue|v=f", "Maximum p-value to report (0-1)", { default => 0.2 }],
				    ["max-distance|d=f", "Maximium distance to report (0-1)", { default => 1 }],
				    ["parallelism|p=i", "Parallelism", { default => 4 }],
				    ["help|h", "Show this help message"]);
print($usage->text), exit 0 if $opt->help;
die($usage->text) if @ARGV != 2;

my $api = P3DataAPI->new();

my $ref = shift;
my $input = shift;

-s $ref or die "Reference database $ref not found\n";
open(I, "<", $input) or die "Cannot open input file $input: $!";
my $l1 = <I>;

my $type;
if ($l1 =~ /^{/)
{
    $type = 'gto';
}
elsif ($l1 =~ /^>/)
{
    $type = 'fasta';
}
else
{
    die "Input file $input doesn't look like either a genome object or a fasta file\n";
}
seek(I, 0, 0);

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
# The reference file 
#

my @cmd = ("mash", "dist",
	   "-p", $opt->parallelism,
	   "-v", $opt->max_pvalue,
	   "-d", $opt->max_distance,
	   $ref,
	   $fasta);

my @vals;
my @genomes;
open(P, "-|", @cmd) or die "Cannot run @cmd: $!";
while (<P>)
{
    chomp;
    my($ref, $me, $dist, $pv, $counts) = split(/\t/);
    my($g, $tax, $idx) = $ref =~ m,genomes/((\d+)\.(\d+)),;
    push(@vals, [$g, $tax, $idx, $dist, $pv, $counts]);
    push(@genomes, $g);
}


#
# Look up genome information.
#
# If user supplied either ref or rep or both, limit search to those.
#

my @limit;
my @refs;
push(@refs, "Representative") if $opt->representative;
push(@refs, "Reference") if $opt->reference;
if (@refs)
{
    push(@limit, ["in", "reference_genome", "(" . join(",", @refs) . ")"]);
}

my %ginfo;
while (@genomes)
{
    my @chunk = splice(@genomes, 0, 100);
    $api->query_cb("genome", sub
	       {
		   my($data) = @_;
		   $ginfo{$_->{genome_id}} = $_ foreach @$data;
	       },
		   @limit, ['in', 'genome_id', "(" . join(",", @chunk) . ")"], ["select", "genome_id,genome_name,reference_genome"]);
}

for my $ent (sort { $a->[3] <=> $b->[3] or $a->[1] <=> $b->[1] or $a->[2] <=> $b->[2] } @vals)
{
    my($g, undef, undef,  $dist, $pv, $counts) = @$ent;
    my $info = $ginfo{$g};
    next unless $info;
    print join("\t", $g, $info->{genome_name}, $dist, $pv, $counts), "\n";
}

