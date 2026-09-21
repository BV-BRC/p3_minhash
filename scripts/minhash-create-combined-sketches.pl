#
# Using the data in the given sketch directory, write combined sketches all.msh for
# all genomes; refs.msh for the reference genomes, and reps.msh for the representatives.
#
# Note for large numbers of genomes we need this:
#
#  sudo sysctl -w  vm.max_map_count=1000000
#
# since mash paste apparently mmaps each sketch.
#
# This code is for the bacterial genomes.
#

use P3DataAPI;
use strict;
use Data::Dumper;
use Getopt::Long::Descriptive;
use Proc::ParallelLoop;
use File::Temp;

my($opt, $usage) = describe_options("%c %o sketch-dir combined-dir",
				    ["missing-data=s", "Write missing data here"],
				    ["help|h" => 'Show this help message']);
print($usage->text), exit 0 if $opt->help;
die($usage->text) if @ARGV != 2;

my $sketch_dir = shift;
my $combined_dir = shift;

my $genome_dir = '/vol/patricftp/ftp/genomes';

#opendir(D, $sketch_dir) or die "Cannot opendir $sketch_dir: $!";
-d $sketch_dir or die "Sketch dir $sketch_dir does not exist\n";
-d $combined_dir or die "Combined sketch dir $combined_dir does not exist\n";

#
# Enumerate all bacterial/archaeal genomes, building ancillary lists of representative and reference genomes.
#

my %repref;
my %refs;
my %reps;
my @all_genomes;

my @rep_sketches;
my @ref_sketches;
my @all_sketches;

my $api = P3DataAPI->new;

my @missing;
my @missing_with_ftp_data;
my @missing_with_empty_ftp_data;

my $missing_fh;
if ($opt->missing_data)
{
    open($missing_fh, ">", $opt->missing_data) or die "Cannot write " . $opt->missing_data . ": $!";
}

my $gsub = sub {
    my($data, $meta) = @_;
    for my $g (@$data)
    {
	my $gid = $g->{genome_id};

	my $sketch = "$sketch_dir/$gid.msh";

	if (! -f $sketch)
	{
	    if ($missing_fh)
	    {
		my $genome = "$genome_dir/$gid/$gid.fna";
		if (-s $genome)
		{
		    push(@missing_with_ftp_data, $gid);
		    print $missing_fh "$gid\thas_ftp_data\t$g->{date_inserted}\n" if $missing_fh;
		}
		elsif (-f $genome)
		{
		    push(@missing_with_empty_ftp_data, $gid);
		    print $missing_fh "$gid\thas_empty_ftp_data\t$g->{date_inserted}\n" if $missing_fh;
		}
		else
		{
		    push(@missing, $gid);
		    print $missing_fh "$gid\tno_ftp_data\t$g->{date_inserted}\n" if $missing_fh;
		}
	    }
	    next;
	}
	
	my $r = $g->{reference_genome};

	push(@all_sketches, $sketch);

	push(@all_genomes, $gid);
	if ($r eq 'Reference')
	{
	    $refs{$gid} = 1;
	    $repref{$gid} = 1;
	    push(@ref_sketches, $sketch);
	}
	elsif ($r eq 'Representative')
	{
	    $reps{$gid} = 1;
	    $repref{$gid} = 1;
	    push(@rep_sketches, $sketch);
	}
    }
    return 1;
};

#
# Enumerate the genomes that belong in the combined sketches.
#
# These filters must match the ones minhash-build-sketches.pl applies, or the
# two scripts disagree about the population and the "missing" tally below
# becomes noise: before this was aligned, the builder targeted the Good public
# genomes while this script enumerated every public genome, so it reported a
# permanent ~330k shortfall that no amount of building would close.
#
# They are spelled in RQL here and as raw Solr fq params there, because the two
# scripts use different P3DataAPI entry points. The correspondence is:
#
#   ne(genome_status,Deprecated)  ==  fq => -genome_status:Deprecated
#   gt(genome_length,0)           ==  fq => genome_length:*
#
# Both pairs were verified to return identical counts (1,035,704) as of
# 2026-09. There is deliberately no shared module holding this list: the
# Makefile's deploy-client target deploys scripts without libs, so a lib
# dependency here would break a client-only deployment.
#
my @res = $api->query_cb('genome', $gsub,
			 ['in', 'superkingdom', '(Bacteria,Archaea)'],
			 ['eq', 'public', 'true'],
			 ['eq', 'genome_quality', 'Good'],
			 ['ne', 'genome_status', 'Deprecated'],
			 ['gt', 'genome_length', '0'],
			 ['select', 'genome_id,reference_genome,date_inserted']);

close($missing_fh) if $missing_fh;
my $nmissing = @missing;
my $nmissing_with_empty_ftp_data = @missing_with_empty_ftp_data;
my $nmissing_with_ftp_data = @missing_with_ftp_data;
my $nrep = keys %reps;
my $nref = keys %refs;
my $nall = @all_genomes;

print STDERR "$nrep reps, $nref refs, $nall total. $nmissing missing $nmissing_with_ftp_data missing with ftp data $nmissing_with_empty_ftp_data with empty ftp data\n";

pareach [[\@all_sketches, "$combined_dir/all"],
	 [\@ref_sketches, "$combined_dir/refs"],
	 [\@rep_sketches, "$combined_dir/reps"], 
	 [[@ref_sketches, @rep_sketches], "$combined_dir/repref"]], sub
{
    my($work) = @_;
    my($files, $out) = @$work;

    my $tmp = File::Temp->new(UNLINK => 0);
    print $tmp "$_\n" foreach @$files;
    close($tmp);
    
    print "Sketch " . scalar(@$files) . " files into $out\n";

    my @cmd = ("mash", "paste", "-l", $out, "$tmp");
    print "@cmd\n";
    my $rc = system(@cmd);
    if ($rc != 0)
    {
	die "Error running @cmd\n";
    }
}, { Max_Workers => 4 };

