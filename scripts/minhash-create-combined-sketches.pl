#
# Using the data in the given sketch directory, write combined sketches all.msh for
# all genomes; refs.msh for the reference genomes, and reps.msh for the representatives.
#
# mash paste mmaps every input sketch at once, so a large paste used to need
#
#  sudo sysctl -w  vm.max_map_count=1000000
#
# That no longer suffices -- the 2026-09 build pasted 1,035,624 sketches
# against a limit of 1,048,576, i.e. it cleared it by 12,952 -- so paste_files()
# below chunks the paste instead and the sysctl is no longer load bearing.
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

    print "Sketch " . scalar(@$files) . " files into $out\n";

    my $tmpdir = File::Temp->newdir();
    paste_files($out, $files, "$tmpdir")
	or die "Error pasting " . scalar(@$files) . " sketches into $out\n";
}, { Max_Workers => 4 };

#
# mash paste mmaps every input sketch at once, so a paste of more files than
# vm.max_map_count simply fails:
#
#   Error: could not memory-map file .../11320.230343.msh of size 8488
#
# Measured on lime (vm.max_map_count = 1048576): 1,000,000 inputs paste fine,
# 1,100,000 fail. The limit is a count of mappings, not a size -- the file it
# names is always small and never the problem, which makes the message
# misleading.
#
# Raising the sysctl is the traditional answer (the note at the top of
# minhash-create-combined-sketches.pl says so) but it does not scale: as of
# 2026-09 Orthomyxoviridae alone has 2,157,909 sketches, more than twice the
# limit, and the bacterial "all" paste cleared it by only 12,952.
#
# Paste in chunks instead and then paste the chunks. mash paste accepts sketch
# files as input, so this composes, and the result is identical: verified on a
# 30,000-sketch sample that one-stage and two-stage pastes agree row for row,
# ids included.
#
sub paste_chunk_size
{
    return $ENV{MINHASH_PASTE_CHUNK} if $ENV{MINHASH_PASTE_CHUNK};

    #
    # Half the limit. mash needs a few hundred mappings of its own and other
    # things on the box may be competing for the same budget, so do not try to
    # use all of it.
    #
    my $max;
    if (open(my $fh, "<", "/proc/sys/vm/max_map_count"))
    {
	chomp($max = <$fh>);
	close($fh);
    }
    $max = 1048576 unless $max && $max =~ /^\d+$/;

    my $n = int($max / 2);
    $n = 100_000 if $n < 100_000;
    return $n;
}

#
# Paste @$files into $out (a path with or without the .msh suffix mash adds),
# chunking if there are too many for one mash invocation. $tmpdir holds the
# intermediate sketches. Returns 1 on success, 0 on failure.
#
sub paste_files
{
    my($out, $files, $tmpdir) = @_;

    my $chunk = paste_chunk_size();

    if (@$files <= $chunk)
    {
	return run_paste($out, $files);
    }

    my $n = @$files;
    my $nchunks = int(($n + $chunk - 1) / $chunk);
    print STDERR "Pasting $n sketches into $out in $nchunks chunks of up to $chunk\n";

    my $base = $out;
    $base =~ s,.*/,,;
    $base =~ s,\.msh$,,;

    my @stage;
    my @todo = @$files;
    my $i = 0;
    while (@todo)
    {
	my @part = splice(@todo, 0, $chunk);
	my $part_out = sprintf("%s/%s.part%03d", $tmpdir, $base, $i++);
	run_paste($part_out, \@part) or return 0;
	push(@stage, "$part_out.msh");
    }

    my $ok = run_paste($out, \@stage);

    #
    # The intermediates are large (a chunk of 500,000 sketches is ~4 GB), so do
    # not leave them for the caller's tmpdir cleanup, which in the viral script
    # is a File::Temp->newdir(CLEANUP => 0) that never fires.
    #
    unlink(@stage);

    return $ok;
}

sub run_paste
{
    my($out, $files) = @_;

    my $list = File::Temp->new();
    print $list "$_\n" foreach @$files;
    close($list);

    my @cmd = ("mash", "paste", "-l", $out, "$list");
    my $rc = system(@cmd);
    if ($rc != 0)
    {
	warn "Failed with $rc: @cmd\n";
	return 0;
    }
    return 1;
}
