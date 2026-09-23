#
# Create combined sketches for viral data.
#
# 
#

use P3DataAPI;
use strict;
use Data::Dumper;
use Getopt::Long::Descriptive;
use LPTScheduler;
use IPC::Run qw(run);
use File::Path qw(make_path);

use File::Temp;

my($opt, $usage) = describe_options("%c %o sketch-dir combined-dir",
				    ['filter=s%' => "Define a family filter file", { default => {} }],
				    ['taxon-filter=s%' => "Define a taxon filter file", { default => {} }],
				    ['parallel|p=i' => "Number of processes", { default => 1 }],
				    ['skip-existing' => "Reuse a family sketch that already exists in the output directory instead of pasting it again. For resuming an interrupted run; assumes the inputs have not changed"],
				    ["help|h" => 'Show this help message']);
print($usage->text), exit 0 if $opt->help;
die($usage->text) if @ARGV != 2;

my $sketch_dir = shift;
my $combined_dir = shift;

-d $combined_dir or die "Combined sketch directory $combined_dir does not exist\n";
my $by_family = "$combined_dir/by-family";
make_path($by_family);

my $tmpdir = File::Temp->newdir(CLEANUP => 0);
print "TMP: " . Dumper($tmpdir);
print "tmpdir=$tmpdir\n";

#
# Look up reference and representative genome lists.
#


my $api = P3DataAPI->new;

# goto x;

my @repref = $api->query("genome",
			  ["eq", "superkingdom", "Viruses"],
			  ["select", "genome_name,genome_id,genome_length,family"],
			  ["in", "reference_genome", "(Representative,Reference)"],
			 );

#
# First create list for the repref data.
#

my $repref_tbl = "$tmpdir/repref.viral.msh";
open(TBL, ">", $repref_tbl) or die "Cannot open $repref_tbl: $!";

for my $ent (@repref)
{
    my($name,$gid,$len,$family) = @$ent{qw(genome_name genome_id genome_length family)};

    my $p1 = "$sketch_dir/$gid.msh";
    my $p2 = "$sketch_dir/$family/$gid.msh";
    if (-s $p1)
    {
	print TBL "$p1\n";
    }
    elsif (-s $p2)
    {
	print TBL "$p2\n";
    }
    else
    {
	warn "Cannot find ref $name $gid $family: neither $p1 nor $p2\n";
    }
}
close(TBL);
{
    open(my $tf, "<", $repref_tbl) or die "Cannot open $repref_tbl: $!";
    chomp(my @rr = <$tf>);
    close($tf);
    @rr = grep { /\S/ } @rr;
    paste_files("$combined_dir/refrep.viral.msh", \@rr, "$tmpdir")
	or die "Error pasting the refrep viral sketches\n";
}

#
# Now create combined sketches for the families.
#
# We apply genome filters as defined in the --filter parameter.
#
# x:

#
# Look up private genomes to remove those from the sketches.
#

my @private = $api->query("genome", ["eq", "public", "false"], ["select", "genome_id"]);

#
# Also filter out partial viral genomes
#
my @partial = $api->query("genome", ["eq", "superkingdom", "Viruses"], ["eq", "genome_status", "Partial"], ["select", "genome_id"]);

my %filter_out = map { $_->{genome_id} => 1 } @private, @partial;


my $sched = LPTScheduler->new($opt->parallel);

my %filter = %{$opt->filter};

#
# Handle taxon filters. These are files that define the set of IDs to be included
# in the given taxon.
#

my %taxon_filter;
while (my($t, $f) = each(%{$opt->taxon_filter}))
{
    open(F, "<", $f) or die "cannot open taxon filter file $f for taxon $t: $!";
    while (<F>)
    {
	if (/(\d+\.\d+)/)
	{
	    $taxon_filter{$t}->{$1} = 1;
	}
    }
    close(F);
}


opendir(D, $sketch_dir);
my @fam_subdirs = grep { $_ ne '.' && $_ ne '..' &&  -d "$sketch_dir/$_" } readdir(D);
closedir(D);

my @work;
print "Writing to $tmpdir\n";

my $combined_file = "$tmpdir/combined.tbl";
my @combined;
my @empty_families;
my $reused = 0;
open(COMBINED, ">", $combined_file) or die "Cannot write $combined_file: $!";
for my $fam (sort @fam_subdirs)
{
    my $fam_name = $fam;
    $fam_name = "missingname" if $fam eq '.';

    my $filter = delete $filter{$fam_name};

    my $combined = "$by_family/$fam_name.msh";
    my %genome_filter;
    if ($filter)
    {
	print "Filtering $fam on $filter\n";
	open(F, "<", $filter) or die "Cannot open genome filter $filter: $!";
	while (<F>)
	{
	    if (/(\d+\.\d+)/)
	    {
		$genome_filter{$1} = 1;
	    }
	}
	close(F);
    }

    my $list = "$tmpdir/$fam_name.tbl";
    open(LIST, ">", $list) or die "Cannot open $list: $!";

    opendir(D, "$sketch_dir/$fam") or die "Cannot opendir $sketch_dir/$fam: $!";
    my $n = 0;
    while (my $f = readdir(D))
    {
	if ($f =~ /((\d+)\.\d+).msh/)
	{
	    my $gid = $1;
	    my $taxon = $2;
	    next if $filter_out{$gid};
	    next if $filter && !$genome_filter{$gid};
	    my $tf = $taxon_filter{$taxon};
	    next if $tf && !$tf->{$gid};
	    print LIST "$sketch_dir/$fam/$f\n";
	    $n++;
	}

    }
    close(LIST);

    #
    # A family can filter down to nothing, and that is not a failure. The
    # private/partial exclusions above remove whole families: 20 of the 354 in
    # the 2026-09 build (Alphaormycoviridae, Chiyouviridae, Yaraviridae, ...)
    # consist entirely of genome_status=Partial genomes. Such a family has
    # nothing to paste and produces no sketch, so it must not go into @combined
    # -- otherwise the completeness check below sees a file that was never
    # supposed to exist and refuses to bucket, which is what happened on the
    # first run with that check in place.
    #
    if ($n == 0)
    {
	unlink($list);
	push(@empty_families, $fam_name);
	next;
    }

    push(@combined, $combined);
    print COMBINED "$combined\n";

    #
    # --skip-existing lets a run that failed late be resumed without repeating
    # the family pastes, which are the expensive part. Off by default: a sketch
    # left over from an earlier run is not re-verified against the current
    # filters, so it is only safe when you know the inputs have not changed.
    #
    if ($opt->skip_existing && -s $combined)
    {
	print "Reusing existing $combined\n";
	$reused++;
	next;
    }

    $sched->add_work([$fam, $fam_name, $combined, $list], $n);
}
close(COMBINED);

if (@empty_families)
{
    printf "%d famil%s had no sketches left after filtering and were skipped: %s\n",
	scalar @empty_families, (@empty_families == 1 ? "y" : "ies"),
	join(", ", @empty_families);
}
print "Reusing $reused existing family sketch(es)\n" if $reused;

if (%filter)
{
    die "Unused family filters: " . join(", ", keys(%filter)), "\n";
}

#
# Paste each family.
#
# A failure here used to die, which killed far more than the family that
# failed: LPTScheduler packs all the work into $opt->parallel bins and runs one
# bin per process, so with the default --parallel 1 there is a single bin
# holding every family, sorted largest first. When Orthomyxoviridae (the
# largest, and the one that exceeded the mmap limit) died, it took all 354
# families with it and by-family/ came out empty. Warn and carry on instead, so
# one bad family costs one family.
#
$sched->run(sub {}, sub {
    my($global, $item) = @_;
    my($fam, $name, $combined, $list) = @$item;

    open(my $lf, "<", $list) or do { warn "Cannot open $list: $!\n"; return; };
    chomp(my @files = <$lf>);
    close($lf);
    @files = grep { /\S/ } @files;

    if (!@files)
    {
	warn "No sketches for family $name, skipping\n";
	return;
    }

    print "$$ Paste " . scalar(@files) . " sketches for $name into $combined\n";

    my $ftmp = File::Temp->newdir();
    paste_files($combined, \@files, "$ftmp")
	or warn "Paste failed for family $name ($combined)\n";
});

#
# Report what did not get built before going on to the bucketing step, which
# reads every file in @combined. Without this the first missing family surfaces
# as an opaque mash error ("could not open .../Ackermannviridae.msh") naming a
# family that was never the problem -- it was just the first one the bucketer
# happened to touch.
#
#
# @combined now holds only families that had at least one sketch to paste, so
# anything missing here really did fail rather than merely being empty.
#
my @failed = grep { ! -s $_ } @combined;
if (@failed)
{
    warn "\n" . scalar(@failed) . " of " . scalar(@combined) . " family sketches were not built:\n";
    warn "  $_\n" for @failed;
    die "Refusing to build the all-viral buckets from an incomplete set.\n";
}

#
# We cannot create a single sketch with all the sequences; mash dist crashes.
# Set an arbitrary limit of 8G (since we know mash dist on a 7.3GB file works),
# and collect the sketches into a minimal set.
#

#
# Create a set of buckets.
# Sort our combined files by size, in decreasing order.
# For each file, try to put in an existing bucket. If it can't be added, start a new bucket.
#

my @buckets;

my @combined_sizes = sort { $b->[1] <=> $a->[1] } map { [$_, -s $_] } @combined;

my $max = 20_000_000_000;
#my $max = 8_000_000_000;

for my $c (@combined_sizes)
{
    my($file, $size) = @$c;

    my $found;
    for my $b (@buckets)
    {
	if ($b->{size} + $size < $max)
	{
	    push(@{$b->{list}}, $c);
	    $b->{size} += $size;
	    $found = 1;
	    last;
	}
    }
    if (!$found)
    {
	push(@buckets, { size => $size, list => [$c] });
    }
}

my $sched = LPTScheduler->new($opt->parallel);

my $n = 1;
for my $bucket (@buckets)
{
    my($list) = $bucket->{list};
    my $in = join("\n", map { $_->[0] } @$list) . "\n";

    my $out = sprintf("$combined_dir/all-viral-%02d.msh", $n++);

    $sched->add_work([$in, $out], $bucket->{size});
}

$sched->run(sub {}, sub {
    my($global, $item) = @_;
    my($in, $out) = @$item;
    
    my @cmd = ("mash", "paste", "-l", $out, "/dev/fd/0");
    my $ok = run(\@cmd, "<", \$in);
});

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
