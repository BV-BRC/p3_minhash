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
				    ['filter=s%' => "Define a family filter file"],
				    ['parallel|p=i' => "Number of processes", { default => 1 }],
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

goto x;

my @repref = $api->query("genome",
			  ["eq", "superkingdom", "Viruses"],
			  ["select", "genome_name,genome_id,genome_length,family"],
			  ["in", "reference_genome", "(Representative,Reference)"],
			 );

#
# First create list for the repref data.
#

my $repref_tbl = "$tmpdir/repref.viral.msh";

for my $ent (@repref)
{
    my($name,$gid,$len,$family) = @$ent{qw(genome_name genome_id genome_length family)};

    my $p = "$sketch_dir/$family/$gid.msh";
    if (-f $p)
    {
	print $repref_tbl "$p\n";
    }
    else
    {
	warn "Cannot find ref $name $gid $family\n";
    }
}
my @cmd = ("mash", "paste", "-l", "$combined_dir/refrep.viral.msh", "$repref_tbl");
my $ok = run(\@cmd);
$ok or die "Error $? running paste: @cmd\n";

#
# Now create combined sketches for the families.
#
# We apply genome filters as defined in the --filter parameter.
#
x:

my $sched = LPTScheduler->new($opt->parallel);

my %filter = %{$opt->filter};

opendir(D, $sketch_dir);
my @fam_subdirs = grep { $_ ne '..' &&  -d "$sketch_dir/$_" } readdir(D);
closedir(D);

my @work;
print "Writing to $tmpdir\n";

my $combined_file = "$tmpdir/combined.tbl";
my @combined;
open(COMBINED, ">", $combined_file) or die "Cannot write $combined_file: $!";
for my $fam (sort @fam_subdirs)
{
    my $fam_name = $fam;
    $fam_name = "missingname" if $fam eq '.';

    my $filter = delete $filter{$fam_name};

    my $combined = "$by_family/$fam_name.msh";
    push(@combined, $combined);
    print COMBINED "$combined\n";
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
	if ($f =~ /(\d+\.\d+).msh/)
	{
	    my $gid = $1;
	    next if $filter && !$genome_filter{$gid};
	    print LIST "$sketch_dir/$fam/$f\n";
	    $n++;
	}

    }
    close(LIST);
    $sched->add_work([$fam, $fam_name, $combined, $list], $n);
}
close(COMBINED);

if (%filter)
{
    die "Unused family filters: " . join(", ", keys(%filter)), "\n";
}

$sched->run(sub {}, sub {
    my($global, $item) = @_;
    my($fam, $name, $combined, $list) = @$item;
    my @cmd = ("mash", "paste", "-l", $combined, $list);
    print "$$ Run @cmd\n";
    my $rc = system(@cmd);
    $rc == 0 or die "Faile with $rc: @cmd\n";
});

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

my $max = 8_000_000_000;

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

my $sched = LPTScheduler->new($parallel);

my $n = 1;
for my $bucket (@buckets)
{
    my($list) = $bucket->{list};
    my $in = join("\n", map { $_->[0] } @$list) . "\n";

    my $out = sprintf("$sketch_dir/all-%02d.msh", $n++);

    $sched->add_work([$in, $out], $bucket->{size});
}

$sched->run(sub {}, sub {
    my($global, $item) = @_;
    my($in, $out) = @$item;
    
    my @cmd = ("mash", "paste", "-l", $out, "/dev/fd/0");
    my $ok = run(\@cmd, "<", \$in);
});
