#
# Using the data in the given sketch directory, write combined sketches all.msh for
# all genomes; refs.msh for the reference genomes, and reps.msh for the representatives.
#

use P3DataAPI;
use strict;
use Data::Dumper;
use Getopt::Long::Descriptive;
use Proc::ParallelLoop;
use File::Temp;

my($opt, $usage) = describe_options("%c %o sketch-dir combined-dir",
				    ["help|h" => 'Show this help message']);
print($usage->text), exit 0 if $opt->help;
die($usage->text) if @ARGV != 2;

my $sketch_dir = shift;
my $combined_dir = shift;

opendir(D, $sketch_dir) or die "Cannot opendir $sketch_dir: $!";
-d $combined_dir or die "Combined sketch dir $combined_dir does not exist\n";

#
# Look up reference and representative genome lists.
#


my %repref;

my $api = P3DataAPI->new;
my @res = $api->query('genome', ['eq','reference_genome', 'Reference'], ['select', 'genome_id']);
my %refs = map { $_->{genome_id} => 1 } @res;
$repref{$_->{genome_id}} = 1 foreach @res;

@res = $api->query('genome', ['eq','reference_genome', 'Representative'], ['select', 'genome_id']);
my %reps = map { $_->{genome_id} => 1} @res;
$repref{$_->{genome_id}} = 1 foreach @res;

#
# Enumerate available sketches and build lists for update.
#

my(@all, @refs, @reps, @repref);


my $errs;

while (my $p = readdir(D))
{
    if ($p =~ /^(\d+\.\d+)\.msh/)
    {
	my $gid = $1;
	my $sp = "$sketch_dir/$p";

	if (-s $sp == 0)
	{
	    warn "Sketch $sp is empty\n";
	    $errs++;
	    next;
	}

	push(@all, $sp);
	push(@refs, $sp) if delete $refs{$gid};
	push(@reps, $sp) if delete $reps{$gid};
	push(@repref, $sp) if delete $repref{$gid};
    }
}

if ($errs)
{
    die "Terminating due to errors\n";
}

closedir(D);

if (%refs)
{
    warn "Not all references had sketches. Missing: " . join(" ", sort keys %refs) . "\n";
}
if (%reps)
{
    warn "Not all representatives had sketches. Missing: " . join(" ", sort keys %reps) . "\n";
}

pareach [[\@all, "$combined_dir/all"],
	 [\@refs, "$combined_dir/refs"],
	 [\@reps, "$combined_dir/reps"], 
	 [\@repref, "$combined_dir/repref"]], sub
{
    my($work) = @_;
    my($files, $out) = @$work;

    my $tmp = File::Temp->new;
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

