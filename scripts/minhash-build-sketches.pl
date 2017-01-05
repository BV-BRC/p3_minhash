#
# Build the minhash sketches from the P3 genome data files.
#

use strict;
use Data::Dumper;
use Getopt::Long::Descriptive;
use Parallel::Loops;
use IPC::Run;

my($opt, $usage) = describe_options("%c %o genome-dir sketch-dir",
				    ["sketch-size|s=i" => "Sketch size", { default => 1000 }],
				    ["parallel|p=i" => "Run with this many parallel threads", { default => 1 }],
				    ["batch-size|b=i" => "Size of genome batches to process in a thread", { default => 100 }],
				    ["help|h" => "Show this help message"]);
print($usage->text), exit 0 if $opt->help;
die($usage->text), exit 0 if @ARGV != 2;

my $genome_dir = shift;
my $sketch_dir = shift;

-d $genome_dir or die "Genome directory $genome_dir is not a directory\n";
-d $sketch_dir or die "Sketch directory $sketch_dir is not a directory\n";

opendir(D, $genome_dir) or die "Cannot opendir $genome_dir: $!";

my @batches;

my $index = 0;
my $batch = [];
push(@batches, $batch);
while (my $p = readdir(D))
{
    my $path = "$genome_dir/$p";
    next unless $p =~ /^\d+\.\d+$/;
    next unless -d $path;
    my $fna = "$path/$p.fna";

    my $sketch = "$sketch_dir/$p.msh";
#    if (-s $fna && ! -s $sketch)
    {
	push(@$batch, [$p, $path, $fna, $sketch]);
	if (@$batch >= $opt->batch_size)
	{
	    $batch = [];
	    push(@batches, $batch);
	}
    }
    # last if $index++ > 20;
}

my $pl = Parallel::Loops->new($opt->parallel);

$pl->foreach(\@batches, sub {
    my $batch = $_;
    for my $ent (@$batch)
    {
	my($genome, $dir, $fna, $sketch) = @$ent;

	if (-s $fna  && ! -s $sketch)
	{
	    my @cmd = ("mash", "sketch", "-s", $opt->sketch_size, "-o", $sketch, $fna);

	    my($stdout, $stderr);

	    my $ok = IPC::Run::run(\@cmd, '>', \$stdout, '2>', \$stderr);
	    if (!$ok)
	    {
		warn "Error running @cmd\nstdout=$stdout\nstderr=$stderr\n";
	    }
	}
	else
	{
	    # warn "No sketch for $fna $sketch\n";
	}
    }
});
