package Bio::P3::Minhash::MinhashImpl;
use strict;
# Use Semantic Versioning (2.0.0-rc.1)
# http://semver.org 
our $VERSION = "0.1.0";

=head1 NAME

Minhash

=head1 DESCRIPTION



=cut

#BEGIN_HEADER

use Data::Dumper;
use Bio::P3::DeploymentConfig;
use P3DataAPI;
use Bio::P3::Workspace::WorkspaceClientExt;
    
#END_HEADER

sub new
{
    my($class, @args) = @_;
    my $self = {
    };
    bless $self, $class;
    #BEGIN_CONSTRUCTOR

    my $cfg = Bio::P3::DeploymentConfig->new($ENV{KB_SERVICE_NAME} || "Minhash");

    $self->{_genome_downloads} = $cfg->setting('genome-downloads');
    $self->{_genome_sketches} = $cfg->setting('genome-sketches');
    $self->{_all_genomes_sketch} = $cfg->setting('all-genomes-sketch');
    $self->{_reference_genomes_sketch} = $cfg->setting('reference-genomes-sketch');
    $self->{_representative_genomes_sketch} = $cfg->setting('representative-genomes-sketch');
    $self->{_all_viral_genomes_sketches} = $cfg->setting('all-viral-genomes-sketches');
    $self->{_refrep_viral_genomes_sketch} = $cfg->setting('refrep-viral-genomes-sketch');

    $self->{_all_viral_genomes_sketches} = [$self->{_all_viral_genomes_sketches}] unless ref($self->{_all_viral_genomes_sketches});

    $self->{_data_api} = P3DataAPI->new();
    delete $self->{data_api}->{token};

    #END_CONSTRUCTOR

    if ($self->can('_init_instance'))
    {
	$self->_init_instance();
    }
    return $self;
}
=head1 METHODS
=head2 compute_genome_distance_for_genome

  $return = $obj->compute_genome_distance_for_genome($genome_id, $max_pvalue, $max_distance, $max_hits, $include_reference, $include_representative)

=over 4


=item Parameter and return types

=begin html

<pre>
$genome_id is a string
$max_pvalue is a float
$max_distance is a float
$max_hits is an int
$include_reference is an int
$include_representative is an int
$return is a reference to a list where each element is a reference to a list containing 4 items:
	0: (genome_id) a string
	1: (distance) a float
	2: (pvalue) a float
	3: (counts) a string
</pre>

=end html

=begin text

$genome_id is a string
$max_pvalue is a float
$max_distance is a float
$max_hits is an int
$include_reference is an int
$include_representative is an int
$return is a reference to a list where each element is a reference to a list containing 4 items:
	0: (genome_id) a string
	1: (distance) a float
	2: (pvalue) a float
	3: (counts) a string

=end text



=item Description


=back

=cut

sub compute_genome_distance_for_genome
{
    my $self = shift;
    my($genome_id, $max_pvalue, $max_distance, $max_hits, $include_reference, $include_representative) = @_;

    my @_bad_arguments;
    (!ref($genome_id)) or push(@_bad_arguments, "Invalid type for argument \"genome_id\" (value was \"$genome_id\")");
    (!ref($max_pvalue)) or push(@_bad_arguments, "Invalid type for argument \"max_pvalue\" (value was \"$max_pvalue\")");
    (!ref($max_distance)) or push(@_bad_arguments, "Invalid type for argument \"max_distance\" (value was \"$max_distance\")");
    (!ref($max_hits)) or push(@_bad_arguments, "Invalid type for argument \"max_hits\" (value was \"$max_hits\")");
    (!ref($include_reference)) or push(@_bad_arguments, "Invalid type for argument \"include_reference\" (value was \"$include_reference\")");
    (!ref($include_representative)) or push(@_bad_arguments, "Invalid type for argument \"include_representative\" (value was \"$include_representative\")");
    if (@_bad_arguments) {
	my $msg = "Invalid arguments passed to compute_genome_distance_for_genome:\n" . join("", map { "\t$_\n" } @_bad_arguments);
	die $msg;
    }

    my $ctx = $Bio::P3::Minhash::Service::CallContext;
    my($return);
    #BEGIN compute_genome_distance_for_genome

    $return = $self->compute_genome_distance_for_genome2($genome_id, $max_pvalue, $max_distance, $max_hits, $include_reference, $include_representative, 1, 0);
	
    #END compute_genome_distance_for_genome
    my @_bad_returns;
    (ref($return) eq 'ARRAY') or push(@_bad_returns, "Invalid type for return variable \"return\" (value was \"$return\")");
    if (@_bad_returns) {
	my $msg = "Invalid returns passed to compute_genome_distance_for_genome:\n" . join("", map { "\t$_\n" } @_bad_returns);
	die $msg;
    }
    return($return);
}


=head2 compute_genome_distance_for_fasta

  $return = $obj->compute_genome_distance_for_fasta($ws_fasta_path, $max_pvalue, $max_distance, $max_hits, $include_reference, $include_representative)

=over 4


=item Parameter and return types

=begin html

<pre>
$ws_fasta_path is a string
$max_pvalue is a float
$max_distance is a float
$max_hits is an int
$include_reference is an int
$include_representative is an int
$return is a reference to a list where each element is a reference to a list containing 4 items:
	0: (genome_id) a string
	1: (distance) a float
	2: (pvalue) a float
	3: (counts) a string
</pre>

=end html

=begin text

$ws_fasta_path is a string
$max_pvalue is a float
$max_distance is a float
$max_hits is an int
$include_reference is an int
$include_representative is an int
$return is a reference to a list where each element is a reference to a list containing 4 items:
	0: (genome_id) a string
	1: (distance) a float
	2: (pvalue) a float
	3: (counts) a string

=end text



=item Description


=back

=cut

sub compute_genome_distance_for_fasta
{
    my $self = shift;
    my($ws_fasta_path, $max_pvalue, $max_distance, $max_hits, $include_reference, $include_representative) = @_;

    my @_bad_arguments;
    (!ref($ws_fasta_path)) or push(@_bad_arguments, "Invalid type for argument \"ws_fasta_path\" (value was \"$ws_fasta_path\")");
    (!ref($max_pvalue)) or push(@_bad_arguments, "Invalid type for argument \"max_pvalue\" (value was \"$max_pvalue\")");
    (!ref($max_distance)) or push(@_bad_arguments, "Invalid type for argument \"max_distance\" (value was \"$max_distance\")");
    (!ref($max_hits)) or push(@_bad_arguments, "Invalid type for argument \"max_hits\" (value was \"$max_hits\")");
    (!ref($include_reference)) or push(@_bad_arguments, "Invalid type for argument \"include_reference\" (value was \"$include_reference\")");
    (!ref($include_representative)) or push(@_bad_arguments, "Invalid type for argument \"include_representative\" (value was \"$include_representative\")");
    if (@_bad_arguments) {
	my $msg = "Invalid arguments passed to compute_genome_distance_for_fasta:\n" . join("", map { "\t$_\n" } @_bad_arguments);
	die $msg;
    }

    my $ctx = $Bio::P3::Minhash::Service::CallContext;
    my($return);
    #BEGIN compute_genome_distance_for_fasta


    #
    # Use the minhash-search script to do the heavy lifting.
    #

    $return = $self->compute_genome_distance_for_fasta2($ws_fasta_path, $max_pvalue, $max_distance, $max_hits, $include_reference, $include_representative, 1, 0);

    #END compute_genome_distance_for_fasta
    my @_bad_returns;
    (ref($return) eq 'ARRAY') or push(@_bad_returns, "Invalid type for return variable \"return\" (value was \"$return\")");
    if (@_bad_returns) {
	my $msg = "Invalid returns passed to compute_genome_distance_for_fasta:\n" . join("", map { "\t$_\n" } @_bad_returns);
	die $msg;
    }
    return($return);
}


=head2 compute_genome_distance_for_genome2

  $return = $obj->compute_genome_distance_for_genome2($genome_id, $max_pvalue, $max_distance, $max_hits, $include_reference, $include_representative, $bacterial, $viral)

=over 4


=item Parameter and return types

=begin html

<pre>
$genome_id is a string
$max_pvalue is a float
$max_distance is a float
$max_hits is an int
$include_reference is an int
$include_representative is an int
$bacterial is an int
$viral is an int
$return is a reference to a list where each element is a reference to a list containing 4 items:
	0: (genome_id) a string
	1: (distance) a float
	2: (pvalue) a float
	3: (counts) a string
</pre>

=end html

=begin text

$genome_id is a string
$max_pvalue is a float
$max_distance is a float
$max_hits is an int
$include_reference is an int
$include_representative is an int
$bacterial is an int
$viral is an int
$return is a reference to a list where each element is a reference to a list containing 4 items:
	0: (genome_id) a string
	1: (distance) a float
	2: (pvalue) a float
	3: (counts) a string

=end text



=item Description


=back

=cut

sub compute_genome_distance_for_genome2
{
    my $self = shift;
    my($genome_id, $max_pvalue, $max_distance, $max_hits, $include_reference, $include_representative, $bacterial, $viral) = @_;

    my @_bad_arguments;
    (!ref($genome_id)) or push(@_bad_arguments, "Invalid type for argument \"genome_id\" (value was \"$genome_id\")");
    (!ref($max_pvalue)) or push(@_bad_arguments, "Invalid type for argument \"max_pvalue\" (value was \"$max_pvalue\")");
    (!ref($max_distance)) or push(@_bad_arguments, "Invalid type for argument \"max_distance\" (value was \"$max_distance\")");
    (!ref($max_hits)) or push(@_bad_arguments, "Invalid type for argument \"max_hits\" (value was \"$max_hits\")");
    (!ref($include_reference)) or push(@_bad_arguments, "Invalid type for argument \"include_reference\" (value was \"$include_reference\")");
    (!ref($include_representative)) or push(@_bad_arguments, "Invalid type for argument \"include_representative\" (value was \"$include_representative\")");
    (!ref($bacterial)) or push(@_bad_arguments, "Invalid type for argument \"bacterial\" (value was \"$bacterial\")");
    (!ref($viral)) or push(@_bad_arguments, "Invalid type for argument \"viral\" (value was \"$viral\")");
    if (@_bad_arguments) {
	my $msg = "Invalid arguments passed to compute_genome_distance_for_genome2:\n" . join("", map { "\t$_\n" } @_bad_arguments);
	die $msg;
    }

    my $ctx = $Bio::P3::Minhash::Service::CallContext;
    my($return);
    #BEGIN compute_genome_distance_for_genome2


    #
    # Use the minhash-search script to do the heavy lifting.
    #

    my @cmd = ("minhash-search");
    push(@cmd, "--max-pvalue", $max_pvalue);
    push(@cmd, "--max-distance", $max_distance);

    my(@bac_refs, @vir_refs);

    if ($bacterial)
    {
	if ($include_reference)
	{
	    push(@bac_refs, $self->{_reference_genomes_sketch});
	}
	if ($include_representative)
	{
	    push(@bac_refs, $self->{_representative_genomes_sketch});
	}
	#
	# If neither refs nor reps selected, use all genomes.
	if (!@bac_refs)
	{
	    push(@bac_refs, $self->{_all_genomes_sketch});
	}
    }

    if ($viral)
    {
	if ($include_reference || $include_representative)
	{
	    push(@vir_refs, $self->{_refrep_viral_genomes_sketch});
	}

	#
	# If neither refs nor reps selected, use all genomes.
	#
	if (!@vir_refs)
	{
	    push(@vir_refs, @{$self->{_all_viral_genomes_sketches}});
	}
    }

    print STDERR Dumper(\@bac_refs, \@vir_refs);

    push(@cmd, @bac_refs, @vir_refs);

    #
    # Retrive genome data.
    #

    local $self->{_data_api}->{token} = $ctx->token;
    # print STDERR Dumper($ctx->token);
    #
    # If we have a sketch for the given genome in the sketch dir, use it.
    # If the genome is present in the downloads, use those contigs. Otherwise we need to
    # pull the contigs using the data api.
    #

    my $genome_sketch = "$self->{_genome_sketches}/$genome_id.msh";
    my $genome_fna = "$self->{_genome_downloads}/$genome_id/$genome_id.fna";

    my $tmp;
    if (-s $genome_sketch)
    {
	push(@cmd, $genome_sketch);
    }
    elsif (-s $genome_fna)
    {
	push(@cmd, $genome_fna);
    }
    else
    {
	$tmp = $self->{_data_api}->retrieve_contigs_in_genome_to_temp($genome_id);
	print STDERR "Retrieving from data api $genome_id: $tmp\n";
	if (! -s "$tmp")
	{
	    die "Could not retrieve contigs for $genome_id";
	}
	push(@cmd, "$tmp");
    }

    print STDERR Dumper(\@cmd);
    open(my $p, "-|", @cmd) or die "Cannot run command @cmd: $!";

    my $return = [];
    my $n = 0;
    my @vals;
    while (<$p>)
    {
	last if $n++ > $max_hits;

	chomp;
	my($gid, $dist, $pv, $counts) = split(/\t/);
	push(@vals, [$gid, $dist, $pv, $counts]);
    }

    #
    # Validate existence of returned ids.
    #

    if (@vals)
    {
	my $set = join(",", map { $_->[0] } @vals);
	
	my @res = $self->{_data_api}->query('genome', ['in', 'genome_id', "($set)"], ["select", "genome_id"]);
	my %ok = map { $_->{genome_id} => 1} @res;
	# print STDERR Dumper(\@res, \%ok);
	@$return = grep { $ok{$_->[0]} } @vals;
    }



    #END compute_genome_distance_for_genome2
    my @_bad_returns;
    (ref($return) eq 'ARRAY') or push(@_bad_returns, "Invalid type for return variable \"return\" (value was \"$return\")");
    if (@_bad_returns) {
	my $msg = "Invalid returns passed to compute_genome_distance_for_genome2:\n" . join("", map { "\t$_\n" } @_bad_returns);
	die $msg;
    }
    return($return);
}


=head2 compute_genome_distance_for_fasta2

  $return = $obj->compute_genome_distance_for_fasta2($ws_fasta_path, $max_pvalue, $max_distance, $max_hits, $include_reference, $include_representative, $bacterial, $viral)

=over 4


=item Parameter and return types

=begin html

<pre>
$ws_fasta_path is a string
$max_pvalue is a float
$max_distance is a float
$max_hits is an int
$include_reference is an int
$include_representative is an int
$bacterial is an int
$viral is an int
$return is a reference to a list where each element is a reference to a list containing 4 items:
	0: (genome_id) a string
	1: (distance) a float
	2: (pvalue) a float
	3: (counts) a string
</pre>

=end html

=begin text

$ws_fasta_path is a string
$max_pvalue is a float
$max_distance is a float
$max_hits is an int
$include_reference is an int
$include_representative is an int
$bacterial is an int
$viral is an int
$return is a reference to a list where each element is a reference to a list containing 4 items:
	0: (genome_id) a string
	1: (distance) a float
	2: (pvalue) a float
	3: (counts) a string

=end text



=item Description


=back

=cut

sub compute_genome_distance_for_fasta2
{
    my $self = shift;
    my($ws_fasta_path, $max_pvalue, $max_distance, $max_hits, $include_reference, $include_representative, $bacterial, $viral) = @_;

    my @_bad_arguments;
    (!ref($ws_fasta_path)) or push(@_bad_arguments, "Invalid type for argument \"ws_fasta_path\" (value was \"$ws_fasta_path\")");
    (!ref($max_pvalue)) or push(@_bad_arguments, "Invalid type for argument \"max_pvalue\" (value was \"$max_pvalue\")");
    (!ref($max_distance)) or push(@_bad_arguments, "Invalid type for argument \"max_distance\" (value was \"$max_distance\")");
    (!ref($max_hits)) or push(@_bad_arguments, "Invalid type for argument \"max_hits\" (value was \"$max_hits\")");
    (!ref($include_reference)) or push(@_bad_arguments, "Invalid type for argument \"include_reference\" (value was \"$include_reference\")");
    (!ref($include_representative)) or push(@_bad_arguments, "Invalid type for argument \"include_representative\" (value was \"$include_representative\")");
    (!ref($bacterial)) or push(@_bad_arguments, "Invalid type for argument \"bacterial\" (value was \"$bacterial\")");
    (!ref($viral)) or push(@_bad_arguments, "Invalid type for argument \"viral\" (value was \"$viral\")");
    if (@_bad_arguments) {
	my $msg = "Invalid arguments passed to compute_genome_distance_for_fasta2:\n" . join("", map { "\t$_\n" } @_bad_arguments);
	die $msg;
    }

    my $ctx = $Bio::P3::Minhash::Service::CallContext;
    my($return);
    #BEGIN compute_genome_distance_for_fasta2

    my @cmd = ("minhash-search");
    push(@cmd, "--max-pvalue", $max_pvalue);
    push(@cmd, "--max-distance", $max_distance);

    my(@bac_refs, @vir_refs);

    if ($bacterial)
    {
	if ($include_reference)
	{
	    push(@bac_refs, $self->{_reference_genomes_sketch});
	}
	if ($include_representative)
	{
	    push(@bac_refs, $self->{_representative_genomes_sketch});
	}
	#
	# If neither refs nor reps selected, use all genomes.
	if (!@bac_refs)
	{
	    push(@bac_refs, $self->{_all_genomes_sketch});
	}
    }

    if ($viral)
    {
	if ($include_reference || $include_representative)
	{
	    push(@vir_refs, $self->{_refrep_viral_genomes_sketch});
	}

	#
	# If neither refs nor reps selected, use all genomes.
	#
	if (!@vir_refs)
	{
	    push(@vir_refs, @{$self->{_all_viral_genomes_sketches}});
	}
    }

    print STDERR Dumper(\@bac_refs, \@vir_refs);


    push(@cmd, @bac_refs, @vir_refs);

    #
    # Retrive genome data from workspace.
    #

    my $ws = Bio::P3::Workspace::WorkspaceClientExt->new(undef, token => $ctx->token);

    my $temp_contigs = File::Temp->new();
    eval {
	$ws->copy_files_to_handles(1, $ctx->token, [[$ws_fasta_path, $temp_contigs]]);
    };
    if ($@)
    {
	die "Error copying $ws_fasta_path:\n$@";
    }

    close($temp_contigs);
    push(@cmd, "$temp_contigs");

    open(my $p, "-|", @cmd) or die "Cannot run command @cmd: $!";

    my $return = [];
    my $n = 0;
    while (<$p>)
    {
	last if $n++ > $max_hits;

	chomp;
	my($gid, $dist, $pv, $counts) = split(/\t/);
	push(@$return, [$gid, $dist, $pv, $counts]);
    }

    #END compute_genome_distance_for_fasta2
    my @_bad_returns;
    (ref($return) eq 'ARRAY') or push(@_bad_returns, "Invalid type for return variable \"return\" (value was \"$return\")");
    if (@_bad_returns) {
	my $msg = "Invalid returns passed to compute_genome_distance_for_fasta2:\n" . join("", map { "\t$_\n" } @_bad_returns);
	die $msg;
    }
    return($return);
}





=head2 version 

  $return = $obj->version()

=over 4

=item Parameter and return types

=begin html

<pre>
$return is a string
</pre>

=end html

=begin text

$return is a string

=end text

=item Description

Return the module version. This is a Semantic Versioning number.

=back

=cut

sub version {
    return $VERSION;
}



=head1 TYPES


=cut

1;
