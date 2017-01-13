package Bio::P3::Minhash::Client;

use JSON::RPC::Client;
use POSIX;
use strict;
use Data::Dumper;
use URI;
use Bio::KBase::Exceptions;
my $get_time = sub { time, 0 };
eval {
    require Time::HiRes;
    $get_time = sub { Time::HiRes::gettimeofday() };
};

use Bio::KBase::AuthToken;

# Client version should match Impl version
# This is a Semantic Version number,
# http://semver.org
our $VERSION = "0.1.0";

=head1 NAME

Bio::P3::Minhash::Client

=head1 DESCRIPTION





=cut

sub new
{
    my($class, $url, @args) = @_;
    
    if (!defined($url))
    {
	$url = 'http://localhost:7138';
    }

    my $self = {
	client => Bio::P3::Minhash::Client::RpcClient->new,
	url => $url,
	headers => [],
    };

    chomp($self->{hostname} = `hostname`);
    $self->{hostname} ||= 'unknown-host';

    #
    # Set up for propagating KBRPC_TAG and KBRPC_METADATA environment variables through
    # to invoked services. If these values are not set, we create a new tag
    # and a metadata field with basic information about the invoking script.
    #
    if ($ENV{KBRPC_TAG})
    {
	$self->{kbrpc_tag} = $ENV{KBRPC_TAG};
    }
    else
    {
	my ($t, $us) = &$get_time();
	$us = sprintf("%06d", $us);
	my $ts = strftime("%Y-%m-%dT%H:%M:%S.${us}Z", gmtime $t);
	$self->{kbrpc_tag} = "C:$0:$self->{hostname}:$$:$ts";
    }
    push(@{$self->{headers}}, 'Kbrpc-Tag', $self->{kbrpc_tag});

    if ($ENV{KBRPC_METADATA})
    {
	$self->{kbrpc_metadata} = $ENV{KBRPC_METADATA};
	push(@{$self->{headers}}, 'Kbrpc-Metadata', $self->{kbrpc_metadata});
    }

    if ($ENV{KBRPC_ERROR_DEST})
    {
	$self->{kbrpc_error_dest} = $ENV{KBRPC_ERROR_DEST};
	push(@{$self->{headers}}, 'Kbrpc-Errordest', $self->{kbrpc_error_dest});
    }

    #
    # This module requires authentication.
    #
    # We create an auth token, passing through the arguments that we were (hopefully) given.

    {
	my $token = Bio::KBase::AuthToken->new(@args);
	
	if (!$token->error_message)
	{
	    $self->{token} = $token->token;
	    $self->{client}->{token} = $token->token;
	}
    }

    my $ua = $self->{client}->ua;	 
    my $timeout = $ENV{CDMI_TIMEOUT} || (30 * 60);	 
    $ua->timeout($timeout);
    bless $self, $class;
    #    $self->_validate_version();
    return $self;
}




=head2 compute_genome_distance

  $return = $obj->compute_genome_distance($genome_id, $max_pvalue, $max_distance, $max_hits, $include_reference, $include_representative)

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

sub compute_genome_distance
{
    my($self, @args) = @_;

# Authentication: optional

    if ((my $n = @args) != 6)
    {
	Bio::KBase::Exceptions::ArgumentValidationError->throw(error =>
							       "Invalid argument count for function compute_genome_distance (received $n, expecting 6)");
    }
    {
	my($genome_id, $max_pvalue, $max_distance, $max_hits, $include_reference, $include_representative) = @args;

	my @_bad_arguments;
        (!ref($genome_id)) or push(@_bad_arguments, "Invalid type for argument 1 \"genome_id\" (value was \"$genome_id\")");
        (!ref($max_pvalue)) or push(@_bad_arguments, "Invalid type for argument 2 \"max_pvalue\" (value was \"$max_pvalue\")");
        (!ref($max_distance)) or push(@_bad_arguments, "Invalid type for argument 3 \"max_distance\" (value was \"$max_distance\")");
        (!ref($max_hits)) or push(@_bad_arguments, "Invalid type for argument 4 \"max_hits\" (value was \"$max_hits\")");
        (!ref($include_reference)) or push(@_bad_arguments, "Invalid type for argument 5 \"include_reference\" (value was \"$include_reference\")");
        (!ref($include_representative)) or push(@_bad_arguments, "Invalid type for argument 6 \"include_representative\" (value was \"$include_representative\")");
        if (@_bad_arguments) {
	    my $msg = "Invalid arguments passed to compute_genome_distance:\n" . join("", map { "\t$_\n" } @_bad_arguments);
	    Bio::KBase::Exceptions::ArgumentValidationError->throw(error => $msg,
								   method_name => 'compute_genome_distance');
	}
    }

    my $result = $self->{client}->call($self->{url}, $self->{headers}, {
	method => "Minhash.compute_genome_distance",
	params => \@args,
    });
    if ($result) {
	if ($result->is_error) {
	    Bio::KBase::Exceptions::JSONRPC->throw(error => $result->error_message,
					       code => $result->content->{error}->{code},
					       method_name => 'compute_genome_distance',
					       data => $result->content->{error}->{error} # JSON::RPC::ReturnObject only supports JSONRPC 1.1 or 1.O
					      );
	} else {
	    return wantarray ? @{$result->result} : $result->result->[0];
	}
    } else {
        Bio::KBase::Exceptions::HTTP->throw(error => "Error invoking method compute_genome_distance",
					    status_line => $self->{client}->status_line,
					    method_name => 'compute_genome_distance',
				       );
    }
}



sub version {
    my ($self) = @_;
    my $result = $self->{client}->call($self->{url}, $self->{headers}, {
        method => "Minhash.version",
        params => [],
    });
    if ($result) {
        if ($result->is_error) {
            Bio::KBase::Exceptions::JSONRPC->throw(
                error => $result->error_message,
                code => $result->content->{code},
                method_name => 'compute_genome_distance',
            );
        } else {
            return wantarray ? @{$result->result} : $result->result->[0];
        }
    } else {
        Bio::KBase::Exceptions::HTTP->throw(
            error => "Error invoking method compute_genome_distance",
            status_line => $self->{client}->status_line,
            method_name => 'compute_genome_distance',
        );
    }
}

sub _validate_version {
    my ($self) = @_;
    my $svr_version = $self->version();
    my $client_version = $VERSION;
    my ($cMajor, $cMinor) = split(/\./, $client_version);
    my ($sMajor, $sMinor) = split(/\./, $svr_version);
    if ($sMajor != $cMajor) {
        Bio::KBase::Exceptions::ClientServerIncompatible->throw(
            error => "Major version numbers differ.",
            server_version => $svr_version,
            client_version => $client_version
        );
    }
    if ($sMinor < $cMinor) {
        Bio::KBase::Exceptions::ClientServerIncompatible->throw(
            error => "Client minor version greater than Server minor version.",
            server_version => $svr_version,
            client_version => $client_version
        );
    }
    if ($sMinor > $cMinor) {
        warn "New client version available for Bio::P3::Minhash::Client\n";
    }
    if ($sMajor == 0) {
        warn "Bio::P3::Minhash::Client version is $svr_version. API subject to change.\n";
    }
}

=head1 TYPES



=cut

package Bio::P3::Minhash::Client::RpcClient;
use base 'JSON::RPC::Client';
use POSIX;
use strict;

#
# Override JSON::RPC::Client::call because it doesn't handle error returns properly.
#

sub call {
    my ($self, $uri, $headers, $obj) = @_;
    my $result;


    my @retries = (1, 2, 5, 10, 20, 60, 60, 60, 60, 60, 60);
    my %codes_to_retry =  map { $_ => 1 } qw(110 408 502 503 504 200) ;
    my $n_retries;

    while (1)

    {
	if ($uri =~ /\?/) {
	    $result = $self->_get($uri);
	}
	else {
	    Carp::croak "not hashref." unless (ref $obj eq 'HASH');
	    $result = $self->_post($uri, $headers, $obj);
	}

	#
	# Bail early on success.
	#
	if ($result->is_success)
	{
	    if ($n_retries)
	    {
		print STDERR strftime("%F %T", localtime), ": Request succeeded after $n_retries retries\n";
	    }
	    last;
	}
	$n_retries++;

	#
	# Failure. See if we need to retry and loop, or bail with
	# a permanent failure.
	#
	
        my $code = $result->code;
	my $msg = $result->message;
	my $want_retry = 0;
	if ($codes_to_retry{$code})
	{
	    $want_retry = 1;
	}
	elsif ($code eq 500 && defined( $result->header('client-warning') )
	       && $result->header('client-warning') eq 'Internal response')
	{
	    #
	    # Handle errors that were not thrown by the web
	    # server but rather picked up by the client library.
	    #
	    # If we got a client timeout or connection refused, let us retry.
	    #
	    
	    if ($msg =~ /timeout|connection refused/i)
	    {
		$want_retry = 1;
	    }
	    
	}
	
        if (!$want_retry || @retries == 0) {
	    last;
        }
	
        #
        # otherwise, sleep & loop.
        #
        my $retry_time = shift(@retries);
        print STDERR strftime("%F %T", localtime), ": Request failed with code=$code msg=$msg, sleeping $retry_time and retrying\n";
        sleep($retry_time);

    }

    my $service = $obj->{method} =~ /^system\./ if ( $obj );

    $self->status_line($result->status_line);

    if ($result->is_success) {

        return unless($result->content); # notification?

        if ($service) {
            return JSON::RPC::ServiceObject->new($result, $self->json);
        }

        return JSON::RPC::ReturnObject->new($result, $self->json);
    }
    elsif ($result->content_type eq 'application/json')
    {
        return JSON::RPC::ReturnObject->new($result, $self->json);
    }
    else {
        return;
    }
}


sub _post {
    my ($self, $uri, $headers, $obj) = @_;
    my $json = $self->json;

    $obj->{version} ||= $self->{version} || '1.1';

    if ($obj->{version} eq '1.0') {
        delete $obj->{version};
        if (exists $obj->{id}) {
            $self->id($obj->{id}) if ($obj->{id}); # if undef, it is notification.
        }
        else {
            $obj->{id} = $self->id || ($self->id('JSON::RPC::Client'));
        }
    }
    else {
        # $obj->{id} = $self->id if (defined $self->id);
	# Assign a random number to the id if one hasn't been set
	$obj->{id} = (defined $self->id) ? $self->id : substr(rand(),2);
    }

    my $content = $json->encode($obj);

    $self->ua->post(
        $uri,
        Content_Type   => $self->{content_type},
        Content        => $content,
        Accept         => 'application/json',
	@$headers,
	($self->{token} ? (Authorization => $self->{token}) : ()),
    );
}



1;
