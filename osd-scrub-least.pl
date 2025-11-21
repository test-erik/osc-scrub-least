#!/usr/bin/env perl
use strict;
use warnings;
use feature 'say';

use Getopt::Long qw(:config no_auto_abbrev);
use Cpanel::JSON::XS qw(decode_json);
use DateTime::Format::ISO8601;
use Try::Tiny;
use List::Util qw(uniq);

# -----------------------------------------------------------------------------
# CLI options
# -----------------------------------------------------------------------------
my $n      = 5;
my $help   = 0;
my $debug  = 0;
my $scrubs = 4;      # target for osd_max_scrubs
my $emit_cmds = 1;   # print copy/paste commands

GetOptions(
    'n=i'       => \$n,
    'scrubs=i'  => \$scrubs,
    'no-cmds'   => sub { $emit_cmds = 0 },
    'debug'     => \$debug,
    'help'      => \$help,
) or die "Invalid options. See --help.\n";

if ($help) {
    print <<"USAGE";
Show the PGs whose last deep scrub is longest ago and emit copy/paste commands.

Usage:
  $0 [-n NUM] [--scrubs NUM] [--no-cmds] [--debug] [--help]

Options:
  -n NUM        Show the NUM oldest PGs (default: 5)
  --scrubs NUM  Set osd_max_scrubs to NUM for involved OSDs (default: 4)
  --no-cmds     Do not print shell command snippets
  --debug       Print parsed epoch timestamps per PG
  --help        Show this help
USAGE
    exit 0;
}

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
sub slurp_cmd {
    my @cmd = @_;
    my $out = qx{@cmd 2>/dev/null};
    return $? == 0 ? $out : undef;
}

# Accepts:
#  - "YYYY-MM-DD HH:MM:SS[.us]" (no TZ -> treat as UTC)
#  - ISO-8601 with Z, +HH:MM, or +HHMM (normalized to +HH:MM)
# Returns epoch seconds; unknown/empty -> 0
sub parse_ts {
    my ($s) = @_;
    return 0 unless defined $s && length $s;
    return 0 if $s eq '0.000000';

    my $norm = $s;
    $norm =~ s/ /T/;                          # space → T
    $norm =~ s/([+\-]\d{2})(\d{2})$/$1:$2/;   # +HHMM → +HH:MM
    $norm .= 'Z' unless $norm =~ /(?:Z|[+\-]\d{2}:\d{2})$/;

    my $epoch = try { DateTime::Format::ISO8601->parse_datetime($norm)->epoch }
                catch { 0 };

    return $epoch // 0;
}

sub extract_pg_array {
    my ($root) = @_;
    return $root if ref($root) eq 'ARRAY';
    return $root->{pg_stats}         if ref($root->{pg_stats}) eq 'ARRAY';
    return $root->{pg_map}{pg_stats} if ref($root->{pg_map}) eq 'HASH'
                                     && ref($root->{pg_map}{pg_stats}) eq 'ARRAY';
    return $root->{entries}          if ref($root->{entries}) eq 'ARRAY';
    return [];
}

sub primary_osd_of {
    my ($pg) = @_;
    return $pg->{acting_primary} if defined $pg->{acting_primary};
    return $pg->{up_primary}     if defined $pg->{up_primary};
    return $pg->{acting}[0]      if ref($pg->{acting}) eq 'ARRAY' && @{$pg->{acting}};
    return $pg->{up}[0]          if ref($pg->{up})     eq 'ARRAY' && @{$pg->{up}};
    return undef;
}

sub acting_set_of {
    my ($pg) = @_;
    my $a = (ref($pg->{acting}) eq 'ARRAY') ? $pg->{acting}
          : (ref($pg->{up})     eq 'ARRAY') ? $pg->{up}
          : [];
    return join(',', @$a);
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
my $json = slurp_cmd(qw(ceph pg ls -f json))
        // slurp_cmd(qw(ceph pg dump_json))
        or die "Unable to read Ceph JSON\n";

my $root = eval { decode_json($json) }
    or die "JSON parse error: $@\n";

my $pgs = extract_pg_array($root);
@$pgs or die "No PGs found\n";

my @rows = map {
    my $pgid = $_->{pgid};
    my $last = $_->{last_deep_scrub_stamp} // 'never';
    my $ts   = parse_ts($last);
    {
        pgid    => $pgid,
        ts      => $ts,
        primary => primary_osd_of($_),
        acting  => acting_set_of($_),
        last    => $last,
    }
} @$pgs;

@rows = sort {
       ($a->{ts} // 0)       <=> ($b->{ts} // 0)
    || ($a->{last} // '')    cmp  ($b->{last} // '')
    || ($a->{pgid} // '')    cmp  ($b->{pgid} // '')
} @rows;

die "No evaluatable PGs\n" unless @rows;

$n = @rows if $n > @rows;

say sprintf "Top %d PGs nach ältestem deep scrub:\n", $n;
say sprintf "%-12s %-10s %-20s %s", "PGID", "Primary", "Acting", "Last Deep Scrub";
say "-" x 70;

my @top = @rows[0 .. $n-1];
for my $r (@top) {
    printf "%-12s osd.%-8s %-20s %s\n",
        $r->{pgid},
        ($r->{primary} // 'unknown'),
        $r->{acting},
        ($r->{last} // 'never');
    printf STDERR "[debug] %s ts=%d\n", $r->{pgid}, $r->{ts} if $debug;
}

# -----------------------------------------------------------------------------
# Copy/paste commands
# -----------------------------------------------------------------------------
if ($emit_cmds) {
    my @pgids = map { $_->{pgid} } @top;
    
    # acting-Sets flatten + deduplizieren (Reihenfolge bleibt erhalten)
    my @osds_flat = uniq grep { defined && length }
    map  { split /[,\s]+/ }
    map  { $_->{acting} } @top;
    
    say "\n# copy/paste:";
    say "for i in @pgids ; do ceph pg deep-scrub \$i ; done";
    say "for i in @osds_flat ; do ceph config set osd.\$i osd_max_scrubs $scrubs ; done";
}
