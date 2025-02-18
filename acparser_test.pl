#!/usr/bin/perl

use Acparser;

require 'acdb.pl';

my $script = shift;
my $customer = shift;
my $asup = shift;
if (!$script) { $script = "v1"; }
if (!$customer) { $customer = "dlink"; }
if (!$asup) { $asup = "hourly.asup"; }

my $asuptext = `cat $asup`;

print "Acparser.pm test executive running with script: $script\n";
print "                                      customer: $customer\n\n";

my $dbh = init_db();
my $cdbh = get_dbh_of_cust($dbh, $customer);

if (!$cdbh) { die "couldn't resolve $customer to a database"; }

print "***EXECUTIVE loading acparser module: $script, $customer\n\n";
my $ac = Acparser->new($script, $customer);
if (!defined($ac)) { die "\nAcparser failed to compile $script\n"; }

print "***EXECUTIVE launching compilation\n\n";
$ac->compile();

print Dumper $ac->{obj};

# Set the rawid.
$ac->setvar('_rawid_', $$);

print "***EXECUTIVE launching parser\n\n";
my $ret = $ac->parse($asuptext);
if (!$ret) { die "Acparser failed to parse $asup using script $script\n"; }

print "***EXECUTIVE calling dumpdebug()\n\n";
sleep 1;
my $logs = $ac->dumpdebug();

print "$logs\n";

print "***EXECUTIVE calling commit()\n\n";
$ac->commit($cdbh);


