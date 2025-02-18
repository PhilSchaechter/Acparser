package Acparser3;

#
# Usage Instructions
#
# Acparser must be initialized via the new() call.
#   new() requires the name of the parscript, and the customer id
#
# You must then call compile() to compile the script.  Compile will
# load and check syntax only, and build the $self->{STACK} data stucture.
#
# Once you have a compiled object, you may call the parse() function.
# parse() requires the text to parse.  
#
# Calling parse() will use the compiled script to parse the text passed
# in and will generate a list of commands to be used to update the 
# database.   parse() may be reused on subsequent text without the 
# need to recompile the script, but before parse can be reused,
# either commit() or reset() needs to be called.  For example,
# in developing a script file, you would presumably never call commit()
#   commit() will commit the commands generated to the database specified
#   reset() will destroy the commands list and ready parse() again
# 
# You may also call stats() to output statistics on the most recent
# parse() call.   

use strict;
use Exporter;
use Data::Dumper;
use Resolver;
use Acdb;
use Sys::Syslog qw( :DEFAULT setlogsock);
use POSIX;

# prototypes
sub debug($$$);
sub varrepl($\%);

# Create a new parser - send in the script NAME, and the customer NAME
sub new {
 	# Create the perl object
        my $this = shift;
        my $class = ref($this) || $this;
        my $self = {};
        bless $self, $class;

        # My acdb connection.
        $self->{acdb} = shift;

        # Initial parameters and debugging setup.	
		$self->{parsescript} = shift;
        $self->{custid} = shift;



        # Set our default debug mode and level.   
        $self->debugmode(1);
        $self->debuglevel(5); # 5 is the highest level of customer visible debugging

		# This is my item / system resolver that I get from the parent
		$self->{r} = shift;

        # If I don't have the resolver from the parent, I'll just use my own.
		if ( ! $self->{r} )
			{
			$self->{r} = Resolver->new($self->{acdb});
			$self->debug("WARNING: no resolver, using my own", 1) if $self->{debuglevel};
			}

        # This should move to somewhere in the DB - perhaps loaded from a template?
		$self->{replace}{num} = '[-+]?[0-9]*\.?[0-9]+';
		$self->{replace}{ip} = '\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}';
        return($self);
}

# compiles the parsescript into the STACK datastructure.
sub compile() {
my $self = shift;

my $retotal = 1; # counter of total regexes parsed

# Load the script from the DB.
my $script = $self->{acdb}->load_script($self->{parsescript});
if ( !$script ) 
   {
   $self->error("couldn't load script name: $self->{parsescript} " . DBI->errstr);
   return(0);
   }

# line number tracking...helpful for error messages
my $ln = 0;
my $line;

# Initialize the STACK object
$self->{STACK}{PARENT} = 0;
$self->{STACK}{KEEP_COUNT} = 0;
$self->{STACK}{REGEX_COUNT} = 0;
$self->{STACK}{COMMAND_COUNT} = 0;

# Set the stack pointer
my $s = $self->{STACK};

$self->debug("Starting compilation of script $self->{parsescript}", 2) if $self->{debuglevel};
# MAIN SCRIPT PARSER
compline:
foreach $line (split /\n/ ,$script) 
{
	$ln++;
	# Remove leading whitespace
	$line =~ s/^\s+//;
	# Remove comments
	$line =~ s/#.*$//;
	# Remove trailing whitespace
	$line =~ s/\s+$//;

    # Skip blank lines
	if (!$line) { next compline; } 

	# Starting a new group - open brace at end of line, with some whitespace between it and a regex.
	if ( $line =~ /^(.*?)\s*{$/ )
		{
        my $recount = $s->{REGEX_COUNT};

		# Replace [things] with their values in this regex.
		my $re = $self->rereplace($1);

		# Create the new hash for this regex
		$s->{regexs}[$recount]{regex} = qr/$re/;  # pre-compile the regex

		# This is only for printing out info in _dev_mod_
		$s->{regexs}[$recount]{ordinal} = $retotal;
		$retotal++;

		# Set the parent to us
		$s->{regexs}[$recount]{PARENT} = $s;

		# Update the regex count
		$s->{REGEX_COUNT}++;

		# Move the stack pointer to the new item
		$s = $s->{regexs}[$recount];

		# initialize values
		$s->{KEEP_COUNT} = 0;
		$s->{REGEX_COUNT} = 0;
		$s->{COMMAND_COUNT} = 0;
		next compline;
		}

    # ending a group - a line consisting of a close brace and a semi-colon.
	elsif ( $line =~ /^};$/ )
		{
		if (! $s->{PARENT} )
			{
			$self->error("Compilation failure: extra '}' detected on line $ln");
			return 0;
			}
		# Set our stack pointer to parent.
		$s = $s->{PARENT};
		next compline;
		}

	# ending a group with an endregexp - a line starting with a close brace and ending with a semicolon.
	elsif ( $line =~ /^}(.*);$/ )
		{
		my $endre = $1;
		# Remove leading whitespace
		$endre =~ s/^\s+//;
		# Remove trailing whitespace
		$endre =~ s/\s+$//;
	
		# expand [things] in end regex also	
		$endre = $self->rereplace($endre);

        if (! $s->{PARENT} )
            {
            $self->error("Compilation failure: extra '}' detected on line $ln");
            return 0;
            }

	 	$s->{endregex} = qr/$endre/;  # pre-compile this regex

        # Set our stack pointer to parent.
        $s = $s->{PARENT};
        next compline;

		}

	# We have a keep command
	elsif ( $line =~ /^keep\s+(.*)\s+as\s+(.*)/ )
		{
		if (!$1 || !$2)
			{
			$self->error("Syntax error on line $ln (keep command)");
			return 0;
			}

		# We do these in arrays so as to iterate over them in order.
        $s->{keep}[$s->{KEEP_COUNT}]{name} = $2;		
        $s->{keep}[$s->{KEEP_COUNT}]{value} = $1;		
		$s->{KEEP_COUNT}++;
		}

	# We want to set a variable
	elsif ( $line =~ /^set\s+(.*)\s+as\s+(.*)/ )
		{
		# We do these in arrays so as to iterate over them in order.
		$s->{commands}[$s->{COMMAND_COUNT}]{value} = $1;
		$s->{commands}[$s->{COMMAND_COUNT}]{name} = $2;
		$s->{commands}[$s->{COMMAND_COUNT}]{type} = 'scalar';
		$s->{COMMAND_COUNT}++;
		}
	elsif ( $line =~ /^push\s+(.*)\s+on\s+(.*)/ )
		{
		$s->{commands}[$s->{COMMAND_COUNT}]{value} = $1;
		$s->{commands}[$s->{COMMAND_COUNT}]{name} = $2;
		$s->{commands}[$s->{COMMAND_COUNT}]{type} = 'push';
		$s->{COMMAND_COUNT}++;
		}
	elsif ( $line =~ /^pop\s+(.*)\s+off\s+(.*)/ )
		{
		$s->{commands}[$s->{COMMAND_COUNT}]{value} = $1;
		$s->{commands}[$s->{COMMAND_COUNT}]{name} = $2;
		$s->{commands}[$s->{COMMAND_COUNT}]{type} = 'pop';
		$s->{COMMAND_COUNT}++;
		}
	else 
		{
		$self->error("Syntax error on line $ln");
		return 0;
		}

}   #end foreach

$self->debug("Finished compilation of script $self->{parsescript}", 2) if $self->{debuglevel};
#print Dumper $self->{STACK};
return 1;
} # end compile

# This actually runs the parsing of an asup, using the STACK structure.
sub parse {
my ($self, $asup) = @_;

# Make an array of lines
my @text = split(/\n/, $asup);

# line counter
my $ln = 0;

my ($regex_group, @matches);

# stack pointer
my $s = $self->{STACK};

# Initialize META object (lasts until reset, used for things like keeping array index)
$self->{META}{$s}{TOP} = 1;

# backstep pointer, for when we walk back up the stack looking for a previous regexp.
my $backstep = 0;

# take a line
my $line = $text[$ln]; $ln++; $self->debug($line, 5) if $self->{debuglevel};
if (!defined($line)) { return 0; }

mainwhile: 
while (1)
	{

    # First thing's first.  If we have an endregex, we're ending a group forcefully.
	# We do this first, because it needs to take precedence over everything else.
	if ( defined($s->{endregex}) )
		{
		$self->{globals}{_attempts_}++;
		if ($line =~ /$s->{endregex}/)
			{
			$self->{globals}{_matched_}++;
			$self->debug("Hit endregex (". $s->{endregex} ."), moving up", 9) if $self->{debuglevel};

			# this moves us up one level in STACK
			$s = $s->{PARENT};

			$line = $text[$ln]; $ln++; $self->debug($line, 5) if $self->{debuglevel};
			if (!defined($line)) { return 1; }
			next mainwhile;
			}
		$self->debug("Endregex defined and checked", 9) if $self->{debuglevel};
		}

    # Since we have no endregex (or no match of it) we now search regeps in the current context for a match.
	# $regex_group is the same structure as our '$s' pointer - but it's for each of the regexs under us.
	foreach $regex_group ( @{$s->{regexs}}  )
		{
		$self->{globals}{_attempts_}++;
		#$self->debug("?> checking " . $regex_group->{regex}, 9);
		# If match 
		if ( @matches = ($line =~ /$regex_group->{regex}/) )
			{
			#my $eom = $+[0];  # keep  the end of the match for splice, currently disabled.
			$self->debug("-> Matched: " . $regex_group->{regex}, 3) if $self->{debuglevel};
			$self->{globals}{_matched_}++;

			# unset backstep, if we have a match, we always need to stop backstepping.
			$backstep = 0;

			my ($match, %m, %d);

			# move matches into match array.  Probably a clearer way to do this.
			my $i = 1;
			foreach $match (@matches) 
				{
				$m{$i} = $match; 

				if ($self->{globals}{_dev_mode_}) 
					{
					$d{$i}{s} = $-[$i]; 
			    	$d{$i}{e} = $+[$i];
			    	$d{$i}{l} = $ln;
					$d{$i}{o} = $regex_group->{ordinal};
					}
				$i++;
				}

			# Do we splice?  commented out because it's totally broken.
#			if ( $regex_group->{regex} !~ /\$/ )   # Do we have a line WITHOUT $ in it?
#            	{
#		 		my $splice = substr($line, $eom);
#				if (length($splice) > 0 ) 
#					{
#					# put the line back on our message
#					$self->addline($text, $splice);
#					$self->debug("    no eol anchor, puting \"$splice\"($eom) back on the text queue", 9);
#					$ln--;  # So line numbers remain correct
#					}
#				}

            # open for debate which of these should happen first.  Keeper will store off data, command
			# execution will update things like variables.   Some discussion lately about making the insert_date
			# in datastore come from the asup...in which case, the order below won't work.

			# Call keep exection function with match array and stack pointer
			# This function will also handle array_index 
			$self->keeper($regex_group, \%m, \%d) if exists($regex_group->{keep});

			# Call command exection function with match array and stack pointer (sets variables)
			$self->setter($regex_group, \%m, \%d) if exists($regex_group->{commands});

			# If the regex we matched has sub-regexs, move into it for the next line.  If not, 
			# just keep execution here
			if ( exists( $regex_group->{regexs} ) )
				{
				$self->debug("    This group has regexs, moving down", 9) if $self->{debuglevel};
				$s = $regex_group;
				}
			else 
				{
				$self->debug("    This group has no regexs, staying put in " . $s->{regex}, 9) if $self->{debuglevel};
				}
		
			# take a line and continue
			$line = $text[$ln]; $ln++; $self->debug($line, 5) if $self->{debuglevel};
			if (!defined($line)) { return 1; }
			next mainwhile;
			}
		}

	# If we're here, we didn't match any regexes or endregex.   It's a miss, we need to decide what to do.
	if ($backstep)
		{
		# We're already in a backstep, and we didn't hit anything at this level either.  

		# If I am NOT the top-of-stack (I have a parent) and NOT hitting a regex with an endregex.
		if ( $s->{PARENT} ) 
			{
			if (defined($s->{endregex}))
				{
				$self->debug("    in backstep, but in a regex with an end regex, resuming", $s->{regex}, 9) if $self->{debuglevel};

				# set stack pointer to backstep
				$s = $backstep;
				# unset backstep
				$backstep = undef;

				# take a line
				$line = $text[$ln]; $ln++; $self->debug($line, 5) if $self->{debuglevel};

				if (!defined($line)) { return 1; }
				# continue
				next mainwhile;

				}

			# move stack pointer to parent
			$s = $s->{PARENT};
			# continue
			next mainwhile;
			}
		# If I am at top-of-stack (we backstepped, but didn't find anything)
		else 
			{
			# set stack pointer to backstep
			$s = $backstep;
			# unset backstep
			$backstep = undef;

			$self->debug("    nothing found in backstep, resuming in ".$s->{regex}, 9) if $self->{debuglevel};

			# take a line
			$line = $text[$ln]; $ln++; $self->debug($line, 5) if $self->{debuglevel};
			if (!defined($line)) { return 1; }
			# continue
			next mainwhile;
			}
		}

	# else (we failed a match, but are not in backstep yet...so we need to set backstep)
	else
		{
		# If we have an endregex, we don't backstep.
		if ( defined($s->{endregex}) )
			{
			$self->debug("    Backstep blocked, endregex defined", 9) if $self->{debuglevel};
			$line = $text[$ln]; $ln++; $self->debug($line, 5) if $self->{debuglevel};
			if (!defined($line)) { return 1; }
			# continue
			next mainwhile;
			}

		# set backstep to current stack, if we're somewhere inside (don't set backstep if we're at top-of-stack)
		if ($s->{PARENT}) 
			{
			$self->debug("    No matches found in ".$s->{regex}.", backstepping!", 9) if $self->{debuglevel};
			$backstep = $s;
			}
		else
			{
			$self->debug("    No matches, but I'm the parent.  No backstep", 9) if $self->{debuglevel};
			# I want to set backstep but I'm already at the parent...so just take a line and move on.
			$line = $text[$ln]; $ln++; $self->debug($line, 5) if $self->{debuglevel};
			if (!defined($line)) { return 1; }
			}

		next mainwhile;
		}

	}



return(1);
}

# This function handles all commands - sets all variables
sub setter {
my ($self, $s, $m, $d) = @_;

my $command;


# s->{commands} is an array of all our set commands
foreach $command ( @{$s->{commands}}) 
	{
    my $name = $command->{name};
	my $value = $command->{value};

	# Variable replacement, both for local values ($m) and global values
	if ( index($name, '%') > -1 ) # If we have variables to replace, replace them.
		{
		$name = $self->varrepl($name, $m); 
		}

	if ( index($value, '%') > -1 )  # If we have variables to replace, replace them.
		{
		$value = $self->varrepl($value, $m); 
		$value = $self->varrepl($value, $self->{globals}); 
		}

	my $store;

    # If value is an int, this is easy.  Just take its correspoinding matchgroup value.
    if ($value =~ /^\d+$/)
        {
        $store = $m->{$value};
        }

	# The value is an expression.  Eval it.
    elsif ($value =~ /^\(.*\)$/)
        {
		$self->debug("Value is an expression: $value", 9 ) if $self->{debuglevel};

		# need to add some error handling here - do we store anything or just die?
        $store = eval $value;

		$self->debug("Value is now: $store", 9 ) if $self->{debuglevel};
        }

    elsif ( $command->{type} eq 'scalar' )
        {
        # Uh oh.
		$self->error("Syntax error in command: must be an integer or an (expression)");
        }


	# If we're in dev mode, print out a line
	if ($self->{globals}{_dev_mode_}) 
		{
		print "=$name: $d->{$value}{o},$d->{$value}{l},$d->{$value}{s},$d->{$value}{e}\n";
		}


	if ( $command->{type} eq 'scalar' ) 
		{
		$self->{globals}{$name} = $store;
		$self->debug("Set $name = ".$self->{globals}{$name}, 5) if $self->{debuglevel};
		}
	elsif ( $command->{type} eq 'push' )
		{
		push(@{$self->{globals}{$name."_ary"}}, $store);
		$self->buildary($name);
		$self->debug("Stack $name is now: " . $self->{globals}{$name}, 9) if $self->{debuglevel};
		}
	elsif ( $command->{type} eq 'pop' )
		{
		$self->{globals}{$store} = pop(@{$self->{globals}{$name."_ary"}});
		$self->buildary($name);
		$self->debug("Stack $name is now: " . $self->{globals}{$name}, 9) if $self->{debuglevel};
		}
	}

}

# This function takes a real, perl array, and turns it into a dotted line which can be used for
# data point names
sub buildary {
my $self = shift;
my $name = shift;

if ( defined (@{$self->{globals}{$name."_ary"}}) )
   {
   $self->{globals}{$name} = join('.', @{$self->{globals}{$name."_ary"}});
   }
else
   {
   $self->error("Array error: $name (did you pop an empty array?)");
   }

}

# this stores a datapoint in the DB.  $sql is a line of mysql text, which gets varrepl'd
sub keeper {

my ($self, $s, $m, $d) = @_;

my ($keep, $store);

# This is the format for sending rows to datastore
my $sql = "(DEFAULT, '%_lastitem_%', '%_lastvalue_%', '%_lastindex_%', '%_sysid_%', '%_rawid_%', '%_insert_date_%')";


# Update the array index for the regex group.
if ( defined ($self->{META}{$s}{index}) )
	{
	$self->{META}{$s}{index}++;
	}
else
	{
	$self->{META}{$s}{index} = 0;
	}

$self->{globals}{_lastindex_} = $self->{META}{$s}{index};

# s->{keep} is an array of all our keep commands
foreach $keep ( @{$s->{keep}}) 
	{
	my $dp = $keep->{name};
    my $value = $keep->{value};
	if ( index($dp, '%') > -1 ) 
		{
		$dp = $self->varrepl($keep->{name}, $m);
		$dp = $self->varrepl($dp, $self->{globals});
		}

	if ( index($value, '%') > -1 ) 
		{
		$value = $self->varrepl($keep->{value}, $m);
		}

	# If value is an int, this is easy.  Just take its correspoinding matchgroup value.
	if ($value =~ /^\d+$/)
		{
		$self->debug("    Store datapoint $dp = ". $m->{$value} ."", 5)  if $self->{debuglevel};
		$store = $m->{$value};
		}
	elsif ($value =~ /^\(.*\)$/)
		{
		$self->debug("    Value is an expression: $value", 5) if $self->{debuglevel};
		my $ret = eval $value;
		$self->debug("      returned $ret", 5) if $self->{debuglevel};
		$store = $ret;

		}
	else
		{
		# Uh oh.
		$self->error("Syntax error in 'keep' command: must be an integer or an (expression)");
		}

	# This is the value which goes to sql.
	$self->{globals}{_lastvalue_} = $store;

	# If we're in dev mode, print out a line
	if ($self->{globals}{_dev_mode_}) 
		{
		# If there is no value set
		# We don't know what matchgroup if any was used, so we try to get statistics on one of them
		# so we can still print out line number and such
		if ( $value !~ /^\d+$/ ) 
			{
			$value = 1;
			$d->{1}{s} = '';
			$d->{1}{e} = '';
			}
		print "$dp: $d->{$value}{o},$d->{$value}{l},$d->{$value}{s},$d->{$value}{e}\n";
		}

	# Resolve the item to an id.  If it doesn't exist, create it.
	my $item_id = $self->{r}->item2id($dp);
	if (!$item_id)
    	{
	    $item_id = $self->{acdb}->newitem($dp);

		# Creating a new item.  So make this resolver dirty.
		$self->{r}->destruct();
		$self->{r} = Resolver->new($self->{acdb});
		}
	$self->{globals}{_lastitem_} = $item_id;

    # Sql always has variables, no need to check
    my $row = $sql;
	$row = $self->varrepl($row, $self->{globals});
	push(@{$self->{globals}{_commandlist_}}, $row)
	}
}

# COMMITS our mysql statements
sub commit() {
my $self = shift;

# Handle to the DB we'll be working with.
my $dbh = $self->{acdb}->handle();

if ( !defined($self->{globals}{sysid}) )
	{
	$self->{error} = "No sysid was found or specified in this asup";
	return 0;
	}

my $sysid_int = $self->{r}->system2id($self->{globals}{sysid});
if ($sysid_int)
	{
	$self->{acdb}->touchsystem($sysid_int, $self->{globals}{'_rawid_'});
	}
else
	{
	$sysid_int = $self->{acdb}->newsystem($self->{globals}{sysid});
	}

$self->{globals}{_sysid_} = $sysid_int;
my $sql = "INSERT INTO datastore VALUES " . join(',', @{$self->{globals}{_commandlist_}});

# Last chance for variables, all aboard!
$sql =~ s/%_sysid_%/$sysid_int/g;

$self->debug($sql, 9) if $self->{debuglevel};

# into the DB with you, datapoints!
my $q = $dbh->prepare($sql);
$q->execute() || die DBI->errstr;   # should never, ever happen..if so, make a big fuss
$q->finish();

return 1;
}

# Resets the module to prepare it for another parse
sub reset() {
my $self = shift;
my $r = shift;

if ($self->{logfile}) 
   {
   delete $self->{logfile};
   }

delete $self->{globals};
delete $self->{META};
delete $self->{error};

if ($r) { $self->{r} = $r; }

return 1;
}

# set which debug mode we should use
# 1: print out
# 2: syslog
# 3: store-and-return
sub debugmode {
my ($self, $dbg_mode)  = @_;
$self->{debugmode} = $dbg_mode;
#$self->debug("Debug mode is now: $self->{debugmode} ", 2);
}

sub debuglevel {
  my $self = shift;
  $self->{debuglevel} = shift;
  #$self->debug("Debug level now $self->{debuglevel}", 2);
}

# send the whole debug log back
sub dumpdebug {
my $self = shift;
my $slurp;
if ($self->{logfile} )
   {
   return($self->{logfile});
   }
}

# Always bad.
sub error () {
my ($self, $message) = @_;

syslog('info', $message);
print "$message\n";
$self->{logfile} .= "ERROR: $message\n";

}

# Decide what to do with a line of debug text, and do it.
sub debug($$$) {

    #level  #self   # These are perf improvements
if ($_[2] > $_[0]->{debuglevel}) { return(0); }

# If we're debugging, no need to worry about performance..
my ($self, $line, $level) = @_;

$self->{logfile} .= "$line\n";

if (!$self->{debugmode} || $self->{debugmode} == 1)
   {
   print "$line\n";
   return(1);
   }
elsif ($self->{debugmode} == 2)
   {
   syslog('info', $line);
   return(2);
   }
elsif ($self->{debugmode} == 3)
   {
   return(3); 
   }
}

# $1 : String to search for variables in
# $2 : Hash of variables
sub varrepl($\%)
  {
  my ($self, $string, $vars) = @_;
  my ($var, $index);

  foreach $var (keys %$vars)
    {
	#$var_perc = '%'.$var.'%';

	$index = index($string, "\%$var\%");
	if ($index > -1 )
	   {
	   substr($string, $index, length($var)+2, $$vars{$var});
	   }
    }

  return($string);
  }

# Sets a global variable
sub setvar() {
my ($self, $name, $value) = @_;

$self->debug("Manual variable set: $name=$value", 6) if $self->{debuglevel};
$self->{globals}{$name} = $value;
}

# Gets a global variable
sub getvar() {
my $self = shift;
my $var = shift;

if ( ref($self->{globals}{$var}) eq 'ARRAY')
	{
	return @{$self->{globals}{$var}};
	}

return $self->{globals}{$var};
}

sub rereplace() {
my $self = shift;
my $re = shift;

my $try;

foreach $try ( keys %{$self->{replace}} )
	{
	$re =~ s/\[$try\]/$self->{replace}{$try}/g;
	}

return ($re);
}
1;
