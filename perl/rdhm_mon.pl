#!/usr/bin/perl -w

##################################################################################################
#####WRITTEN BY ERIC LYONS 12/2012 for CASA, UNIVERSITY OF MASSACHUSETTS##########################
##################################################################################################
#  TESTED FUNCTIONALITY:                                                                         #
#  
#  -RECURSIVELY MONITORS DIRECTORIES FOR INCOMING FILES AND PQINSERTS                            #
#  #                                                                                                #
##################################################################################################

use POSIX qw(setsid);
use File::Copy;
use File::Monitor;
use threads;
use threads::shared;
#use lib "/home/ldm/perl";

our $input_data_dir;
our @qpefiles;

&command_line_parse;
#&daemonize;

my $file_mon = new threads \&file_monitor;

sleep 900000000;

sub file_monitor {
    
    my $dir_monitor = File::Monitor->new();
        
    $dir_monitor->watch( {
	name        => "$input_data_dir",
	recurse     => 1,
        callback    => \&new_files,
    } );
    
    $dir_monitor->scan;
    for ($i=0; $i < 9000000000; $i++) {
	our $qpefiles_contained = 0;
	my @changes = $dir_monitor->scan;   
	if (($qpefiles_contained == 0) && (@qpefiles)) {
	    my $startymd = substr($qpefiles[0], -13, 8);
	    my $starthm = substr($qpefiles[0], -5, 4);
	    my $starttime = $startymd . "T" . $starthm;
	    my $endymd = substr($qpefiles[-1], -13, 8);
	    my $endhm = substr($qpefiles[-1], -5, 4);
	    my $endtime =$endymd . "T". $endhm;
	    my $wfcall = "python3.6 /home/ldm/rdhmworkflow/run_rdhm.py -s " . $starttime . " -f " . $endtime . " -i Realtime_RSRT2_CASA_container.card";
	    print $wfcall . "\n";
	    system($wfcall);
            @qpefiles = ();
	}
	sleep 10;
    }
    
    sub new_files 
    {
	my ($name, $event, $change) = @_;

	@new_data_files = $change->files_created;
	my @dels = $change->files_deleted;
	print "Added: ".join("\nAdded: ", @new_data_files)."\n" if @new_data_files;
	foreach $file (@new_data_files) {
	    sleep 1;
	    my $pathstr;
            my $filename;
            ($pathstr, $filename) = $file =~ m|^(.*[/\\])([^/\\]+?)$|;
	    my @pathstr_arr = split('/', $pathstr);
	    my $pathsuffix = $pathstr_arr[-1];
	    print $pathsuffix . "\n";
	    print $pathstr . " " . $filename . "\n";
	    my $filesuffix = substr($filename, -3);	    
	    
	    if ($pathsuffix eq "qpe") {
		$qpefiles_contained = 1;
		push(@qpefiles, $file);
	    }
	    if ($filesuffix eq ".gz") {
		my $link_call = "ln -f -s " . $file . " " . $pathstr . "current_" . $pathsuffix . ".gz";
		print "link call: " . $link_call . "\n";
		system($link_call);
	    }
	}
	
	    #if ($suffix eq "xml") 
	    #{
#		my $pqins_call = "/home/ldm/bin/pqinsert -f EXP -p " . $filename . " " . $file;
#		system($pqins_call);
#		unlink($file);
#	    }
	
    }
}

sub daemonize {
    chdir '/'                 or die "Can't chdir to /: $!";
    open STDIN, '/dev/null'   or die "Can't read /dev/null: $!";
    open STDOUT, '>>/dev/null' or die "Can't write to /dev/null: $!";
    open STDERR, '>>/dev/null' or die "Can't write to /dev/null: $!";
    defined(my $pid = fork)   or die "Can't fork: $!";
    exit if $pid;
    setsid                    or die "Can't start a new session: $!";
    umask 0;
}

sub command_line_parse {
    if ($#ARGV < 0) { 
	print "Usage:  rdhm_mon.pl <shared_dir>\n";
   	exit; 
    }
    $input_data_dir = $ARGV[0];
    my @rdd = split(/ /, $input_data_dir);
    foreach $w (@rdd) {
	print "Will recursively monitor $w for incoming files\n";
    }
}
