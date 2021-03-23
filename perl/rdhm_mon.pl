#!/usr/bin/perl -w

##################################################################################################
#####WRITTEN BY ERIC LYONS 03/2021 for CASA, UNIVERSITY OF MASSACHUSETTS##########################
##################################################################################################
#  TESTED FUNCTIONALITY:                                                                         #
#                                                                                                #  
#  -RECURSIVELY MONITORS DIRECTORIES FOR INCOMING FILES AND EXECUTES DATA DRIVEN WORKFLOW        #
#                                                                                                #
##################################################################################################

use POSIX qw(setsid);
use File::Copy;
use File::Monitor;
use threads;
use threads::shared;

our $input_data_dir;
our @qpefiles;
our @runfiles;
our $leftover_file = "";
our @delete_queue;

&command_line_parse;
&daemonize;

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
	    #just the initial case
	    if ($leftover_file eq "") {
		$leftover_file = $qpefiles[0];
	    }

	    #use the last file of the previous batch as the start time
	    my $startymd = substr($leftover_file, -13, 8);
            my $starthm = substr($leftover_file, -5, 4);
            my $starttime = $startymd . "T" . $starthm;

	    #make sure we have at least two elements
	    my $numQPEfiles = @qpefiles;
	    
	    my $endymd;
	    my $endhm;

	    if ($numQPEfiles < 2) {
		#if not, use the leftover as endtime too
		$endymd = $startymd;
		$endhm = $starthm;
	    }
	    else {
		#otherwise use the second to last file as the end time
		$endymd = substr($qpefiles[-2], -13, 8);
		$endhm = substr($qpefiles[-2], -5, 4);
	    }
	    my $endtime =$endymd . "T". $endhm;
	    
	    #reset the leftover file
	    $leftover_file = $qpefiles[-1];

	    #good time to send along latest data for backing up current state
	    system("sh /home/ldm/rdhmworkflow/send_latest_output.sh");

	    #call the pegasus workflow
	    my $wfcall = "python3.6 /home/ldm/rdhmworkflow/run_rdhm.py -s " . $starttime . " -f " . $endtime . " -i Realtime_RSRT2_CASA_container.card";
	    print $wfcall . "\n";
	    system($wfcall);

	    for $qpefile (@qpefiles) {
		push(@delete_queue, $qpefile);
	    }
	    @qpefiles = ();
	}
	sleep 5;
    }
    
    sub new_files  {
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
	    #print $pathsuffix . "\n";
	    #print $pathstr . " " . $filename . "\n";
	    my $filesuffix = substr($filename, -3);	    
	    
	    if ($pathsuffix eq "qpe") {
		$qpefiles_contained = 1;
		push(@qpefiles, $file);
	    }
	    elsif ($filesuffix eq ".gz") {
		my $link_call = "ln -f -s " . $file . " " . $pathstr . "current_" . $pathsuffix . ".gz";
		#print "link call: " . $link_call . "\n";
		system($link_call);
		if (($pathsuffix eq "surfaceFlow") || ($pathsuffix eq "discharge") || ($pathsuffix eq "returnp")) {
		    my $unzip_fn = substr($filename, 0, -3);
		    my $gunzip_call = "gunzip -c " . $file . " > /nfs/shared/rdhm/unzip/" . $unzip_fn;
		    #print "gunzip call: " . $gunzip_call . "\n";
		    system($gunzip_call);
		}
	    }
	    elsif ($pathsuffix eq "unzip") {
		my $xmrgToAsc_call = "singularity exec -B /nfs/shared:/nfs/shared /nfs/shared/ldm/rdhm_singularity.simg /opt/rdhm/bin/xmrgtoasc -i " . $file . " -o /nfs/shared/rdhm/asc/" . $filename . ".asc -f \"-5.3f\"";
		#print $xmrgToAsc_call . "\n";
		system($xmrgToAsc_call);
	    }
	    elsif ($pathsuffix eq "asc") {
		my $short_filename = substr($filename, 0, -4);
		my $unzipped_del_file = "/nfs/shared/rdhm/unzip/" . $short_filename;
		unlink($unzipped_del_file);
		my @typesplit = split /(\d+)/, $filename;
		my $conv_code = "/home/ldm/rdhmworkflow/geojson_conversions/" . $typesplit[0] . "_to_geojson.py";
		my $geojson_conv_cmd = "python3.6 " . $conv_code . " -i " . $file . " -o /nfs/shared/rdhm/geojson/" . $short_filename . ".geojson -x /home/ldm/rdhmworkflow/geojson_conversions/nx.txt -y /home/ldm/rdhmworkflow/geojson_conversions/ny.txt";
		print $geojson_conv_cmd . "\n";
		system($geojson_conv_cmd);
	    }
	    elsif ($pathsuffix eq "geojson") {
		my $pqins_call = "/home/ldm/bin/pqinsert -f EXP -p " . $filename . " " . $file;
		system($pqins_call);
		my $short_filename = substr($filename, 0, -8);
		my $asc_del_file = "/nfs/shared/rdhm/asc/" . $short_filename;
		unlink($asc_del_file);
	    }
	    elsif ($pathsuffix eq "output") {
		#possible race condition here... 
		#going to try using basin_info files as indication that rdhm pegasus run has completed
		#then delete the qpe xmrg files that went into the rdhm run
		my $ftype = substr($filename, 0, 6);
		if ($ftype eq "basin_") {
		    for $delfile (@delete_queue) {
			#unlink($delfile);
		    }
		    @delete_queue = ();
		}
		#unlink($file);
	    }
	}
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
