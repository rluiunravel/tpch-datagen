#!/usr/bin/perl -w

###############################################################################
# This script can be used to generate TPCH data in a single machine and load them into HDFS.
#
# Usage:
#  perl gen_data.pl scale_factor num_files zipf_factor host_list local_dir hdfs_dir
#  
#  where:
#    scale_factor = TPCH Scale factor (GB of data to generate)
#    num_files    = The number of files to generate for each table
#    zipf_factor  = Zipfian distribution factor (0-4, 0 means uniform)
#    host_list    = File containing a single line that says localhost
#    local_dir    = Local directory to use in the host machines
#    hdfs_dir     = HDFS directory to store the generated data
#
# Assumptions/Requirements:
#
# Software required:
#   a. perl
#   b. gcc / g++
#   c. tar
#
# 1. The enviromental variable $HADOOP_HOME is defined in this node. It 
#       default to /usr such that /usr/bin/hadoop is used 
# 2. The local directory does not exist on this node
# 3. The HDFS directory does not exist
# 4. There is enough local disk space on this node to generate a local copy of the data
# 5. The number of files must be greater than half the scale factor to ensure
#    that we don't try to generate a file that is greater than 2GB
#
# The data is loaded into HDFS. The name for each file is of the form
# "tablename.tbl.x", where tablename is lineitem, orders etc, and 
# x is a number between 1 and <num_files>.
# Each table is placed in a corresponding directory under <hdfs_dir>.
#
# Author: Shivnath Babu
# Date: March 04, 2015
#
##############################################################################

# Simple method to print new lines
sub println {
    local $\ = "\n";
    print @_;
}

# Make sure we have all the arguments
if ($#ARGV != 5)
{
   println qq(Usage: perl $0 scale_factor num_files zipf_factor host_list local_dir hdfs_dir);
   println qq(  scale_factor: TPCH Scale factor \(GB of data to generate\));
   println qq(  num_files:    The number of files to generate for each table);
   println qq(  zipf_factor:  Zipfian distribution factor \(0-4, 0 means uniform\));
   println qq(  host_list:    File containing a single line that says localhost);
   println qq(  local_dir:    Local directory to use in the host machines);
   println qq(  hdfs_dir:     HDFS directory to store the generated data);
   exit(-1);
}

# Get the input data
my $SCALE_FACTOR    = $ARGV[0];
my $NUM_FILE_SPLITS = $ARGV[1];
my $ZIPF_FACTOR     = $ARGV[2];
my $HOST_LIST       = $ARGV[3];
my $LOCAL_DIR       = $ARGV[4];
my $HDFS_DIR        = $ARGV[5];

# Start data generation
println qq(Starting data generation at: ) . `date`;
println qq(Input Parameters:);
println qq(  Scale Factor:    $SCALE_FACTOR);
println qq(  Number of Files: $NUM_FILE_SPLITS);
println qq(  ZIPF Factor:     $ZIPF_FACTOR);
println qq(  Host List:       $HOST_LIST);
println qq(  Local Directory: $LOCAL_DIR);
println qq(  HDFS Directory:  $HDFS_DIR);
println qq();

# Error checking
if ($SCALE_FACTOR <= 0)
{
   println qq(ERROR: The scale factor must be greater than 0);
   exit(-1);
}

if ($NUM_FILE_SPLITS < $SCALE_FACTOR / 2)
{
   println qq(ERROR: The number of files must be greater than half the scale factor);
   exit(-1);
}

if ($ZIPF_FACTOR < 0 || $ZIPF_FACTOR > 4)
{
   println qq(ERROR: The zipf factor must be between 0 and 4);
   exit(-1);
}

if (!-e $HOST_LIST)
{
   println qq(ERROR: The file '$HOST_LIST' does not exist);
   exit(-1);
}

if (!$ENV{'HADOOP_HOME'})
{
   println qq(WARN: \$HADOOP_HOME is not defined);
   println qq(Using: \$HADOOP_HOME = /usr so that /usr/bin/hadoop will be used)
#  exit(-1);
}

# Not running hadoop-env.sh on modern clusters
# Execute the hadoop-env.sh script for environmental variable definitions
#!system qq(. \$HADOOP_HOME/conf/hadoop-env.sh) or die $!;

my $hadoop_home = ($ENV{'HADOOP_HOME'}) ? $ENV{'HADOOP_HOME'} : "/usr";
#println "$hadoop_home";

# Get the hosts
open INFILE, "<", $HOST_LIST;
my @hosts = ();
while ($line = <INFILE>)
{
   $line =~ s/(^\s+)|(\s+$)//g;
   push(@hosts, $line) if $line =~ /\S/
}
close INFILE;

# Make sure we have some hosts
my $num_hosts = scalar(@hosts);
if ($num_hosts != 1)
{
   println qq(ERROR: The hosts file '$HOST_LIST' should contain a single line that says localhost);
   exit(-1);
}

# Create all the HDFS directories
my $RES = `$hadoop_home/bin/hadoop fs -stat $HDFS_DIR 2>&1`;
#println("$RES");
if ($RES =~ /No such file or directory/) {
#  this directory does not exist   
}
elsif ($RES !~ /cannot stat/) {
   println qq(ERROR: The directory '$HDFS_DIR' already exists);
   exit(-1);
}
else {
}

println qq(Creating all the HDFS directories);
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/lineitem) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/orders) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/customer) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/partsupp) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/part) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/supplier) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/nation) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/region) or die $!;
println qq();

# Create the execution script that will be sent to the hosts
open OUTFILE, ">", "gen_and_load.sh" or die $!;
#print OUTFILE qq(unzip -n tpch_data_gen.zip\n);
print OUTFILE qq(tar -zxvf tpch_data_gen.tar.gz\n);
print OUTFILE qq(perl tpch_gen_data.pl data.properties\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/lineitem.tbl* $HDFS_DIR/lineitem\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/orders.tbl*   $HDFS_DIR/orders\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/customer.tbl* $HDFS_DIR/customer\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/partsupp.tbl* $HDFS_DIR/partsupp\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/part.tbl*     $HDFS_DIR/part\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/supplier.tbl* $HDFS_DIR/supplier\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/nation.tbl*   $HDFS_DIR/nation\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/region.tbl*   $HDFS_DIR/region\n);
print OUTFILE qq(rm -rf data/*.tbl*\n);
close OUTFILE;
chmod 0744, "gen_and_load.sh";

# Each host will generate a certain range of the file splits
my $num_splits_per_host = int($NUM_FILE_SPLITS / $num_hosts);
$num_splits_per_host = 1 if $num_splits_per_host < 1;
my $first_file_split = 1;

# Connect to each host and generate the data
for ($host = 0; $host < $num_hosts; $host++)
{
   # Calculate the last file split generated by this host
   $last_file_split = ($host == $num_hosts-1) 
                      ? $NUM_FILE_SPLITS 
                      : $first_file_split + $num_splits_per_host - 1;
   
   # Create the data.properties file and copy it to the host
   open OUTFILE, ">", "data.properties" or die $!;
   print OUTFILE qq(scaling_factor = $SCALE_FACTOR \n);
   print OUTFILE qq(num_file_splits = $NUM_FILE_SPLITS \n);
   print OUTFILE qq(first_file_split = $first_file_split \n);
   print OUTFILE qq(last_file_split = $last_file_split \n);
   print OUTFILE qq(zipf = $ZIPF_FACTOR \n);
   print OUTFILE qq(tpch_home = $LOCAL_DIR/data \n);
   close OUTFILE;

   # Copy the necessary files to the host
   println qq(Copying files to host: $hosts[$host]);
   !system qq(mkdir $LOCAL_DIR) or die $!;
   !system qq(cp gen_and_load.sh $LOCAL_DIR) or die $!;
   !system qq(chmod a+x $LOCAL_DIR/gen_and_load.sh) or die $!;
   !system qq(cp tpch_data_gen.tar.gz $LOCAL_DIR) or die $!; 
   !system qq(cp data.properties $LOCAL_DIR) or die $!;

   # Start the data generation in a child process
   println qq(Starting data generation at host: $hosts[$host]\n);
   unless (fork)
   {
      chdir($LOCAL_DIR) or die "$!";
      `./gen_and_load.sh >& gen_and_load.out`;
      println qq(Data generation completed at host: $hosts[$host]\n);
      exit(0);
   }

   # Exit the loop if we have generated all file splits
   $first_file_split = $last_file_split + 1;
   last if $last_file_split == $NUM_FILE_SPLITS;
}

# Wait for the hosts to complete
println qq(Waiting for the data generation to complete);
for ($host = 0; $host < $num_hosts; $host++)
{
   wait;
}

# Clean up
system qq(rm data.properties);
system qq(rm gen_and_load.sh);

# Done
$time = time - $^T;
println qq();
println qq(Data generation is complete!);
println qq(Time taken (sec):\t$time);

