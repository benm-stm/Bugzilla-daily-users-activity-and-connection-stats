#!/usr/bin/perl

#Created by: MED RAFIK BEN MANSOUR
#Created on: 10/05/2015
#Version No: V1.1

#Change description (if any):
#Export data in CSV format

#Purpose: extract userlist from the database (by croning the script or by giving the date range) and save the result in a json file

use strict;
use warnings;

use JSON::XS;
use lib "/var/www/html/bugzilla/bugzilla/lib";  # use the parent directory
use DBI;
use DBD::mysql;
use POSIX qw(strftime);
use strict;
use warnings;
use DateTime;

########################################################################
# JSON Manipulation
########################################################################

#Open the json file in write mode and append data
sub write_json {
        my ($json, $file, $header, @newData) = @_;

        local $/; #Enable 'slurp' mode
        open(my $fh, '>', $file) or die "Impossible d'ouvrir le fichier '$file' $!";
        my $data = decode_json($json);
        push @{ $data->{$header} }, @newData;
        print $fh encode_json($data);
        close $fh;
}

########################################################################
## Files Manipulation
########################################################################

# create folder if it does not exist
sub create_dir{
        my ($dir) = @_;
        mkdir $dir unless -d $dir; # Check if dir exists. If not create it.
}

#Open the json file in read mode
sub read_file {
        my ($file) = @_;

        local $/; #Enable 'slurp' mode
        open my $fh, "<", "$file" or die "Impossible d'ouvrir le fichier '$file' $!";
        my $json = <$fh>;
        close $fh;
        return $json
}

#Open the file in write mode and append data
sub write_file {
        my ($file, @data) = @_;
        local $/; #Enable 'slurp' mode
        open(my $fh, '>>', $file) or die "Impossible d'ouvrir le fichier '$file' $!";
        print $fh @data;
        close $fh;
}

#Reinit the given file with the given header
sub init_file {
        my ($dir, $filename, $header, $is_log, $type) = @_;
        my $path = "$dir/$filename";
        if (!-e $path || $is_log == 1) {
                local $/;
                open my $fh, ">>", $path or die "Can't open '$path'\n";
                if($is_log == 1){
                        print $fh "\n";
                        print $fh "[$header]: ";
                } else {
                        if ($type eq "csv") {
                                print $fh "Date;User number;users mails; Total bugzilla users; activated users\n";
                        } else {
                                print $fh "{\"$header\":[]}";
                        }
                }
                close $fh;
        }
}

#########################################################################################
#DB Manipulation
##########################################################################################

#single DB connection instance
sub db_connect {
        my($platform, $database, $host, $port, $user, $pw, $socket, $dbh) = @_;
        return $dbh if defined $dbh;
        my $dsn = "dbi:$platform:$database:$host:$port;$socket";
        $dbh = DBI->connect($dsn, $user, $pw);
        return $dbh;
}

#get the users connection per day
sub get_users {
        my ($dbh, $using_activity, @date_array) = @_;
        my @users = ();
        my $sql;
        my $sth;
        my @result = ();
        for my $i (0 .. $#date_array)
        {
                @users = ();
                if($using_activity == 1) {
                        print "Activity extraction :\n";
                        $sql = qq{
                                SELECT DISTINCT(login_name)
                                FROM profiles
                                WHERE profiles.userid IN
                                (
                                    SELECT DISTINCT(who)
                                    FROM longdescs
                                     WHERE bug_when BETWEEN "$date_array[$i] 00:00:00" AND "$date_array[$i] 23:59:59"
                                UNION DISTINCT
                                SELECT DISTINCT(who)
                                FROM bugs_activity
                                WHERE bug_when BETWEEN "$date_array[$i] 00:00:00" AND "$date_array[$i] 23:59:59"
                                )
                                ORDER BY login_name
                                };
                } else {
                        print "Connection extraction :\n";
                        $sql = qq{
                                SELECT login_name
                                FROM profiles
                                WHERE last_seen_date BETWEEN "$date_array[$i] 00:00:00" AND "$date_array[$i] 23:59:59"
                                };
                }
                $sth= $dbh->prepare($sql);
                $sth->execute();

                while (my @data = $sth->fetchrow_array()) {
                        push @users, $data[0];
                }
                my $users_concat =  join(', ',  @users);

                push @result, $users_concat;
                print "processing ".($i+1)."/".($#date_array+1)." ($date_array[$i])\n";
        }
        return @result;
}

#get bugzilla user's number
sub count_users{
        my ($enabled_users, $dbh) = @_;
        my $sql;
        my $count = 0;
        if($enabled_users == 1) {
                $sql = qq{select count(userid) as "" from profiles Where profiles.login_name NOT LIKE "%.deleted" and is_enabled = "1"};
        } else {
                $sql = qq{select count(userid) as "" from profiles};
        }
        my $sth= $dbh->prepare($sql);
        $sth->execute();

        while (my @data = $sth->fetchrow_array()) {
                $count = $data[0];
        }
        return $count;
}

####################################################################
# Date manipulation
######################################################################

# extract a range from given start and end date (used by the user in the interface)
sub data_from_pool{
        my ($start_date, $end_date) = @_;

        my @date_array = ();
        my @start_date = split /-/, $start_date;
        my @end_date = split /-/, $end_date;

        my $start = DateTime->new(
                        year   => $start_date[0],
                        month => $start_date[1],
                        day  => $start_date[2],
                        );

        my $end = DateTime->new(
                        year   => $end_date[0],
                        month => $end_date[1],
                        day  => $end_date[2],
                        );

        do {
                push @date_array, $start->ymd('-');
        }while ( $start->add(days => 1) <= $end );
        return @date_array;
}
# extract the days from given month-year (used by a monthly cron)
sub month_days{
        my ($year, $month) = @_;

        my @date_array = ();
        my $date = DateTime->new(
                        year  =>  $year,
                        month => $month,
                        day   => 1,
                        );

        my $date2 = $date->clone;

        $date2->add( months => 1 )->subtract( days => 1 );

        push @date_array, $date->ymd('-');
        push @date_array, $date2->ymd('-');
        return @date_array;
}

#transform the collected datas to json array or to csv array
sub format_query{
        my ($date_array, $dbh, $count_enabled_users, $count_all_users, $type, @users) = @_;
        my @date_array = @{$date_array};
        my @date = ();
        my @data_array = ();
        my $data = "";

        my $user_nb_per_day;
        for my $i (0 .. $#date_array) {
                $user_nb_per_day = 0;
                my $x = ", ";
                my @c = $users[$i] =~ /$x/g;
                my $user_nb_per_day = @c;
                if($users[$i] ne "") {
                        $user_nb_per_day = $user_nb_per_day+1;
                }
                @date = split /-/, $date_array[$i];
                if ($type eq "json") {
                        $data = {year=>$date[0],month=>$date[1],day=>$date[2], number=>$user_nb_per_day, users=>"$users[$i]", all_users=>$count_all_users, enabled_users=>$count_enabled_users};
                } else {
                        $data = $date[0]."-".$date[1]."-".$date[2].";".$user_nb_per_day.";"."$users[$i]".";".$count_all_users.";".$count_enabled_users."\n";
                }
                push @data_array, $data;
        }
        return @data_array;
}

#Construct data from the exported datas from the DB and append the result to the given file
sub construct_array {
        my($dbh, $using_activity, $count_enabled_users, $count_all_users, $header, $file, $log_file, $stats_dir, $log_dir, $type, @date_array) = @_;

        my @users = get_users($dbh, $using_activity, @date_array);
        my @array =  format_query(\@date_array, $dbh, $count_enabled_users, $count_all_users, $type, @users);
        init_file($stats_dir, $file, $header, 0, $type);
        my $data = read_file("$stats_dir/$file");
        if ($type eq "json") {
                write_json($data, "$stats_dir/$file", $header, @array);
        } else {
                write_file("$stats_dir/$file", @array);
        }
}

########################################################################
# Main
########################################################################
# connection instance
my $dbh = undef;

# CONFIG VARIABLES
my $platform = "mysql";
my $database = "bugzilla";
my $host = "localhost";
my $port = "3307";
my $user = "bgzuser";
my $pw = "password";
my $socket = "mysql_socket=";

#type of output
my $type = "csv"; #csv or json

#dirs
my $log_dir = "logs";
my $stats_dir = "stats";

#log file
my $log_file = "stats_log.log";
#first json
my $header_activity = "activity_stats";
my $file_activity = "data_activity.".$type;
#second json
my $header_cnx = "connection_stats";
my $file_cnx = "data_cnx.".$type;

my @date_array = ();

my $num_args = $#ARGV + 1;

if ($num_args == 1 &&  $ARGV[0] eq "cron"){
        my $current = strftime "%Y-%m-%d", localtime;
        $file_cnx = (strftime "%Y-%m", localtime)."-".$file_cnx;
        $date_array[0] = ($current);
        $dbh = db_connect($platform, $database, $host, $port, $user, $pw, $socket, $dbh);

        #count users
        my $count_enabled_users = count_users(1, $dbh);
        my $count_all_users = count_users(0, $dbh);

        #init directories and log file
        create_dir($stats_dir);
        create_dir($log_dir);
        my $current_date = strftime "%d-%m-%Y", localtime;
        init_file($log_dir, $log_file, $current_date, 1, $type);

        #create cnx file
        construct_array($dbh, 0, $count_enabled_users, $count_all_users, $header_cnx, $file_cnx, $log_file, $stats_dir, $log_dir, $type, @date_array);

        #Create activity file
        $file_activity = (strftime "%Y-%m", localtime)."-".$file_activity;
        construct_array($dbh, 1, $count_enabled_users, $count_all_users, $header_activity, $file_activity, $log_file, $stats_dir, $log_dir, $type, @date_array);
        $dbh->disconnect;
}
elsif ($num_args == 2) {

        my $start_date = $ARGV[0];
        my $end_date = $ARGV[1];
        @date_array = data_from_pool($start_date, $end_date);

        $dbh = db_connect($platform, $database, $host, $port, $user, $pw, $socket,$dbh);

        #count users
        my $count_enabled_users = count_users(1, $dbh);
        my $count_all_users = count_users(0, $dbh);

        #init directories and log file
        create_dir($stats_dir);
        create_dir($log_dir);
        my $current_date = strftime "%d-%m-%Y", localtime;
        init_file($log_dir, $log_file, $current_date, 1, $type);

        $file_activity = $start_date."--".$end_date."_".$file_activity;
        #create activity file
        construct_array($dbh, 1, $count_enabled_users, $count_all_users, $header_activity, $file_activity, $log_file, $stats_dir, $log_dir, $type, @date_array);
        $dbh->disconnect;
}
else {
        print"\nUsage :\t$0 YYYY-MM-DD YYYY-MM-DD\n\nor simply  cron it (daily) to get the exact daily results\nUsage :\t$0 cron >> logs/stats_log.log 2>&1\n\n";
}
print "End of operation";
