#!/usr/bin/env perl
use strict;
use warnings;
use utf8;
use 5.014;
use List::MoreUtils qw(firstidx);
use DBI;
use Data::Dumper;
use DateTime;
use Getopt::Long;


my @MONTHS = qw( Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec );
my $DATABASE_LOCATION='common_log.db';
my $BATCH_SIZE=1000;

GetOptions ("db=s" => \$DATABASE_LOCATION,
            'batch=i'=>\$BATCH_SIZE) or die("Error in command line arguments\n");


sub main{
    
    #Your code here!!
    
    my ($output, $db) = check_database();
    
    if($output == -1){
        warn "Problem getting the database!";
        return -1;
    }
    
    my $batch_counter=0;
    
    $db->begin_work;
    while(<>){
        my @extraction = split(' ', $_);
        my $ip  = $extraction[0];

        my $client = $extraction[1];

        my $user = $extraction[2];

        my $timestamp;
        eval{
            $timestamp = extract_timestamp(substr(join(' ', @extraction[3,4]),1,-1));
        };
        if($@){
            say 'Error extracting timestamp '.join(' ', @extraction[3,4]);
            return -1;
        }

        my $request_method = substr $extraction[5], 1;

        my $resource = $extraction[6];

        my ($protocol, $protocol_version) = split('/', substr($extraction[7],0,-1));

        my $http_status = $extraction[8];

        my $bytes_transferred = $extraction[9];

        if(scalar @extraction < 10){
            $bytes_transferred = $extraction[-1];
            $http_status = $extraction[-2];
            $resource = join(' ', @extraction[5..($#extraction-2)]);
            
            $protocol_version = undef;
            $protocol=undef;
            $request_method = undef;
        }
        
    insert_in_database( $db, $ip, $client, $user, $timestamp,
                        $request_method, $resource, $protocol, $protocol_version,
                        $http_status, $bytes_transferred, \$batch_counter);
    
    }    
    
    commit_transaction($db);
    
    create_indexes($db);
    
    return 0;
}

#Why we would want to drop the indexes?
#If we have a big database and we want to add a new batch of requests, for every insert done the
#indexes will be rebuild. If we drop them and then perform all the inserts and finally recreate them,
#the indexes are only rebuild one time which will improve performance
sub drop_indexes{
    
    my $db = shift;
    $db->begin_work;
    eval{
        $db->do('DROP INDEX IF EXISTS IDX_IP');
        $db->do('DROP INDEX IF EXISTS IDX_CLIENT');
        $db->do('DROP INDEX IF EXISTS IDX_USER');
        $db->do('DROP INDEX IF EXISTS IDX_TIMESTAMP');
        $db->do('DROP INDEX IF EXISTS IDX_RESOURCE');
        $db->do('DROP INDEX IF EXISTS IDX_RESOURCE_STATUS');

    };
    commit_transaction ($db);
    
}


sub create_indexes{
    
    my $db = shift;
    
    $db->begin_work;
    
    eval{
        $db->do('CREATE INDEX IF NOT EXISTS IDX_IP ON REQUESTS(ORIGIN_IP)');
        $db->do('CREATE INDEX IF NOT EXISTS IDX_CLIENT ON REQUESTS(USER_CLIENT)');
        $db->do('CREATE INDEX IF NOT EXISTS IDX_USER ON REQUESTS(USER)');
        $db->do('CREATE INDEX IF NOT EXISTS IDX_TIMESTAMP ON REQUESTS(TIMESTAMP)');
        $db->do('CREATE INDEX IF NOT EXISTS IDX_RESOURCE ON REQUESTS(RESOURCE)');
        $db->do('CREATE INDEX IF NOT EXISTS IDX_RESOURCE_STATUS ON REQUESTS(RESOURCE, STATUS)');

    };
    commit_transaction ($db);
    
}

sub insert_in_database{
    
    my ($db, $ip, $client, $user, $timestamp,
        $request_method, $resource, $protocol, $protocol_version,
        $http_status, $bytes_transferred, $batch_counter
        ) = @_;
    
    
    my $stmt = $db->prepare('INSERT INTO REQUESTS(ORIGIN_IP, USER_CLIENT,
                        USER, TIMESTAMP, REQUEST_METHOD, RESOURCE,
                        PROTOCOL, PROTOCOL_VERSION, STATUS, BYTES_TRANSFERRED)
                        VALUES(?,?,?,?,?,?,?,?,?,?)') or die $db->errstr;
    
    my $results = $stmt->execute(($ip, $client, $user,$timestamp, $request_method,
                                  $resource, $protocol,
                                  $protocol_version, $http_status, $bytes_transferred));
    
    if($$batch_counter == $BATCH_SIZE){
        $$batch_counter=0;
        commit_transaction( $db);
        $db->begin_work;
    
    }else{
        $$batch_counter+=1;
    }
    
    
}

sub commit_transaction{
    
    my $db = shift;

    $db->commit;
    
    if($@){
        warn "Transaction aborted because $@";
        eval{$db->rollback;}
    }
    
}

sub extract_timestamp{

    my $date = shift;
    
    $date =~ /(\d{2})\/(\w{3})\/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-]*\d{4})/;
    my $dt = DateTime->new(year=>$3,
                            day=>$1,
                            hour=>$4,
                            minute=>$5,
                            second=>$6,
                            time_zone=>$7,
                            month=>1+firstidx { $_ eq $2 } @MONTHS,
                            );

                            
                            
    return $dt->epoch;
}

sub check_database{
    
    my $database;
    
    if(-e $DATABASE_LOCATION){
    
        #I cannot do this at the beggining of the function because it will automatically create the file,
        #preventing me to create the database
        $database = DBI->connect("dbi:SQLite:$DATABASE_LOCATION",'','',{RaiseError => 1, AutoCommit => 1});
        
        #An arbitrary number: 10 Megs. It's a lot of inserts! So lets drop the indexes
        #Please check the comments on the drop_indexes 
        if(-s $DATABASE_LOCATION > 10*1024*1024){
            drop_indexes($database);
        }
        
        return  (0,$database);
    }
    
    #I cannot do this at the beggining of the function because it will automatically create the file,
    #preventing me to create the database
    $database = DBI->connect("dbi:SQLite:$DATABASE_LOCATION",'','',{RaiseError => 1, AutoCommit => 1});
    
    eval{
        $database->do('CREATE TABLE REQUESTS(ORIGIN_IP TEXT NOT NULL, USER_CLIENT TEXT,
                        USER TEXT , TIMESTAMP INTEGER NOT NULL, REQUEST_METHOD TEXT NOT NULL, RESOURCE TEXT NOT NULL,
                        PROTOCOL TEXT, PROTOCOL_VERSION REAL, STATUS INTEGER NOT NULL, BYTES_TRANSFERRED INTEGER)');
                
    };
    if($@){
        warn "Database creation aborted because $@";
 #       eval{$database->rollback;};
        return ( -1, undef);
    }
    return (0,$database);
}



exit(main(@ARGV));