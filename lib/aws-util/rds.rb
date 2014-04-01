require 'thor'
require 'yaml'
require 'fog'
require 'cocaine'
require 'mysql2'

module Aws
  module Util
    class RDS < Thor
      def initialize(*args)
        super
      end

      desc "mysql", "Runs a mysqldump from a restored snapshot of the specified RDS instance"
      method_option :aws_region, :default => "ap-northeast-1", :desc => "Region of your RDS server"
      method_option :aws_access_key_id, :desc => "Access key of your aws account"
      method_option :aws_secret_access_key, :desc => "Secret access key of your aws account"
      # RDS
      method_option :rds_instance_id, :desc => "InstanceID of RDS"
      method_option :rds_instance_type, :default => "db.t1.micro", :desc => "Instance type of RDS"
      # MySQL
      method_option :mysql_port, :default => "3306", :desc => "Port of MySQL server"
      
      method_option :dump_directory, :default => '/tmp/', :desc => "Where to store the temporary sql dump file."
      method_option :file, :desc => "YAML file of defaults for any option. Options given during execution override these."

      def mysql
        std_date = DateTime.now
        settings = options
        rds = Fog::AWS::RDS.new(:aws_access_key_id => settings[:aws_access_key_id],
                                :aws_secret_access_key => settings[:aws_secret_access_key],
                                :region => settings[:aws_region])
        rds_server = rds.servers.get(settings[:rds_instance_id])

        s3 = Fog::Storage.new(:provider => 'AWS',
                              :aws_access_key_id => settings[:aws_access_key_id],
                              :aws_secret_access_key => settings[:aws_secret_access_key],
                              :region => settings[:aws_region],
                              :path_style => true,
                              :scheme => 'https')

        backup_server_id = "dump-#{rds_server.id}-#{std_date.strftime('%Y%m%d%H%M%S')}"
        backup_file_sql_name = "#{rds_server.id}-mysqldump-#{std_date.strftime('%Y%m%d%H%M%S')}.sql"
        backup_file_gz_name = "#{rds_server.id}-mysqldump-#{std_date.strftime('%Y%m%d%H%M%S')}.sql.gz"
        backup_file_sql_filepath = File.join(settings[:dump_directory], backup_file_sql_name)
        backup_file_gz_filepath = File.join(settings[:dump_directory], backup_file_gz_name)
        backup_database = "dump_#{settings[:mysql_database]}_#{std_date.strftime('%Y%m%d%H%M%S')}"
        
        rds.restore_db_instance_to_point_in_time(rds_server.id,
                                                 backup_server_id,
                                                 'DBInstanceClass' => settings[:db_instance_class],
                                                 'UseLatestRestorableTime' => true,
                                                 'AvailabilityZone' => settings[:db_availability_zone],
                                                 'DBSubnetGroupName' => settings[:db_subnet_group_name],
                                                 'MultiAz' => false,
                                                 )
        backup_server = rds.servers.get(backup_server_id)
        backup_server.wait_for(7200) { ready? }

        rds.modify_db_instance(backup_server_id, true, 'VpcSecurityGroups' => settings[:vpc_security_groups])
        sleep(120)

        mysqldump = Cocaine::CommandLine.new('mysqldump', "--opt --add-drop-table --single-transaction --order-by-primary -h :host_address -P :mysql_port -u :mysql_username --password=:mysql_password :mysql_database > :backup_filepath")

        mysqlimport = Cocaine::CommandLine.new('mysql', "-h :host_address -P :mysql_port -u :mysql_username --password=:mysql_password :mysql_database < :backup_filepath")

        gzipcompress = Cocaine::CommandLine.new('gzip', "--fast :backup_filepath")

        begin
          mysqldump.run(
                        :host_address    => backup_server.endpoint['Address'],
                        #                      :host_address    => "127.0.0.1",
                        :mysql_username  => settings[:mysql_username],
                        :mysql_password  => settings[:mysql_password],
                        :mysql_database  => settings[:mysql_database],
                        :mysql_port      => settings[:mysql_port].to_s,
                        :backup_filepath => backup_file_sql_filepath,
                        :logger          => Logger.new(STDOUT))

          client = Mysql2::Client.new(:host => settings[:mysql_backup_host],
                                      :username => settings[:mysql_backup_username],
                                      :password => settings[:mysql_backup_password],
                                      :port => settings[:mysql_backup_port])
          client.query("DROP DATABASE IF EXISTS #{backup_database}")
          client.query("CREATE DATABASE #{backup_database}")
          
          mysqlimport.run(
                          :host_address    => settings[:mysql_backup_host],
                          :mysql_username  => settings[:mysql_backup_username],
                          :mysql_password  => settings[:mysql_backup_password],
                          :mysql_database  => backup_database,
                          :mysql_port      => settings[:mysql_backup_port].to_s,
                          :backup_filepath => backup_file_sql_filepath,
                          :logger          => Logger.new(STDOUT))

          if settings[:mysql_backup_ttl] > 0
            prune_dumpdbs(client, "dump_#{settings[:mysql_database]}", settings[:mysql_backup_ttl])
          end
          client.close

          gzipcompress.run(:backup_filepath => backup_file_sql_filepath)

        rescue Cocaine::ExitStatusError, Cocaine::CommandNotFoundError => e
          puts "Dump failed with error #{e.message}"
          cleanup(backup_server, backup_file_sql_filepath, backup_file_gz_filepath)
          exit(1)
        end
        
        s3_bucket  = s3.directories.get(settings[:s3_bucket])
        tries = 1
        saved_dump = begin
                       s3_bucket.files.new(:key => File.join(settings[:s3_prefix], backup_file_gz_name),
                                           :body => File.open(backup_file_gz_filepath),
                                           :acl => 'private',
                                           :content_type => 'application/x-gzip'
                                           ).save
                     rescue Exception => e
                       if tries < 3
                         puts "Retrying S3 upload after #{tries} tries"
                         tries += 1
                         retry
                       else
                         puts "Trapped exception #{e} on try #{tries}"
                         false
                       end
                     end
        if saved_dump
          if settings[:s3_save_ttl] > 0
            prune_dumpfiles(s3_bucket, File.join(settings[:s3_prefix], "#{rds_server.id}-mysqldump-"), settings[:s3_save_ttl])
          end
        else
          puts "S3 upload failed!"
        end

        cleanup(backup_server, backup_file_sql_filepath, backup_file_gz_filepath)
      end

      private
      def options
        merged_options = {}
        original_options = super
        merged_options = original_options
        filename = original_options[:file] || ''
        if File.exists?(filename)
          defaults = ::YAML::load_file(filename) || {}
          merged_options = original_options.merge(defaults)
        end
        merged_options
      end

      def cleanup(backup_server, backup_file_sql_filepath, backup_file_gz_filepath)
        backup_server.wait_for { ready? }
        backup_server.destroy(nil)
        File.unlink(backup_file_sql_filepath) if File.exists?(backup_file_sql_filepath)
        File.unlink(backup_file_gz_filepath) if File.exists?(backup_file_gz_filepath)
      end

      def prune_dumpdbs(client, backup_db_prefix, dump_ttl)
        results = client.query("SHOW DATABASES").select{ |r| r["Database"][/^dump_/] }
        (results.count - dump_ttl).times do |i|
          client.query("DROP DATABASE IF EXISTS #{results[i]["Database"]}")
        end
      end

      def prune_dumpfiles(s3_bucket, backup_file_prefix, dump_ttl)
        my_files = s3_bucket.files.all('prefix' => backup_file_prefix)
        if my_files.count > dump_ttl
          files_by_date = my_files.sort {|x,y| x.last_modified <=> y.last_modified}
          (files_by_date.count - dump_ttl).times do |i|
            files_by_date[i].destroy
          end
        end
      end
    end
  end
end
