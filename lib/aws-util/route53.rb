require 'thor'
require 'yaml'
require 'fog'
require 'net/http'

module Aws
  module Util
    class Route53 < Thor
      def initialize(*args)
        super
      end
      desc "upsert", "Upsert your records managed by Route53"

      method_option :aws_region, :default => "ap-northeast-1", :desc => "Region of your RDS server"
      method_option :aws_access_key_id, :desc => "Access key of your aws account"
      method_option :aws_secret_access_key, :desc => "Secret access key of your aws account"
      method_option :aws_route53_hosted_zone_id, :desc => "Route53 hosted zone id of your aws account"
      
      method_option :file, :desc => "YAML file of defaults for any option. Options given during execution override these."

      def upsert
        settings = options
        dns = Fog::DNS::AWS.new(:aws_access_key_id => settings[:aws_access_key_id],
                                :aws_secret_access_key => settings[:aws_secret_access_key],
                                )
        hosted_zone_id = settings[:aws_route53_hosted_zone_id]
        resource_records = settings[:aws_route53_resource_records]
        options = { :comment => 'upsert records from cui.'}

        resource_records = resource_records.map{|r|
          if (r[:type] == 'DDNS')
            r[:type] = 'A'
            ip = Net::HTTP.get('ifconfig.me', '/ip').chomp
            r[:resource_records] = [ip]
          end
          r
        }
        dns.change_resource_record_sets(hosted_zone_id,
                                        resource_records,
                                        options)
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

    end
  end
end
