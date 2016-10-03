#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'cool.io'
require 'yajl'

require 'fluent/input'
require 'fluent/config/error'
require 'fluent/parser'

module Fluent
  class SyslogInput < Input
    Plugin.register_input('syslog', self)

    SYSLOG_REGEXP = /^\<([0-9]+)\>(.*)/

    FACILITY_MAP = {
      0   => 'kern',
      1   => 'user',
      2   => 'mail',
      3   => 'daemon',
      4   => 'auth',
      5   => 'syslog',
      6   => 'lpr',
      7   => 'news',
      8   => 'uucp',
      9   => 'cron',
      10  => 'authpriv',
      11  => 'ftp',
      12  => 'ntp',
      13  => 'audit',
      14  => 'alert',
      15  => 'at',
      16  => 'local0',
      17  => 'local1',
      18  => 'local2',
      19  => 'local3',
      20  => 'local4',
      21  => 'local5',
      22  => 'local6',
      23  => 'local7'
    }

    PRIORITY_MAP = {
      0  => 'emerg',
      1  => 'alert',
      2  => 'crit',
      3  => 'err',
      4  => 'warn',
      5  => 'notice',
      6  => 'info',
      7  => 'debug'
    }

    def initialize
      super
      require 'fluent/plugin/socket_util'
    end

    desc 'The port to listen to.'
    config_param :port, :integer, default: 5140
    desc 'The bind address to listen to.'
    config_param :bind, :string, default: '0.0.0.0'
    desc 'The prefix of the tag. The tag itself is generated by the tag prefix, facility level, and priority.'
    config_param :tag, :string
    desc 'The transport protocol used to receive logs.(udp, tcp)'
    config_param :protocol_type, default: :udp do |val|
      case val.downcase
      when 'tcp'
        :tcp
      when 'udp'
        :udp
      else
        raise ConfigError, "syslog input protocol type should be 'tcp' or 'udp'"
      end
    end
    desc 'If true, add source host to event record.'
    config_param :include_source_host, :bool, default: false
    desc 'Specify key of source host when include_source_host is true.'
    config_param :source_host_key, :string, default: 'source_host'.freeze
    config_param :blocking_timeout, :time, default: 0.5

    def configure(conf)
      super

      if conf.has_key?('format')
        @parser = Plugin.new_parser(conf['format'])
        @parser.configure(conf)
      else
        conf['with_priority'] = true
        @parser = TextParser::SyslogParser.new
        @parser.configure(conf)
        @use_default = true
      end
    end

    def start
      callback = if @use_default
                   method(:receive_data)
                 else
                   method(:receive_data_parser)
                 end

      @loop = Coolio::Loop.new
      @handler = listen(callback)
      @loop.attach(@handler)

      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @loop.watchers.each {|w| w.detach }
      @loop.stop
      @handler.close
      @thread.join
    end

    def run
      @loop.run(@blocking_timeout)
    rescue
      log.error "unexpected error", error: $!.to_s
      log.error_backtrace
    end

    private

    def receive_data_parser(data, addr)
      m = SYSLOG_REGEXP.match(data)
      unless m
        log.warn "invalid syslog message: #{data.dump}"
        return
      end
      pri = m[1].to_i
      text = m[2]

      @parser.parse(text) { |time, record|
        unless time && record
          log.warn "pattern not match: #{text.inspect}"
          return
        end

        record[@source_host_key] = addr[2] if @include_source_host
        emit(pri, time, record)
      }
    rescue => e
      log.error data.dump, error: e.to_s
      log.error_backtrace
    end

    def receive_data(data, addr)
      @parser.parse(data) { |time, record|
        unless time && record
          log.warn "invalid syslog message", data: data
          return
        end

        pri = record.delete('pri')
        record[@source_host_key] = addr[2] if @include_source_host
        emit(pri, time, record)
      }
    rescue => e
      log.error data.dump, error: e.to_s
      log.error_backtrace
    end

    private

    def listen(callback)
      log.info "listening syslog socket on #{@bind}:#{@port} with #{@protocol_type}"
      if @protocol_type == :udp
        @usock = SocketUtil.create_udp_socket(@bind)
        @usock.bind(@bind, @port)
        SocketUtil::UdpHandler.new(@usock, log, 2048, callback)
      else
        # syslog family add "\n" to each message and this seems only way to split messages in tcp stream
        Coolio::TCPServer.new(@bind, @port, SocketUtil::TcpHandler, log, "\n", callback)
      end
    end

    def emit(pri, time, record)
      facility = FACILITY_MAP[pri >> 3]
      priority = PRIORITY_MAP[pri & 0b111]

      tag = "#{@tag}.#{facility}.#{priority}"

      router.emit(tag, time, record)
    rescue => e
      log.error "syslog failed to emit", error: e.to_s, error_class: e.class.to_s, tag: tag, record: Yajl.dump(record)
    end
  end
end