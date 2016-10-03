require 'fluent/input'

module Fluent
  class ElephantTailInput < Input

    Fluent::Plugin.register_input('elephant-tail', self)

    config_param :namenode_host, :string,  :default => "127.0.0.1"
    config_param :namenode_port, :integer, :default => 50070
    config_param :username,      :string,  :default => nil
    config_param :tag,           :string,  :default => "elephant"
    config_param :tail_directory,:string,  :default => "/"
    config_param :tracker_file,  :string,  :default => "/home/flytxt/Desktop/FLUENT/track"

    def initialize
      super
      require 'json'
      require 'webhdfs'
    end

    # This method is called before starting.
    # 'conf' is a Hash that includes configuration parameters.
    # If the configuration is invalid, raise Fluent::ConfigError.
    
    def configure(conf)
      super

        log.debug("preparing client: user = \"#{@username}\" @ #{@namenode_host}:#{@namenode_port}")
        @client = prepare_client(@namenode_host, @namenode_port, @username)

        @transported_
    end

    def start
      super

      if namenode_available?(@client)
        log.info "webhdfs connection confirmed: #{@namenode_host}:#{@namenode_port}"
      end

      emit_records [{"hello" => "world"}]
    end

    # This method is called when shutting down.
    # Shutdown the thread and close sockets or files here.
    def shutdown
    
    end





    

    def emit_records(records)
        es = MultiEventStream.new
        records.each { |record|
            es.add(Engine.now, record)
        }
        router.emit_stream(@tag, es)
    end

    #############################################
    def prepare_client(host, port, username)
        client = WebHDFS::Client.new(host, port, username)
        client
    end

    def namenode_available?(client)
        if client
        available = true
        begin
            client.list('/')
        rescue => e
            log.warn "webhdfs check request failed. (namenode: #{client.host}:#{client.port}, error: #{e.message})"
            available = false
        end
        available
        else
        false
        end
    end

    def wait_after(time)
        start_time = Time.now
        yield
        sleep_time = time - (Time.now - start_time)
        sleep sleep_time if sleep_time > 0
    end
    ############################################
  end
end