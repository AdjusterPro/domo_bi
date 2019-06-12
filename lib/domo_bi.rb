class DomoBIException < Exception
end

class DomoBI
    def initialize(client_id, secret, scope, logger, debug=false)
        raise(DomoBIException, 'please provide a Client ID and Secret for Domo') if client_id.nil? or secret.nil?
        @logger = logger
        @debug = debug
        @access_token = JSON.parse(
            self.get(
                "/oauth/token?grant_type=client_credentials&scope=#{scope}",
                { 'Authorization' => 'Basic ' + Base64.urlsafe_encode64( "#{client_id}:#{secret}" ).chomp }
            )
        )['access_token']
    end

    def request(path, headers = {})
        headers['Authorization'] = "Bearer #{@access_token}" unless headers.has_key?('Authorization')
        path = "/v1#{path}" unless /^\/oauth/.match(path)

        url = "https://api.domo.com#{path}"

        @logger.debug("requesting #{url}" + (@debug ? " with headers [#{headers.inspect}]" : ""))
        
        response = nil
        loop do
            begin
                response = yield(url, headers)
                break
            rescue Exception => e
                if /429/.match(e.message)
                    @logger.warning('got 429, waiting half a minute for Domo')
                    sleep 30
                    next
                end

                @logger.error("exception: #{e.inspect}, response detail: #{e.response.read_body.inspect}")
                raise e
            end
        end

        response.tap { |r| @logger.debug("response: #{r.inspect}") if @debug }
    end

    def get(path, headers = {})
        self.request(path, headers) do |url, final_headers|
            output = ''
            open(url, final_headers) do |f|
                f.each_line { |line| output += line }
            end
            output
        end
    end

    def post(path, payload, headers = {})
        self.request(path, headers) do |url, final_headers|
            uri = URI(url)
            http = Net::HTTP.new(uri.host, uri.port)
            http.use_ssl = true

            req = Net::HTTP::Post.new(uri)
            final_headers.each { |name, value| req['name'] = 'value' }
            req.body = payload.to_json
            @logger.debug("POST body: #{req.body.inspect}") if @debug

            http.request(req).value
        end
    end
    
    def list_datasets
        JSON.parse(self.get("/datasets"))
    end
end

class DomoDataSet < DomoBI
    def initialize(client_id, secret, logger, set_id, debug=false)
        super(client_id, secret, 'data', logger, debug)
        @set_id = set_id
    end

    def retrieve
        JSON.parse(self.get("/datasets/#{@set_id}"))
    end

    def export(options = {})
        CSV.parse(
            self.get("/datasets/#{@set_id}/data?includeHeader=true"),
            { :headers => true }
        )
    end

    def query(sql)
        JSON.parse(
            self.post("/datasets/query/execute/#{@set_id}", { 'sql': sql })
        )
    end
end
