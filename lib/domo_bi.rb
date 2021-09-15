require 'json'
require 'base64'
require 'net/http'
require 'open-uri'
require 'csv'

class DomoBIException < Exception
end

class DomoBI
    def initialize(client_id, secret, scope, logger, debug=false)
        raise(DomoBIException, 'please provide a Client ID and Secret for Domo') if client_id.nil? or secret.nil?
        @client_id = client_id
        @secret = secret
        @logger = logger
        @debug = debug
        @access_token = JSON.parse(
            self.get(
                "/oauth/token?grant_type=client_credentials&scope=#{scope}",
                { 'Authorization' => 'Basic ' + Base64.urlsafe_encode64( "#{@client_id}:#{@secret}" ).chomp }
            )
        )['access_token']
    end

    def debug(msg)
        @logger.debug(msg) if @debug
    end

    def request(path, headers = {})
        headers['Authorization'] = "Bearer #{@access_token}" unless headers.has_key?('Authorization')
        path = "/v1#{path}" unless /^\/oauth/.match(path)

        url = "https://api.domo.com#{path}"

        begin
            self.debug("requesting #{url} with headers [#{headers.inspect}]")
            response = yield(url, headers)
        rescue Exception => e
            unless /429/.match(e.message)
              @logger.error("exception: #{e.inspect}, response detail: #{e.response.read_body.inspect}")
              raise e
            end

            @logger.warning('got 429, waiting half a minute for Domo')
            sleep 30
            retry
        end

        response.tap { |r| self.debug("response: #{r.inspect}") }
    end

    def get(path, headers = {})
        self._request(path, headers) do |uri, final_headers|
            Net::HTTP::Get.new(uri, final_headers)
        end
    end

    def _request(path, headers = {})
        headers['Authorization'] = "Bearer #{@access_token}" unless headers.has_key?('Authorization')
        path = "/v1#{path}" unless /^\/oauth/.match(path)

        url = "https://api.domo.com#{path}"

        uri = URI(url)
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = true
        req = yield(uri, headers)

        begin
            self.debug("requesting #{url} with headers [#{headers.inspect}]")
            r = http.request(req) 
            r.value
        rescue Exception => e
            unless /429/.match(e.message)
              @logger.error("exception: #{e.inspect}, response detail: #{e.response.read_body.inspect}")
              raise e
            end

            @logger.warning('got 429, waiting half a minute for Domo')
            sleep 30
            retry
        end

        r.read_body.tap { |r| self.debug("response: #{r.inspect}") }
    end

    def post(path, payload, headers = {})
        self._request(path, headers) do |uri, final_headers|
            final_headers['Content-Type'] = 'application/json'
            req = Net::HTTP::Post.new(uri, final_headers)
            (req.body = payload.to_json).tap { |b| self.debug("POST body: #{b.inspect}") }
            req
        end
    end

    def http_delete(path, headers = {})
        self._request(path, headers) do |uri, final_headers|
            Net::HTTP::Delete.new(uri, final_headers)
        end
    end

    def put_csv(path, payload, headers = {})
        self._request(path, headers) do |uri, final_headers|
            final_headers['Content-Type'] = 'text/csv'
            req = Net::HTTP::Put.new(uri, final_headers)
            (req.body = payload).tap { |b| self.debug("PUT body: #{b.inspect}") }
            req
        end
    end

    def list_datasets
        offset = 0
        all_items = []
        loop do
          items = JSON.parse(self.get("/datasets?offset=#{offset}&limit=50"))
          all_items += items
          break if items.size < 50
          offset += 50
        end
        all_items
    end

    def create_dataset(name, description, schema)
      set_id = JSON.parse(post(
        "/datasets",
        {
          :name => name,
          :description => description,
          :schema => schema
        }
      ))['id']

      DomoDataSet.new(@client_id, @secret, @logger, set_id, @debug)
    end
end

class DomoDataSet < DomoBI
    def initialize(client_id, secret, logger, set_id, debug=false)
        raise(DomoBIException, 'please provide a Domo Dataset ID') if set_id.nil?
        super(client_id, secret, 'data', logger, debug)
        @set_id = set_id
    end

    def retrieve
        JSON.parse(self.get("/datasets/#{@set_id}"))
    end

    def export(options = {})
        CSV.parse(
            self.get("/datasets/#{@set_id}/data?includeHeader=true"),
            headers: true 
        )
    end

    def query(sql)
        JSON.parse(
            self.post("/datasets/query/execute/#{@set_id}", { 'sql': sql })
        )
    end

    def query_luxe(sql, options = {})
        r = query(sql)
        cols = r['columns']

        r['rows'].map do |values|
          Hash[values.map.with_index { |v, i| [options[:symbolize] ? cols[i].to_sym : cols[i], v]}]
        end
    end


    def delete
      http_delete("/datasets/#{@set_id}")
    end

    def import(csv_string)
      put_csv(
        "/datasets/#{@set_id}/data",
        csv_string
      )
    end
end
