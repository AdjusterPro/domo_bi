class DomoException < Exception
end

class Domo
	def initialize(client_id, secret, scope, logger)
        raise(DomoException, 'please provide a Client ID and Secret for Domo') if client_id.nil? or secret.nil?
		@logger = logger
		@access_token = JSON.parse(
            self.api_get(
                "/oauth/token?grant_type=client_credentials&scope=#{scope}",
                { 'Authorization' => 'Basic ' + Base64.encode64( "#{client_id}:#{secret}" ).chomp }
            )
        )['access_token']
	end

	def api_get(path, headers = nil)
        path = "/v1#{path}" unless /^\/oauth/.match(path)
	    headers = { 'Authorization' => "Bearer #{@access_token}" } if headers.nil?

		url = "https://api.domo.com#{path}"
		@logger.debug("fetching #{url} with headers #{headers.inspect}")

		output = ''
		loop do
			begin
				open(url, headers) do |f|
					f.each_line { |line| output += line }
				end
				break
			rescue Exception => e
				raise e unless /429/.match(e.message)
				@logger.warning('got 429, waiting half a minute for Domo')
				sleep 30
				next
			end
		end
		output.tap { |o| @logger.debug("response: #{o.inspect}") }
	end
end

class DomoDataSet < Domo
    def initialize(client_id, secret, logger, set_id)
		super(client_id, secret, 'data', logger)
        @set_id = set_id
    end

    def get_all(options)
        self.api_get("/datasets/#{@set_id}/data?includeHeader=true")
        [{}]
    end
end
