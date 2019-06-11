require 'test-helper'

class DomoTest < Test::Unit::TestCase
    def test_check_for_credentials
        assert_raise_message('please provide a Client ID and Secret for Domo') do
            Domo.new(nil,'fake-secret', 'data', Logger.new(STDOUT))
        end 

        assert_raise_message('please provide a Client ID and Secret for Domo') do
            Domo.new('fake-client-id', nil, 'data', Logger.new(STDOUT))
        end
    end

    def test_domo_connection
        omit
        assert_not_nil(Domo.new(ENV['DOMO_CLIENT_ID'], ENV['DOMO_SECRET'], 'data', Logger.new(STDOUT)))
    end
end
