require 'test-helper'

class DomoTest < Test::Unit::TestCase
    def test_check_for_credentials
        assert_raise_message('please provide a Client ID and Secret for Domo') do
            DomoBI.new(nil,'fake-secret', 'data', Logger.new(STDOUT))
        end 

        assert_raise_message('please provide a Client ID and Secret for Domo') do
            DomoBI.new('fake-client-id', nil, 'data', Logger.new(STDOUT))
        end
    end

    def test_domo_connection
        omit
        assert_not_nil(DomoBI.new(ENV['DOMO_CLIENT_ID'], ENV['DOMO_SECRET'], 'data', Logger.new(STDOUT)))
    end

    def test_pull_data
        dataset = DomoDataSet.new(ENV['DOMO_CLIENT_ID'], ENV['DOMO_SECRET'], Logger.new(STDOUT), ENV['DOMO_TEST_DATA'])
        assert_equal(
            ['col1', 'col2', 'col3'],
            dataset.get_all[0].headers.first(3)
        )
    end
end
