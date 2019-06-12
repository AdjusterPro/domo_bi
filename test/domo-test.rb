require 'test-helper'

class DomoTest < Test::Unit::TestCase
    def dataset
        @dataset ||= DomoDataSet.new(ENV['DOMO_CLIENT_ID'], ENV['DOMO_SECRET'], Logger.new(STDOUT), ENV['DOMO_TEST_DATA'], true)
    end

    def domo
        @domo ||= DomoBI.new(ENV['DOMO_CLIENT_ID'], ENV['DOMO_SECRET'], 'data', Logger.new(STDOUT), true)
    end

    def test_check_for_credentials
        assert_raise_message('please provide a Client ID and Secret for Domo') do
            DomoBI.new(nil,'fake-secret', 'data', Logger.new(STDOUT))
        end 

        assert_raise_message('please provide a Client ID and Secret for Domo') do
            DomoBI.new('fake-client-id', nil, 'data', Logger.new(STDOUT))
        end
    end

    def test_domo_connection
        omit # this is implied in the tests below
        assert_not_nil(self.domo)
    end

    def test_list_datasets
        assert_compare(1, '<=', self.domo.list_datasets.size)
    end

    def test_export_data
        assert_equal(
            ['col1', 'col2', 'col3'],
            self.dataset.export[0].headers.first(3)
        )
    end


    def test_query_data
        assert_compare(1, '<=', self.dataset.query('select * from `API Test Data`'))
    end
end
