require 'test-helper'

class DomoTest < Test::Unit::TestCase
    def dataset
        @dataset ||= DomoDataSet.new(ENV['DOMO_CLIENT_ID'], ENV['DOMO_SECRET'], Logger.new(STDOUT), ENV['DOMO_TEST_DATA'], true)
    end

    def domo
        @domo ||= DomoBI.new(ENV['DOMO_CLIENT_ID'], ENV['DOMO_SECRET'], 'data', Logger.new(STDOUT), true)
    end

    def test_check_for_credentials
        #omit
        assert_raise_message('please provide a Client ID and Secret for Domo') do
            DomoBI.new(nil,'fake-secret', 'data', Logger.new(STDOUT))
        end 

        assert_raise_message('please provide a Client ID and Secret for Domo') do
            DomoBI.new('fake-client-id', nil, 'data', Logger.new(STDOUT))
        end
    end

    def test_check_for_set_id
        #omit
        assert_raise_message('please provide a Domo Dataset ID') do
            DomoDataSet.new(ENV['DOMO_CLIENT_ID'], ENV['DOMO_SECRET'], Logger.new(STDOUT), ENV['UNDEFINED_ENV_VAR'], true)
        end
    end

    def test_domo_connection
        #omit # this is implied in the tests below
        assert_not_nil(self.domo)
    end

    def test_list_datasets
        #omit
        assert_compare(100, '<=', self.domo.list_datasets.size)
    end

    def test_retrieve_dataset
        #omit
        assert_equal(
            ENV['DOMO_TEST_DATA'],
            self.dataset.retrieve['id']
        )
    end

    def test_export_data
        #omit
        assert_equal(
            ['col1', 'col2', 'col3'],
            self.dataset.export[0].headers.first(3)
        )
    end


    def test_query_data
        #omit
        assert_compare(1, '<=', self.dataset.query('select * from table')['rows'].size)
    end

    def test_query_luxe
        #omit
        r = self.dataset.query_luxe('select * from table')
        assert(r.is_a?(Array))
        assert_compare(1, '<=', r.size)
        assert(r[0].is_a?(Hash))
        assert_equal(
            ['col1', 'col2', 'col3'],
            r[0].keys.reject { |k| k =~ /_BATCH/ }
        )
    end

    def test_query_luxe_with_symbols
        #omit
        r = self.dataset.query_luxe('select * from table', :symbolize => true)
        assert(r.is_a?(Array))
        assert_compare(1, '<=', r.size)
        assert(r[0].is_a?(Hash))
        assert_equal(
            [:col1, :col2, :col3],
            r[0].keys.reject { |k| k =~ /_BATCH/ }
        )
    end

    def test_import
      #omit
      dataset = domo.create_dataset(
        '[test] API unit test', 'placeholder',
        {
          columns: [
            { type: 'STRING', name: 'Col1' },
            { type: 'STRING', name: 'Col2' },
            { type: 'STRING', name: 'Col3' }
          ]
        }
      )
      assert_kind_of(DomoDataSet, dataset)

      assert_nothing_raised { dataset.import("testval1,testval2,testval3") }

      assert_nothing_raised { dataset.delete }
    end
end
