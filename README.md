# domo_bi

* https://github.com/AdjusterPro/domo_bi

## Description

A Ruby micro-SDK for the [Domo API](https://developer.domo.com/explorer)

## Features

`DomoDataSet` has convenience methods for the following [DataSet endpoints](https://developer.domo.com/docs/dataset-api-reference/dataset):

* retrieve

* query 

* export

You should also be able to use `DomoBI#get` and `#post` for any other GET or POST endpoints.

`DomoBI#list_datasets` naively wraps the [List DataSets](https://developer.domo.com/docs/dataset-api-reference/dataset#List%20DataSets) endpoint but doesn't yet support its options (`sort`, `limit`, and `offset`).

## Examples

```
dataset = DomoDataSet.new(client_id, client_secret, logger, dataset_id)

dataset.query('select * from table') # returns a query response object[1]

dataset.export # returns all accessible data as an array of CSV::Row objects
```
[1] https://developer.domo.com/docs/dataset-api-reference/dataset#Query%20a%20DataSet

## Install
Add this line to your Gemfile:
`gem 'domo_bi', :git => 'https://github.com/AdjusterPro/domo_bi'`

Use at your own risk! This is just a quick wrapper library that I threw together to automate exporting and querying from Domo. It probably won't be published on rubygems and may not be developed much past v0.0.3.

## Author

* Ben Dunlap

