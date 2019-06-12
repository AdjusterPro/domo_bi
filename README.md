# domo_bi

* https://github.com/AdjusterPro/domo_bi

## Description

A Ruby SDK for the [Domo API](https://developer.domo.com/explorer)

## Features

DomoDataSet has convenience methods for the following [DataSet endpoints](https://developer.domo.com/docs/dataset-api-reference/dataset):

* retrieve

* query 

* list

* export

You should also be able to use DomoBI#get and #post for any other GET or POST endpoints.

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