# badgerquery
enchance badger db with index and query

## data structs
- db
    - seq
        - meta:seq
- table
    - config
        - meta:table:config:$tableName
    - record
        - data
            - $tableDataID:$dataKey
        - index
            - $tableIndexID:$dataKey
- index
    - config
        - meta:table:index:config:$tableName:$indexName
    - seq
        - meta:table:index:seq:$tableName:$indexName
    - data
        - $indexID:$dataKey:$indexSeq

## components
    - GC
        - badger's gc
        - clean deleted table, index , data

## create item
- create item
- create index

## update item
- save item
- add changed index
- delete old index

## delete item
- delete item
- delete index

## create table
- create default id index
-


## drop table
- delete items
- delete index
- delete index data
- delete

## create index
- build index on exists items

## drop index
- delete index data

## query
- get queue

## txn too big
- do not update more than 1000 items in one update.