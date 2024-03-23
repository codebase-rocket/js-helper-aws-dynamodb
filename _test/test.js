// Info: Test Cases
'use strict';

// Shared Dependencies
var Lib = {};

// Set Configrations
const nodb_config = {
  'KEY': require('./.aws.json')['KEY'],
  'SECRET': require('./.aws.json')['SECRET'],
  'REGION': require('./.aws.json')['REGION']
};

// Dependencies
Lib.Utils = require('js-helper-utils');
Lib.Debug = require('js-helper-debug')(Lib);
Lib.Instance = require('js-helper-instance')(Lib);
const NoDB = require('js-helper-aws-dynamodb')(Lib, nodb_config);


////////////////////////////SIMILUTATIONS//////////////////////////////////////

function test_output_simple(err, response){ // Result are from previous function

  if(err){
    Lib.Debug.logErrorForResearch(err);
  }

  Lib.Debug.log('response', response);

};


function test_output_query(err, response, count, last_evaluated_key){ // Result are from previous function

  if(err){
    Lib.Debug.logErrorForResearch(err);
  }

  Lib.Debug.log('response', response);
  Lib.Debug.log('count', count);
  Lib.Debug.log('last_evaluated_key', last_evaluated_key);

};

///////////////////////////////////////////////////////////////////////////////


/////////////////////////////STAGE SETUP///////////////////////////////////////

// Initialize 'instance'
var instance = Lib.Instance.initialize();

// Set test url
var table = 'test_table';
var table_2 = 'test_table2';

// Dummy record
var dummy_record = {
  'part_id': 'fruits',
  'sort_id': 'apple',
  'color': 'red',
  'types': 20
};

var dummy_record2 = {
  'part_id': 'fruits',
  'sort_id': 'pear_small',
  'color': 'green',
  'types': 2
};

var dummy_record3 = {
  'part_id': 'fruits',
  'sort_id': 'pear_large',
  'color': 'yellow',
  'types': 2
};

///////////////////////////////////////////////////////////////////////////////


/////////////////////////////////TESTS/////////////////////////////////////////


/*
// Test commandBuilderForAddRecord()
var add_command = NoDB.commandBuilderForAddRecord(
  table,
  dummy_record2
);

console.log(
  'commandBuilderForAddRecord()',
  add_command
);


// Test commandAddRecord()
NoDB.commandAddRecord(
  instance,
  test_output_simple,
  add_command
);
*/



// Test addRecord()
NoDB.addRecord(
  instance,
  test_output_simple,
  table,
  dummy_record2
);



/*
// Test addBatchRecords()
var dummy_records_in_table_1 = [];
for(let i = 1; i<= 200; i++){ // Generate Dummy Records
  dummy_record = {
    'part_id': 'fruits',
    'sort_id': 'bulk_' + i,
    'color': 'red',
    'types': i
  };
  dummy_records_in_table_1.push(dummy_record);
}

var dummy_records_in_table_2 = [];
for(let i = 1; i<= 30; i++){ // Generate Dummy Records
  dummy_record = {
    'part_id': 'deserts',
    'sort_id': 'bulk_' + i,
    'color': 'green',
    'types': i
  };
  dummy_records_in_table_2.push(dummy_record);
}
NoDB.addBatchRecords(
  instance,
  test_output_simple,
  [table, table_2],
  [dummy_records_in_table_1, dummy_records_in_table_2]
);
*/



/*
// Test commandBuilderForDeleteRecord()
var delete_command = NoDB.commandBuilderForDeleteRecord(
  table,
  { // Record to be deleted
    'part_id': 'fruits',
    'sort_id': 'bulk_1'
  }
);

console.log(
  'commandBuilderForDeleteRecord()',
  delete_command
);


// Test commandDeleteRecord()
NoDB.commandDeleteRecord(
  instance,
  test_output_simple,
  delete_command
);
*/



/*
// Test deleteRecord()
NoDB.deleteRecord(
  instance,
  test_output_simple,
  table,
  { // Record to be deleted
    'part_id': 'fruits',
    'sort_id': 'bulk_1'
  }
);
*/



/*
// Test deleteBatchRecords()
var dummy_records_for_table_1 = [];
for(let i = 10; i<= 50; i++){ // Generate Dummy Records
  dummy_record = {
    'part_id': 'fruits',
    'sort_id': 'bulk_' + i,
  };
  dummy_records_for_table_1.push(dummy_record);
}

var dummy_records_for_table_2 = [];
for(let i = 1; i<= 10; i++){ // Generate Dummy Records
  dummy_record = {
    'part_id': 'deserts',
    'sort_id': 'bulk_' + i,
  };
  dummy_records_for_table_2.push(dummy_record);
}
NoDB.deleteBatchRecords(
  instance,
  test_output_simple,
  [table, table_2],
  [dummy_records_for_table_1, dummy_records_for_table_2]
);
*/



/*
// Test commandBuilderForUpdateRecord()
var update_command = NoDB.commandBuilderForUpdateRecord(
  table,
  { // Key
    'part_id': 'fruits',
    'sort_id': 'apple'
  },
  null, // No Update Data
  null, // No Remove Keys
  { // Increments
    'price': 10, // Increment price by 10
    'discount': 15, // Increment discount by 15
  },
  { // Decrements
    'key1': 35, // Decrement key1 by 35
  },
  'ALL_NEW', //'ALL_NEW', | 'ALL_OLD' | 'UPDATED_NEW' | 'UPDATED_OLD' | 'NONE'
);

console.log(
  'commandBuilderForUpdateRecord()',
  update_command
);


// Test commandUpdateRecord()
NoDB.commandUpdateRecord(
  instance,
  test_output_simple,
  update_command
);
*/



/*
// Test update() - Update-Data & Remove-Keys
NoDB.updateRecord(
  instance,
  test_output_simple,
  table,
  { // Key
    'part_id': 'fruits',
    'sort_id': 'apple'
  },
  { // Updated Data
    'price': 30,
    'discount': 10,
    'key1': 90
  },
  [ // Remove Keys
    'types'
  ],
  null, // No Increments
  null, // No Decrements
  'ALL_NEW', //'ALL_NEW', | 'ALL_OLD' | 'UPDATED_NEW' | 'UPDATED_OLD' | 'NONE'
);
*/



// Test update() - Increment
/*
NoDB.updateRecord(
  instance,
  test_output_simple,
  table,
  { // Key
    'part_id': 'fruits',
    'sort_id': 'apple'
  },
  null, // No Update Data
  null, // No Remove Keys
  { // Increments
    'price': 10, // Increment price by 10
    'discount': 15, // Increment discount by 15
  },
  { // Decrements
    'key1': 25, // Decrement key1 by 35
  },
  'ALL_NEW', //'ALL_NEW', | 'ALL_OLD' | 'UPDATED_NEW' | 'UPDATED_OLD' | 'NONE'
);
*/



/*
// Test getRecord()
NoDB.getRecord( // With no conditions
  instance,
  test_output_simple,
  table,
  {
    'part_id': 'fruits',
    'sort_id': 'apple'
  } // Key
);
*/



/*
// Test getBatchRecords()
NoDB.getBatchRecords( // With no conditions
  instance,
  test_output_simple,
  [table, table_2],
  [
    [{'part_id': 'fruits', 'sort_id': 'apple'}], // Table 1 Keys
    [{'part_id': 'deserts', 'sort_id': 'bulk_11'}, {'part_id': 'deserts', 'sort_id': 'bulk_12'}] // Table 2 Keys
  ]
);
*/



/*
// Test queryRecords() - With no conditions
NoDB.queryRecords(
  instance,
  test_output_query,
  table,
  null, // secondary_index
  'part_id', // id_key
  'fruits', // id_value
  null, // feilds_list
  null, // paging
  null, // condition
  null, // select
);
*/



/*
// Test queryRecords() - With Conditions on Sort-key (begins-with)
NoDB.queryRecords(
  instance,
  test_output_query,
  table,
  null, // secondary_index
  'part_id', // id_key
  'fruits', // id_value
  null, // feilds_list
  null, // paging
  { // condition
    'key': 'sort_id',
    'value': 'pear',
    'operator': 'begins_with',
    'asc': true
  },
  null, // select
);
*/



/*
// Test queryRecords() - With Conditions on Sort-key (between)
NoDB.queryRecords(
  instance,
  test_output_query,
  table,
  null, // secondary_index
  'part_id', // id_key
  'fruits', // id_value
  null, // feilds_list
  null, // paging
  { // condition
    'key': 'sort_id',
    'value': 'bulk_101',
    'value2': 'bulk_102',
    'operator': 'between',
    'asc': true
  },
  null, // select
);
*/



/*
// Test count()
NoDB.count(
  instance,
  test_output_simple,
  table,
  null, // secondary_index
  'part_id', // id_key
  'fruits', // id_value
  null, // condition
);
*/



/*
// Test transactWrite()
var add_command = NoDB.commandBuilderForAddRecord(
  table,
  dummy_record2
);
var delete_command = NoDB.commandBuilderForDeleteRecord(
  table,
  { // Record to be deleted
    'part_id': 'fruits',
    'sort_id': 'bulk_2'
  }
);
var update_command = NoDB.commandBuilderForUpdateRecord(
  table,
  { // Key
    'part_id': 'fruits',
    'sort_id': 'apple'
  },
  null, // No Update Data
  null, // No Remove Keys
  { // Increments
    'price': 10, // Increment price by 10
    'discount': 15, // Increment discount by 15
  },
  { // Decrements
    'key1': 35, // Decrement key1 by 35
  },
  'ALL_NEW', //'ALL_NEW', | 'ALL_OLD' | 'UPDATED_NEW' | 'UPDATED_OLD' | 'NONE'
);
NoDB.transactWrite(
  instance,
  test_output_simple,
  null, // No Check-Records
  [ // List of 'Add-Records'
    add_command
  ],
  [ // List of 'Update-Records'
    update_command
  ],
  [ // List of 'Delete-Records'
    delete_command
  ]
);
*/

///////////////////////////////////////////////////////////////////////////////
