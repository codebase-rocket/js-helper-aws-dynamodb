// Info: Boilerplate library. Connects to No-Sql Database. Contains Wraper Functions for AWS Dynamodb functions
'use strict';

// Shared Dependencies (Managed by Loader)
let Lib = {};

// For lazy loading of AWS SDK Services
let DynamoDBDocumentClient,
  GetCommand, PutCommand, DeleteCommand, UpdateCommand, QueryCommand,
  BatchGetCommand, BatchWriteCommand, TransactWriteCommand;

// Exclusive Dependencies
let CONFIG = require('./config'); // Loader can override it with Custom-Config


/////////////////////////// Module-Loader START ////////////////////////////////

  /********************************************************************
  Load dependencies and configurations

  @param {Set} shared_libs - Reference to libraries already loaded in memory by other modules
  @param {Set} config - Custom configuration in key-value pairs

  @return nothing
  *********************************************************************/
  const loader = function(shared_libs, config){

    // Shared Dependencies (Must be loaded in memory already)
    Lib.Utils = shared_libs.Utils;
    Lib.Debug = shared_libs.Debug;
    Lib.Instance = shared_libs.Instance;

    // Override default configuration
    if( !Lib.Utils.isNullOrUndefined(config) ){
      Object.assign(CONFIG, config); // Merge custom configuration with defaults
    }

  };

//////////////////////////// Module-Loader END /////////////////////////////////



///////////////////////////// Module Exports START /////////////////////////////
module.exports = function(shared_libs, config){

  // Run Loader
  loader(shared_libs, config);

  // Return Public Funtions of this module
  return NoDB;

};//////////////////////////// Module Exports END //////////////////////////////



///////////////////////////Public Functions START///////////////////////////////
const NoDB = { // Public functions accessible by other modules

  /********************************************************************
  Service-Param Builder for 'Put'

  @param {String} table_name - Table in which new entry is to be created
  @param {Set} data - JSON to be saved

  @return {set} service_params - Service Params for AWS DynamoDB 'Put' API
  *********************************************************************/
  commandBuilderForAddRecord: function(table_name, data){

    // Service Params
    var service_params = {
      'TableName'    : table_name,   // DynamoDB Table to which new record is to be saved
      'Item'         : data          // ID
    };


    // Return Service-Params
    return service_params;

  },


  /********************************************************************
  Add new entry to No-sql database (Using Pre-Built Command Object)

  @param {reference} instance - Request Instance object reference
  @param {Function} cb - Callback function to be invoked once async execution of this function is finished

  @param {Set} service_params - Command-Obj for record to be added

  @return Thru request Callback.

  @callback - Request Callback(err, is_success)
  * @callback {Error} err - In case of error
  * @callback {Boolean} is_success - true on successful add-record
  * @callback {Boolean} is_success - false on unsuccessful add-record or error
  *********************************************************************/
  commandAddRecord: function(instance, cb, service_params){

    // Initialize AWS service Object if not already Initialized
    _NoDB.initIfNot(instance);


    // Add entry in DynamoDB
    Lib.Debug.timingAuditLog('Start', 'AWS DynamoDB - Add Record', instance['time_ms']);
    instance.aws.nodb.dynamodb.send( new PutCommand(service_params) )

      .then(function(response){
        Lib.Debug.timingAuditLog('End', 'AWS DynamoDB - Add Record', instance['time_ms']);

        // Record Successfully added
        cb(null, true);

      })

      .catch(function(err){

        // Log error for research
        Lib.Debug.logErrorForResearch(
          err,
          'Cause: AWS DynamoDB' +
          '\ncmd: Add Record' +
          '\nparams: ' + JSON.stringify(service_params)
        );

        // Invoke Callback and forward error from dynamodb
        cb(err);

      });

  },


  /********************************************************************
  Add new entry to No-sql database

  @param {reference} instance - Request Instance object reference
  @param {Function} cb - Callback function to be invoked once async execution of this function is finished

  @param {String} table_name - Table in which new entry is to be created
  @param {Set} data - JSON to be saved

  @return Thru request Callback.

  @callback - Request Callback(err, is_success)
  * @callback {Error} err - In case of error
  * @callback {Boolean} is_success - true on successful add-record
  * @callback {Boolean} is_success - false on unsuccessful add-record or error
  *********************************************************************/
  addRecord: function(instance, cb, table_name, data){

    // Service Params
    var service_params = NoDB.commandBuilderForAddRecord(
      table_name, data
    );


    // Add entry in DynamoDB
    NoDB.commandAddRecord(instance, cb, service_params);

  },


  /********************************************************************
  Add multiple new entries to No-sql database (AWS Limit - Maximum 25 items. This function Internally loops when more then 25 items)
  DynamoDB currently retains up five minutes (300 seconds) of unused read and write capacity for burst

  @param {reference} instance - Request Instance object reference
  @param {Function} cb - Callback function to be invoked once async execution of this function is finished

  @param {String} table_names - Table in which new entry is to be created
  @param {Set} data_items - JSON entries to be saved

  @return Thru request Callback.

  @callback - Request Callback(err, is_success)
  * @callback {Error} err - In case of error
  * @callback {Boolean} is_success - true on successful add-record
  * @callback {Boolean} is_success - false on unsuccessful add-record or error
  *********************************************************************/
  addBatchRecords: function(instance, cb, table_names, data_items){

    // ~Sample Input~
    // table_name = [ table1, table2, table3 ]
    // data_items = [ [{table1_data1},{table1_data2}], [{table2_data1}], [{table3_data1},{table3_data2}] ]


    // Initialize AWS service Object if not already Initialized
    _NoDB.initIfNot(instance);


    // Counter to work-around WS limit of 25 keys at a time
    var available_limit = 25;
    var remaining_table_names = []; // Tables that couldn't be processed in this query
    var remaining_data_items = []; // IDs that couldn't be processed in this query


    // Service Params
    var service_params = {
      'RequestItems': {
        // Nested Items
      },
      'ReturnConsumedCapacity': "TOTAL"
    };


    // Loop and add multiple requets to service-params
    table_names.forEach(function(table_name, index){

      if( available_limit > 0 ){

        // Copy all the keys for respective table, if number of keys is within available limit
        if( data_items[index].length <= available_limit ){

          service_params['RequestItems'][table_name] = [];

          // Copy all items in table to AWS Service Request
          data_items[index].forEach(function(data_item){ // Copy all keys to this AWS Service Request
            service_params['RequestItems'][table_name].push( {'PutRequest':{'Item':data_item}} );
          });

          // Reduce Available Limit
          available_limit = available_limit - data_items[index].length;

        }
        else{
          service_params['RequestItems'][table_name] = [];

          data_items[index].slice(0, available_limit).forEach(function(data_item){ // Copy limited keys to this AWS Service Request
            service_params['RequestItems'][table_name].push( {'PutRequest':{'Item':data_item}} );
          });


          // Move rest of keys to remaining-data
          remaining_table_names.push(table_name);
          remaining_data_items.push( data_items[index].slice(available_limit, data_items[index].length) );

          // Reached Available Limit
          available_limit = 0; // Reached Available Limit
        }

      }
      else{

        // Move rest of keys to remaing-data
        remaining_table_names.push(table_name);
        remaining_data_items.push(data_items[index]);

      }

    });


    // Add entry in DynamoDB
    Lib.Debug.timingAuditLog('Start', 'AWS DynamoDB - Add Batch Record', instance['time_ms']);
    instance.aws.nodb.dynamodb.send( new BatchWriteCommand(service_params) )

      .then(function(response){
        Lib.Debug.timingAuditLog('End', 'AWS DynamoDB - Add Batch Record', instance['time_ms']);

        //Lib.Debug.log("AWS response: " +  JSON.stringify(response) );

        // If more records remaining because of AWS limit reached, recursive call to same function with remaining keys
        if( remaining_table_names.length > 0 ){
          NoDB.addBatchRecords(instance, cb, remaining_table_names, remaining_data_items)
        }
        else{
          // All good. All Records Successfully added
          cb(null, true);
        }

      })

      .catch(function(err){

        // Log error for research
        Lib.Debug.logErrorForResearch(
          err,
          'Cause: AWS DynamoDB' +
          '\ncmd: Add Batch Record' +
          '\nparams: ' + JSON.stringify(service_params)
        );

        // Invoke Callback and forward error from dynamodb
        return cb(err);

      });
  },


  /********************************************************************
  Service-Param Builder for 'Delete'

  @param {String} table_name - Table in which new entry is to be removed
  @param {Set} id - Partition key + Sort key of record to be deleted

  @return {set} service_params - Service Params for AWS DynamoDB 'Delete' API
  *********************************************************************/
  commandBuilderForDeleteRecord: function(table_name, id){

    // Service Params
    var service_params = {
      'TableName'    : table_name,   // DynamoDB Table from which record is to be deleted
      'Key'          : id            // ID of record to be deleted
    };


    // Return Service-Params
    return service_params;

  },


  /********************************************************************
  Delete entry from No-sql database (Using Pre-Built Command Object)

  @param {reference} instance - Request Instance object reference
  @param {Function} cb - Callback function to be invoked once async execution of this function is finished

  @param {Set} service_params - Command-Obj for record to be removed

  @return Thru request Callback.

  @callback - Request Callback(err, is_success)
  * @callback {Error} err - In case of error
  * @callback {Boolean} is_success - true on successful add-record
  * @callback {Boolean} is_success - false on unsuccessful add-record or error
  *********************************************************************/
  commandDeleteRecord: function(instance, cb, service_params){

    // Initialize AWS service Object if not already Initialized
    _NoDB.initIfNot(instance);


    // Delete entry from DynamoDB
    Lib.Debug.timingAuditLog('Start', 'AWS DynamoDB - Delete Record', instance['time_ms']);
    instance.aws.nodb.dynamodb.send( new DeleteCommand(service_params) )

      .then(function(response){
        Lib.Debug.timingAuditLog('End', 'AWS DynamoDB - Delete Record', instance['time_ms']);

        // Record Successfully added
        cb(null, true);

      })

      .catch(function(err){

        // Log error for research
        Lib.Debug.logErrorForResearch(
          err,
          'Cause: AWS DynamoDB' +
          '\ncmd: Delete Record' +
          '\nparams: ' + JSON.stringify(service_params)
        );

        // Invoke Callback and forward error from dynamodb
        return cb(err);

      });

  },


  /********************************************************************
  Delete entry from No-sql database

  @param {reference} instance - Request Instance object reference
  @param {Function} cb - Callback function to be invoked once async execution of this function is finished

  @param {String} table_name - Table in which new entry is to be removed
  @param {Set} id - Partition key + Sort key of record to be deleted

  @return Thru request Callback.

  @callback - Request Callback(err, is_success)
  * @callback {Error} err - In case of error
  * @callback {Boolean} is_success - true on successful add-record
  * @callback {Boolean} is_success - false on unsuccessful add-record or error
  *********************************************************************/
  deleteRecord: function(instance, cb, table_name, id){

    // Service Params
    var service_params = NoDB.commandBuilderForDeleteRecord(
      table_name,
      id
    );


    // Delete entry from DynamoDB
    NoDB.commandDeleteRecord(instance, cb, service_params);

  },


  /********************************************************************
  Delete multiple entries from No-sql database (AWS Limit - Maximum 25 items)
  DynamoDB currently retains up five minutes (300 seconds) of unused read and write capacity for burst

  @param {reference} instance - Request Instance object reference
  @param {Function} cb - Callback function to be invoked once async execution of this function is finished

  @param {String[]} table_names - Tables from which records are to be deleted
  @param {Set[]} data_items - JSON entries to be deleted

  @return Thru request Callback.

  @callback - Request Callback(err, is_success)
  * @callback {Error} err - In case of error
  * @callback {Boolean} is_success - true on successful delete-record
  * @callback {Boolean} is_success - false on unsuccessful delete-record or error
  *********************************************************************/
  deleteBatchRecords: function(instance, cb, table_names, data_items){

    // ~Sample Input~
    // table_name = [ table1, table2, table3 ]
    // data_items = [ [{table1_data1},{table1_data2}], [{table2_data1}], [{table3_data1},{table3_data2}] ]


    // Initialize AWS service Object if not already Initialized
    _NoDB.initIfNot(instance);


    // Counter to work-around WS limit of 25 keys at a time
    var available_limit = 25;
    var remaining_table_names = []; // Tables that couldn't be processed in this query
    var remaining_data_items = []; // IDs that couldn't be processed in this query


    // Service Params
    var service_params = {
      'RequestItems': {
        // Nested Items
      },
      'ReturnConsumedCapacity': "TOTAL"
    };


    // Loop and add multiple requets to service-params
    table_names.forEach(function(table_name, index){

      if( available_limit > 0 ){

        // Copy all the keys for respective table, if number of keys is within available limit
        if( data_items[index].length <= available_limit ){
          service_params['RequestItems'][table_name] = [];

          data_items[index].forEach(function(data_item){ // Copy all keys to this AWS Service Request
            service_params['RequestItems'][table_name].push( {'DeleteRequest':{'Key':data_item}} );
          });

          available_limit = available_limit - data_items[index].length; // Reduce Available Limit
        }
        else{
          service_params['RequestItems'][table_name] = [];

          data_items[index].slice(0, available_limit).forEach(function(data_item){ // Copy limited keys to this AWS Service Request
            service_params['RequestItems'][table_name].push( {'DeleteRequest':{'Key':data_item}} );
          });


          // Move rest of keys to remaing-data
          remaining_table_names.push(table_name);
          remaining_data_items.push( data_items[index].slice(available_limit, data_items[index].length) );

          // Reached Available Limit
          available_limit = 0; // Reached Available Limit
        }

      }
      else{

        // Move rest of keys to remaing-data
        remaining_table_names.push(table_name);
        remaining_data_items.push(data_items[index]);

      }

    });


    // Add entry in DynamoDB
    Lib.Debug.timingAuditLog('Start', 'AWS DynamoDB - Delete Batch Record', instance['time_ms']);
    instance.aws.nodb.dynamodb.send( new BatchWriteCommand(service_params) )

      .then(function(response){
        Lib.Debug.timingAuditLog('End', 'AWS DynamoDB - Delete Batch Record', instance['time_ms']);

        // If more records remaining because of AWS limit reached, recursive call to same function with remaining keys
        if( remaining_table_names.length > 0 ){
          NoDB.deleteBatchRecords(instance, cb, remaining_table_names, remaining_data_items);
        }
        else{
          // All good. All Records Successfully deleted
          cb(null, true);
        }
      })

      .catch(function(err){

        // Log error for research
        Lib.Debug.logErrorForResearch(
          err,
          'Cause: AWS DynamoDB' +
          '\ncmd: Delete Batch Record' +
          '\nparams: ' + JSON.stringify(service_params)
        );

        // Invoke Callback and forward error from dynamodb
        return cb(err);

      });

  },


  /********************************************************************
  Service-Param Builder for 'Update'
  Ref: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html

  @param {String} table_name - Table in which new entry is to be created
  @param {Set} id - Partition key + Sort key of record to be modified
  @param {Set} [update_data] - (Optional) Data to be updated against above id
  @param {String[]} [remove_keys] - (Optional) List of Keys to be removed
  @param {Set} [increment] - (Optional) List of Keys whose value is to increased
  * @param {String} [key] - Key-Name whose value is to be incremented
  * @param {Number} [value] - Number to be added to original Key-Value
  @param {Set} [decrement] - (Optional) List of Keys whose value is to decreased
  * @param {String} [key] - Key-Name whose value is to be decremented
  * @param {Number} [value] - Number to be added to original Key-Value
  @param {String} [return_state] - (Optional) Retrieve which form of updated object. Enum ( 'ALL_NEW' | 'ALL_OLD' | 'UPDATED_NEW' | 'UPDATED_OLD' | 'NONE' - Default )

  @return {set} service_params - Service Params for AWS DynamoDB 'update' API
  *********************************************************************/
  commandBuilderForUpdateRecord: function(
    table_name, id,
    update_data, remove_keys,
    increment, decrement,
    return_state
  ){

    // Construct DynamoDB update-expression & values-map
    var update_expression = ''; // "SET info.rating = :r, info.plot = :p, info.actors = :a REMOVE key6, key8"
    var update_expression_items = []; // "info.rating = :r, info.plot = :p, info.actors = :a"
    var inserts = {}; // {":r":5.5, ":p":"whatever",":a":["Larry", "Moe", "Curly"]}
    var count = 0;


    // Handle Keys whose values are to be 'Updated'/'Incremented'/'Decremented'

    // Iterate all keys to be 'Updated'
    for( let key in update_data ){
      count++;
      update_expression_items.push( key + ` = :` + count ); // "key_name = :1"
      inserts[`:` + count] = update_data[key]; // inserts{ ":1" : "key_value" }
    };

    // Iterate all keys to be 'Incremented'
    for( let key in increment ){
      count++;
      update_expression_items.push( `${key} = ${key} + :${count}` ); // "key_name = key_name + :1"
      inserts[`:` + count] = increment[key]; // inserts{ ":1" : "key_value" }
    };

    // Iterate all keys to be 'Decremented'
    for( let key in decrement ){
      count++;
      update_expression_items.push( `${key} = ${key} - :${count}` ); // "key_name = key_name - :1"
      inserts[`:` + count] = decrement[key]; // inserts{ ":1" : "key_value" }
    };


    // Deflate update_expression_items into string
    if( !Lib.Utils.isEmpty(update_expression_items) ){ // Only if not empty List
      update_expression += ` SET `;
      update_expression += update_expression_items.toString(); // Convert array into 'comma' seperated string
    }


    // Handle Keys to 'Removed'

    // Deflate remove_keys into string
    if( !Lib.Utils.isEmpty(remove_keys) ){ // Only if not empty Array
      update_expression += ` REMOVE `;
      update_expression += remove_keys.toString(); // Convert array into 'comma' seperated string
    }


    // Initialize Service Params
    var service_params = {
      'TableName': table_name,
      'Key': id, // ID
      'UpdateExpression': update_expression, // 'set' statement
      'ExpressionAttributeValues': inserts, // values to be inserted
      'ReturnValues': 'NONE' // Default - None
    };

    // Response Data Feilds
    if( !Lib.Utils.isNullOrUndefined(return_state) ){
      service_params['ReturnValues'] = return_state
    }


    // Return Service-Params
    return service_params;

  },


  /********************************************************************
  Update an existing entry in No-sql database (Using Pre-Built Command Object)
  Ref: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html

  @param {reference} instance - Request Instance object reference
  @param {Function} cb - Callback function to be invoked once async execution of this function is finished

  @param {Set} service_params - Command-Obj for record to be updated

  @return Thru request Callback.

  @callback - Request Callback(err, is_success)
  * @callback {Error} err - In case of error
  * @callback {Set} response - Updated record json on success
  *********************************************************************/
  commandUpdateRecord: function(instance, cb, service_params){

    // Initialize AWS service Object if not already Initialized
    _NoDB.initIfNot(instance);


    // Update entry in DynamoDB
    Lib.Debug.timingAuditLog('Start', 'AWS DynamoDB - Update Record', instance['time_ms']);
    instance.aws.nodb.dynamodb.send( new UpdateCommand(service_params) )

      .then(function(response){
        Lib.Debug.timingAuditLog('End', 'AWS DynamoDB - Update Record', instance['time_ms']);
        //Lib.Debug.log("update succeeded with response: " +  JSON.stringify(response) );

        // Build Response
        var ret = {}; // Default as Empty-Object
        if(
          !Lib.Utils.isEmpty(response) && // Not an empty response
          'Attributes' in response
        ){
          ret = response['Attributes'];
        }

        // Return
        cb(null, ret); // Record Successfully updated

      })

      .catch(function(err){

        // Log error for research
        Lib.Debug.logErrorForResearch(
          err,
          'Cause: AWS DynamoDB' +
          '\ncmd: Update Record' +
          '\nparams: ' + JSON.stringify(service_params)
        );

        // Invoke Callback and forward error from dynamodb
        return cb(err);

      });

  },


  /********************************************************************
  Update an existing entry in No-sql database
  Ref: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html

  @param {reference} instance - Request Instance object reference
  @param {Function} cb - Callback function to be invoked once async execution of this function is finished

  @param {String} table_name - Table in which new entry is to be created
  @param {Set} id - Partition key + Sort key of record to be modified
  @param {Set} [update_data] - (Optional) Data to be updated against above id
  @param {String[]} [remove_keys] - (Optional) List of Keys to be removed
  @param {Set} [increment] - (Optional) List of Keys whose value is to increased
  * @param {String} [key] - Key-Name whose value is to be incremented
  * @param {Number} [value] - Number to be added to original Key-Value
  @param {Set} [decrement] - (Optional) List of Keys whose value is to decreased
  * @param {String} [key] - Key-Name whose value is to be decremented
  * @param {Number} [value] - Number to be added to original Key-Value
  @param {String} [return_state] - (Optional) Retrieve which form of updated object. Enum ( 'ALL_NEW' | 'ALL_OLD' | 'UPDATED_NEW' | 'UPDATED_OLD' | 'NONE' - Default )

  @return Thru request Callback.

  @callback - Request Callback(err, is_success)
  * @callback {Error} err - In case of error
  * @callback {Set} response - Updated record json on success
  *********************************************************************/
  updateRecord: function(
    instance, cb,
    table_name, id,
    update_data, remove_keys,
    increment, decrement,
    return_state
  ){

    // Service Params
    var service_params = NoDB.commandBuilderForUpdateRecord(
      table_name, id,
      update_data, remove_keys,
      increment, decrement,
      return_state
    );


    // Update entry in DynamoDB
    NoDB.commandUpdateRecord(instance, cb, service_params);

  },


  /********************************************************************
  No-sql get Single record for particular Primary key

  @param {reference} instance - Request Instance object reference
  @param {Function} cb - Callback function to be invoked once async execution of this function is finished

  @param {String} table_name - Table from wich record is to be fetched
  @param {Set} id - Partition key + Sort key of record to be fetched

  @return Thru request Callback.

  @callback - Request Callback(err, response)
  * @callback {Error} err - In case of error
  * @callback {Boolean} response - record json on success
  * @callback {Boolean} response - false if record not found or error
  *********************************************************************/
  getRecord: function(instance, cb, table_name, id){

    // Initialize AWS service Object if not already Initialized
    _NoDB.initIfNot(instance);


    // Service Params
    var service_params = {
      'TableName'  : table_name,  // DynamoDB Table to which new item is to be saved
      'Key'        : id           // File name
    };


    // Get record from DynamoDB
    Lib.Debug.timingAuditLog('Start', 'AWS DynamoDB - Get Record', instance['time_ms']);
    instance.aws.nodb.dynamodb.send( new GetCommand(service_params) )

      .then(function(response){
        Lib.Debug.timingAuditLog('End', 'AWS DynamoDB - Get Record', instance['time_ms']);

        // If 'undefined' is retured, means record not found. Return false.
        if( Lib.Utils.isNullOrUndefined(response) ){
          return cb(null, false); // Record not found. Return false.
        }


        // If 'Item' key is not found in respone, means record not found. Return false
        if( !('Item' in response) ){
          return cb(null, false); // Record not found. Return false.
        }


        // All good. Record Found.
        cb(null, response.Item); // Return record data

      })

      .catch(function(err){

        // Log error for research
        Lib.Debug.logErrorForResearch(
          err,
          'Cause: AWS DynamoDB' +
          '\ncmd: Get Record' +
          '\nparams: ' + JSON.stringify(service_params)
        );

        // Invoke Callback and forward error from dynamodb
        return cb(err);

      });

  },


  /********************************************************************
  No-sql get Single record for particular Primary key (AWS limit - Maximum 100 items)
  DynamoDB currently retains up five minutes (300 seconds) of unused read and write capacity for burst

  @param {reference} instance - Request Instance object reference
  @param {Function} cb - Callback function to be invoked once async execution of this function is finished

  @param {Array} table_names - Array of Tables in which new entry is to be created
  @param {Array} ids - Array of Set of Partition key + Sort key of records to be fetched
  @param {Set} [result_chain] - (Optional) To overcome AWS limits, recursive call to this function with carry over results

  @return Thru request Callback.

  @callback - Request Callback(err, response)
  * @callback {Error} err - In case of error
  * @callback {Boolean} response - record json on success
  *********************************************************************/
  getBatchRecords: function(instance, cb, table_names, ids, result_chain){

    // ~Sample Input~
    // table_name = [ table1, table2, table3 ]
    // data_items = [ [{table1_id1},{table1_id2}], [{table2_id1}], [{table3_bad_id1},{table3_bad_id2}] ]

    // ~Sample Output~
    // {table1: [result_for_id1, result_for_id2], table2: [result_for_id1], table3: [] }


    // Initialize AWS service Object if not already Initialized
    _NoDB.initIfNot(instance);


    // Counter to work-around WS limit of 100 keys at a time
    var available_limit = 100;
    var remaining_table_names = []; // Tables that couldn't be processed in this query
    var remaining_ids = []; // IDs that couldn't be processed in this query


    // Service Params
    var service_params = {
      'RequestItems': {
        // Nested Items
      },
      'ReturnConsumedCapacity': "TOTAL"
    };


    // Loop and add multiple requets to service-params
    table_names.forEach(function(table_name, index){

      if( available_limit > 0 ){

        // Copy all the keys for respective table, if number of keys is within available limit
        if( ids[index].length <= available_limit ){
          service_params['RequestItems'][table_name] = {'Keys' : ids[index]}; // Copy all keys to this AWS Service Request
          available_limit = available_limit - ids[index].length; // Reduce Available Limit
        }
        else{
          service_params['RequestItems'][table_name] = {'Keys' : ids[index].slice(0, available_limit)}; // Copy limited keys to this AWS Service Request

          // Move rest of keys to remaing-data
          remaining_table_names.push(table_names);
          remaining_ids.push( ids[index].slice(available_limit, ids[index].length) );

          // Reached Available Limit
          available_limit = 0; // Reached Available Limit
        }

      }
      else{

        // Move rest of keys to remaing-data
        remaining_table_names.push(table_name);
        remaining_ids.push(ids[index]);

      }

    });


    // Get multiple records from DynamoDB
    Lib.Debug.timingAuditLog('Start', 'AWS DynamoDB - Get Batch Record', instance['time_ms']);
    instance.aws.nodb.dynamodb.send( new BatchGetCommand(service_params) )

      .then(function(response){
        Lib.Debug.timingAuditLog('End', 'AWS DynamoDB - Get Batch Record', instance['time_ms']);
        //Lib.Debug.log("AWS response: " +  JSON.stringify(response) );

        // Consolidate results from recursive chain
        if( Lib.Utils.isNullOrUndefined(result_chain) ){ // No chain of records
          result_chain = {};
        }


        // Loop and extract response for each table sent in params
        for( let table_name in response["Responses"] ){

          if( !(table_name in result_chain) ){
            result_chain[table_name] = response["Responses"][table_name];
          }
          else{
            result_chain[table_name] = result_chain[table_name].concat( response["Responses"][table_name] ); // Concat results for same table
          }

        }


        // If more records remaining because of AWS limit reached, recursive call to same function with remaining keys
        if( remaining_table_names.length > 0 ){
          NoDB.getBatchRecords(instance, cb, remaining_table_names, remaining_ids, result_chain)
        }
        else{
          // All good. Record Found.
          cb(null, result_chain); // Return record data
        }

      })

      .catch(function(err){

        // Log error for research
        Lib.Debug.logErrorForResearch(
          err,
          'Cause: AWS DynamoDB' +
          '\ncmd: Get Batch Record' +
          '\nparams: ' + JSON.stringify(service_params)
        );

        // Invoke Callback and forward error from dynamodb
        return cb(err);

      });

  },


  /********************************************************************
  No-sql get Multiple records for known partition key (Max 100 Records or 1MB of Data)
  (Only Costs 0.5 read unit for fetching 100 Records or 1MB of Data)
  (Ex Src: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.NodeJs.04.html)

  @param {reference} instance - Request Instance object reference
  @param {Function} cb - Callback function to be invoked once async execution of this function is finished

  @param {String} table_name - Table in which new entry is to be created
  @param {String} [secondary_index] - If global secondary-index, then index-id. null for normal table
  @param {String} id_key - Partition-key name of records to be fetched
  @param {String} id_value - Partition-key value of records to be fetched
  @param {String} [fields_list] - (Optional) Retrieve only particular feilds value. send null to get all feilds
  @param {String} [paging] - (Optional) Start and Limit for pagination
  * @param {String} [paging.start] - (Optional) Starting Key - Exclusive (Send undefined in case no starting pointer)
  * @param {String} [paging.limit] - Number of records to be fetched
  @param {String} [condition] - (Optional) Additional condition on sort key
  * @param {String} [condition.operator] - (Optional) Special comparison operator '=' | '<=' | '<' | '>=' | '>' | 'begins_with' | 'between'
  * @param {String} condition.key - sort key on which comparison is to be done
  * @param {Integer|String} condition.value - sort key value for comparison
  * @param {Integer|String} condition.value2 - sort key second value for comparison (Only in case of 'between' operator) (including this value if exact-match, else excluding this value if string starts-with)
  * @param {Boolean} condition.asc - (Optional) True means get results in Ascending order. Default in Decending order
  @param {String} [select] - (Optional) 'Select' rule (Enum) (ALL_ATTRIBUTES | ALL_PROJECTED_ATTRIBUTES | SPECIFIC_ATTRIBUTES | COUNT)
  @param {Set} [result_chain] - (Optional) To overcome AWS limits, recursive call to this function with carry over results

  @return Thru request Callback.

  @callback - Request Callback(err, response, count, last_evaluated_key)
  * @callback {Error} err - In case of error
  * @callback {Boolean} response - array of records on success
  * @callback {Boolean} response - false if record not found or error
  * @callback {Integer} count - Number of records found
  * @callback {Set} last_evaluated_key - Last evaluaed key in case more records exist on next page
  *********************************************************************/
  queryRecords: function(
    instance, cb,
    table_name, secondary_index, id_key, id_value,
    fields_list, paging, condition, select,
    result_chain
  ){

    // Initialize AWS service Object if not already Initialized
    _NoDB.initIfNot(instance);


    // Service Params
    var service_params = {
      'TableName': table_name, // DynamoDB Table to which new item is to be saved
      'KeyConditionExpression': `${id_key} = :1`, // "key_name = :1" // Variable for key-name
      'ExpressionAttributeValues': { ':1': id_value },
      'ScanIndexForward': false, // Fetch records in decending order of sort-key (Default)
      'ReturnConsumedCapacity': 'TOTAL'
    };

    // If Seconday Index
    if( !Lib.Utils.isNullOrUndefined(secondary_index) ){ // Optional
      service_params['IndexName'] = secondary_index;
    }

    // If only particular feilds value is to be fetched
    if( !Lib.Utils.isNullOrUndefined(fields_list) ){ // Optional
      service_params['ProjectionExpression'] = fields_list.join(', ');
    }

    // If sort-key condition is sent
    if(
      !Lib.Utils.isNullOrUndefined(condition) && // Optional
      !Lib.Utils.isNullOrUndefined(condition['value']) // For comparision
    ){

      service_params['ExpressionAttributeValues'][':2'] = condition['value']; // Variable for 1st value

      // Different operators have different type of syntax
      if(condition.operator === 'begins_with'){ // Construct for 'begins_with'
        service_params['KeyConditionExpression'] += ` and ${condition.operator}(${condition.key}, :2)`; // Id = :id and begins_with(key, :value)
      }
      else if( condition.operator === 'between' ){ // Construct for 'between'
        service_params['ExpressionAttributeValues'][':3'] = condition['value2']; // Variable for 2nd Value
        service_params['KeyConditionExpression'] += ` and ${condition.key} BETWEEN :2 AND :3`; // Id = :id and key BETWEEN :value1 AND :value2
      }
      else if( condition.operator ){ // Operator is sent in param
        service_params['KeyConditionExpression'] += ` and ${condition.key} ${condition.operator} :2`; // Id = :id and key OPERATOR :value
      }
      else{ // Default as '='
        service_params['KeyConditionExpression'] += ` and ${condition.key} = :2`; // Id = :id and key = :value
      }

    }

    // If Results are to be sorted in Ascending Order
    if(
      !Lib.Utils.isNullOrUndefined(condition) && // Optional
      condition['asc'] // True
    ){
      service_params['ScanIndexForward'] = true;
    }


    // If Select is sent
    if( !Lib.Utils.isNullOrUndefined(select) ){ // Optional
      service_params['Select'] = select;
    }


    // Cleanup Paging Data if both Start and Limit is set but as NULL
    if(
      !Lib.Utils.isNullOrUndefined(paging) &&
      Lib.Utils.isNullOrUndefined(paging['start']) &&
      Lib.Utils.isNullOrUndefined(paging['limit'])
    ){
      paging = null;
    }


    // Manual Paging
    if( !Lib.Utils.isNullOrUndefined(paging) ){ // Optional

      if( !Lib.Utils.isNullOrUndefined(paging['start']) ){ // Optional Start
        service_params['ExclusiveStartKey'] = paging['start'];
      }

      if( !Lib.Utils.isNullOrUndefined(paging['limit']) ){ // optional Limit on number of records
        service_params['Limit'] = paging['limit'];
      }

    }

    // result_chain Paging
    else if(
      !Lib.Utils.isNullOrUndefined(result_chain) &&
      !Lib.Utils.isNullOrUndefined(result_chain.LastEvaluatedKey)
    ){
      service_params['ExclusiveStartKey'] = result_chain.LastEvaluatedKey;
    }


    // Query DynamoDB
    Lib.Debug.timingAuditLog('Start', 'AWS DynamoDB - Query', instance['time_ms']);
    instance.aws.nodb.dynamodb.send( new QueryCommand(service_params) )

      .then(function(response){
        Lib.Debug.timingAuditLog('End', 'AWS DynamoDB - Query', instance['time_ms']);

        // Initialize result-chain as query-response if there is no previous data in result-chain
        if( Lib.Utils.isNullOrUndefined(result_chain) ){
          result_chain = response;
        }

        // Else Merge query-response to result-chain
        else{
          result_chain = {
            'Items': result_chain['Items'] ? result_chain['Items'].concat(response.Items) : false,
            'Count': result_chain['Count'] + response.Count,
            'LastEvaluatedKey': response.LastEvaluatedKey
          }
        }


        // If no records found. Return false.
        if(
          Lib.Utils.isNullOrUndefined(result_chain) || // If 'undefined' is retured, means record not found
          result_chain.Count == 0 // If 0 records found. Return false.
        ){
          return cb(null, false, 0, null); // Record not found. Return false.
        }


        // If LastEvaluatedKey is present, means more records on next page
        var last_evaluated_key = ( !Lib.Utils.isUndefined(result_chain.LastEvaluatedKey) ? result_chain.LastEvaluatedKey : null); // Set as null in-case it's undefined


        // Auto fetch all data recursively if user is not manually managing pagination and there is more data beyond AWS 1MB limit
        if(
          Lib.Utils.isNullOrUndefined(paging) && // Manual paging data is not set
          !Lib.Utils.isNullOrUndefined(last_evaluated_key) // There is a data on next page (LastEvaluatedKey is only present when there is data on next page)
        ){
          NoDB.queryRecords(
            instance, cb,
            table_name, secondary_index, id_key, id_value,
            fields_list, paging, condition, select,
            result_chain
          );
        }

        // All good. Record Found. Return to callback
        else{
          cb(null, result_chain.Items, result_chain.Count, last_evaluated_key); // Return records and count
        }

      })

      .catch(function(err){

        // Log error for research
        Lib.Debug.logErrorForResearch(
          err,
          'Cause: AWS DynamoDB' +
          '\ncmd: Query Records' +
          '\nparams: ' + JSON.stringify(service_params)
        );

        // Invoke Callback and forward error from dynamodb
        return cb(err);

      });

  },


  /********************************************************************
  No-sql get count of records using Query
  (Only Costs 0.5 read unit for Counting 100 Records or 1MB of Data)

  @param {reference} instance - Request Instance object reference
  @param {Function} cb - Callback function to be invoked once async execution of this function is finished

  @param {String} table_name - Table in which new entry is to be created
  @param {String} [secondary_index] - If global secondary-index, then index-id. null for normal table
  @param {String} id_key - Partition-key name of records to be fetched
  @param {String} id_value - Partition-key value of records to be fetched
  @param {String} [condition] - (Optional) Additional condition on sort key
  * @param {String} [condition.operator] - (Optional) Special comparison operator = | le | lt | ge | gt | begins_with | between
  * @param {String} condition.key - sort key on which comparison is to be done
  * @param {String} condition.value - sort key value for comparison

  @return Thru request Callback.

  @callback - Request Callback(err, count)
  * @callback {Error} err - In case of error
  * @callback {Integer} count - Number of records found
  *********************************************************************/
  count: function(
    instance, cb,
    table_name, secondary_index, id_key, id_value,
    condition
  ){

    // Get Count of Records
    NoDB.queryRecords(
      instance,
      function(err, response, count){
        cb(err, count); // Forward Response
      },
      table_name,
      secondary_index,
      id_key,
      id_value,
      null, // Counting doesn't need feilds-list
      null, // Counting doesn't need paging
      condition,
      'COUNT'
    )

  },


  /********************************************************************
  TODO: Batch write operation that groups up to any number requests (Non-Atomically)

  @param {reference} instance - Request Instance object reference
  @param {Function} cb - Callback function to be invoked once async execution of this function is finished

  @param {Set[]} [add_records] - (Optional) Command-Objs for records to be added
  @param {Set[]} [update_records] - (Optional) Command-Objs of records to be updated
  @param {Set[]} [delete_records] - (Optional) Command-Objs of records to be deleted

  @return Thru request Callback.

  @callback - Request Callback(err, is_success)
  * @callback {Error} err - In case of error
  * @callback {Boolean} is_success - true on successful transaction
  * @callback {Boolean} is_success - false on unsuccessful transaction or error
  *********************************************************************/
  batchWrite: function(
    instance, cb,
    add_records, update_records, delete_records
  ){

    // TODO

  },


  /********************************************************************
  Synchronous write operation that groups up to 10 action requests (Atomically)

  @param {reference} instance - Request Instance object reference
  @param {Function} cb - Callback function to be invoked once async execution of this function is finished

  @param {Set[]} [check_records] - (Optional) Command-Objs for records to be checked (TODO)
  @param {Set[]} [add_records] - (Optional) Command-Objs for records to be added
  @param {Set[]} [update_records] - (Optional) Command-Objs of records to be updated
  @param {Set[]} [delete_records] - (Optional) Command-Objs of records to be deleted

  @return Thru request Callback.

  @callback - Request Callback(err, is_success)
  * @callback {Error} err - In case of error
  * @callback {Boolean} is_success - true on successful transaction
  * @callback {Boolean} is_success - false on unsuccessful transaction or error
  *********************************************************************/
  transactWrite: async function(
    instance, cb,
    check_records, add_records, update_records, delete_records
  ){

    // Initialize AWS service Object if not already Initialized
    _NoDB.initIfNot(instance);

    // Initialize list of Transaction-Items
    var transact_items = [];

    // Insert 'AddRecord' commands
    if(!Lib.Utils.isEmpty(add_records)){
      add_records.forEach(function(command){
        transact_items.push({ Put: command });
      });
    }

    // Insert 'UpdateRecord' commands
    if(!Lib.Utils.isEmpty(update_records)){
      update_records.forEach(function(command){
        transact_items.push({ Update: command });
      });
    }

    // Insert 'DeleteRecord' commands
    if(!Lib.Utils.isEmpty(delete_records)){
      delete_records.forEach(function(command){
        transact_items.push({ Delete: command });
      });
    }


    // Service Params
    var params = { TransactItems: transact_items };

    // Do Transaction in DynamoDB
    Lib.Debug.timingAuditLog('Start', 'AWS DynamoDB - Transact Write', instance['time_ms']);

    instance.aws.nodb.dynamodb.send( new TransactWriteCommand(params) )

      .then(function(response){
        Lib.Debug.timingAuditLog('End', 'AWS DynamoDB - Transact Write', instance['time_ms']);

        // Transaction Successful
        cb(null, true);

      })

      .catch(function(err){

        // Log error for research
        Lib.Debug.logErrorForResearch(
          err,
          'Cause: AWS DynamoDB' +
          '\ncmd: Transact Write' +
          '\nparams: ' + JSON.stringify(params)
        );

        // Invoke Callback and forward error from dynamodb
        cb(err);

      });

  },

};///////////////////////////Public Functions END//////////////////////////////



//////////////////////////Private Functions START//////////////////////////////
const _NoDB = { // Private functions accessible within this modules only

  /********************************************************************
  Initialize AWS S3 Service Object - Only if not already initialized

  @param {reference} instance - Request Instance object reference

  @return - None
  *********************************************************************/
  initIfNot: function(instance){

    // Create 'aws' object in instance if it's not already present
    if( !('aws' in instance) ){
      instance['aws'] = {};
    }


    // Create 'nodb' object in instance.aws if it's not already present
    if( !('nodb' in instance.aws) ){
      instance.aws['nodb'] = {};
    }


    // Initialize only if 'dynamodb' object is not already Initialized
    if( !Lib.Utils.isNullOrUndefined(instance.aws.nodb.dynamodb) ){
      return; // Do not proceed since already initalized
    }


    // Reach here means DynamoDB service is not initalized and initialize it now
    Lib.Debug.timingAuditLog('Init-Start', 'AWS DynamoDB Connection (nodb)', instance['time_ms']);

    // Import AWS DynamoDB SDK
    const { DynamoDBClient } = require('@aws-sdk/client-dynamodb'); // AWS SDK - Dynamodb Client
    ({
      DynamoDBDocumentClient,
      GetCommand, PutCommand, DeleteCommand, UpdateCommand, QueryCommand,
      BatchGetCommand, BatchWriteCommand, TransactWriteCommand
    } = require('@aws-sdk/lib-dynamodb')); // AWS SDK - Dynamodb Document Services

    // Initialize dynamodb client
    const dynamodb_client = new DynamoDBClient({
      region: CONFIG.REGION,
      credentials: {
        accessKeyId: CONFIG.KEY,
        secretAccessKey: CONFIG.SECRET,
      },
      maxAttempts: CONFIG.MAX_RETRIES,
      // HTTP options such as 'timeout' aren't directly available in v3
    });

    // Initialize DynamoDb Options
    const dynamodb_options = {
      marshallOptions: {
        removeUndefinedValues: true
      }
    };

    // Initialize dynamodb document client
    instance.aws.nodb.dynamodb = DynamoDBDocumentClient.from(dynamodb_client, dynamodb_options);

    Lib.Debug.timingAuditLog('Init-End', 'AWS DynamoDB Connection (nodb)', instance['time_ms']);

  },

};//////////////////////////Private Functions END//////////////////////////////
