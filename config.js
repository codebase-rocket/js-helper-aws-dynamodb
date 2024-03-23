// Info: Configuration file
'use strict';


// Export configration as key-value Map
module.exports = {

  // AWS Account Info
  KEY           : '', // AWS account Key
  SECRET        : '', // AWS account Secret
  REGION        : 'us-east-1', // Default: US East (N. Virginia)

  // Config for Outgoing HTTP requests to AWS server
  MAX_RETRIES   : 2,
  TIMEOUT       : 5000, // Request timeout in 5 seconds

}
