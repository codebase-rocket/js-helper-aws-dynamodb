# NoSQL Library (For AWS DynamoDB)#

This library provides interface to AWS DynamoDB Services. Use configuration params to set `AWS Key`, `AWS Secret` and `AWS Region`


**************************************************


Private Library
---------------
To use this library in a project, setup your Github SSH in development machine.


Setup SSH for you github account
--------------------------------
https://help.github.com/en/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent


Test
----
* Create test IAM profile, IAM User, S3 bucket on AWS
* Change AWS `key` and `secret` in test file
* Install 'Test' project dependencies `$ npm install`
* Run 'test' script `$ node test.js`


**************************************************


Usage
-----
### Reference this library in your Project's package.json
```
"dependencies": {
  "js-helper-aws-dynamodb": "git+https://github.com/<git_org_name>/js-helper-aws-dynamodb.git"
}
```
### Reference this library in your Project's npm
```
"dependencies": {
  "js-helper-aws-dynamodb": "npm:@<git_org_name>/js-helper-aws-dynamodb@^1.0.0"
}
```


### Include this library in your Project
```javascript
const NoDB = require('js-helper-aws-dynamodb');
```


### Set or Override default configuration as per you development or production environment needs
```javascript
NoDB.config({
  'KEY': 'your-aws-key',
  'SECRET': 'your-aws-secret',
  'REGION': 'your-aws-region' // Default: US East (N. Virginia)
});
```


**************************************************
