------------
Create Table
------------
Test table for DynamoDB

* Table Name: test_table
* Partition Key: part_id [string]
* Sort Key: sort_id [string]

* Secondary Index: [NONE]
* Read/write capacity mode: On-demand



-----------------
Create IAM policy
-----------------
* Create Your Own Policy -> Select 'JSON'
* Name: `test-policy-dynamodb`
* Description: Test policy for dynamodb test table
* Policy Document:
```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowUserToAccessTestTable",
      "Effect": "Allow",
      "Action": [
        "dynamodb:BatchGetItem",
        "dynamodb:BatchWriteItem",
        "dynamodb:ConditionCheckItem",
        "dynamodb:PutItem",
        "dynamodb:DeleteItem",
        "dynamodb:GetItem",
        "dynamodb:Scan",
        "dynamodb:Query",
        "dynamodb:UpdateItem",
        "dynamodb:UpdateTable"
      ],
      "Resource": [
        "arn:aws:dynamodb:*:*:table/test_table",
        "arn:aws:dynamodb:*:*:table/test_table/*"
      ]
    }
  ]
}
```



---------------
Create IAM User
---------------
* Name: `test-user`
* Access type: Programmatic access
* Attach existing policies directly: `test-policy-dynamodb`
* Note down AWS Key and Secret
