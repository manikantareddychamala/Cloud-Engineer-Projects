import json
import boto3
from datetime import datetime

dynamodb = boto3.client('dynamodb')

def Lambda_handler(event, context):
    for record in event['Records']:
        message = json.loads(record['body'])
        
        voucher_code = message.get('voucher_code')
        txn_type = message.get("txn_type")
        txn_date = message.get('txn_date')
        acc_no = message.get('acc_no')
        txn_amt = message.get('txn_amt')
        source_system_id = message.get('source_system_id')
        source_system_txn_id = message.get('source_system_txn_id')
        
        valid_c = (txn_date is not None) and (txn_amt is not None) and (txn_amt > 0)
        
        if not valid_c:
            invalid_item = {
                'voucher_code': {'S': voucher_code},
                'txn_type': {'S': txn_type},
                'txn_date': {'S': txn_date if txn_date else 'Invalid Date'},
                'acc_no': {'N': str(acc_no)},
                'txn_amt': {'N': str(txn_amt) if txn_amt else 'Invalid Amount'},
                'source_system_id': {'N': str(source_system_id)},
                'source_system_txn_id': {'S': source_system_txn_id},
            }
            dynamodb.put_item(TableName='team_invalid_trans_project3', Item=invalid_item)
            print(f"Invalid transaction values pushed to team_invalid_trans_project3: {invalid_item}")
            continue
        
        txn_id = int(datetime.now().timestamp() * 1000)
        item = {
            'txn_id': {'N': str(txn_id)},
            'voucher_code': {'S': voucher_code},
            'txn_type': {'S': txn_type},
            'txn_date': {'S': txn_date},
            'acc_no': {'N': str(acc_no)},
            'txn_amt': {'N': str(txn_amt)},
            'source_system_id': {'N': str(source_system_id)},
            'source_system_txn_id': {'S': source_system_txn_id}
        }
        dynamodb.put_item(TableName='ledger_txn_t6', Item=item)
        print(f"Items inserted to ledger_txn_t6 table: {item}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Transactions processed successfully')
    }