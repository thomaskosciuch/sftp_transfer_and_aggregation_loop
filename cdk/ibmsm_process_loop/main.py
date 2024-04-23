import json

def handler(event, context):
    sns_message = event['Records'][0]['Sns']['Message']
    parsed_message = json.loads(sns_message)
    message_content = parsed_message['default']['message']  # Assuming the message structure is 'default'
    print(f"Received message from SNS: {message_content}")
