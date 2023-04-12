# from gevent import monkey;
#
# monkey.patch_all()
from flask import Flask, Response, render_template, signals, request, abort, jsonify
from flask_cors import CORS, cross_origin
from Broadcaster import Broadcaster
import boto3, os, time, json
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)
cors = CORS(app)
broadcaster = Broadcaster()


# signal = signals.Namespace()

def getClient(service):
    return boto3.client(
        service,
        region_name=os.getenv('REGION_NAME'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        aws_session_token=os.getenv('AWS_SESSION_TOKEN')
    )


@app.route('/')
def sse_test():  # put application's code here
    return render_template("index.html")


@app.route('/signup', methods=['POST'])
def signUp():
    data = request.get_json()

    cognito = getClient('cognito-idp')

    try:
        response = cognito.sign_up(
            ClientId="3ggsbp2l6ttvjv95rssgceikup",
            Username=data['username'],
            Password=data['password'],
            UserAttributes=[
                {
                    'Name': 'email',
                    'Value': data['email']
                },
            ]
        )
        return response
    except Exception as e:
        # return json.dumps('{"error": "User can not be created"}')
        abort(400, str(e))


@app.route('/verifyemail', methods=['POST'])
def verifyEmail():
    data = request.get_json()
    cognito = getClient('cognito-idp')

    try:
        response = cognito.confirm_sign_up(
            ClientId='3ggsbp2l6ttvjv95rssgceikup',
            Username=data['username'],
            ConfirmationCode=data['code'])
        return response
    except Exception as e:
        abort(400, str(e))


@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    print(os.getenv('AWS_ACCESS_KEY_ID'))
    cognito = getClient('cognito-idp')

    try:
        response = cognito.initiate_auth(
            ClientId="3ggsbp2l6ttvjv95rssgceikup",
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={
                'USERNAME': data['email'],
                'PASSWORD': data['password']
            }
        )
        return response
    except:
        return json.dumps('{"error": "User invalid"}')


@app.route('/createGame', methods=['POST'])
@cross_origin()
def createGame():
    data = request.get_json()
    # print(data)
    db = getClient('dynamodb')

    tableName = 'UserGame'

    # print(data)
    tableData = {
        "username": {"S": data['username']},
        "pin": {"S": data['pin']}
    }

    try:
        response = db.put_item(TableName=tableName, Item=tableData)
        if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
            questionData = {
                "pin": {"S": data['pin']},
                "questions": {"S": json.dumps(data['questions'])},
                "status": {'S': 'Created'}
            }
            try:
                response = db.put_item(TableName="Questions", Item=questionData)
            except Exception as e:
                abort(400, str(e))
    except Exception as e:
        abort(400, str(e))

    return "Data received"


@app.route('/startgame', methods=['POST'])
@cross_origin()
def startGame():
    data = request.get_json()

    sqs = boto3.client('sqs', region_name='us-east-1')
    db = getClient('dynamodb')
    try:
        table = 'Questions'
        item = {
            'pin': {'S': data['pin']},
            'status': {'S': 'Live'}
        }
        response = db.update_item(
            TableName=table,
            Key={
                'pin': {'S': data['pin']}
            },
            AttributeUpdates={
                'status': {
                    'Value': {'S': 'Live'}
                }
            }
        )
        # print({'Questions_update': response})
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            table = 'Questions'
            response = db.get_item(
                TableName=table,
                Key={
                    'pin': {'S': data['pin']}
                }
            )
            print({'Questions_get': response})
            if 'Item' in response:
                item = response['Item']
                questions = item['questions']['S']
                print(questions)
                while(True):
                    resp = sqs.create_queue(QueueName=data['pin'])
                    if 'QueueUrl' in resp:
                        break
                print(resp)
                ques = json.loads(questions)
                for que in ques:
                    print(que)
                    response = sqs.send_message(QueueUrl=resp['QueueUrl'], MessageBody=json.dumps(que))
                db = getClient('dynamodb')
                db_resp = db.create_table(
                    TableName=data['pin'],
                    KeySchema=[
                        {
                            'AttributeName': 'username',
                            'KeyType': 'HASH'
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'username',
                            'AttributeType': 'S'
                        },
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
                return response
            else:
                return response
        else:
            return json.dumps('{"statuscode": ' + response['ResponseMetadata']['HTTPStatusCode'] + '}')

    except Exception as e:
        # print(e)
        abort(400, str(e))


@app.route('/getque', methods=['POST'])
@cross_origin()
def getQuestion():
    data = request.get_json()
    print(data)
    sqs = getClient('sqs')
    try:
        printqueUrl = 'https://sqs.us-east-1.amazonaws.com/387022035222/' + data['pin']
        response = sqs.receive_message(
            QueueUrl=printqueUrl,
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            MessageAttributeNames=['All'],
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )
        if(response['ResponseMetadata']['HTTPStatusCode'] == 200):

            time.sleep(1)
            print(response)
            receiptHandle = response['Messages'][0]['ReceiptHandle']
            sqs.delete_message(
                QueueUrl=printqueUrl,
                ReceiptHandle=receiptHandle
            )
            message = response['Messages'][0]['Body']
            # print(message)
            message_json = json.loads(message)
            # print(message_json)

            return message
        else:
            abort(400, 'data took too long to come.')
    except Exception as e:
        abort(400, str(e))

@app.route('/getNextQue', methods=['POST'])
def getNextQuestion():
    data = request.get_json()
    sqs = getClient('sqs')
    printqueUrl = 'https://sqs.us-east-1.amazonaws.com/387022035222/' + data['pin']
    try:
        response = sqs.receive_message(
            QueueUrl=printqueUrl,
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            MessageAttributeNames=['All'],
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )
        print(response)
        if 'Messages' in response:
            receiptHandle = response['Messages'][0]['ReceiptHandle']
            sqs.delete_message(
                QueueUrl=printqueUrl,
                ReceiptHandle=receiptHandle
            )
            message = response['Messages'][0]['Body']
            return message
        else:
            sqs.delete_queue(
                QueueUrl=printqueUrl
            )
            abort(400, "Quiz Completed")
    except:
        abort(400, "Quiz completed.")

@app.route('/getScore', methods=['POST'])
def getScore():
    data = request.get_json()
    db = getClient('dynamodb')
    table = data['pin']
    response = db.scan(TableName=table)
    items = response.get('Items', [])
    while 'LastEvaluatedKey' in response:
        response = db.scan(
            TableName=table,
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response['Items'])
    return jsonify(items)

@app.route('/ping', methods=['POST'])
def ping():
    data = request.get_json()
    msg = f'event:{data["event"]}\ndata: {data["data"]}\n\n'
    print(msg)
    broadcaster.broadcast(msg)
    return {}, 200


@app.route('/listen')
def listen():
    def stream():
        msgs = broadcaster.listen()
        while True:
            msg = msgs.get()
            yield msg

    return Response(stream(), mimetype='text/event-stream')


if __name__ == '__main__':
    app.run(threading=True, host='0.0.0.0')