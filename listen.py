import sseclient

messages = sseclient.SSEClient('http://localhost:5000/listen')

for msg in messages:
    print(msg)