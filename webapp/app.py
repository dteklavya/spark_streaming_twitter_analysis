from flask import Flask, render_template, Response, request

import logging, redis
from logging.handlers import TimedRotatingFileHandler

app = Flask(__name__)
r = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)

import json
from flask import jsonify, Response


def event_stream():
    pubsub = r.pubsub()
    pubsub.subscribe('TweetChannel')
    for message in pubsub.listen():
        # print message
        yield 'data: %s\n\n' % message['data']


@app.route('/')
def show_homepage():
    app.logger.info("Home: " + request.remote_addr)
    return render_template("index.html")


@app.route('/stream')
def stream():
    app.logger.info("Stream: " + request.remote_addr)
#     return Response(event_stream(), mimetype="text/event-stream")
    data = r.get('TweetSentiments')
    data = data.decode()
#     print(data, type(data))
    data = json.loads(data)
    rdata = json.dumps(data)
    resp = Response(rdata, status=200, mimetype='application/json')
    return resp


@app.route('/hello', methods=['GET'])
def api_hello():
    data = {
        'hello'  : 'world',
        'number' : 3
    }
    js = json.dumps(data)

    resp = Response(js, status=200, mimetype='application/json')
    resp.headers['Link'] = 'http://luisrei.com'

    return resp


@app.route('/updateData', methods=['POST'])
def update_data():
    if not request.json:
        return "error", 400
    data = request.json
    print(data, type(data))
    r.set(data['sentiment'], data['count'])
    return "success", 201


if __name__ == '__main__':
    formatter = logging.Formatter("%(asctime)s -- %(message)s")
    handler = TimedRotatingFileHandler('/tmp/Spark-MLlib-Twitter-Sentiment-Analysis/Viz_Server.log',
                                       when="d",
                                       interval=1,
                                       backupCount=50)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    app.logger.addHandler(handler)
    app.logger.setLevel(logging.INFO)
    app.run(threaded=True,
    host='0.0.0.0',
    port='9999')
