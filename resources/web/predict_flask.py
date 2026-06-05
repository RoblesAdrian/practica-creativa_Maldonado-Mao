import os
import json
import threading
import uuid

from flask import Flask, render_template, request
from flask_socketio import SocketIO, join_room
from bson import json_util
from kafka import KafkaProducer, KafkaConsumer
from cassandra.cluster import Cluster

import config
import predict_utils

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev")
socketio = SocketIO(app, cors_allowed_origins="*")

producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
)

PREDICTION_TOPIC = "flight-delay-ml-request"
RESPONSE_TOPIC = "flight-delay-ml-response"

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
cassandra_cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
cassandra_session = cassandra_cluster.connect("agile_data_science")


def get_flight_distance(origin, dest):
    row = cassandra_session.execute(
        "SELECT distance FROM origin_dest_distances WHERE origin=%s AND dest=%s",
        (origin, dest),
    ).one()
    if row is None:
        raise ValueError(f"Distance not found for route {origin}-{dest}")
    return row.distance


def consume_predictions():
    consumer = KafkaConsumer(
        RESPONSE_TOPIC,
        bootstrap_servers=["kafka:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="web-prediction-consumer",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    for message in consumer:
        prediction = message.value
        unique_id = prediction.get("UUID")
        pending_results[unique_id] = prediction
        event = pending_events.get(unique_id)
        if event:
            event.set()

def start_consumer():
    thread = threading.Thread(target=consume_predictions, daemon=True)
    thread.start()

@app.route("/")
@app.route("/delays")
def delays():
    return render_template("delays.html")

@socketio.on("connect", namespace="/predictions")
def predictions_connect():
    # join_room(request.sid)
    print("PREDICTIONS CONNECT", request.sid)

pending_requests = {}
pending_results = {}
pending_events = {}

@socketio.on("predict_flight_delay", namespace="/predictions")
def predict_flight_delay_socket(data):
    required = ["DepDelay", "Carrier", "FlightDate", "Dest", "Origin"]
    for field in required:
        if not data.get(field):
            return {"status": "ERROR", "message": f"Missing field {field}"}

    api_form_values = {
        field: data.get(field)
        for field in ["DepDelay", "Carrier", "FlightDate", "Dest", "FlightNum", "Origin"]
    }

    api_form_values["DepDelay"] = float(api_form_values["DepDelay"])
    api_form_values["Carrier"] = str(api_form_values["Carrier"])
    api_form_values["FlightDate"] = str(api_form_values["FlightDate"])
    api_form_values["Dest"] = str(api_form_values["Dest"])
    api_form_values["FlightNum"] = str(api_form_values.get("FlightNum", ""))
    api_form_values["Origin"] = str(api_form_values["Origin"])

    prediction_features = dict(api_form_values)
    prediction_features["Distance"] = get_flight_distance(
        api_form_values["Origin"], api_form_values["Dest"]
    )
    prediction_features.update(
        predict_utils.get_regression_date_args(api_form_values["FlightDate"])
    )
    prediction_features["Timestamp"] = predict_utils.get_current_timestamp()

    unique_id = str(uuid.uuid4())
    prediction_features["UUID"] = unique_id

    event = threading.Event()
    pending_events[unique_id] = event

    producer.send(PREDICTION_TOPIC, json.dumps(prediction_features).encode("utf-8"))
    producer.flush()

    if event.wait(timeout=20):
        result = pending_results.pop(unique_id, None)
        pending_events.pop(unique_id, None)
        pending_requests.pop(unique_id, None)
        return {"status": "OK", "id": unique_id, "prediction": result}

    pending_events.pop(unique_id, None)
    pending_requests.pop(unique_id, None)
    return {"status": "WAIT", "id": unique_id}


@app.route("/flights/delays/predict_kafka")
def flight_delays_page_kafka():
    form_config = [
        {"field": "DepDelay", "label": "Departure Delay", "value": 5},
        {"field": "Carrier", "value": "AA"},
        {"field": "FlightDate", "label": "Date", "value": "2016-12-25"},
        {"field": "Origin", "value": "ATL"},
        {"field": "Dest", "label": "Destination", "value": "SFO"},
    ]
    return render_template("flight_delays_predict_kafka.html", form_config=form_config)


@app.route("/shutdown")
def shutdown():
    shutdown_server()
    return "Server shutting down..."


def shutdown_server():
    func = request.environ.get("werkzeug.server.shutdown")
    if func is None:
        raise RuntimeError("Not running with the Werkzeug Server")
    func()


if __name__ == "__main__":
    start_consumer()
    socketio.run(app, host="0.0.0.0", port=5000, debug=True, use_reloader=False)