from flask import Flask, jsonify, request
from data_processing import DataProcessing

app = Flask(__name__)


@app.route('/api/info', methods=['GET'])
def get_filters_info():
    dp = DataProcessing()
    filters = dp.filter_info()
    return jsonify(filters)


@app.route('/api/timeline', methods=['GET'])
def get_timeline_data():
    parameters = {key: value for key, value in request.args.items()}
    dp = DataProcessing()
    data = dp.timeline_data(parameters)
    response = {"timeline": data}
    return jsonify(response)


if __name__ == '__main__':
    app.run()
