from flask import Flask, Response
app = Flask(__name__)


@app.route('/')
def index():
    with open("tone_analysis.txt", "r", encoding="utf8") as f:
        tone_analysis = f.read()
    return Response(tone_analysis, mimetype='text/plain')


if __name__ == "__main__":
    app.run()
