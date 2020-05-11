from flask import Flask, Response
from watson_tone_analyzer import analyze
app = Flask(__name__)


@app.route('/')
def index():
    analyze("comment.txt")
    with open("tone_analysis.txt", "r") as f:
        tone_analysis = f.read()
    return Response(tone_analysis, mimetype='text/plain')