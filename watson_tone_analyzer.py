from ibm_watson import ToneAnalyzerV3
from ibm_watson import LanguageTranslatorV3
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from googletrans import Translator

# Sets the text file to get comments from.
file = "comment.txt"

# Set up Tone Analyzer and Translator from Watson
api_key_ta = '1zk1LfROS3ccoX0iEOBomvK6euIpbJSkp9K-wu2IzS_A'
api_key_tl = '2Q5-kJJN8MqVEgGxf5VM2Dv063cL7r5VTp44IcreG3EN'
version = '2020-05-08'
url_ta = 'https://api.eu-de.tone-analyzer.watson.cloud.ibm.com/instances/d0f39694-bd5f-4d32-900c-e689a9109a31'
url_tl = 'https://api.eu-de.language-translator.watson.cloud.ibm.com/instances/371a365a-dafb-4d40-8f7a-0da85693ee4a'

authenticator_ta = IAMAuthenticator(api_key_ta)
tone_analyzer = ToneAnalyzerV3(
    version=version,
    authenticator=authenticator_ta
)

authenticator_tl = IAMAuthenticator(api_key_tl)
translator = LanguageTranslatorV3(
    version=version,
    authenticator=authenticator_tl
)

tone_analyzer.set_service_url(url_ta)
translator.set_service_url(url_tl)
translator = Translator()


# Analyze the tone of the input text.
def analyze(file):

    # Get comment from text file.
    with open(file, "r", encoding="utf8") as f:
        comment = f.read().replace("\n", " ")

    # Translate comment and analyze the tone.
    # translation = translator.translate(comment, model_id='de-en').get_result()['translations'][0]['translation']
    translation = translator.translate(comment).text
    tone_analysis = tone_analyzer.tone({'text': translation}, content_type='application/json').get_result()

    with open("tone_analysis.txt", "w", encoding="utf8") as f:
        f.write("Comment: " + "\n" + comment + "\n"*2)
        f.write("Translation: " + "\n" + translation + "\n" * 2)
        f.write("Tone:" + "\n")
        for tone in tone_analysis['document_tone']['tones']:
            f.write(tone['tone_name'] + ": " + str(tone['score']) + "\n")

    return ""


print(analyze(file))
