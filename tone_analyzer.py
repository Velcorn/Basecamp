from ibm_watson import LanguageTranslatorV3
from ibm_watson import ToneAnalyzerV3
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from googletrans import Translator
from psycopg2 import connect, Error
from config import dbconfig

# Set up Tone Analyzer and Translator from Watson
api_key_tl = '2Q5-kJJN8MqVEgGxf5VM2Dv063cL7r5VTp44IcreG3EN'
api_key_ta = '1zk1LfROS3ccoX0iEOBomvK6euIpbJSkp9K-wu2IzS_A'
url_tl = 'https://api.eu-de.language-translator.watson.cloud.ibm.com/instances/371a365a-dafb-4d40-8f7a-0da85693ee4a'
url_ta = 'https://api.eu-de.tone-analyzer.watson.cloud.ibm.com/instances/d0f39694-bd5f-4d32-900c-e689a9109a31'
version = '2020-05-08'

authenticator_tl = IAMAuthenticator(api_key_tl)
translator = LanguageTranslatorV3(
    version=version,
    authenticator=authenticator_tl
)

authenticator_ta = IAMAuthenticator(api_key_ta)
tone_analyzer = ToneAnalyzerV3(
    version=version,
    authenticator=authenticator_ta
)

translator.set_service_url(url_tl)
tone_analyzer.set_service_url(url_ta)

# Temporarily using googletrans to avoid using up quota from Watson Translator.
googletrans = Translator()


def fetch_comments():
    try:
        params = dbconfig()
        connection = connect(**params)
        cursor = connection.cursor()
        print("DB connected.")

        print("Fetching comments...")
        cursor.execute("SELECT text FROM comments WHERE translation is NULL ORDER BY id ASC;")
        comments = cursor.fetchmany(10)

        # Close everything
        cursor.close()
        connection.close()
        print("DB disconnected.")

        return comments
    except (Exception, Error) as error:
        return error


# Analyze the tone of comments from the DB.
def analyze():
    # Get comments from the DB.
    comments = fetch_comments()

    try:
        # Open DB connection.
        params = dbconfig()
        connection = connect(**params)
        cursor = connection.cursor()
        print("DB connected.")

        print("Writing entries to DB...")
        # Translate each comment and generate tone analysis.
        for comment in comments:
            comment = str(comment[0]).replace("\n", " ")
            # Translate comment and analyze the tone.
            # translation = translator.translate(comment, model_id='de-en').get_result()['translations'][0]['translation']
            translation = googletrans.translate(comment).text
            tone_analysis = tone_analyzer.tone({'text': translation}, content_type='text/plain').get_result()
            analysis = []
            for tone in tone_analysis['document_tone']['tones']:
                analysis.append(tone['tone_name'] + ": " + str(tone['score']))

            # Write translation and analysis to DB.
            cursor.execute("update comments "
                           "set translation=(%s) "
                           "where id=(select id from comments where translation is null order by id asc limit 1);",
                           (translation,))

            cursor.execute("update comments "
                           "set tone=(%s) "
                           "where id=(select id from comments where tone is null order by id asc limit 1);",
                           (analysis,))

        connection.commit()
        cursor.close()
        connection.close()
        return "Committed entries to DB."
    except (Exception, Error) as error:
        return error


print(analyze())
