from ibm_watson import LanguageTranslatorV3
from ibm_watson import ToneAnalyzerV3
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from googletrans import Translator
from psycopg2 import connect, Error
from config import db_target

# Set up Tone Analyzer and Translator from Watson
API_KEY_TL = '2Q5-kJJN8MqVEgGxf5VM2Dv063cL7r5VTp44IcreG3EN'
API_KEY_TA = '1zk1LfROS3ccoX0iEOBomvK6euIpbJSkp9K-wu2IzS_A'
URL_TL = 'https://api.eu-de.language-translator.watson.cloud.ibm.com/instances/371a365a-dafb-4d40-8f7a-0da85693ee4a'
URL_TA = 'https://api.eu-de.tone-analyzer.watson.cloud.ibm.com/instances/d0f39694-bd5f-4d32-900c-e689a9109a31'
VERSION = '2020-05-08'

authenticator_tl = IAMAuthenticator(API_KEY_TL)
translator = LanguageTranslatorV3(
    version=VERSION,
    authenticator=authenticator_tl
)

authenticator_ta = IAMAuthenticator(API_KEY_TA)
tone_analyzer = ToneAnalyzerV3(
    version=VERSION,
    authenticator=authenticator_ta
)

translator.set_service_url(URL_TL)
tone_analyzer.set_service_url(URL_TA)

# Temporarily using googletrans to avoid using up quota from Watson Translator.
googletrans = Translator()


# Fetch comments from DB.
def fetch_comments():
    try:
        params = db_target()
        connection = connect(**params)
        cursor = connection.cursor()
        print("DB connected.")

        print("Fetching comments...")
        cursor.execute("SELECT text "
                       "FROM comments "
                       "WHERE translation IS NULL "
                       "ORDER BY id ASC;")
        comments = cursor.fetchall()

        cursor.close()
        connection.close()
        print("DB disconnected.")
        return comments
    except (Exception, Error) as error:
        return error
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("DB disconnected.")


# Analyze the tone of comments from the DB and write results to it.
def analyze():
    # Get comments from the DB.
    comments = fetch_comments()

    try:
        # Open DB connection.
        params = db_target()
        connection = connect(**params)
        cursor = connection.cursor()
        print("DB connected.")

        print("Generating analysis and writing results to DB...")
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
            cursor.execute("UPDATE comments "
                           "SET translation=(%s) "
                           "WHERE id=(SELECT id FROM comments WHERE translation IS NULL ORDER BY id ASC LIMIT 1);",
                           (translation, ))

            cursor.execute("UPDATE comments "
                           "SET tone=(%s) "
                           "WHERE id=(SELECT id FROM comments WHERE tone IS NULL ORDER BY id ASC LIMIT 1);",
                           (analysis, ))
        connection.commit()

        cursor.close()
        connection.close()
        print("DB disconnected.")
        return "Committed entries to DB."
    except (Exception, Error) as error:
        return error
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("DB disconnected.")


print(analyze())
