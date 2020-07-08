from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_watson import LanguageTranslatorV3, ToneAnalyzerV3
from config import api_config
from json import dumps


# Set up Translator and Tone Analyzer from Watson
api = api_config()
API_KEY_TL = api["key_tl"]
API_KEY_TA = api["key_ta"]
API_KEY_PI = api["key_pi"]
URL_TL = api["url_tl"]
URL_TA = api["url_ta"]
URL_PI = api["url_pi"]
VERSION = api["version"]

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


test = "wo soll man hier immer anfangen mit den Richtigstellungen? Mal von unten her: Dass R steigt natürlich an, " \
       "wenn sich massenweise Arbeiter anstecken, deren Haltung für Hühner verboten wäre. Das hat aber " \
       "keinerlei Auswirungen auf die Gefahr durch das Virus. Das RKI weist darauf auch hin , aber meist wird " \
       "einfach nur Angst gemacht, mit dem gestiegenen R-Wert. Kenne ich zur Genüge von Unterhaltungen mit anderen, " \
       "die das nicht durchblicken. Bei der Tabelle fällt auf, dass jetzt auch Deutschland rausgefallen ist. Spahn " \
       "will eine Aufarbeitung mit ausgewiesenen Experten. Da toll. Da fragt man dann wieder die gleichen, die " \
       "schon am ersten lockdown schuld waren. Das wird uns weiterbringen. Der ebenso unsäglich inkompetente Heil " \
       "will sich irgendwie an der Rechtsfindung beteiligen. Banalitäten, was der verkündet. Soll sich mal um die " \
       "unzumutbaren Beschäftigungsbedingungen kümmern, nicht um Haftungsfragen, für die er gar nicht zuständig ist. " \
       "Der immer maskierte Risikopatient Kretschmann hat Angst um seine überalterten Lehrer und will daher die " \
       "Schulen wieder lahmlegen. Geht's noch? Die zweite Welle schwappt in die Kliniken? Wo schon seit Monaten die " \
       "Hälfte der Stationen leer waren und es auch keine erste Welle gab? Südkorea in der zweiten Welle? Zahlen " \
       "werden verschwiegen. Na dann, wird schon stimmen, was Frau Jeong sagt. Der Ex-Ärztin für Family Medicine " \
       "wurde zwar wegen Mismanagement bei der MERS-Affäre das Gehalt gekürzt, aber egal.Weiterer Tennisspieler " \
       "infiziert? Weia, die Überschrift macht Angst. Weiter unten steht dann, dass es ihm gut geht und er keine " \
       "Symptome hat. Richtigerweise also: Weiterer Tennisspieler positiv getestet. Aber an richtigen Headlines hat " \
       "der Spiegel ja kein Interesse. Das schürt zu wenig Panik. Dann noch eine Studie aus China (!) " \
       "mit 2 mal 37 (!) Teilnehmern. Echt jetzt? Da sagen sogar die sicher vertrauenswürdigen Chinesen, dass das " \
       "recht wenig sei. Aber der Spiegel bringts, auch wieder zu dem allseits bekannten Zweck."

translation = translator.translate(test, model_id='de-en').get_result()['translations'][0]['translation']
tone_analysis = tone_analyzer.tone({'text': translation}, content_type='text/plain').get_result()
with open("test.json", "w") as f:
    f.write(dumps(tone_analysis))
