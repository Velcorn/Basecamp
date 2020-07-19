# Projekt base.camp an der UHH (SoSe2020)


This repo contains three Python scripts, the third script (update.py) is there to execute the other two in order. The first (create_data.py) is used to connect to a provided database (of Spiegel Online articles and their comments), extract "relevant" data and write that to newly created tables in order to be analyzed. The second (analyze_data.py) is used to analyze the previously extracted data, specifically the comments, first translating them from German to English with the IBM Watson Translator, then analyzing them with the IBM Watson Tone Analyzer and writing the results back to the DB. Finally, personality insights are generated using IBM Watson Personality Insights on a predefined amount of comments from a few users.

Setting the scripts up requires installing the required Python packages by using *pip install -r requirements.txt*. Further, it is necessary to fill the provided example .ini files and rename them accordingly.
