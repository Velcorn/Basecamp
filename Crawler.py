from selenium import webdriver
from selenium.webdriver.common.by import By

driver = webdriver.Chrome()
driver.get("https://www.ndr.de/sport/fussball/Politik-gibt-gruenes-Licht-fuer-Bundesliga-Wiederbeginn,bundesliga1704.html")
driver.find_element_by_link_text("Kommentare anzeigen").click()
comments = driver.find_element_by_xpath("//*[@id='soforumHolder']")
print(comments.text)
'''with open("Comments.txt", "w") as f:
    f.write(comments.text())'''
