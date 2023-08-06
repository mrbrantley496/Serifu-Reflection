import re

#first remove timestamps, then remove subtitle lines
#finally, clean up whitespace in the text to make it
#more visually concise
timestamps = re.compile('[0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3} --> [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}\n')
sublines = re.compile('[0-9]{1,4}\n')

#access the file and edit it
with open('C:/Users/pc/Documents/chardet-test3', 'r+', encoding='shift-jis') as file:
    text = file.read()
    text = re.sub(timestamps,"",text)
    file.seek(0)
    file.write(text)
    file.truncate()
    text = re.sub(sublines,"",text)
    file.seek(0)
    file.write(text)
    file.truncate()
    file.close()
