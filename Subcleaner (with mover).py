import os
import glob
import gzip
import shutil
import tqdm
import chardet
import re
import csv
import pandas as pd

#cause python requires functions to be defined before executing script

#First we check the text encoding of a file by opening it as bytes and running it through chardet 
#when then will add that encoding into a dictionary to be referenced later (we need to fill out our CSV file)
def encoding_check(file_name):
    print(f"Checking and Unzippering {file_name}")
    with gzip.open(file_name,"rb",) as f:
        rawdata = b''.join([f.readline() for _ in range(2500)])
    encoding_method = chardet.detect(rawdata)["encoding"]

#after that since we know the encoding used we go ahead and strip away timestamps and subtitle lines    
    timestamps = re.compile('[0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3} --> [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}\n')
    sublines = re.compile('[0-9]{1,4}\n')

    with gzip.open(file_name, 'rt', encoding= encoding_method, errors= "ignore") as f_in, open(file_name[:-3], 'wt', encoding= encoding_method) as f_out:
        text = f_in.read()
        text = re.sub(timestamps,"",text)
        text = re.sub(sublines,"",text)
        f_out.seek(0)
        f_out.writelines(text)
        f_out.truncate()
        f_out.close()

    IDSubtitleFile = os.path.basename(file_name[:-3])
    encodings_dictionary[IDSubtitleFile] = encoding_method
#finally we move the file to the "directory" folder where they will sit clean until further manipulation
    shutil.move(file_name[:-3], directory)



#Setting the 'directory'. This is where the program will start searching for files. 
#This is also where all the files will be extracted in the end.
directory = "C:/Users/pc/Documents/Subtitles/files"
#os.chdir(directory)

#file globbing - look for any and all .gz files within the 'directory'
#those files are then saved into a list called 'zipbox'
globbedfiles = glob.iglob('**/*.gz', root_dir= directory, recursive= True)
zipbox = [os.path.join(directory, file_name) for file_name in globbedfiles] 


#a dictionary to contain the encoding of each particular file
encodings_dictionary = dict()
for item in zipbox:
    encoding_check(item)

#adding the set of SubtitleFileID:Encoding to a CSV file
#will merge with the master CSV later, don't wanna tamper with it
with open("Encodings Master List.csv", 'w') as csv_file:
    writer = csv.writer(csv_file)
    for IDSubtitleFile, encoding_method in encodings_dictionary.items():
        writer.writerow([IDSubtitleFile, encoding_method])