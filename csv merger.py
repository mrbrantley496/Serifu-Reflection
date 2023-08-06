import pandas as pd

Encodings_Master_List = pd.read_csv("C:/Users/pc/Documents/Serifu Reflection/Encodings_Master_List.csv")
Japanese_Subtitles_CSV = pd.read_csv("C:/Users/pc/Documents/Serifu Reflection/Japanese_Subtitles_CSV.csv")

Kansei_csv =  pd.merge(Japanese_Subtitles_CSV, Encodings_Master_List[['IDSubtitleFile','Encoding']], on= 'IDSubtitleFile')

Kansei_csv.to_csv("Kansei_csv", index= False)