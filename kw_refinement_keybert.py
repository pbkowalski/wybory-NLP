import json
from google.cloud import storage
from google.cloud.sql.connector import Connector
from google.auth import compute_engine
import pymysql.cursors
from dotenv import load_dotenv
import re 
import os
from sentence_transformers import SentenceTransformer, util
from keybert import KeyBERT
from sklearn.feature_extraction.text import CountVectorizer
import torch

load_dotenv()

# Create a Google Cloud SQL connection using a service account
connector = Connector()
conn = connector.connect(instance_connection_string=os.getenv("Google_cloud_connection_name"), 
                            db=os.getenv("database_name"),
                            user=os.getenv("database_user"),
                            password=os.getenv("database_password"),
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor,
                            driver = 'pymysql',
                            autocommit=True)


#Get all the entries from a single session 
def get_session(conn,table_name, exclude_existing = True, existing_table = 'sk_keybert_average'):
    cursor = conn.cursor()
    rows = []
    if exclude_existing:
        cursor.execute(f"""SELECT * 
        FROM {table_name} 
        WHERE NOT EXISTS (
            SELECT *
            FROM sk_keybert_average
            WHERE sk_keybert_average.posiedzenie = {table_name}.posiedzenie AND
            sk_keybert_average.dzien = {table_name}.dzien AND
            sk_keybert_average.nr_wypowiedzi = {table_name}.nr_wypowiedzi    
        )
        """)
    else:
        cursor.execute(f"SELECT * FROM {table_name}")
    rows.extend(cursor.fetchall())
    return rows


# extract and clean all keywords identified by the LLM
def get_keywords(rows):
    all_keywords =[]
    for d in rows:   
        kws = d['keywords']
        kws = re.split(',|\n', kws)
        all_keywords.extend(kws)
    #remove strings with more than 4 words
    all_keywords = [x for x in all_keywords if len(x.split()) <= 4]
    #strip
    all_keywords = [x.strip() for x in all_keywords]
    #remove strings with no alphanumeric characters
    all_keywords = [x for x in all_keywords if re.search('[a-zA-Z0-9]', x)]
    #remove strings with "kluczowe" (where "słowa kluczowe" was repeated in the text)
    all_keywords = [x for x in all_keywords if not re.search('kluczowe', x)]
    unique_keywords = list(set(all_keywords))
    return unique_keywords

def filter_and_sort_tuples(input_list, kw_min, kw_max, threshold):
    # Remove duplicates and keep the maximum float value for each string
    unique_dict = {}
    for string, value in input_list:
        if string not in unique_dict or value > unique_dict[string]:
            unique_dict[string] = value

    # Sort the unique values by the float values in descending order
    sorted_tuples = sorted(unique_dict.items(), key=lambda x: x[1], reverse=True)

    # Filter by threshold and ensure at least kw_min values are included
    filtered_tuples = [t for t in sorted_tuples if t[1] > threshold]
    if len(filtered_tuples) < kw_min:
        failed_tuples = [t for t in sorted_tuples if t[1] <= threshold]
        while len(filtered_tuples) < kw_min and len(filtered_tuples) < len(unique_dict.items()):
            filtered_tuples.append(failed_tuples.pop(0))
    
    # Return the top kw_max values
    if len (filtered_tuples) > kw_max:
        result = filtered_tuples[:kw_max]
    else:
        result = filtered_tuples
    return result

#List of sentence transformers models to use
model_names = ['piotr-rybak/poleval2021-task4-herbert-large-encoder',
 'sentence-transformers/distiluse-base-multilingual-cased-v2', 
 'sentence-transformers/LaBSE',
 'sentence-transformers/paraphrase-xlm-r-multilingual-v1']
#Initialize sentence transformer models
models = [SentenceTransformer(x) for x in model_names]
#Intiialize KeyBERT model
kw_model = KeyBERT(models[0])
#example stopwords
words = ['wysoka izbo', 'Wysoki Sejmie', 'Wysoka Izbo', 'Sejm', 'Marszałek', 'czas','dziękuję', 'wicemarszałek', 'pośle', 'osób','rząd','rządu', 'poseł', "panie", "premierze", "premier", "państwa", "marszałek", "polski", "polska", "polsce", "polacy", "chodzi", "sejmu",  "sejm", "mówił"]
#in general we do not need 'proper' stopwords as we already have a keyword vocab, however we may need to trim it anyway with some 


#get list of tables in database
cursor = conn.cursor()
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'posiedzenia' AND table_name NOT LIKE 'entries';")
tables = cursor.fetchall()
kw_min = 3
kw_max = 8
threshold = 0.01
force_lowercase = True
out_table = 'sk_keybert_average'
for table in tables:
    table_name = table['TABLE_NAME']
    rows = get_session(conn, table_name)
    if not rows:
        continue
    unique_keywords = get_keywords(rows)
    #get embeddings using all models for all keywords
    embeddings = [m.encode(unique_keywords, convert_to_tensor=True) for m in models]
    unique_lower = list(set([c.lower() for c in unique_keywords]))
    if force_lowercase:
        words = list(set([w.lower() for w in words]))
        vectorizer_model = CountVectorizer(#lowercase=False,
                        ngram_range=(1,3),
                        stop_words=words,
                        min_df=1,
                        vocabulary=unique_lower)
    else:
        vectorizer_model = CountVectorizer(lowercase=False,
                        ngram_range=(1,3),
                        stop_words=words,
                        min_df=1,
                        vocabulary=unique_keywords)
    for speech in rows:
        if (speech['posiedzenie'] == 1
        and speech['dzien'] == 1 
        and speech['nr_wypowiedzi']>1 and speech['nr_wypowiedzi'] < 27):
            #slubowania
            continue
        if force_lowercase:
            llm_candidates = [kw.lower() for kw in speech['keywords'].split(',') if kw.lower() in unique_lower] #remove the ones that were already cleaned
        else:
            llm_candidates = [kw for kw in speech['keywords'].split(',') if kw in unique_keywords] #remove the ones that were already cleaned
        doc = speech['tekst']
        #Use single model to get keybert candidates
        keybert_candidates =  kw_model.extract_keywords(doc, vectorizer= vectorizer_model, top_n = 10)
        if not keybert_candidates:
        #try again without the vocabulary
            keybert_candidates =  kw_model.extract_keywords(doc, keyphrase_ngram_range = (1,3), stop_words = words, min_df = 1, top_n = 10)
        keybert_candidates = [x[0] for x in keybert_candidates]
        #Use multiple models for llm candidates and take mean of scores
        candidates = list(set(llm_candidates + keybert_candidates))
        llm_candidate_embeddings = [[e[i] for i, x in enumerate(unique_keywords) if x in llm_candidates] for e in embeddings]
        keybert_candidate_embeddings = [m.encode(keybert_candidates, convert_to_tensor=True) for m in models]
        #candidate_embeddings = [torch.cat(llm_candidate_embeddings[i],  [keybert_candidate_embeddings[i]], dim = 0) for i in range(len(models))]
        candidate_embeddings = [list(torch.split(keybert_candidate_embeddings[i], 1, dim=0)) + llm_candidate_embeddings[i] for i in range(len(models))]
        doc_embeddings = [model.encode(doc, convert_to_tensor=True) for model in models]
        cosine_scores = [[util.pytorch_cos_sim(e, doc_embeddings[i]) for e in x] for i,x in enumerate(candidate_embeddings)]
        cosine_scores_transposed = list(map(list, zip(*cosine_scores)))
        mean_cosine_scores = [torch.cat(row).mean().cpu().tolist() for row in cosine_scores_transposed]
        #kws_out = kws + list(zip(llm_candidates, mean_cosine_scores))
        kws_out = list(zip(candidates, mean_cosine_scores))
        kws_out = filter_and_sort_tuples(kws_out, kw_min, kw_max, threshold)
        print(kws_out)
        keywords = ",".join([x[0] for x in kws_out])
        columns = ["posiedzenie", "dzien", "nr_wypowiedzi", "keywords"]
        values = [speech['posiedzenie'],speech['dzien'],speech['nr_wypowiedzi'], keywords]
        insert_query = f"INSERT INTO {out_table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(values))})"
        cursor.execute(insert_query, values)
    
