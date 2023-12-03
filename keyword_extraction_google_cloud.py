import json
from langchain.llms import LlamaCpp
from langchain import PromptTemplate, LLMChain
from langchain.text_splitter import RecursiveCharacterTextSplitter
import os
from google.cloud import storage
from google.cloud.sql.connector import Connector
from google.auth import compute_engine
import pymysql.cursors
from dotenv import load_dotenv
import re 

load_dotenv()

credentials = compute_engine.Credentials()
#Initialize GC Storage
storage_client = storage.Client()
bucket_name = os.getenv("Google_cloud_bucket_name")
bucket = storage_client.bucket(bucket_name)


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
cursor = conn.cursor()

#Langchain setup


text_splitter = RecursiveCharacterTextSplitter(
    chunk_size = 800,
    chunk_overlap  = 60,
    length_function = len,
    add_start_index = True,
)

#initialize llm_chain only when it is required
def start_llm_chain():
    llm = LlamaCpp(
        model_path="../models/trurl-2-13b-instruct-q4_K_M.gguf",  
        verbose=True,
        temperature=0.5,
        n_ctx=4096,
        n_gpu_layers=30,
        mlock = True,
        stop = ['</s>'],
    )
    llm_chain = LLMChain(prompt=prompt, llm=llm)
    return llm_chain

def extract_keywords(tekst, llm_chain):
    texts = text_splitter.create_documents([tekst])
    responses = []
    if llm_chain is None:
        llm_chain = start_llm_chain()
    for doc in texts:
        response = llm_chain.run(doc.page_content)
        responses.append(response)
    return responses

template = """<s>[INST]<<SYS>> Wymień słowa kluczowe z dokumentu, oddzielone przecinkami: <</SYS>>
Dokument:  Szanowna Pani Marszałek! Abraham Lincoln miał takie powiedzenie: Czasem możesz kogoś oszukać, ale nie możesz okłamywać wszystkich cały czas. Premier Morawiecki konsekwentnie stara się tę mądrość obalić. Nic dziwnego, że pana wystąpienie zostało nazwane exposé kłamstw. I pozostał symbol tego wystąpienia - prezes Marian Banaś tam siedzący, oklaskujący na stojąco premiera. Każdego dnia pojawiają się nowe informacje, jak jego współpracownicy okradali, oszukując na VAT, polskich emerytów, pacjentów i niepełnosprawnych. Premier Morawiecki może mówić, że stworzył dobrobyt i Polacy na Wyspach pakują już walizki, żeby tu wrócić. A jaka jest, Wysoka Izbo, sytuacja? W 2018 r. zmarło 414 tys. osób, najwięcej od II wojny światowej, zapaść służby zdrowia, armagedon na SOR i rekordowy poziom skrajnego ubóstwa. Żyje w nim 5,4%  Polaków. Wstyd, panie premierze, za te kłamstwa.[/INST]
Słowa kluczowe: służba zdrowia, ubóstwo, kłamstwa, exposé, afery </s>
<s>[INST]Dokument: Jacka Rostowskiego ze słynnym: na te obietnice, które składa Prawo i Sprawiedliwość, pieniędzy nie ma i w ciągu 4 najbliższych lat nie będzie, czy też samego Donalda Tuska: jeżeli ktoś wie, gdzie leżą zakopane w Polsce miliardy, które można porozdawać ludziom, to nie powinien z tym zwlekać.Z tego miejsca odpowiem panu premierowi Tuskowi. Tymi osobami, które wiedziały, gdzie nie są zakopane, ale ukradzione przez mafie VAT-owskie pieniądze, byli pan prezes Jarosław Kaczyński oraz pan premier Mateusz Morawiecki.Również minister Banaś, tak jest.Ale oczywiście o sukcesach polskiej gospodarki świadczy nie tylko wzrost przychodów budżetowych. Wszak do woli możemy żonglować wskaźnikami finansowymi i gospodarczymi. Bezrobocie z poziomu 8% w 2015 r. zjechało do 3,3% według najnowszych danych[/INST]
Słowa kluczowe: gospodarka, finanse, bezrobocie, mafia VAT-owska </s>
<s>[INST]Dokument: {question} [/INST]
Słowa kluczowe:
"""
prompt = PromptTemplate(template=template, input_variables=["question"])

llm_chain = start_llm_chain()
#Get list of relevant files in GC Storage
blobs = [blob for blob in bucket.list_blobs() if "posiedzenie" in blob.name and blob.name.endswith(".json")]
for blob in blobs:
    #load json from Google Cloud Storage
    posiedzenie = json.loads(blob.download_as_string())
    nr_posiedzenia = int(blob.name.split('/')[-1].split("_")[1].split(".")[0])
    #get table name from blob name
    table_name = f"posiedzenie{nr_posiedzenia}"
    for przemowienie in posiedzenie:
        nr_wypowiedzi = przemowienie['nr_wypowiedzi']
        #check if already in database
        if not cursor.execute("SELECT * FROM {table_name} WHERE nr_wypowiedzi = {id}".format(table_name=table_name, id=nr_wypowiedzi)):
            dict_repr = przemowienie.copy()
            print(f"Posiedzenie {nr_posiedzenia}, przemowienie {nr_wypowiedzi}")
            kwords = extract_keywords(przemowienie['tekst'], llm_chain)
            print(f"Response: {kwords}")
            keywords = ','.join(kwords)
            kw_as_list = keywords.split(',')
            kw_cleaned = [re.sub(r'[^\w\s]','',x.replace('\\n','')).strip()  for x in kw_as_list if re.search('\w{4,}',x)]
            dict_repr['keywords'] = ','.join(kw_cleaned)
            columns = ', '.join(dict_repr.keys())
            values = tuple(dict_repr.values())
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES"
            insert_query = insert_query + " (" + "%s,"*(len(values)-1) + "%s)"
            print(f"Cleaned keywords: {','.join(kw_cleaned)}")
            print(insert_query)
            cursor.execute(insert_query, values )

conn.close()
