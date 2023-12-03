import requests
from bs4 import BeautifulSoup
import re
import json
import time
import random
import logging
logging.basicConfig(filename='app.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'}

def parse_speech(soup):
    escapes = ''.join([chr(char) for char in range(1, 32)]) 
    stenogram = soup.find('div', {'class' : 'stenogram'})
    data = soup.find(string=re.compile("w dniu")).getText()
    data = re.search(r'\d{2}-\d{2}-\d{4}', data).group()
    punkt = '' if stenogram.find('h2', {'class': 'punkt'}) is None else stenogram.find('h2', {'class': 'punkt'}).getText()
    tytul = '' if stenogram.find('p', {'class': 'punkt-tytul'}) is None else stenogram.find('p', {'class': 'punkt-tytul'}).getText()
    tytul = tytul.translate(str.maketrans('', '', escapes))
    mowca = stenogram.find('h2', {'class': 'mowca'}).getText()
    wypowiedz = ''
    interruption = False
    for p in stenogram.find_all('p', {'class': ''}):
        #remove leading whitespace
        text = p.getText().removeprefix('\xa0\xa0\xa0 ')
        #remove "official interruptions"
        if interruption is True:
            if text.endswith(':'):
                interruption = False
            continue
        if interruption is False and text.endswith('Marszałek:'):
            interruption = True
            continue
        #remove linebreaks
        #escapes = ''.join([chr(char) for char in range(1, 32)]) 
        text = text.translate(str.maketrans('', '', escapes))
        #remove interruptions
        text=re.sub(r"\(.*?\)","",text)
        #remove ellipses due to interruption and replace one with whitespace
        if text.endswith('...'):
            text = text.removesuffix('...')
            text+=(' ')
        text = text.removeprefix('...') 
        wypowiedz += text
    return {"data": data, "punkt": punkt, "tytul": tytul, "mowca": mowca, "tekst": wypowiedz}


url = "https://www.sejm.gov.pl/sejm9.nsf/wypowiedz.xsp?posiedzenie={posiedzenie}&dzien={dzien}&wyp={wypowiedz}"
failed_list = []
failed_posiedzenie = []
failed_dzien = []
wypowiedzi = []
posiedzenie = 1
while True:
    wypowiedzi_posiedzenie = []
    dzien = 1
    nr_wypowiedzi = 1
    for attempt in range(10):
        try:
            page = requests.get(url.format(posiedzenie = posiedzenie, dzien = dzien, wypowiedz = nr_wypowiedzi), headers = headers, timeout = 300)
            time.sleep(2*random.random())
        except:
            logging.error(f'RequestFailed: Posiedzenie: {posiedzenie}, retrying')
            #failed_dzien.append((posiedzenie, dzien))
        else:
            break
    else:
        #loop hit an exception every time
        logging.critical(f'RequestFailed: Posiedzenie: {posiedzenie}, skipping')
        failed_posiedzenie.append((posiedzenie))
        continue
    soup = BeautifulSoup(page.content, "html.parser")
    stenogram = soup.find('div', {'class' : 'stenogram'})
    if stenogram.text == '':
        print(dzien)
        print(nr_wypowiedzi)
        logging.info(f'Finished Parsing')
        break
    while True:
        #nr_wypowiedzi = 1
        nr_wypowiedzi = 1
        for attempt in range(10):
            try:
                page = requests.get(url.format(posiedzenie = posiedzenie, dzien = dzien, wypowiedz = nr_wypowiedzi), headers = headers, timeout = 300)
                time.sleep(2*random.random())
            except:
                logging.error(f'RequestFailed: Posiedzenie: {posiedzenie}, Dzien: {dzien}, retrying')
                #failed_dzien.append((posiedzenie, dzien))
            else:
                break
        else:
            #loop hit an exception every time
            logging.critical(f'RequestFailed: Posiedzenie: {posiedzenie}, Dzien: {dzien}, skipping')
            failed_dzien.append((posiedzenie, dzien))
            continue
        
        soup = BeautifulSoup(page.content, "html.parser")
        stenogram = soup.find('div', {'class' : 'stenogram'})
        if stenogram.text == '':
            logging.info(f'FinishedParsing: P:{posiedzenie}, final: D:{dzien} W:{nr_wypowiedzi}')
            break
        while True:
            try:
                time.sleep(2*random.random())
                page = requests.get(url.format(posiedzenie = posiedzenie, dzien = dzien, wypowiedz = nr_wypowiedzi), headers = headers, timeout = 300)
            except:
                failed_list.append((posiedzenie, dzien, nr_wypowiedzi))
                logging.error(f'RequestFailed:{posiedzenie}, {dzien}, {nr_wypowiedzi}')
                continue
            if page.status_code != 200:
                failed_list.append((posiedzenie, dzien, nr_wypowiedzi))
                logging.error(f'ParseFailed:{posiedzenie}, {dzien}, {nr_wypowiedzi}')
                continue
            soup = BeautifulSoup(page.content, "html.parser")
            stenogram = soup.find('div', {'class' : 'stenogram'})
            if stenogram.text == '':
                #logging.info(f'Parsed: Wypowiedz: {nr_wypowiedzi} ({posiedzenie},{dzien})')
                logging.info(f'Parsed: Dzien: {dzien} (P: {posiedzenie}, wypowiedzi: {nr_wypowiedzi})')
                #print(nr_wypowiedzi)
                break
            else: 
                try:
                    wyp = parse_speech(soup)
                    wyp['posiedzenie'] = posiedzenie
                    wyp['dzien'] = dzien
                    wyp['nr_wypowiedzi'] = nr_wypowiedzi
                    print(f'scrapped:{posiedzenie}, {dzien}, {nr_wypowiedzi}')
                    logging.debug(f'Parsed: ({posiedzenie}, {dzien}, {nr_wypowiedzi})')
                except:
                    print(f'błąd:{posiedzenie}, {dzien}, {nr_wypowiedzi}')
                    failed_list.append((posiedzenie, dzien, nr_wypowiedzi))
                    logging.error(f'ParseFailed:{posiedzenie}, {dzien}, {nr_wypowiedzi}')
                wypowiedzi.append(wyp)
                wypowiedzi_posiedzenie.append(wyp)
                nr_wypowiedzi +=1
        dzien +=1
    with open(f'posiedzenie_{posiedzenie}.json', 'w', encoding = 'utf8') as fout:
        json.dump(wypowiedzi_posiedzenie, fout)
    posiedzenie +=1
with open('kadencja9.json', 'w', encoding = 'utf8') as fout:
    json.dump(wypowiedzi, fout)

with open('fail_list.json', 'w', encoding = 'utf8') as fout:
    json.dump(failed_list, fout)

with open('fail_pos.json', 'w', encoding = 'utf8') as fout:
    json.dump(failed_posiedzenie, fout)

with open('fail_d.json', 'w', encoding = 'utf8') as fout:
    json.dump(failed_dzien, fout)