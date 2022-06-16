from pyexpat.errors import messages
import pdfplumber
import pandas as pd
import re
from sqlalchemy import create_engine
import os
import shutil
import subprocess
from subprocess import *
from io import StringIO

from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser

pdfs = ['Eduardo_Ferreira.pdf', 'jean_barddal.pdf', 'LucasLoezer.pdf', 'maylin.pdf', 'parchen.pdf', 'TASSIA_ERBANO.pdf']

lista = []
# for documento in pdfs:
#     numpage = 0
#     numphrase = 0
#     with pdfplumber.open(f'/home/caeta/tcc-procad/{documento}') as pdf:
#         for page in pdf.pages:
#             numpage += 1
#             char = page.extract_text().replace("\n", "").split(".")
#             for phrase in char:
#                 numphrase += 1
#                 phrase = ''.join([i for i in phrase if not i.isdigit()])
#                 phrase = re.sub(r'\W+', ' ', phrase)
#                 phrase = phrase.strip()
#                 if len(phrase) >= 5:
#                     lista.append([documento, phrase, numpage, numphrase])
for documento in pdfs:
    output_string = StringIO()
    numpage = 0
    numphrase = 0
    with open(f'/home/caeta/tcc-procad/{documento}', 'rb') as in_file:
        parser = PDFParser(in_file)
        doc = PDFDocument(parser)
        rsrcmgr = PDFResourceManager()
        device = TextConverter(rsrcmgr, output_string, laparams=LAParams())
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        for page in PDFPage.create_pages(doc):
            numpage += 1
            interpreter.process_page(page)
        char = output_string.getvalue().replace("\n", " ").split(".")
        for phrase in char:
            numphrase += 1
            phrase = ''.join([i for i in phrase if not i.isdigit()])
            #phrase = re.sub(r'\W+', ' ', phrase)
            phrase = phrase.strip()
            if len(phrase) >= 3:
                lista.append([documento, phrase, numpage, numphrase])

messages = pd.DataFrame(lista, columns= ['Documento', 'Frase', 'Num_Pagina', 'Num_Frase'])

messages = messages[messages['Frase'] != '']

messages = messages[messages['Frase'] != ' ']

messages = messages[messages['Frase'].notnull()]

#messages.to_excel('teste.xlsx')

messages["_id"] = messages.index + 1

messages = messages[['_id', 'Documento', 'Frase']]

messages.rename(columns={"Documento": "key_remote_jid", "Frase": "data"}, inplace=True)

d = os.path.dirname(os.getcwd())

directory = 'tcc-procad/42.userdata'

path = os.path.join(d, directory)

os.mkdir(path)

engine = create_engine('sqlite:///msgstore.db', echo=True)
sqlite_connection = engine.connect()

sqlite_table = "messages"

messages.to_sql(sqlite_table, sqlite_connection, if_exists='replace')

sqlite_connection.close()

shutil.move(f"{d}/tcc-procad/msgstore.db", f"{d}/tcc-procad/42.userdata/msgstore.db")

call(["7z", "a -tiso", f"{d}/tcc-procad/42.userdata/"], shell=False)

