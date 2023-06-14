from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import requests
import matplotlib.pyplot as plt
import smtplib
import ssl
import pandas as pd
from email.message import EmailMessage
url = 'https://restcountries.com/v3.1/all'
postgresql = {
    'pguser': 'uca',
    'pgpasswd': 'pwdausar',
    'pghost': 'localhost',
    'pgport': 5432,
    'pgdb': 'uca'
}

def get_engine(user, passwd, host, port, db):
    url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
    if not database_exists(url):
        create_database(url)
    engine = create_engine(url, pool_size=50, echo=False)
    return engine

engine = get_engine(postgresql['pguser'], postgresql['pgpasswd'],
                    postgresql['pghost'], postgresql['pgport'], postgresql['pgdb'])

def cargarCapitales(data, idPais):
    if 'capital' in data:
        for cap in data['capital']:
            query = "SELECT * FROM capital WHERE nombre = %s"
            res = engine.execute(query, cap).fetchall()
            if res == []:
                query = "INSERT INTO capital(nombre, idpais) VALUES(%s, %s)"
                engine.execute(query, cap, idPais)

def cargarMonedas(data):
    if 'currencies' in data:
        query = "INSERT INTO moneda(nombre) SELECT (%s) WHERE NOT EXISTS (SELECT nombre FROM moneda WHERE nombre = %s);"
        for cur in data['currencies']:
            engine.execute(query, cur, cur)

def cargarIdioma(data):
    if 'languages' in data:
        query = "INSERT INTO idioma(nombre) SELECT (%s) WHERE NOT EXISTS (SELECT nombre FROM idioma WHERE nombre = %s);"
        for lan in data['languages']:
            engine.execute(query, lan, lan)

def cargarPaises(data):
    nombre = data['name']['common']
    continente = data['continents'][0]
    poblacion = data['population']
    bandera = data['flags']['png']
    query = "SELECT * FROM paises WHERE nombre = %s"
    res = engine.execute(query, nombre).fetchall()
    if res == []:
        query = "INSERT INTO paises(nombre, continente, poblacion, bandera) VALUES(%s, %s, %s, %s)"
        engine.execute(query, nombre, continente, poblacion, bandera)
    q2 = "SELECT id from paises WHERE nombre = %s"
    res = engine.execute(q2, nombre).fetchone().id
    return res

def insertarPaisMoneda(idPais, idMoneda):
    query = "SELECT * FROM pais_moneda WHERE idpais = %s AND idMoneda = %s"
    res = engine.execute(query, idPais, idMoneda).fetchall()
    if(res == []):
        query = "INSERT INTO pais_moneda(idpais, idmoneda) VALUES (%s, %s)"
        engine.execute(query, idPais, idMoneda)

def insertarPaisIdioma(idPais, idIdioma):
    query = "SELECT * FROM pais_idioma WHERE idpais = %s AND idIdioma = %s"
    res = engine.execute(query, idPais, idIdioma).fetchall()
    if(res == []):
        query = "INSERT INTO pais_idioma(idpais, idIdioma) VALUES (%s, %s)"
        engine.execute(query, idPais, idIdioma)

def cargarElemento(data):
    idPais = cargarPaises(data)
    cargarCapitales(data, idPais)
    cargarMonedas(data)
    cargarIdioma(data)

def uploadData(data):
    for i in range(0, len(data)):
        cargarElemento(data[i])
        q1 = "SELECT id FROM paises WHERE nombre = %s"
        idPais = engine.execute(q1, data[i]['name']['common']).fetchone().id
        if 'currencies' in data[i]:
            q2 = "SELECT id FROM moneda WHERE nombre = %s"
            for cur in data[i]['currencies']:
                idMoneda = engine.execute(q2, cur).fetchone().id
                insertarPaisMoneda(idPais, idMoneda)
        q1 = "SELECT id FROM paises WHERE nombre = %s"
        idPais = engine.execute(q1, data[i]['name']['common']).fetchone().id
        if 'languages' in data[i]:
            q2 = "SELECT id FROM idioma WHERE nombre = %s"
            for idioma in data[i]['languages']:
                idIdioma = engine.execute(q2, idioma).fetchone().id
                insertarPaisIdioma(idPais, idIdioma)

def obtenerDataPaises():
    paises = []
    dbPais = engine.execute("SELECT * FROM paises;").fetchall()
    for pais in dbPais:
        info = {
            'nombre': pais.nombre,
            'continente': pais.continente,
            'poblacion': pais.poblacion,
            'bandera': pais.bandera,
            'capitales': [],
            'lenguajes': [],
            'monedas': []
        }
        capitales = engine.execute("SELECT * FROM capital WHERE idPais = %s", pais.id).fetchall()
        for cap in capitales:
            info['capitales'].append(cap.nombre)
        lenguajes = engine.execute("SELECT idioma.nombre AS nombre FROM pais_idioma INNER JOIN idioma ON pais_idioma.ididioma = idioma.id WHERE idPais = %s", pais.id).fetchall()
        for len in lenguajes:
            info['lenguajes'].append(len.nombre)
        monedas = engine.execute("SELECT moneda.nombre AS nombre FROM pais_moneda INNER JOIN moneda ON pais_moneda.idmoneda = moneda.id WHERE idPais = %s", pais.id).fetchall()
        for mon in monedas:
            info['monedas'].append(mon.nombre)
        paises.append(info)
    return pd.DataFrame(paises)
    
def exportarExcelFile():
    dfPaises = obtenerDataPaises()
    query = "SELECT continente, SUM(poblacion) AS poblacion FROM paises GROUP BY continente;"
    df = pd.DataFrame(engine.execute(query).fetchall())
    x_values = df['continente'] 
    y_value = df['poblacion']
    plt.bar(x_values, y_value)
    plt.ylabel('Poblacion')
    plt.xlabel('Continente')
    plt.title("Poblacion por continente")
    plt.savefig('barChart.png')
    plt.clf()
    q2 = "SELECT idioma.nombre, COUNT(*) AS Cant_Paises from pais_idioma INNER JOIN idioma ON pais_idioma.ididioma = idioma.id GROUP BY idioma.id;"
    df2 = pd.DataFrame(engine.execute(q2).fetchall())
    y = df2['cant_paises']
    labels = df2['nombre']
    plt.pie(y, labels=labels, textprops={'size': 'small'})
    plt.title('Cantidad de paises en los que es usado el idioma')
    plt.savefig('pieChart.png')
    with pd.ExcelWriter('Paises.xlsx') as writer:
        dfPaises.to_excel(writer, sheet_name='Paises', index=False)
        pd.DataFrame([{}]).to_excel(writer, sheet_name='Metricas', index=False)
        ws = writer.sheets['Metricas']
        ws.insert_image('C2', 'barChart.png')
        ws.insert_image('O2', 'piechart.png')

def enviarMail():
    email_emisor = 'marcosmarcello17@gmail.com'
    email_contrasena = 'pmlyqvghxzujzzem'
    email_receptor = 'marchemarcos@gmail.com'
    asunto = 'Archivo Excel'
    cuerpo = 'Adjunto archivo'
    em = EmailMessage()
    em['From'] = email_emisor
    em['To'] = email_receptor
    em['Subject'] = asunto
    em.set_content(cuerpo)
    with open('Paises.xlsx', 'rb') as f:
        file_data = f.read()
        em.add_attachment(file_data, maintype="application", subtype="xlsx", filename='Paises.xlsx')
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
            smtp.login(email_emisor, email_contrasena)
            smtp.sendmail(email_emisor, email_receptor, em.as_string())

data = requests.request("GET", url=url).json()
df = pd.json_normalize(data)
uploadData(data)
exportarExcelFile()
enviarMail()
