import boto3
import csv
import datetime
from bs4 import BeautifulSoup

s3_client = boto3.client("s3")
BUCKET_DESTINO = "recibiendo"  # cambia si usas otro bucket

def app(event, context):
    bucket_origen = event["Records"][0]["s3"]["bucket"]["name"]
    archivo_html = event["Records"][0]["s3"]["object"]["key"]
    print(f"Procesando archivo: {archivo_html} desde {bucket_origen}")

    # Descargar archivo HTML desde S3
    response = s3_client.get_object(Bucket=bucket_origen, Key=archivo_html)
    contenido_html = response["Body"].read().decode("utf-8")
    soup = BeautifulSoup(contenido_html, "html.parser")

    # Detectar nombre del periódico por el nombre del archivo
    if "espectador" in archivo_html.lower():
        periodico = "elespectador"
    else:
        periodico = "eltiempo"

    noticias = []

    # Extraer noticias según el periódico
    if periodico == "eltiempo":
        for noticia in soup.select("article"):
            categoria = noticia.find("a", class_="category")
            titulo = noticia.find("h3")
            link = noticia.find("a", href=True)

            if categoria and titulo and link:
                noticias.append({
                    "Categoria": categoria.text.strip(),
                    "Titular": titulo.text.strip(),
                    "Link": "https://www.eltiempo.com" + link["href"]
                })
    elif periodico == "elespectador":
        for noticia in soup.select("section article"):
            categoria = noticia.find("span", class_="section")
            titulo = noticia.find("h2") or noticia.find("h3")
            link = noticia.find("a", href=True)

            if categoria and titulo and link:
                noticias.append({
                    "Categoria": categoria.text.strip(),
                    "Titular": titulo.text.strip(),
                    "Link": link["href"] if link["href"].startswith("http") else "https://www.elespectador.com" + link["href"]
                })

    # Fecha para la ruta
    hoy = datetime.datetime.now()
    year = hoy.strftime("%Y")
    month = hoy.strftime("%m")
    day = hoy.strftime("%d")

    nombre_csv = archivo_html.replace(".html", ".csv").split("/")[-1]
    ruta_local = f"/tmp/{nombre_csv}"
    ruta_s3 = f"headlines/final/periodico={periodico}/year={year}/month={month}/day={day}/{nombre_csv}"

    # Crear CSV
    with open(ruta_local, "w", newline="", encoding="utf-8-sig") as csvfile:
        campos = ['Categoria', 'Titular', 'Link']
        writer = csv.DictWriter(csvfile, fieldnames=campos)
        writer.writeheader()
        writer.writerows(noticias)

    # Subir CSV a S3
    s3_client.upload_file(ruta_local, BUCKET_DESTINO, ruta_s3)
    print(f"CSV subido a s3://{BUCKET_DESTINO}/{ruta_s3}")

    return {}
