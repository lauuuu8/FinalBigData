import boto3
import requests
import datetime

BUCKET_NAME = "eltiempop"  # Cambiá esto si usás otro bucket

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

# Diccionario de medios y sus URLs principales
NEWS_SITES = {
    "eltiempo": "https://www.eltiempo.com/",
    "publimetro": "https://www.publimetro.co/"
}

def download_headlines():
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    s3_client = boto3.client("s3")

    for site_name, url in NEWS_SITES.items():
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                file_name = f"contenido-{today}.html"
                temp_file = f"/tmp/{site_name}-{file_name}"
                s3_key = f"headlines/raw/{site_name}-{file_name}"
                
                with open(temp_file, "w", encoding="utf-8") as file:
                    file.write(response.text)

                s3_client.upload_file(temp_file, BUCKET_NAME, s3_key)
                print(f"Subido: s3://{BUCKET_NAME}/{s3_key}")
            else:
                print(f"Error descargando {site_name}: {response.status_code}")
        except Exception as e:
            print(f"Error con {site_name}: {str(e)}")

def lambda_handler(event, context):
    download_headlines()
    return {}
