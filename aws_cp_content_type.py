import boto3
import sys

def update_content_type(bucket_name, prefix):
    # Connect to S3
    s3 = boto3.client('s3')

    # Lista para armazenar todas as chaves dos objetos
    all_objects = []

    print(f"Iniciando.")
    try:
        # Definindo o parâmetro de paginação inicial
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        # Iterar sobre todas as páginas de resultados
        for idx, page in enumerate(page_iterator, start=1):
            print(f"Lendo a página {idx}...")
            if 'Contents' in page:
                # Adicionar todas as chaves dos objetos à lista
                all_objects.extend([obj['Key'] for obj in page['Contents']])

        print(f"Total de {len(all_objects)} objetos encontrados no bucket.")

        # Iterar sobre as chaves dos objetos e atualizar o content-type
        for key in all_objects:
            print(f"Processando objeto '{key}'...")
            # Verificar se o objeto já teve o content-type corrigido
            content_type = s3.head_object(Bucket=bucket_name, Key=key).get('ContentType', '')

            if content_type != '' and content_type != 'application/octet-stream':
                print(f"O arquivo '{key}' já teve o content-type corrigido. Ignorando...")
                continue

            # Extrair a extensão do arquivo
            extension = key.split('.')[-1].lower()

            # Determinar content-type com base na extensão do arquivo
            if extension == 'pdf':
                content_type = 'application/pdf'
            elif extension in ('jpg', 'jpeg'):
                content_type = 'image/jpeg'
            elif extension == 'png':
                content_type = 'image/png'
            else:
                print(f"Ignorando arquivo '{key}' com extensão desconhecida '{extension}'.")
                continue  # Ignorar arquivos com extensões desconhecidas

            # Atualizar content-type
            print(f"Atualizando content-type de '{key}' para '{content_type}'...")
            try:
                s3.copy_object(
                    Bucket=bucket_name,
                    Key=key,
                    CopySource={'Bucket': bucket_name, 'Key': key},
                    ContentType=content_type,
                    MetadataDirective='REPLACE'
                )
                print(f"Content-type de '{key}' atualizado com sucesso para '{content_type}'.")
            except Exception as e:
                print(f"Erro ao atualizar content-type de '{key}': {str(e)}")

    except Exception as e:
        print(f"Erro durante a iteração sobre as páginas de resultados: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    bucket_name = 'pesqbrasil-pescadorprofissional-homol'  # Nome do bucket
    prefix = 'arquivos/'  # Prefixo da pasta
    update_content_type(bucket_name, prefix)
