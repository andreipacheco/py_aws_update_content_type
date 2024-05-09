import boto3

def update_content_type(bucket_name, prefix):
    # Connect to S3
    s3 = boto3.client('s3')

    # List objects in the bucket
    print("Listando objetos no bucket...")
    objetos_corrigidos = set()
    list_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Se não houver objetos no bucket, retorne
    if 'Contents' not in list_objects:
        print("Nenhum objeto encontrado no bucket.")
        return

    print("Atualizando content-type dos objetos...")

    # Iterando sobre os objetos e atualizando content-type
    for obj in list_objects['Contents']:
        key = obj['Key']
        
        # Verificando se o objeto já foi corrigido
        content_type = s3.head_object(Bucket=bucket_name, Key=key).get('ContentType', '')

        # Se o content-type já foi corrigido, pule para o próximo
        if content_type != '' and content_type != 'application/octet-stream':
            print(f"O arquivo '{key}' já teve o content-type corrigido. Ignorando...")
            continue

        # Extrair extensão do arquivo
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

if __name__ == '__main__':
    bucket_name = 'bucket' #Bucket
    prefix = 'pasta/' # Pasta
    update_content_type(bucket_name, prefix)
