import boto3
import sys

def process_objects(objects_to_process, bucket_name, prefix):
    # Connect to S3
    s3 = boto3.client('s3')

    # Variáveis para contagem
    total_files = len(objects_to_process)
    files_with_octet_stream = 0

    # Iterar sobre as chaves dos objetos e atualizar o content-type
    for key in objects_to_process:
        full_key = f"{prefix}/{key}"
        print(f"Processando objeto '{full_key}'...")
        
        try:
            # Verificar se o objeto já teve o content-type corrigido
            content_type = s3.head_object(Bucket=bucket_name, Key=full_key).get('ContentType', '')

            if content_type == 'application/octet-stream':
                files_with_octet_stream += 1

            if content_type != '' and content_type != 'application/octet-stream':
                print(f"O arquivo '{full_key}' já teve o content-type corrigido. Ignorando...")
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
                print(f"Ignorando arquivo '{full_key}' com extensão desconhecida '{extension}'.")
                continue  # Ignorar arquivos com extensões desconhecidas

            # Atualizar content-type
            print(f"Atualizando content-type de '{full_key}' para '{content_type}'...")

            # Obter a data de modificação do objeto
            last_modified = s3.head_object(Bucket=bucket_name, Key=full_key).get('LastModified', '')
            if last_modified:
                last_modified_str = last_modified.strftime('%Y-%m-%d %H:%M:%S')
                print(f"Data de modificação de '{full_key}': {last_modified_str}")

            s3.copy_object(
                Bucket=bucket_name,
                Key=full_key,
                CopySource={'Bucket': bucket_name, 'Key': full_key},
                ContentType=content_type,
                MetadataDirective='REPLACE'
            )
            print(f"Content-type de '{full_key}' atualizado com sucesso para '{content_type}'.")

        except s3.exceptions.NoSuchKey:
            print(f"Chave '{full_key}' não encontrada. Ignorando...")

        except Exception as e:
            print(f"Erro ao processar '{full_key}': {str(e)}")

    return total_files, files_with_octet_stream

def update_content_type(bucket_name, prefix, keys_to_update):
    # Connect to S3
    s3 = boto3.client('s3')

    print(f"Iniciando...")

    try:
        total_octet_stream_files = 0
        total_files = 0

        # Processar os objetos fornecidos na lista keys_to_update
        print(f"Processando {len(keys_to_update)} objetos...")
        page_total_files, page_octet_stream_files = process_objects(keys_to_update, bucket_name, prefix)
        # Atualizar contagens totais
        total_files += page_total_files
        total_octet_stream_files += page_octet_stream_files

        print("Processamento de arquivos concluído.")
        print(f"Total de arquivos: {total_files}")
        print(f"Total de arquivos com 'application/octet-stream': {total_octet_stream_files}")

    except Exception as e:
        print(f"Erro durante o processamento dos objetos: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    bucket_name = 'bucket'  # Nome do bucket
    prefix = 'public'  # Prefixo da pasta
    keys_to_update = ['arquivo1.pdf','teste.pdf', 'arquivo2.pdf', 'arquivo3.pdf', 'arquivo4.pdf', 'arquivo5.png']  # Lista de chaves a serem atualizadas
    update_content_type(bucket_name, prefix, keys_to_update)
