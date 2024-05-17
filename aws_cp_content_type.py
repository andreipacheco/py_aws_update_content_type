import boto3
import sys
import logging

# Configurar logging para salvar em arquivo extracao.log
logging.basicConfig(filename='extracao.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def process_objects(objects_to_process, bucket_name):
    # Connect to S3
    s3 = boto3.client('s3')

    # Variáveis para contagem
    total_files = len(objects_to_process)
    files_with_octet_stream = 0
    updated_files = 0

    # Iterar sobre as chaves dos objetos e atualizar o content-type
    for key in objects_to_process:
        print(f"Processando objeto '{key}'...")
        # Verificar se o objeto já teve o content-type corrigido
        content_type = s3.head_object(Bucket=bucket_name, Key=key).get('ContentType', '')

        if content_type == 'application/octet-stream':
            files_with_octet_stream += 1

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

        # Obter a data de modificação do objeto
        last_modified = s3.head_object(Bucket=bucket_name, Key=key).get('LastModified', '')
        if last_modified:
            last_modified_str = last_modified.strftime('%Y-%m-%d %H:%M:%S')
            print(f"Data de modificação de '{key}': {last_modified_str}")

        try:
            s3.copy_object(
                Bucket=bucket_name,
                Key=key,
                CopySource={'Bucket': bucket_name, 'Key': key},
                ContentType=content_type,
                MetadataDirective='REPLACE'
            )
            print(f"Content-type de '{key}' atualizado com sucesso para '{content_type}'.")
            updated_files += 1
        except Exception as e:
            print(f"Erro ao atualizar content-type de '{key}': {str(e)}")

    return total_files, files_with_octet_stream, updated_files

def count_total_pages(paginator, bucket_name, prefix):
    total_pages = 0
    for _ in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        total_pages += 1
    return total_pages

def update_content_type(bucket_name, prefix):
    # Connect to S3
    s3 = boto3.client('s3')

    print("Iniciando...")

    try:
        # Definindo o parâmetro de paginação inicial com ordenação por data
        paginator = s3.get_paginator('list_objects_v2')
        print("Contando total de páginas, aguarde...")
        total_pages = count_total_pages(paginator, bucket_name, prefix)
        page_iterator = paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix
        )

        total_octet_stream_files = 0
        total_files = 0
        total_updated_files = 0

        # Iterar sobre todas as páginas de resultados
        for idx, page in enumerate(page_iterator, start=1):
            print(f"Lendo a página {idx}/{total_pages}...")
            if 'Contents' in page:
                # Adicionar todas as chaves dos objetos à lista
                objects_to_process = [obj['Key'] for obj in page['Contents']]
                print(f"Processando {len(objects_to_process)} objetos nesta página...")
                # Processar os objetos desta página e obter contagens
                page_total_files, page_octet_stream_files, page_updated_files = process_objects(objects_to_process, bucket_name)
                # Atualizar contagens totais
                total_files += page_total_files
                total_octet_stream_files += page_octet_stream_files
                total_updated_files += page_updated_files

                # Registrar informações da página processada no log
                log_message = (f"Página {idx}/{total_pages} processada. "
                               f"Total de arquivos: {page_total_files}, "
                               f"Arquivos com 'application/octet-stream': {page_octet_stream_files}, "
                               f"Arquivos atualizados: {page_updated_files}")
                print(log_message)
                logging.info(log_message)

        # Registrar total de páginas processadas no log
        total_pages_message = f"Total de páginas processadas: {total_pages}"
        print(total_pages_message)
        logging.info(total_pages_message)

        print("Processamento de arquivos concluído.")
        print(f"Total de arquivos: {total_files}")
        print(f"Total de arquivos com 'application/octet-stream': {total_octet_stream_files}")
        print(f"Total de arquivos atualizados: {total_updated_files}")

    except Exception as e:
        error_message = f"Erro durante a iteração sobre as páginas de resultados: {str(e)}"
        print(error_message)
        logging.error(error_message)
        sys.exit(1)

if __name__ == '__main__':
    bucket_name = 'bucket'  # Nome do bucket
    prefix = 'pasta/'  # Prefixo da pasta
    update_content_type(bucket_name, prefix)
