import boto3
import sys

def count_objects(objects_to_count, bucket_name):
    # Connect to S3
    s3 = boto3.client('s3')

    # Variáveis para contagem
    total_files = len(objects_to_count)
    files_with_octet_stream = 0

    # Iterar sobre as chaves dos objetos e verificar o content-type
    for key in objects_to_count:
        # Verificar o content-type do objeto
        content_type = s3.head_object(Bucket=bucket_name, Key=key).get('ContentType', '')

        # Contar arquivos com content-type 'application/octet-stream'
        if content_type == 'application/octet-stream':
            files_with_octet_stream += 1

    return total_files, files_with_octet_stream

def count_content_type(bucket_name, prefix):
    # Connect to S3
    s3 = boto3.client('s3')

    print(f"Iniciando contagem...")

    try:
        # Definindo o parâmetro de paginação inicial com ordenação por data
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix,
            PaginationConfig={'Sort': 'descending'}  # Ordenar por data descendente
        )

        total_octet_stream_files = 0
        total_files = 0

        # Iterar sobre todas as páginas de resultados
        for idx, page in enumerate(page_iterator, start=1):
            print(f"Lendo a página {idx}...")
            if 'Contents' in page:
                # Adicionar todas as chaves dos objetos à lista
                objects_to_count = [obj['Key'] for obj in page['Contents']]
                print(f"Contando {len(objects_to_count)} objetos nesta página...")
                # Contar objetos nesta página
                page_total_files, page_octet_stream_files = count_objects(objects_to_count, bucket_name)
                # Atualizar contagens totais
                total_files += page_total_files
                total_octet_stream_files += page_octet_stream_files

        print("Contagem de arquivos concluída.")
        print(f"Total de arquivos: {total_files}")
        print(f"Total de arquivos com 'application/octet-stream': {total_octet_stream_files}")

    except Exception as e:
        print(f"Erro durante a iteração sobre as páginas de resultados: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    bucket_name = 'bucket'  # Nome do bucket
    prefix = 'public/'  # Prefixo da pasta
    count_content_type(bucket_name, prefix)
