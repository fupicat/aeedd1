import csv
import os
import datetime
import sys
from unidecode import (
    unidecode,
)  # Biblioteca necessária para converter corretamente de UTF-8 para ASCII
import locale
import tempfile
import heapq
from typing import Dict, Optional, TextIO, Iterable, List, Tuple


# Mudo o locale pra inglês pra ele poder ler as datas de lançamento do dataset.
locale.setlocale(locale.LC_ALL, "en_US.UTF-8")


# Arquivos usados em cada passo de geração dos índices.
csv_original: str = "Google-Playstore.csv"  # Dataset original sem alterações.
csv_small: str = "playstore_small.csv"  # Dataset reduzido, transformado em ascii, e com tamanhos fixos para cada entrada.
csv_ordered: str = "playstore_ordered.csv"  # Dataset reduzido ordenado pela chave primária (app_id) em ordem alfabética.
csv_ordered_time: str = "playstore_ordered_time.csv"  # Dataset reduzido ordenado pela data de lançamento em ordem crescente.


bin_data: str = "playstore_binary.dat"  # Arquivo binário com todos os dados.
entry_size: int = 197  # Tamanho de cada entrada em bytes.


# Índices em arquivo.
app_id_index: str = "app_id_index.dat"  # Índice de ids de aplicativos.
app_index_entry_size: int = 5  # Tamanho de cada entrada no índice de ids de aplicativo.
date_index: str = "date_index.dat"  # Índice de datas de lançamento.
date_index_entry_size: int = (
    68  # Tamanho de cada entrada no índice de datas de lançamento.
)


########################
## Limpeza do dataset ##
########################

# Esse passo serve para deixar o dataset menor e mais fácil de trabalhar, só lidando com
# os quatro campos que eu vou usar: app_id (chave primária), categoria, desenvolvedor, e
# data de lançamento.
# Além disso, ele também prepara o dataset para a sua transformação em arquivo binário
# de tamanhos fixos, transformando todas as strings em ASCII (para que cada caractere seja
# um byte) e com limite de 64 caracteres, adicionando espaços caso necessário.

if not os.path.exists(csv_small):
    print("Criando CSV reduzido...")
    with open(csv_small, "w", encoding="ascii") as output:
        csvwriter = csv.writer(output, lineterminator="\n")
        with open(csv_original, encoding="utf8") as csvfile:
            first_row = False
            csvreader = csv.reader(csvfile)
            # Para cada linha do dataset
            for row in csvreader:
                # Exceto a primeira
                if not first_row:
                    first_row = True
                    continue
                # Campo 1 (chave)
                app_id = (
                    unidecode(row[1])
                    .lower()
                    .encode("ascii")
                    .ljust(64, b" ")[:64]
                    .decode("ascii")
                )
                # Campo 2
                category = (
                    unidecode(row[2])
                    .lower()
                    .encode("ascii")
                    .ljust(64, b" ")[:64]
                    .decode("ascii")
                )
                # Campo 3
                developer_id = (
                    unidecode(row[13])
                    .lower()
                    .encode("ascii")
                    .ljust(64, b" ")[:64]
                    .decode("ascii")
                )
                # Campo 4
                release_date = int(
                    datetime.datetime.strptime(row[16], "%b %d, %Y").timestamp()
                    if row[16]
                    else 0
                )
                csvwriter.writerow([app_id, category, developer_id, release_date])


#################################
## Ordenação da chave primária ##
#################################

# Esse passo ordena o dataset pelo id do aplicativo em ordem alfabética.
# Ainda usando o formato CSV pois é mais fácil de trabalhar, e este
# código só precisa rodar uma vez.
# Aqui eu uso arquivos temporários para dividir o arquivo inicial enorme
# em arquivos menores, ordená-los em memória, e depois juntá-los.
# Para juntar os arquivos temporários eu estou usando uma função do
# Python chamada heap queue merge, que serve exatamente pra isso.
# Certo, eu não implementei do zero, mas convenhamos que eu teria só
# pesquisado a solução na internet de qualquer jeito.
# Eu tentei fazer uma solução própria do zero usando o método mais simples
# de tratar cada arquivo temporário como uma fila, e só ir escrevendo os
# valores menores, mas não consegui mesmo assim.

if not os.path.exists(csv_ordered):
    print("Ordenando arquivo csv...")

    # Número máximo de bytes por arquivo.
    chunk_size: int = 100000

    # Função pra alimentar entradas nos arquivos temporários.
    def read_entries(file: TextIO, size: int) -> Iterable[list[str]]:
        while True:
            chunk = file.readlines(size)
            if not chunk:
                break
            yield chunk

    # Abre o CSV reduzido.
    with open(csv_small, "r", encoding="ascii") as file:
        temp_files: List[str] = []

        # Lê e ordena entradas em arquivos temporários.
        for chunk in read_entries(file, chunk_size):
            reader = csv.reader(chunk)

            # A primeira coluna é a que tem o app_id, então é a chave da ordenação.
            sorted_rows = sorted(reader, key=lambda row: row[0])

            # Escreve a partição ordenada no arquivo temporário.
            temp_file = tempfile.NamedTemporaryFile(delete=False).name
            temp_files.append(temp_file)
            with open(temp_file, "w", encoding="ascii") as temp:
                writer = csv.writer(temp, lineterminator="\n")
                writer.writerows(sorted_rows)

        # Combina os arquivos temporários em um único arquivo.
        # Todas essas funções usam "iterables", o que significa que
        # os dados não são todos carregados na memória ao mesmo tempo.
        # O carregamento e processamento são feitos item por item.
        with open(csv_ordered, "w", encoding="ascii") as output:
            merged_chunks = heapq.merge(
                *[open(temp_file, "r", encoding="ascii") for temp_file in temp_files]
            )
            for chunk in merged_chunks:
                output.write(chunk)

        # Deleta os arquivos temporários.
        for temp_file in temp_files:
            os.remove(temp_file)


################################
## Geração do arquivo binário ##
################################

# Usa o CSV ordenado para gerar o arquivo binário que será usado para todas as consultas.
# O arquivo gerado tem o seguinte formato:
#
# app_id (64 bytes ASCII) + categoria (64 bytes ASCII) + desenvolvedor (64 bytes ASCII) +
# data_de_lançamento (unix timestamp, uint32 little endian, 4 bytes) + \n (1 byte)
#
# Ou seja, cada entrada tem 197 bytes de tamanho.
# Tecnicamente, como o arquivo tem entradas com tamanho fixo, eu não precisaria colocar
# o \n no final como separador, mas o PDF especificando o trabalho pediu.

if not os.path.exists(bin_data):
    print("Criando arquivo binário...")

    with open(bin_data, "wb") as output:
        with open(csv_ordered, encoding="ascii") as csvfile:
            csvreader = csv.reader(csvfile)
            for row in csvreader:
                app_id = row[0].encode("ascii").ljust(64, b" ")[:64]
                category = row[1].encode("ascii").ljust(64, b" ")[:64]
                developer_id = row[2].encode("ascii").ljust(64, b" ")[:64]
                release_date = int(row[3])
                line: bytes = (
                    app_id
                    + category
                    + developer_id
                    + release_date.to_bytes(4, "little", signed=False)
                    + b"\n"
                )

                # Essa linha certifica se todas as entradas têm o tamanho certo.
                if len(line) != entry_size:
                    print(
                        f"Erro de tamanho: {app_id.decode('ascii').strip()} tem {len(line)} bytes"
                    )
                    quit(1)
                output.write(line)


##############################
## Busca binária no arquivo ##
##############################

# Este passo implementa e testa uma busca binária no arquivo binário recém-criado.


# Função que decodifica uma entrada do arquivo binário.
# Recebe bytes e devolve uma lista com cada campo decodificado, e as strings com os
# espaços extra removidos.
def decode_entry(
    entry: bytes,
) -> Optional[
    Tuple[Optional[str], Optional[str], Optional[str], Optional[datetime.datetime]]
]:
    if not entry:
        return None
    app_id = entry[:64].decode("ascii").strip()
    category = entry[64:128].decode("ascii").strip()
    developer_id = entry[128:192].decode("ascii").strip()
    release_date_timestamp = int.from_bytes(entry[192:196], "little", signed=False)
    release_date = (
        datetime.datetime.fromtimestamp(release_date_timestamp)
        if release_date_timestamp > 0
        else None
    )
    return app_id, category, developer_id, release_date


# Função de busca binária.
# Recebe uma chave (app_id) para pesquisar, e, opcionalmente, uma entrada mínima e máxima
# para a busca.
# Caso encontrar, retorna uma lista com a entrada decodificada, e a sua posição na lista de entradas.
def binary_search_in_datafile(
    target_key: str,
    starting_lower_bound: int = 0,
    starting_upper_bound: int = -1,
) -> Tuple[
    Tuple[Optional[str], Optional[str], Optional[str], Optional[datetime.datetime]]
    | None,
    int,
]:
    # Pega o tamanho do arquivo e divide para obter a quantidade de entradas.
    file_size: int = os.path.getsize(bin_data)
    last_entry: int = file_size // entry_size

    # Codifica a chave de busca para comparar com as chaves do arquivo.
    encoded_key = target_key.lower().encode("ascii").ljust(64, b" ")[:64]

    with open(bin_data, "rb") as file:
        lower_bound: int = starting_lower_bound
        upper_bound: int = (
            last_entry if starting_upper_bound == -1 else starting_upper_bound
        )
        last_midpoint: int = -1

        while lower_bound < upper_bound:
            midpoint: int = (lower_bound + upper_bound) // 2
            # Certifica que o ponto do meio não vai ficar preso.
            if midpoint == last_midpoint:
                break
            last_midpoint = midpoint
            # Vai até a posição em bytes do arquivo em que a entrada do meio está, e lê.
            file.seek(midpoint * entry_size)
            entry = file.read(entry_size)

            # Extrai a chave para comparação.
            key = entry[:64]
            if key == encoded_key:
                return decode_entry(entry), midpoint

            if key < encoded_key:
                lower_bound = midpoint
            else:
                upper_bound = midpoint

        # Nenhuma entrada encontrada.
        return None, last_midpoint


# Teste da função.
print("##############################")
print("## Busca binária no arquivo ##")
print("##############################")

# Obtém informações sobre o aplicativo "Fruit Ninja Classic".
app_id = "com.halfbrick.fruitninja"
print(f"Procurando aplicativo com id {app_id}")
result, position = binary_search_in_datafile(app_id)

if result:
    app_id, category, developer_id, release_date = result
    print("Aplicativo encontrado!")
    print(f"App ID: {app_id}")
    print(f"Categoria: {category}")
    print(f"Desenvolvedor: {developer_id}")
    print(f"Data de lançamento: {release_date}")
else:
    print(f"Entrada não encontrada, posição do último item checado: {position}.")

######################
## Índice de App ID ##
######################

# Cria um índice sobre as chaves primárias.
# É uma simples lista de letras e o número da primeira entrada que começa com aquela letra.
# Cada entrada tem 5 bytes: 1 byte pra letra e 4 bytes para o número uint32 little endian.


# Função que obtém uma entrada de acordo com a posição dela no arquivo binário.
def get_entry_by_number(
    number: int,
) -> Optional[
    Tuple[Optional[str], Optional[str], Optional[str], Optional[datetime.datetime]]
]:
    if number < 0:
        return None
    with open(bin_data, "rb") as file:
        file.seek(number * entry_size)
        return decode_entry(file.read(entry_size))


# Criando o índice.
if not os.path.exists(app_id_index):
    print("Criando arquivo de índice de app id...")
    with open(app_id_index, "wb") as output:
        letters = "abcdefghijklmnopqrstuvwxyz"
        with open(bin_data, "rb") as file:
            entry_number = 0
            for letter in letters:
                # Escreve a letra.
                output.write(letter.encode("ascii"))
                entry_first_letter = ""
                # Procura a primeira entrada que começa com aquela letra.
                while not entry_first_letter == letter:
                    file.seek(entry_number * entry_size)
                    entry = file.read(1)
                    entry_first_letter = entry.decode("ascii")
                    entry_number += 1
                # Escreve a posição da entrada.
                output.write(int.to_bytes(entry_number - 1, 4, "little", signed=False))


# Realiza busca binária no índice de app_ids.
# Exatamente a mesma lógica da função de busca binária anterior, mas ao invés
# de ser direto no arquivo binário, é feito no índice.
# Aceita um app_id como entrada, e retorna a posição do primeiro e último
# itens que começam com a mesma primeira letra que o app_id dado, para reduzir
# os itens que devem ser buscados na busca binária pelo arquivo.
def binary_search_in_appid_index(
    target_key: str,
) -> Tuple[int, int]:
    file_size: int = os.path.getsize(app_id_index)
    last_entry: int = file_size // app_index_entry_size

    with open(app_id_index, "rb") as file:
        lower_bound: int = 0
        upper_bound: int = last_entry
        last_midpoint: int = -1

        encoded_key = target_key.lower().encode("ascii")[0]

        while lower_bound < upper_bound:
            midpoint: int = (lower_bound + upper_bound) // 2
            if midpoint == last_midpoint:
                break
            last_midpoint = midpoint
            file.seek(midpoint * app_index_entry_size)
            entry = file.read(app_index_entry_size)

            key = entry[0]
            if key == encoded_key:
                file.seek((midpoint + 1) * app_index_entry_size)
                upper_bound_entry = file.read(app_index_entry_size)
                return int.from_bytes(entry[1:5], "little", signed=False), (
                    (int.from_bytes(upper_bound_entry[1:5], "little", signed=False) - 1)
                    if upper_bound_entry
                    else -1
                )

            if key < encoded_key:
                lower_bound = midpoint
            else:
                upper_bound = midpoint

        # Se não achar nenhuma letra, retorna os valores padrões de limite menor e maior
        # da busca: as posições do primeiro e último itens da lista inteira.
        return 0, -1


# Função que usa busca binária em ambos o índice e o arquivo binário
# para obter uma entrada de acordo com o app_id fornecido.
# Retorna a entrada decodificada.
def get_entry_by_app_id(
    app_id: str,
) -> Optional[
    Tuple[Optional[str], Optional[str], Optional[str], Optional[datetime.datetime]]
]:
    lower, upper = binary_search_in_appid_index(app_id)
    result, _ = binary_search_in_datafile(app_id, lower, upper)
    return result


# Teste da função.
print("###################################")
print("## Busca de app id usando índice ##")
print("###################################")

# Pesquisa informações do aplicativo "Roblox".
app_id = "com.roblox.client"
print(f"Procurando aplicativo com id {app_id} usando o índice de chave primária.")
result = get_entry_by_app_id(app_id)
if result:
    app_id, category, developer_id, release_date = result
    print("Aplicativo encontrado!")
    print(f"ID: {app_id}")
    print(f"Categoria: {category}")
    print(f"Desenvolvedor: {developer_id}")
    print(f"Data de lançamento: {release_date}")
else:
    print("Aplicativo não encontrado.")


##################################
## Índice de data de lançamento ##
##################################

# Cria mais um CSV ordenado por data de lançamento, e depois converte para um novo arquivo
# binário, com o seguinte formato:
# Data de lançamento (unix timestamp, uint32 little endian, 4 bytes) +
# App ID (64 bytes ASCII)
# Então, cada item nesse índice tem 68 bytes.
# Esse índice serve para consultas em relação à data de lançamento dos aplicativos, como
# quais aplicativos foram lançados em um dia específico, ou quantas entradas não fornecem
# essa informação.

if not os.path.exists(date_index):
    print("Criando índice de data...")

    input_file = csv_ordered
    output_file = csv_ordered_time

    sort_column_index = 3

    chunk_size = 100000

    # Ordena e combina os arquivos.
    def sort_and_merge(input_files, output_file):
        with open(output_file, "w", newline="") as output_fh:
            writer = csv.writer(output_fh)

            # Aqui, o programa usa a heap diretamente para manter os arquivos em ordem.
            # Não tenho certeza do porquê, mas para esses dados, simplesmente usar
            # heapq.merge não ordenou corretamente.
            heap = []
            for file in input_files:
                reader = csv.reader(file)
                row = next(reader, None)
                if row is not None:
                    heapq.heappush(heap, (row, reader, file))

            while heap:
                row, reader, file = heapq.heappop(heap)
                writer.writerow(row)
                next_row = next(reader, None)
                if next_row is not None:
                    heapq.heappush(heap, (next_row, reader, file))
                else:
                    file.close()

    # Lê e ordena o arquivo de dados em partições.
    with open(input_file, "r") as input_fh:
        reader = csv.reader(input_fh)
        chunks = []
        for i, chunk in enumerate(iter(lambda: list(csv.reader(input_fh)), [])):
            chunk.sort(key=lambda row: row[sort_column_index])
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            with open(temp_file.name, "w", newline="") as temp_fh:
                writer = csv.writer(temp_fh)
                writer.writerows(chunk)
            chunks.append(temp_file)

        # Ordena as partições de 2 em 2 até terminar.
        while len(chunks) > 1:
            new_chunks = []
            for i in range(0, len(chunks), 2):
                if i + 1 < len(chunks):
                    temp_file = tempfile.NamedTemporaryFile(delete=False)
                    sort_and_merge([chunks[i], chunks[i + 1]], temp_file.name)
                    new_chunks.append(temp_file)
                else:
                    new_chunks.append(chunks[i])
            chunks = new_chunks

        # Escreve no arquivo final.
        with open(output_file, "w", newline="") as output_fh:
            writer = csv.writer(output_fh)

            for chunk in chunks:
                with open(chunk.name, "r") as chunk_fh:
                    reader = csv.reader(chunk_fh)
                    writer.writerows(reader)

            # Remove arquivos temporários.
            for chunk in chunks:
                chunk.close()
                os.remove(chunk.name)

    input_file = csv_ordered_time
    output_file = date_index

    # Converte o CSV ordenado por data para o formato binário.
    with open(input_file, "r") as csv_file, open(output_file, "wb") as binary_file:
        csv_reader = csv.reader(csv_file)

        for row in csv_reader:
            release_date = int(row[3]).to_bytes(4, "little", signed=False)
            app_id = row[0].encode("ascii")

            binary_file.write(release_date + app_id)


# Recebe uma data em formato unix timestamp e retorna uma lista de app_ids de
# aplicativos lançados naquele dia usando pesquisa binária.
def binary_search_in_date_index(
    target_key: int,
) -> list[str]:
    file_size: int = os.path.getsize(date_index)
    last_entry: int = file_size // date_index_entry_size

    with open(date_index, "rb") as file:
        lower_bound: int = 0
        upper_bound: int = last_entry
        last_midpoint: int = -1

        while True:
            midpoint: int = (lower_bound + upper_bound) // 2
            if midpoint == last_midpoint:
                break
            last_midpoint = midpoint
            file.seek(midpoint * date_index_entry_size)
            entry = file.read(date_index_entry_size)
            key = int.from_bytes(entry[0:4], "little", signed=False)

            # Após achar uma entrada com essa data...
            if key == target_key:
                # Volte para trás até achar uma entrada que NÃO tem essa data.
                while key == target_key and midpoint > 0:
                    midpoint -= 1
                    file.seek(midpoint * date_index_entry_size)
                    entry = file.read(date_index_entry_size)
                    key = int.from_bytes(entry[0:4], "little", signed=False)

                result = []
                # Adicione ao resultado o primeiro app_id do arquivo, se estivermos nele.
                if midpoint == 0:
                    result.append(entry[4:].decode("ascii"))
                midpoint += 1
                file.seek(midpoint * date_index_entry_size)
                entry = file.read(date_index_entry_size)
                key = int.from_bytes(entry[0:4], "little", signed=False)

                # Vá para frente até achar a próxima entrada que NÃO tem essa data, ou seja,
                # a data que vem depois dessa, e vá adicionando todos os IDs ao resultado.
                while key == target_key and midpoint < last_entry:
                    midpoint += 1
                    file.seek(midpoint * date_index_entry_size)
                    entry = file.read(date_index_entry_size)
                    key = int.from_bytes(entry[0:4], "little", signed=False)
                    result.append(entry[4:].decode("ascii"))
                return result
            elif key < target_key:
                lower_bound = midpoint + 1
            else:
                upper_bound = midpoint

        return []


# Função que retorna uma lista de app_ids de aplicativos lançados no dia, mês e ano dados.
def entries_released_in_date(day: int, month: int, year: int) -> list[str]:
    return binary_search_in_date_index(
        int(datetime.datetime(year, month, day).timestamp())
    )


# Função que retorna uma lista de app_ids de aplicativos sem data de lançamento especificada.
def entries_with_no_date() -> list[str]:
    return binary_search_in_date_index(0)


# Teste das funções.
print("##################################")
print("## Índice de data de lançamento ##")
print("##################################")

# Quantos aplicativos foram lançados em 1/1/2020?
print(
    f"{len(entries_released_in_date(1, 1, 2020))} aplicativos foram lançados no dia 1 de janeiro de 2020."
)
# Quantos aplicativos não tem data de lançamento?
print(
    f"Há {len(entries_with_no_date())} aplicativos sem data de lançamento registrada."
)

############################################
## Índice de desenvolvedores (em memória) ##
############################################

# Esse índice é um simples dicionário. Um nome de desenvolvedor se relaciona com
# uma lista de app_ids lançados por ele.


def create_developer_index() -> Dict[str, List[str]]:
    print("Criando índice de desenvolvedores...")
    developer_index: Dict[str, List[str]] = {}

    # Passa pelo arquivo binário procurando por desenvolvedores novos,
    # ou aplicativos novos para adicionar ao índice.
    with open(bin_data, "rb") as file:
        while True:
            entry = file.read(entry_size)
            if not entry:
                break
            app_id, _, dev_name, _ = decode_entry(entry)
            if dev_name not in developer_index:
                developer_index[dev_name] = []
            developer_index[dev_name].append(app_id)

    return developer_index


developer_index = create_developer_index()


# Função que retorna uma lista de app_ids de aplicativos de um desenvolvedor.
def apps_created_by(developer: str) -> list[Tuple[str, str, str, int]]:
    if developer not in developer_index:
        return []
    result = []
    for app_id in developer_index[developer]:
        result.append(get_entry_by_app_id(app_id))
    return result


# Teste da função.
print("###############################")
print("## Índice de desenvolvedores ##")
print("###############################")

# Quantos e quais aplicativos foram desenvolvidos pela empresa Mojang?
target_dev = "mojang"
print(f"Procurando aplicativos desenvolvidos por {target_dev}.")
apps_created = apps_created_by(target_dev)
print(
    f"O desenvolvedor {target_dev} já publicou {len(apps_created)} aplicativos. Sendo esses:"
)
for app in apps_created:
    print(f"- {app[0]}")


#######################################################
## Índice de aplicativos com árvore AVL (em memória) ##
#######################################################

# Uma árvore balanceada em que cada nó tem uma chave, que é o nome da categoria,
# e uma lista contendo app_ids dos aplicativos que pertencem a essa categoria.
# Código adaptado de: https://www.programiz.com/dsa/avl-tree


# Create a tree node
class TreeNode(object):
    def __init__(self, key: str, contents: List[str]):
        self.key = key
        self.contents = contents
        self.left = None
        self.right = None
        self.height = 1


class AVLTree(object):
    # Function to insert a node
    def insert(self, root, key: str, contents: List[str]):
        # Find the correct location and insert the node
        if not root:
            return TreeNode(key, contents)
        elif key < root.key:
            root.left = self.insert(root.left, key, contents)
        else:
            root.right = self.insert(root.right, key, contents)

        root.height = 1 + max(self.getHeight(root.left), self.getHeight(root.right))

        # Update the balance factor and balance the tree
        balanceFactor = self.getBalance(root)
        if balanceFactor > 1:
            if key < root.left.key:
                return self.rightRotate(root)
            else:
                root.left = self.leftRotate(root.left)
                return self.rightRotate(root)

        if balanceFactor < -1:
            if key > root.right.key:
                return self.leftRotate(root)
            else:
                root.right = self.rightRotate(root.right)
                return self.leftRotate(root)

        return root

    def search(self, root, key):
        if not root:
            return None
        elif key < root.key:
            return self.search(root.left, key)
        elif key > root.key:
            return self.search(root.right, key)
        else:
            return root

    # Function to delete a node
    def delete(self, root, key):
        # Find the node to be deleted and remove it
        if not root:
            return root
        elif key < root.key:
            root.left = self.delete(root.left, key)
        elif key > root.key:
            root.right = self.delete(root.right, key)
        else:
            if root.left is None:
                temp = root.right
                root = None
                return temp
            elif root.right is None:
                temp = root.left
                root = None
                return temp
            temp = self.getMinValueNode(root.right)
            root.key = temp.key
            root.right = self.delete(root.right, temp.key)
        if root is None:
            return root

        # Update the balance factor of nodes
        root.height = 1 + max(self.getHeight(root.left), self.getHeight(root.right))

        balanceFactor = self.getBalance(root)

        # Balance the tree
        if balanceFactor > 1:
            if self.getBalance(root.left) >= 0:
                return self.rightRotate(root)
            else:
                root.left = self.leftRotate(root.left)
                return self.rightRotate(root)
        if balanceFactor < -1:
            if self.getBalance(root.right) <= 0:
                return self.leftRotate(root)
            else:
                root.right = self.rightRotate(root.right)
                return self.leftRotate(root)
        return root

    # Function to perform left rotation
    def leftRotate(self, z):
        y = z.right
        T2 = y.left
        y.left = z
        z.right = T2
        z.height = 1 + max(self.getHeight(z.left), self.getHeight(z.right))
        y.height = 1 + max(self.getHeight(y.left), self.getHeight(y.right))
        return y

    # Function to perform right rotation
    def rightRotate(self, z):
        y = z.left
        T3 = y.right
        y.right = z
        z.left = T3
        z.height = 1 + max(self.getHeight(z.left), self.getHeight(z.right))
        y.height = 1 + max(self.getHeight(y.left), self.getHeight(y.right))
        return y

    # Get the height of the node
    def getHeight(self, root):
        if not root:
            return 0
        return root.height

    # Get balance factore of the node
    def getBalance(self, root):
        if not root:
            return 0
        return self.getHeight(root.left) - self.getHeight(root.right)

    def getMinValueNode(self, root):
        if root is None or root.left is None:
            return root
        return self.getMinValueNode(root.left)

    def preOrder(self, root):
        if not root:
            return
        print(f"{root.key} ", end="")
        self.preOrder(root.left)
        self.preOrder(root.right)

    # Print the tree
    def printHelper(self, currPtr, indent, last):
        if currPtr != None:
            sys.stdout.write(indent)
            if last:
                sys.stdout.write("R----")
                indent += "     "
            else:
                sys.stdout.write("L----")
                indent += "|    "
            print(currPtr.key)
            self.printHelper(currPtr.left, indent, False)
            self.printHelper(currPtr.right, indent, True)


print("Criando índice de categorias...")

# Primeiro eu crio um dicionário, assim como o índice de desenvolvedores, para
# depois inserir na árvore.
category_index: Dict[str, List[str]] = {}

with open(bin_data, "rb") as file:
    while True:
        entry = file.read(entry_size)
        if not entry:
            break
        app_id, category, _, _ = decode_entry(entry)
        if category not in category_index:
            category_index[category] = []
        category_index[category].append(app_id)

category_tree = AVLTree()
root = None
for category, app_ids in category_index.items():
    root = category_tree.insert(root, category, app_ids)

print("#######################################################")
print("## Índice de aplicativos com árvore AVL (em memória) ##")
print("#######################################################")

print("Visualização da árvore:")
category_tree.printHelper(root, "", True)

target_category = "food & drink"
print(f"Pesquisando aplicativos na categoria {target_category}:")
social = category_tree.search(root, target_category)
print(
    f"Existem {len(social.contents)} aplicativos na categoria {target_category}. Por exemplo:"
)
for i in range(5):
    print(f"- {social.contents[i]}")

print("#########")
print("## Fim ##")
print("#########")
