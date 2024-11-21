import struct
from btree import Pair

def read_page(file, page_id, page_size):
    file.seek(page_id * page_size)
    return file.read(page_size)

def decode_pairs(page):
    pairs = []
    offset = 8  # ページの先頭にはメタデータがあるため、データはオフセット8から始まる
    num_pairs = struct.unpack('>I', page[4:8])[0]
    for _ in range(num_pairs):
        pair_size = struct.unpack('>I', page[offset:offset+4])[0]
        pair_data = page[offset+4:offset+4+pair_size]
        pair = Pair.from_bytes(pair_data)
        pairs.append(pair)
        offset += 4 + pair_size
    return pairs

def main():
    file_path = "simple.rly"
    page_size = 4096

    try:
        with open(file_path, "rb") as file:
            for page_id in range(4):  # 最初の10ページを確認
                page = read_page(file, page_id, page_size)
                print(f"Page {page_id}:")
                try:
                    if page.strip(b'\x00'):  # ページが空でない場合のみデコード
                        pairs = decode_pairs(page)
                        for pair in pairs:
                            key = struct.unpack('>Q', pair.key)[0]
                            value = pair.value.decode('utf-8', errors='replace')
                            print(f"Key: {key}, Value: {value}")
                    else:
                        print("Empty page")
                except Exception as e:
                    print(f"Could not decode page {page_id}: {e}")

    except FileNotFoundError:
        print(f"File {file_path} not found.")

if __name__ == "__main__":
    main()
