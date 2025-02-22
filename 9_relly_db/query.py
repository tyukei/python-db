from typing import Iterator, List
import fmt
import memcmpable
import cmp

ESCAPE_LENGTH = 9

def encode(elems: Iterator[bytes], bytes_list: List[int]) -> None:
    """
    要素をエンコードしてバイトリストに追加します。

    :param elems: エンコードするバイト列のイテレーター
    :param bytes_list: エンコードされたバイトを格納するリスト
    """
    for elem in elems:
        elem_bytes = elem
        length = memcmpable.encoded_size(len(elem_bytes))
        bytes_list.reserve(length)
        memcmpable.encode(elem_bytes, bytes_list)

def decode(bytes_data: bytes, elems: List[bytes]) -> None:
    """
    バイトデータをデコードして要素リストに追加します。

    :param bytes_data: デコードするバイトデータ
    :param elems: デコードされたバイトを格納するリスト
    """
    rest = bytes_data
    while rest:
        elem, rest = memcmpable.decode(rest)
        elems.append(elem)

class Pretty:
    """
    デバッグ用のPrettyクラス。バイト列を人間が読みやすい形式で表示します。
    """
    def __init__(self, data: List[bytes]):
        self.data = data

    def __repr__(self) -> str:
        debug_tuple = "Tuple("
        fields = []
        for elem in self.data:
            try:
                s = elem.decode('utf-8')
                fields.append(f'"{s}" {elem.hex()}')
            except UnicodeDecodeError:
                fields.append(f'{elem.hex()}')
        debug_tuple += ", ".join(fields) + ")"
        return debug_tuple

def encoded_size(length: int) -> int:
    """
    エンコードされたサイズを計算します。

    :param length: 元のデータの長さ
    :return: エンコード後のデータの長さ
    """
    return ((length + (ESCAPE_LENGTH - 1)) // (ESCAPE_LENGTH - 1)) * ESCAPE_LENGTH

def encode_data(src: bytes, dst: List[int]) -> None:
    """
    データをエンコードしてバッファに追加します。

    :param src: エンコードする元のデータ
    :param dst: エンコードされたデータを格納するリスト
    """
    while True:
        copy_len = min(ESCAPE_LENGTH - 1, len(src))
        dst.extend(src[:copy_len])
        src = src[copy_len:]
        if not src:
            pad_size = ESCAPE_LENGTH - 1 - copy_len
            if pad_size > 0:
                dst.extend([0] * pad_size)
            dst.append(copy_len)
            break
        dst.append(ESCAPE_LENGTH)

def decode_data(src: bytes, dst: bytearray) -> None:
    """
    エンコードされたデータをデコードして出力に追加します。

    :param src: デコードするエンコード済みデータ
    :param dst: デコードされたデータを格納するバイト配列
    """
    while src:
        extra = src[ESCAPE_LENGTH - 1]
        length = min(ESCAPE_LENGTH - 1, extra)
        dst.extend(src[:length])
        src = src[ESCAPE_LENGTH:]
        if extra < ESCAPE_LENGTH:
            break

def test():
    """
    エンコードおよびデコードのテスト関数。
    """
    org1 = b"helloworld!memcmpable"
    org2 = b"foobarbazhogehuga"

    enc = []
    encode(org1, enc)
    encode(org2, enc)

    rest = bytes(enc)

    dec1 = bytearray()
    decode(rest, dec1)
    assert org1 == bytes(dec1)

    dec2 = bytearray()
    decode(rest, dec2)
    assert org2 == bytes(dec2)

if __name__ == "__main__":
    test()
