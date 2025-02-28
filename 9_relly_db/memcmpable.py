from typing import Iterator, List, Tuple

# エスケープ長を定義します。
ESCAPE_LENGTH = 9

def encoded_size(length: int) -> int:
    """
    エンコードされたサイズを計算します。

    :param length: 元のデータの長さ
    :return: エンコード後のデータの長さ
    """
    return ((length + (ESCAPE_LENGTH - 1)) // (ESCAPE_LENGTH - 1)) * ESCAPE_LENGTH

def encode(src: bytes, dst: List[int]) -> None:
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

def decode(src: bytes) -> Tuple[bytes, bytes]:
    """
    エンコードされたデータ src をデコードし、(decoded_element, remaining_bytes) を返す。

    エンコード方式:
      - データは ESCAPE_LENGTH バイト単位のブロックに分割され、
        各ブロックの最後のバイトはそのブロックで実際に使用されたバイト数（ESCAPE_LENGTH未満の場合）または
        ESCAPE_LENGTH（次ブロックが存在する場合）が格納される。
    """
    decoded = bytearray()
    i = 0
    while i < len(src):
        block = src[i:i+ESCAPE_LENGTH]
        if len(block) < ESCAPE_LENGTH:
            # 不完全なブロックがあれば終了
            break
        marker = block[-1]
        copy_len = marker if marker < ESCAPE_LENGTH else ESCAPE_LENGTH - 1
        decoded.extend(block[:copy_len])
        i += ESCAPE_LENGTH
        if marker < ESCAPE_LENGTH:
            break
    return bytes(decoded), src[i:]

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

    dec1, rest = decode(rest)

    assert org1 == dec1, "デコードされたデータが一致しません（org1）"
    dec2, rest = decode(rest)
    assert org2 == dec2, "デコードされたデータが一致しません（org2）"
    print("すべてのテストが成功しました。")

if __name__ == "__main__":
    test()
