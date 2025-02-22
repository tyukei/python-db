from typing import Iterator, List

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

def decode(src: bytearray, dst: bytearray) -> None:
    """
    エンコードされたデータをデコードして出力に追加します。

    :param src: デコードするエンコード済みデータ
    :param dst: デコードされたデータを格納するバイト配列
    """
    while src:
        extra = src[ESCAPE_LENGTH - 1]
        length = min(ESCAPE_LENGTH - 1, extra)
        dst.extend(src[:length])
        del src[:ESCAPE_LENGTH]
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

    rest = bytearray(enc)

    dec1 = bytearray()
    decode(rest, dec1)
    assert org1 == bytes(dec1), "デコードされたデータが一致しません。"

    dec2 = bytearray()
    decode(rest, dec2)
    assert org2 == bytes(dec2), "デコードされたデータが一致しません。"

    print("すべてのテストが成功しました。")

if __name__ == "__main__":
    test()
