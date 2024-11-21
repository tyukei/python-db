def read_page(file, page_id, page_size):
    file.seek(page_id * page_size)
    return file.read(page_size)

def main():
    file_path = "simple.rly"
    page_size = 4096

    try:
        with open(file_path, "rb") as file:
            page0 = read_page(file, 0, page_size)
            page1 = read_page(file, 1, page_size)
            page2 = read_page(file, 2, page_size)
            page3 = read_page(file, 3, page_size)

            print("Page 0:", page0[:64])  # 最初の64バイトを表示
            print("Page 1:", page1[:64])
            print("Page 2:", page2[:64])
            print("Page 3:", page3[:64])

    except FileNotFoundError:
        print(f"File {file_path} not found.")

if __name__ == "__main__":
    main()
